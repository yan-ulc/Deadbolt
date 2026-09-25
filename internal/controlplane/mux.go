package controlplane

import (
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Ryanakml/Deadbolt/internal/artifacts"
	"github.com/Ryanakml/Deadbolt/internal/auth"
	"github.com/Ryanakml/Deadbolt/internal/deployment"
	"github.com/Ryanakml/Deadbolt/internal/execution"
	"github.com/Ryanakml/Deadbolt/internal/gateway"
	"github.com/Ryanakml/Deadbolt/internal/outbox"
	"github.com/Ryanakml/Deadbolt/internal/recovery"
	"github.com/Ryanakml/Deadbolt/internal/storage"
	"github.com/Ryanakml/Deadbolt/internal/tenant"
	"github.com/Ryanakml/Deadbolt/internal/worker"
)

// BuildMux wires all production routes onto a new http.ServeMux.
// This canonical constructor is shared between production (cmd/control-plane)
// and integration tests to guarantee identical route mountings and middleware.
func BuildMux(cfg auth.Config, pool *pgxpool.Pool, healthChecker *gateway.HealthChecker, logger *log.Logger) *http.ServeMux {
	return BuildMuxWithMetrics(cfg, pool, healthChecker, nil, logger)
}

// BuildMuxWithMetrics wires all production routes and attaches an optional outbox.Metrics collector.
func BuildMuxWithMetrics(cfg auth.Config, pool *pgxpool.Pool, healthChecker *gateway.HealthChecker, outboxMetrics *outbox.Metrics, logger *log.Logger) *http.ServeMux {
	return BuildMuxWithComponents(cfg, pool, healthChecker, outboxMetrics, logger, nil, nil)
}

// bearerToken extracts a Bearer credential without validating it.
func bearerToken(r *http.Request) (string, bool) {
	raw := r.Header.Get("Authorization")
	if raw == "" || !strings.HasPrefix(raw, "Bearer ") {
		return "", false
	}
	return strings.TrimPrefix(raw, "Bearer "), true
}

// BuildMuxWithComponents wires all production routes with optional shared execution hub and worker engine.
func BuildMuxWithComponents(cfg auth.Config, pool *pgxpool.Pool, healthChecker *gateway.HealthChecker, outboxMetrics *outbox.Metrics, logger *log.Logger, hub *execution.EventHub, engine *execution.WorkerEngine) *http.ServeMux {
	mux := http.NewServeMux()
	if healthChecker != nil {
		healthChecker.Routes(mux)
	}

	if pool != nil {
		if outboxMetrics == nil {
			outboxMetrics = outbox.NewMetrics(pool)
		}
		mux.Handle("GET /metrics", outboxMetrics)

		store := auth.NewSessionStore(pool)
		storagePool := storage.NewPool(pool)
		tenantService := tenant.NewService(storagePool)
		tenantHandler := tenant.NewHTTPHandler(tenantService, pool, store, cfg)
		tenantHandler.RegisterRoutes(mux)
		deploymentSvc := deployment.NewService(storagePool, tenantService)
		deploymentHandler := deployment.NewHTTPHandler(deploymentSvc, tenantService)
		mux.Handle("POST /api/v1/deployments", tenantHandler.WithRequestID(tenantHandler.RequireAuth(tenantHandler.RequireOrgScope(tenant.CapDeploymentsRegister, deploymentHandler.Register))))
		mux.Handle("POST /v1/deployments", tenantHandler.WithRequestID(tenantHandler.RequireAuth(tenantHandler.RequireOrgScope(tenant.CapDeploymentsRegister, deploymentHandler.Register))))
		// Activation selects its required staging/production capability only after
		// resolving the authoritative environment. This middleware still supplies
		// scoped auth, environment isolation, and durable command idempotency.
		mux.Handle("POST /api/v1/workflows/{name}/activate", tenantHandler.WithRequestID(tenantHandler.RequireAuth(tenantHandler.RequireOrgScope("", deploymentHandler.Activate))))
		mux.Handle("POST /v1/workflows/{name}/activate", tenantHandler.WithRequestID(tenantHandler.RequireAuth(tenantHandler.RequireOrgScope("", deploymentHandler.Activate))))

		eventHub := hub
		if eventHub == nil {
			eventHub = execution.NewEventHub()
		}
		workerEngine := engine
		if workerEngine == nil {
			workerEngine = execution.NewWorkerEngine(storagePool, eventHub)
		}
		workerEngine.SetCommands(tenantService)
		workerSvc := worker.NewService(storagePool, deploymentSvc, workerEngine)
		outboxMetrics.SetTaskLogDroppedCounter(workerSvc.DroppedLogsCount)
		workerHandler := worker.NewHTTPHandler(workerSvc, tenantService)
		workerHandler.RegisterRoutes(mux)

		executionSvc := execution.NewService(storagePool, tenantService, eventHub)
		executionHandler := execution.NewHTTPHandler(executionSvc, tenantService)
		executionHandler.SetWorkerEngine(workerEngine)
		mux.Handle("POST /api/v1/workflows/{name}/runs", tenantHandler.WithRequestID(tenantHandler.RequireAuth(tenantHandler.RequireOrgScope(tenant.CapRunsCreate, executionHandler.CreateRun))))
		mux.Handle("POST /v1/workflows/{name}/runs", tenantHandler.WithRequestID(tenantHandler.RequireAuth(tenantHandler.RequireOrgScope(tenant.CapRunsCreate, executionHandler.CreateRun))))
		mux.Handle("GET /api/v1/runs", tenantHandler.WithRequestID(tenantHandler.RequireAuth(tenantHandler.RequireOrgScope(tenant.CapRunsRead, executionHandler.ListRuns))))
		mux.Handle("GET /v1/runs", tenantHandler.WithRequestID(tenantHandler.RequireAuth(tenantHandler.RequireOrgScope(tenant.CapRunsRead, executionHandler.ListRuns))))
		mux.Handle("GET /api/v1/runs/{id}", tenantHandler.WithRequestID(tenantHandler.RequireAuth(tenantHandler.RequireOrgScope(tenant.CapRunsRead, executionHandler.GetRun))))
		mux.Handle("GET /v1/runs/{id}", tenantHandler.WithRequestID(tenantHandler.RequireAuth(tenantHandler.RequireOrgScope(tenant.CapRunsRead, executionHandler.GetRun))))
		mux.Handle("GET /api/v1/runs/{id}/events", tenantHandler.WithRequestID(tenantHandler.RequireAuth(tenantHandler.RequireOrgScope(tenant.CapRunsRead, executionHandler.GetRunEvents))))
		mux.Handle("GET /v1/runs/{id}/events", tenantHandler.WithRequestID(tenantHandler.RequireAuth(tenantHandler.RequireOrgScope(tenant.CapRunsRead, executionHandler.GetRunEvents))))
		mux.Handle("GET /api/v1/runs/{id}/stream", tenantHandler.WithRequestID(tenantHandler.RequireAuth(tenantHandler.RequireOrgScope(tenant.CapRunsRead, executionHandler.StreamRunEvents))))
		mux.Handle("GET /v1/runs/{id}/stream", tenantHandler.WithRequestID(tenantHandler.RequireAuth(tenantHandler.RequireOrgScope(tenant.CapRunsRead, executionHandler.StreamRunEvents))))
		mux.Handle("GET /api/v1/runs/{id}/logs", tenantHandler.WithRequestID(tenantHandler.RequireAuth(tenantHandler.RequireOrgScope(tenant.CapPayloadRead, executionHandler.GetRunLogs))))
		mux.Handle("GET /v1/runs/{id}/logs", tenantHandler.WithRequestID(tenantHandler.RequireAuth(tenantHandler.RequireOrgScope(tenant.CapPayloadRead, executionHandler.GetRunLogs))))
		mux.Handle("POST /api/v1/reconciliation-cases/{id}/resolve", tenantHandler.WithRequestID(tenantHandler.RequireAuth(tenantHandler.RequireOrgScope(tenant.CapRunsReconcile, executionHandler.ResolveCase))))
		mux.Handle("POST /v1/reconciliation-cases/{id}/resolve", tenantHandler.WithRequestID(tenantHandler.RequireAuth(tenantHandler.RequireOrgScope(tenant.CapRunsReconcile, executionHandler.ResolveCase))))
		mux.Handle("POST /api/v1/runs/{id}/cancel", tenantHandler.WithRequestID(tenantHandler.RequireAuth(tenantHandler.RequireOrgScope(tenant.CapRunsControl, executionHandler.CancelRun))))
		mux.Handle("POST /v1/runs/{id}/cancel", tenantHandler.WithRequestID(tenantHandler.RequireAuth(tenantHandler.RequireOrgScope(tenant.CapRunsControl, executionHandler.CancelRun))))

		// Scoped artifacts: worker sessions (dbs_ bearers) authenticate
		// through the worker chain with attempt-ownership checks inside the
		// handler; all other callers use the tenant chain with the route
		// capability. CSRF/Origin rules for cookie callers are preserved by
		// delegating to the same middleware.
		// Unconfigured stores fail closed per endpoint with 503.
		artifactStore, _ := artifacts.StoreFromEnv()
		artifactSvc := artifacts.NewService(storagePool, artifactStore)
		workerEngine.SetArtifacts(artifactSvc)
		artifactHandler := artifacts.NewHTTPHandler(artifactSvc, tenantService)
		artifactRoute := func(pattern, capability string, handle http.HandlerFunc) {
			mux.Handle(pattern, tenantHandler.WithRequestID(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if token, ok := bearerToken(r); ok && worker.IsSessionTokenFormat(token) {
					workerHandler.RequireWorkerSession(handle)(w, r)
					return
				}
				tenantHandler.RequireAuth(tenantHandler.RequireOrgScope(capability, handle))(w, r)
			})))
		}
		artifactRoute("POST /api/v1/artifacts", tenant.CapArtifactsWrite, artifactHandler.Create)
		artifactRoute("POST /v1/artifacts", tenant.CapArtifactsWrite, artifactHandler.Create)
		artifactRoute("POST /api/v1/artifacts/{id}/finalize", tenant.CapArtifactsWrite, artifactHandler.Finalize)
		artifactRoute("POST /v1/artifacts/{id}/finalize", tenant.CapArtifactsWrite, artifactHandler.Finalize)
		artifactRoute("GET /api/v1/artifacts/{id}", tenant.CapPayloadRead, artifactHandler.Download)
		artifactRoute("GET /v1/artifacts/{id}", tenant.CapPayloadRead, artifactHandler.Download)

		mux.Handle("GET /api/v1/workers", tenantHandler.WithRequestID(tenantHandler.RequireAuth(tenantHandler.RequireOrgScope(tenant.CapWorkersRead, executionHandler.ListWorkers))))
		mux.Handle("GET /v1/workers", tenantHandler.WithRequestID(tenantHandler.RequireAuth(tenantHandler.RequireOrgScope(tenant.CapWorkersRead, executionHandler.ListWorkers))))

		mux.Handle("POST /api/v1/environments/{envId}/worker-enrollments", tenantHandler.WithRequestID(tenantHandler.RequireAuth(tenantHandler.RequireOrgScope(tenant.CapDeploymentsWrite, workerHandler.HandleCreateEnrollmentToken))))
		mux.Handle("POST /v1/environments/{envId}/worker-enrollments", tenantHandler.WithRequestID(tenantHandler.RequireAuth(tenantHandler.RequireOrgScope(tenant.CapDeploymentsWrite, workerHandler.HandleCreateEnrollmentToken))))
		mux.Handle("POST /api/v1/workers/{workerId}/revoke", tenantHandler.WithRequestID(tenantHandler.RequireAuth(tenantHandler.RequireOrgScope(tenant.CapWorkersDrain, workerHandler.HandleRevokeWorker))))
		mux.Handle("POST /v1/workers/{workerId}/revoke", tenantHandler.WithRequestID(tenantHandler.RequireAuth(tenantHandler.RequireOrgScope(tenant.CapWorkersDrain, workerHandler.HandleRevokeWorker))))
		mux.Handle("POST /api/v1/workers/{workerId}/drain", tenantHandler.WithRequestID(tenantHandler.RequireAuth(tenantHandler.RequireOrgScope(tenant.CapWorkersDrain, workerHandler.HandleDrainWorker))))
		mux.Handle("POST /v1/workers/{workerId}/drain", tenantHandler.WithRequestID(tenantHandler.RequireAuth(tenantHandler.RequireOrgScope(tenant.CapWorkersDrain, workerHandler.HandleDrainWorker))))

		recoveryMgr := recovery.NewManager(storagePool)
		recoveryMgr.SetArtifacts(artifactSvc)
		recoveryHandler := recovery.NewHTTPHandler(recoveryMgr, tenantService)
		recoveryRoute := func(pattern, capability string, handle http.HandlerFunc) {
			mux.Handle(pattern, tenantHandler.WithRequestID(tenantHandler.RequireAuth(tenantHandler.RequireOrgScope(capability, handle))))
		}
		// Read-only disaster recovery visibility for tenant operators.
		// Mutating recovery operations (prepare / rpo-gap / resume) are
		// platform-operator actions executed via the control-plane recovery
		// CLI with direct database authority (same trust as --migrate); they
		// are intentionally not exposed to tenant-scoped credentials so one
		// organization can never freeze or resume the whole platform (INV-01).
		recoveryRoute("GET /api/v1/system/recovery", tenant.CapRunsRead, recoveryHandler.GetRecovery)
		recoveryRoute("GET /v1/system/recovery", tenant.CapRunsRead, recoveryHandler.GetRecovery)

		oidcClient := auth.NewOIDCClient(cfg.OIDC, http.DefaultClient)
		bff := auth.NewBFFHandler(cfg, oidcClient, store, pool)
		if logger != nil {
			bff.SetLogger(logger)
		}

		mux.Handle("/api/auth/", bff.Routes())

		if cfg.RuntimeMode == auth.ModeLocal && cfg.DevAuthEnabled {
			devAuth := auth.NewDevAuthHandler(cfg, store, pool)
			mux.HandleFunc("/api/auth/dev-login", devAuth.HandleDevLogin)
			if logger != nil {
				logger.Printf("Local developer authentication endpoint enabled at /api/auth/dev-login")
			}
		}

		candidates := []string{"/app/dashboard", "apps/dashboard/dist", "apps/dashboard", "../../apps/dashboard/dist"}
		if customDir := os.Getenv("DEADBOLT_DASHBOARD_DIR"); customDir != "" {
			candidates = append([]string{customDir}, candidates...)
		}
		for _, dir := range candidates {
			if info, err := os.Stat(dir); err == nil && info.IsDir() {
				fs := http.FileServer(http.Dir(dir))
				mux.Handle("GET /dashboard/", http.StripPrefix("/dashboard/", fs))
				mux.Handle("GET /dashboard", http.RedirectHandler("/dashboard/", http.StatusMovedPermanently))
				break
			}
		}
	}

	return mux
}
