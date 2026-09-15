package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/nats-io/nats.go"

	"github.com/Ryanakml/Deadbolt/internal/auth"
	"github.com/Ryanakml/Deadbolt/internal/controlplane"
	"github.com/Ryanakml/Deadbolt/internal/gateway"
	"github.com/Ryanakml/Deadbolt/internal/outbox"
	"github.com/Ryanakml/Deadbolt/internal/scheduling"
	"github.com/Ryanakml/Deadbolt/internal/storage"
	"github.com/Ryanakml/Deadbolt/internal/storage/migrator"
)

var (
	// Version is the semantic release version
	Version = "0.1.0"
	// CommitSHA is the exact git commit SHA, injected via -ldflags
	CommitSHA = "dev"
	// BuildTime is the ISO 8601 build timestamp, injected via -ldflags
	BuildTime = ""
	// ImageDigest is the immutable container image digest (@sha256:...), injected via -ldflags or env
	ImageDigest = ""
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	logger := log.New(os.Stdout, "[DEADBOLT_CONTROL_PLANE] ", log.LstdFlags|log.Lmsgprefix)

	migrateFlag := flag.Bool("migrate", false, "Run database schema migrations and exit")
	flag.Parse()

	if *migrateFlag || os.Getenv("DEADBOLT_RUN_MIGRATIONS") == "true" {
		return runMigrations(logger)
	}

	runtimeMode := strings.ToLower(strings.TrimSpace(os.Getenv("RUNTIME_MODE")))
	if runtimeMode == "" {
		runtimeMode = auth.ModeHosted
	}

	listenAddr := strings.TrimSpace(os.Getenv("LISTEN_ADDR"))
	if listenAddr == "" {
		port := strings.TrimSpace(os.Getenv("PORT"))
		if port != "" {
			if runtimeMode == auth.ModeLocal {
				listenAddr = "127.0.0.1:" + port
			} else {
				listenAddr = ":" + port
			}
		} else {
			if runtimeMode == auth.ModeLocal {
				listenAddr = "127.0.0.1:8080"
			} else {
				listenAddr = ":8080"
			}
		}
	}

	// Determine listen host for loopback validation
	listenHost := listenAddr
	if h, _, err := net.SplitHostPort(listenAddr); err == nil {
		listenHost = h
	}

	cookieSecure := runtimeMode == auth.ModeHosted
	if val := os.Getenv("COOKIE_SECURE"); val != "" {
		if parsed, err := strconv.ParseBool(val); err == nil {
			cookieSecure = parsed
		}
	}

	devAuthEnabled := false
	if val := os.Getenv("DEV_AUTH_ENABLED"); val != "" {
		if parsed, err := strconv.ParseBool(val); err == nil {
			devAuthEnabled = parsed
		}
	}

	containerLocal := false
	if val := os.Getenv("DEADBOLT_CONTAINER_LOCAL"); val != "" {
		if parsed, err := strconv.ParseBool(val); err == nil {
			containerLocal = parsed
		}
	}

	var allowedOrigins []string
	if rawOrigins := os.Getenv("DEADBOLT_ALLOWED_ORIGINS"); rawOrigins != "" {
		for _, o := range strings.Split(rawOrigins, ",") {
			if trimmed := strings.TrimSpace(o); trimmed != "" {
				allowedOrigins = append(allowedOrigins, trimmed)
			}
		}
	}

	cfg := auth.Config{
		RuntimeMode:            runtimeMode,
		DevAuthEnabled:         devAuthEnabled,
		ContainerLocal:         containerLocal,
		DevKey:                 os.Getenv("DEADBOLT_DEV_KEY"),
		DevKeyPath:             os.Getenv("DEADBOLT_DEV_KEY_PATH"),
		CookieSecure:           cookieSecure,
		AllowedOrigins:         allowedOrigins,
		SessionIdleTimeout:     auth.DefaultSessionIdleTimeout,
		SessionAbsoluteTimeout: auth.DefaultSessionAbsoluteTimeout,
		OIDC: auth.OIDCConfig{
			Issuer:       os.Getenv("DEADBOLT_OIDC_ISSUER"),
			ClientID:     os.Getenv("DEADBOLT_OIDC_CLIENT_ID"),
			ClientSecret: os.Getenv("DEADBOLT_OIDC_CLIENT_SECRET"),
			RedirectURL:  os.Getenv("DEADBOLT_OIDC_REDIRECT_URL"),
		},
	}

	// Validate configuration boundaries according to Blueprint §22.2, §24.1 & §24.4
	if err := cfg.Validate(listenHost); err != nil {
		return fmt.Errorf("configuration validation failed: %w\n"+
			"Remediation:\n"+
			"  - Hosted mode (RUNTIME_MODE=hosted) strictly requires:\n"+
			"      COOKIE_SECURE=true\n"+
			"      DEV_AUTH_ENABLED=false (dev auth and development keys are barred in hosted mode)\n"+
			"      DEADBOLT_OIDC_ISSUER, DEADBOLT_OIDC_CLIENT_ID, and DEADBOLT_ALLOWED_ORIGINS configured\n"+
			"  - Local mode (RUNTIME_MODE=local) requires:\n"+
			"      LISTEN_ADDR bound strictly to loopback (127.0.0.1 or localhost), or DEADBOLT_CONTAINER_LOCAL=true in containers\n"+
			"      Valid DevKeyPath if configured", err)
	}

	dbURL := strings.TrimSpace(os.Getenv("DATABASE_URL"))
	if dbURL == "" && runtimeMode == auth.ModeHosted {
		return errors.New("DATABASE_URL is required in hosted mode\n" +
			"Remediation: Configure DATABASE_URL=postgres://<user>:<password>@<host>:<port>/<dbname>?sslmode=...")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var pool *pgxpool.Pool
	if dbURL != "" {
		poolConfig, err := pgxpool.ParseConfig(dbURL)
		if err != nil {
			return fmt.Errorf("invalid DATABASE_URL: %w", err)
		}
		poolConfig.MaxConns = 25
		poolConfig.MinConns = 2
		poolConfig.MaxConnIdleTime = 5 * time.Minute

		p, err := pgxpool.NewWithConfig(ctx, poolConfig)
		if err != nil {
			return fmt.Errorf("failed to initialize database pool: %w", err)
		}
		pool = p
		defer pool.Close()
		logger.Printf("Database connection pool initialized.")
	}

	runtimeImageDigest := ImageDigest
	if envDigest := os.Getenv("DEADBOLT_IMAGE_DIGEST"); envDigest != "" {
		runtimeImageDigest = envDigest
	}

	versionInfo := gateway.VersionInfo{
		Version:     Version,
		CommitSHA:   CommitSHA,
		BuildTime:   BuildTime,
		ImageDigest: runtimeImageDigest,
		RuntimeMode: cfg.RuntimeMode,
	}

	// Wire NATS connectivity checker
	natsURL := os.Getenv("DEADBOLT_NATS_URL")
	if natsURL == "" {
		natsURL = os.Getenv("NATS_URL")
	}
	if natsURL == "" {
		if runtimeMode == auth.ModeLocal {
			natsURL = "127.0.0.1:4222"
		} else {
			natsURL = "nats:4222"
		}
	}
	natsChecker := gateway.NewTCPNATSChecker(natsURL)
	var natsConn *nats.Conn
	var wakeupSubscriber *outbox.WakeupSubscriber

	// Latest expected migration is kept in the migrator package and includes the
	// Issue #13 outbox dispatch-state migration.
	healthChecker := gateway.NewHealthChecker(versionInfo, pool, natsChecker, migrator.LatestSchemaVersion)

	// Wire active scheduler freshness ticker through authoritative reconciler sweeps (Blueprint §24.3 & §25.2)
	systemDBURL := strings.TrimSpace(os.Getenv("SYSTEM_DATABASE_URL"))
	if systemDBURL == "" {
		systemDBURL = strings.TrimSpace(os.Getenv("DEADBOLT_SYSTEM_DATABASE_URL"))
	}

	var systemPool *pgxpool.Pool
	if systemDBURL != "" {
		sysPoolConfig, err := pgxpool.ParseConfig(systemDBURL)
		if err != nil {
			logger.Printf("[SCHEDULER] Warning: invalid SYSTEM_DATABASE_URL: %v", err)
		} else {
			sysPoolConfig.MaxConns = 5
			sysPoolConfig.MinConns = 1
			sp, err := pgxpool.NewWithConfig(ctx, sysPoolConfig)
			if err != nil {
				logger.Printf("[SCHEDULER] Warning: failed to connect to system database pool: %v", err)
			} else {
				systemPool = sp
				defer systemPool.Close()
				logger.Printf("System database pool (deadbolt_system) initialized.")
			}
		}
	}

	reconcilerPool := systemPool
	if reconcilerPool == nil {
		reconcilerPool = pool
	}

	if reconcilerPool != nil {
		reconciler := scheduling.NewReconciler(reconcilerPool, 5*time.Second, logger)
		healthChecker.SetSchedulerTicker(reconciler.Ticker(), gateway.DefaultSchedulerTimeout)
		go func() {
			if err := reconciler.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
				logger.Printf("[SCHEDULER] Reconciler loop terminated: %v", err)
			}
		}()

		// NATS is a wake-up transport only. A broker outage must not prevent the
		// control plane from starting; periodic PostgreSQL reconciliation remains
		// the safety net.
		conn, natsErr := nats.Connect(natsURL,
			nats.Name("deadbolt-control-plane"),
			nats.MaxReconnects(-1),
			nats.ReconnectWait(2*time.Second),
			nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
				logger.Printf("[NATS] disconnected: %v", err)
			}),
			nats.ReconnectHandler(func(_ *nats.Conn) {
				logger.Printf("[NATS] reconnected")
			}),
		)
		if natsErr != nil {
			logger.Printf("[NATS] wake-up transport unavailable; PostgreSQL reconciliation remains active: %v", natsErr)
		} else {
			natsConn = conn
			js, jsErr := conn.JetStream()
			if jsErr != nil {
				logger.Printf("[NATS] JetStream unavailable; PostgreSQL reconciliation remains active: %v", jsErr)
			} else if streamErr := outbox.EnsureWakeupStream(js); streamErr != nil {
				logger.Printf("[NATS] wake-up stream setup failed; PostgreSQL reconciliation remains active: %v", streamErr)
			} else {
				wakeupSubscriber = outbox.NewWakeupSubscriber(js, reconciler.Wake)
				if subErr := wakeupSubscriber.Start(); subErr != nil {
					logger.Printf("[NATS] wake-up subscriber failed: %v", subErr)
					wakeupSubscriber = nil
				} else {
					runtimePool := storage.NewPool(pool)
					if pool != nil && reconcilerPool != nil {
						dispatcher := outbox.NewDispatcher(runtimePool, outbox.NewSQLTenantSource(reconcilerPool), outbox.NewJetStreamPublisher(js), outbox.Config{}, logger)
						go func() {
							if err := dispatcher.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
								logger.Printf("[OUTBOX] dispatcher terminated: %v", err)
							}
						}()
					}
				}
			}
		}
	}

	mux := BuildMux(cfg, pool, healthChecker, logger)

	server := &http.Server{
		Addr:         listenAddr,
		Handler:      mux,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	serverErr := make(chan error, 1)
	go func() {
		logger.Printf("Starting Deadbolt Control Plane [%s] on %s (commit: %s, digest: %s)", runtimeMode, listenAddr, CommitSHA, runtimeImageDigest)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	select {
	case err := <-serverErr:
		return fmt.Errorf("http server failed: %w", err)
	case sig := <-quit:
		logger.Printf("Received termination signal %s; starting graceful shutdown...", sig)
	}

	// Stop background reconciliation, outbox dispatch, and broker callbacks
	// before closing their database/broker resources.
	cancel()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer shutdownCancel()
	if wakeupSubscriber != nil {
		if err := wakeupSubscriber.Drain(shutdownCtx); err != nil {
			logger.Printf("[NATS] subscriber drain error: %v", err)
		}
	}
	if natsConn != nil {
		natsConn.Close()
	}

	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Printf("Server shutdown error: %v", err)
	}

	logger.Printf("Control plane shutdown cleanly completed.")
	return nil
}

// BuildMux wires all production routes onto a new http.ServeMux using the canonical constructor.
func BuildMux(cfg auth.Config, pool *pgxpool.Pool, healthChecker *gateway.HealthChecker, logger *log.Logger) *http.ServeMux {
	return controlplane.BuildMux(cfg, pool, healthChecker, logger)
}

// runMigrations executes database schema migrations using DDL-capable migrator credentials
// and session-level advisory locking (Blueprint §26.3).
func runMigrations(logger *log.Logger) error {
	migratorDBURL := os.Getenv("MIGRATOR_DATABASE_URL")
	if migratorDBURL == "" {
		migratorDBURL = os.Getenv("DEADBOLT_MIGRATOR_DATABASE_URL")
	}
	if migratorDBURL == "" && strings.ToLower(strings.TrimSpace(os.Getenv("RUNTIME_MODE"))) == auth.ModeLocal {
		migratorDBURL = os.Getenv("DATABASE_URL")
	}
	if migratorDBURL == "" {
		return errors.New("MIGRATOR_DATABASE_URL is required for migration execution (DDL privileges)")
	}

	migrationsDir := os.Getenv("DEADBOLT_MIGRATIONS_DIR")
	if migrationsDir == "" {
		if _, err := os.Stat("/migrations"); err == nil {
			migrationsDir = "/migrations"
		} else {
			migrationsDir = "migrations"
		}
	}

	logger.Printf("Opening database connection with migrator credentials...")
	db, err := sql.Open("pgx", migratorDBURL)
	if err != nil {
		return fmt.Errorf("failed to open database for migrations: %w", err)
	}
	defer db.Close()

	runner := migrator.NewRunner(db, migrationsDir)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	logger.Printf("Executing forward schema migrations with advisory lock (%d) from %s...", migrator.MigrationAdvisoryLockID, migrationsDir)
	if err := runner.Up(ctx); err != nil {
		return fmt.Errorf("migration execution failed: %w", err)
	}

	logger.Printf("Database schema migrations successfully applied.")
	return nil
}
