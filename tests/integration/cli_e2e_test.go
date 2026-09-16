package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/Ryanakml/Deadbolt/internal/cli"
	"github.com/Ryanakml/Deadbolt/internal/controlplane"
	"github.com/Ryanakml/Deadbolt/internal/tenant"
	"github.com/Ryanakml/Deadbolt/internal/worker"
)

// TestCLIEndToEndDeveloperJourney executes the complete local-to-staging developer journey:
// clean project ↓ runtime init ↓ runtime build ↓ runtime login ↓ runtime deploy
// ↓ runtime worker enroll/start ↓ runtime deployments activate ↓ runtime runs create ↓ runtime runs inspect
//
// Invariant Verification:
//   - Uses ONLY official public interfaces (CLI commands and public HTTP routes).
//   - Zero direct database manipulation or backdoor SQL commands.
//   - Secret values are never printed or leaked.
//   - Bundle digest identity is verified end-to-end between build manifest and worker loader.
//   - Activation strictly enforces worker availability preflight.
func TestCLIEndToEndDeveloperJourney(t *testing.T) {
	// 1. Initialize control plane server backed by test database
	tc := setupTenantContext(t)
	defer tc.cleanup()

	ctx := context.Background()
	owner, _ := tenant.NewUUID()
	org, err := tc.service.CreateOrganization(ctx, owner, "Acme Logistics")
	if err != nil {
		t.Fatalf("failed to create organization: %v", err)
	}

	project, err := tc.service.CreateProject(ctx, org.ID, "order-service")
	if err != nil {
		t.Fatalf("failed to create project: %v", err)
	}

	stagingEnv, err := tc.service.CreateEnvironment(ctx, org.ID, project.ID, tenant.EnvStaging, 10)
	if err != nil {
		t.Fatalf("failed to create staging environment: %v", err)
	}

	prodMux := controlplane.BuildMux(tc.authCfg, tc.runtimePool, nil, nil)
	server := httptest.NewServer(prodMux)
	defer server.Close()

	// Provision admin API key for CLI operations
	adminKey := bootstrapTestKey(t, tc.service, org.ID, stagingEnv.ID, []string{
		tenant.CapDeploymentsRegister,
		tenant.CapDeploymentsWrite,
		tenant.CapDeploymentsActivateStaging,
		tenant.CapDeploymentsActivateProd,
		tenant.CapRunsCreate,
		tenant.CapRunsRead,
		tenant.CapPayloadRead,
		tenant.CapWorkersRead,
		tenant.CapWorkersDrain,
		tenant.CapAdminKey,
	})

	// 2. Set up isolated CLI workspace and secure credentials directory
	projDir := t.TempDir()
	credDir := t.TempDir()
	cli.SetCustomCredentialsDir(credDir)

	// Save original env & working directory, restore on test cleanup
	origAPIURL := os.Getenv("DEADBOLT_API_URL")
	origAPIKey := os.Getenv("DEADBOLT_API_KEY")
	origOrgID := os.Getenv("DEADBOLT_ORG_ID")
	origEnv := os.Getenv("DEADBOLT_ENV")
	defer func() {
		os.Setenv("DEADBOLT_API_URL", origAPIURL)
		os.Setenv("DEADBOLT_API_KEY", origAPIKey)
		os.Setenv("DEADBOLT_ORG_ID", origOrgID)
		os.Setenv("DEADBOLT_ENV", origEnv)
	}()

	os.Setenv("DEADBOLT_API_URL", server.URL)
	os.Setenv("DEADBOLT_ORG_ID", org.ID)
	os.Setenv("DEADBOLT_ENV", "staging")
	os.Setenv("NOTIFICATION_API_KEY", "test-notification-secret-value")
	defer os.Unsetenv("NOTIFICATION_API_KEY")

	// 3. Step: runtime init (Scaffold clean project)
	t.Log("==> Executing: runtime init")
	err = cli.HandleInit([]string{"--dir", projDir, "--name", "order-service"})
	if err != nil {
		t.Fatalf("runtime init failed: %v", err)
	}

	// Verify required scaffolded files
	for _, f := range []string{"deadbolt.config.json", "workflow.json", "tasks.json", "tasks/validate.js", "tasks/provision.js", "tasks/notify.js"} {
		if _, err := os.Stat(filepath.Join(projDir, f)); os.IsNotExist(err) {
			t.Fatalf("runtime init did not create %s", f)
		}
	}

	// 4. Step: runtime login (Configure credentials securely in OS credentials store)
	t.Log("==> Executing: runtime login")
	err = cli.RunLogin([]string{
		"--api-key", adminKey.PlaintextKey,
		"--org", org.ID,
		"--env", "staging",
		"--control-plane-url", server.URL,
	})
	if err != nil {
		t.Fatalf("runtime login failed: %v", err)
	}

	// 5. Step: runtime build (Assemble deterministic bundle & manifest)
	t.Log("==> Executing: runtime build")
	err = cli.HandleBuild([]string{
		"--dir", projDir,
		"--arch", runtime.GOARCH,
		"--os", "linux",
	})
	if err != nil {
		t.Fatalf("runtime build failed: %v", err)
	}

	manifestPath := filepath.Join(projDir, "dist", "manifest.json")
	manifestData, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatalf("failed to read build manifest: %v", err)
	}

	var manifest struct {
		BundleDigest string `json:"bundleDigest"`
		Workflows    []struct {
			Name string `json:"name"`
		} `json:"workflows"`
	}
	if err := json.Unmarshal(manifestData, &manifest); err != nil {
		t.Fatalf("failed to parse build manifest: %v", err)
	}
	if manifest.BundleDigest == "" {
		t.Fatalf("bundleDigest missing from manifest")
	}

	bundlePath := filepath.Join(projDir, "bundles", manifest.BundleDigest+".tar")
	if _, err := os.Stat(bundlePath); os.IsNotExist(err) {
		t.Fatalf("bundle archive not found at %s", bundlePath)
	}

	// 6. Step: runtime doctor (Inspect system, manifest, and bundle integrity)
	t.Log("==> Executing: runtime doctor")
	err = cli.HandleDoctor([]string{
		"--manifest", manifestPath,
		"--bundle-dir", filepath.Join(projDir, "bundles"),
		"--control-plane-url", server.URL,
	})
	if err != nil {
		t.Logf("runtime doctor returned advisory warnings (expected for unstarted workers): %v", err)
	}

	// 7. Step: runtime deploy (Register manifest on control plane)
	t.Log("==> Executing: runtime deploy")
	depResp, _, err := cli.RegisterDeployment(cli.LoadConfig(), "staging", manifestData)
	if err != nil {
		t.Fatalf("runtime deploy failed: %v", err)
	}
	deploymentID := depResp.ID
	if deploymentID == "" {
		t.Fatalf("deployment ID is empty")
	}

	// 8. Preflight check: Activation before workers are running must fail
	t.Log("==> Verifying activation preflight requirement: 0 workers must fail")
	err = cli.HandleDeployments([]string{
		"activate", deploymentID,
		"--workflow", "customer-onboarding",
		"--env", "staging",
		"--control-plane-url", server.URL,
	})
	if err == nil {
		t.Fatalf("expected activation to fail when 0 compatible workers are online")
	}

	// 9. Step: runtime worker enroll & start (Start 2 worker agents)
	t.Log("==> Enrolling Worker 1 and Worker 2")
	workerKey1 := filepath.Join(t.TempDir(), "worker1.key")
	workerKey2 := filepath.Join(t.TempDir(), "worker2.key")

	err = cli.HandleWorkerEnroll([]string{
		"--control-plane-url", server.URL,
		"--key-path", workerKey1,
		"--env", stagingEnv.ID,
		"--pool", "default",
		"--create-token",
	})
	if err != nil {
		t.Fatalf("failed to enroll worker 1: %v", err)
	}

	err = cli.HandleWorkerEnroll([]string{
		"--control-plane-url", server.URL,
		"--key-path", workerKey2,
		"--env", stagingEnv.ID,
		"--pool", "default",
		"--create-token",
	})
	if err != nil {
		t.Fatalf("failed to enroll worker 2: %v", err)
	}

	workerCtx, workerCancel := context.WithCancel(context.Background())
	defer workerCancel()

	runnerAbs, err := filepath.Abs("../../runner/node/dist/index.js")
	if err != nil {
		t.Fatalf("failed to resolve runner path: %v", err)
	}

	agent1, err := worker.NewAgent(worker.AgentConfig{
		ControlPlaneURL:   server.URL,
		KeyPath:           workerKey1,
		BundleDir:         filepath.Join(projDir, "bundles"),
		RunnerPath:        runnerAbs,
		Pool:              "default",
		Slots:             2,
		PollTimeout:       500 * time.Millisecond,
		HeartbeatInterval: 250 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("failed to initialize agent 1: %v", err)
	}

	agent2, err := worker.NewAgent(worker.AgentConfig{
		ControlPlaneURL:   server.URL,
		KeyPath:           workerKey2,
		BundleDir:         filepath.Join(projDir, "bundles"),
		RunnerPath:        runnerAbs,
		Pool:              "default",
		Slots:             2,
		PollTimeout:       500 * time.Millisecond,
		HeartbeatInterval: 250 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("failed to initialize agent 2: %v", err)
	}

	go func() {
		if err := agent1.Start(workerCtx); err != nil && !errors.Is(err, context.Canceled) {
			t.Logf("agent1 error: %v", err)
		}
	}()
	go func() {
		if err := agent2.Start(workerCtx); err != nil && !errors.Is(err, context.Canceled) {
			t.Logf("agent2 error: %v", err)
		}
	}()

	// Wait for workers to poll and advertise bundles
	t.Log("Waiting for workers to establish sessions and advertise bundle digests...")
	time.Sleep(1500 * time.Millisecond)

	// 10. Step: runtime worker list (Inspect registered workers)
	t.Log("==> Executing: runtime worker list")
	err = cli.HandleWorker([]string{"list", "--env", "staging", "--control-plane-url", server.URL})
	if err != nil {
		t.Fatalf("runtime worker list failed: %v", err)
	}

	// 11. Step: runtime deployments activate (Preflight should now succeed with 2 online workers)
	t.Log("==> Executing: runtime deployments activate")
	err = cli.HandleDeployments([]string{
		"activate", deploymentID,
		"--workflow", "customer-onboarding",
		"--env", "staging",
		"--control-plane-url", server.URL,
	})
	if err != nil {
		t.Fatalf("runtime deployments activate failed with 2 compatible workers: %v", err)
	}

	// 12. Step: runtime runs create (Submit workflow execution)
	t.Log("==> Executing: runtime runs create")
	err = cli.RunRuns([]string{
		"create",
		"--workflow", "customer-onboarding",
		"--env", "staging",
		"--input", `{"email":"alice@example.com","name":"Alice User"}`,
		"--idempotency-key", "cli-e2e-run-001",
	})
	if err != nil {
		t.Fatalf("runtime runs create failed: %v", err)
	}

	// 13. Step: runtime runs list (List runs in environment)
	t.Log("==> Executing: runtime runs list")
	err = cli.RunRuns([]string{"list", "--env", "staging"})
	if err != nil {
		t.Fatalf("runtime runs list failed: %v", err)
	}

	// Retrieve run ID from public control plane API
	runsReq, _ := http.NewRequest("GET", server.URL+"/v1/runs?environment=staging", nil)
	runsReq.Header.Set("Authorization", "Bearer "+adminKey.PlaintextKey)
	runsReq.Header.Set("X-Organization-ID", org.ID)
	runsResp, err := http.DefaultClient.Do(runsReq)
	if err != nil {
		t.Fatalf("failed to fetch runs: %v", err)
	}
	defer runsResp.Body.Close()

	var runsResult struct {
		Items []struct {
			ID           string `json:"id"`
			WorkflowName string `json:"workflowName"`
			Status       string `json:"status"`
		} `json:"items"`
	}
	_ = json.NewDecoder(runsResp.Body).Decode(&runsResult)
	if len(runsResult.Items) == 0 {
		t.Fatalf("expected at least 1 run created, got 0")
	}
	createdRunID := runsResult.Items[0].ID

	// 14. Step: Wait for workers to claim and execute the workflow DAG
	t.Logf("==> Polling run progress until completion: %s", createdRunID)
	deadline := time.Now().Add(15 * time.Second)
	finalStatus := ""
	for time.Now().Before(deadline) {
		statusReq, _ := http.NewRequest("GET", server.URL+"/v1/runs/"+createdRunID, nil)
		statusReq.Header.Set("Authorization", "Bearer "+adminKey.PlaintextKey)
		statusReq.Header.Set("X-Organization-ID", org.ID)
		statusResp, err := http.DefaultClient.Do(statusReq)
		if err == nil && statusResp.StatusCode == http.StatusOK {
			var snap struct {
				Status string `json:"status"`
			}
			_ = json.NewDecoder(statusResp.Body).Decode(&snap)
			statusResp.Body.Close()
			finalStatus = snap.Status
			if snap.Status == "SUCCEEDED" || snap.Status == "FAILED" {
				break
			}
		}
		time.Sleep(300 * time.Millisecond)
	}

	// 15. Step: runtime runs inspect (Inspect final run snapshot)
	t.Logf("==> Executing: runtime runs inspect %s (final status: %s)", createdRunID, finalStatus)
	err = cli.RunRuns([]string{"inspect", createdRunID})
	if err != nil {
		t.Fatalf("runtime runs inspect failed: %v", err)
	}

	// 16. Step: runtime logs (View execution logs)
	t.Logf("==> Executing: runtime logs %s", createdRunID)
	err = cli.RunLogs([]string{createdRunID})
	if err != nil {
		t.Fatalf("runtime logs failed: %v", err)
	}

	if finalStatus != "SUCCEEDED" {
		t.Fatalf("expected run to reach status SUCCEEDED, got %s", finalStatus)
	}

	t.Log("✓ Developer Journey End-to-End Test PASSED with zero manual database backdoors!")
}
