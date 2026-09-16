package cli

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/Ryanakml/Deadbolt/internal/worker"
)

// RunDev handles "runtime dev [--no-worker] [--bundle-dir <dir>] [--compose-file <path>] [--control-plane-url <url>] [--pool <pool>]"
func RunDev(args []string) error {
	fs := flag.NewFlagSet("dev", flag.ContinueOnError)
	noWorkerFlag := fs.Bool("no-worker", false, "Do not start a local background worker agent")
	bundleDirFlag := fs.String("bundle-dir", "./bundles", "Directory where local task bundles are stored")
	composeFileFlag := fs.String("compose-file", "", "Path to docker-compose.yml")
	cpURLFlag := fs.String("control-plane-url", "http://127.0.0.1:8080", "Control plane API base URL")
	poolFlag := fs.String("pool", "default", "Worker pool name")
	emailFlag := fs.String("email", "dev-admin@deadbolt.local", "Local developer session email")

	if err := fs.Parse(args); err != nil {
		return err
	}

	apiURL := strings.TrimRight(*cpURLFlag, "/")

	// 1. Actionable Docker check (Blueprint §25 / §27)
	fmt.Println("==> Step 1/5: Checking local Docker prerequisites...")
	if err := checkDockerRunning(); err != nil {
		return fmt.Errorf("docker check failed: %w\n  Remediation:\n    Ensure Docker daemon is installed and running (`docker info`).\n    Local mode requires Docker to orchestrate database and control plane services", err)
	}

	// 2. Locate docker-compose.yml
	composePath := *composeFileFlag
	if composePath == "" {
		candidates := []string{
			"deploy/compose/docker-compose.yml",
			"../deploy/compose/docker-compose.yml",
			"../../deploy/compose/docker-compose.yml",
		}
		for _, c := range candidates {
			if _, err := os.Stat(c); err == nil {
				composePath = c
				break
			}
		}
	}
	if composePath == "" {
		return fmt.Errorf("could not locate deploy/compose/docker-compose.yml. Pass --compose-file <path>")
	}

	// 3. Start core services with docker compose
	fmt.Printf("==> Step 2/5: Starting local core services via Docker Compose (%s)...\n", composePath)
	composeCmd := exec.Command("docker", "compose", "-f", composePath, "--profile", "core", "up", "-d")
	composeCmd.Stdout = os.Stdout
	composeCmd.Stderr = os.Stderr
	if err := composeCmd.Run(); err != nil {
		return fmt.Errorf("failed to start core containers via docker compose: %w", err)
	}

	// 4. Wait for control plane readiness
	fmt.Printf("==> Step 3/5: Waiting for Deadbolt control plane at %s to become healthy...\n", apiURL)
	if err := waitForControlPlane(apiURL, 60*time.Second); err != nil {
		return fmt.Errorf("control plane did not become ready: %w", err)
	}

	// 5. Authenticate and bootstrap developer entities
	fmt.Println("==> Step 4/5: Bootstrapping local developer identity and credentials...")
	cfg := Config{
		APIURL: apiURL,
		Env:    "development",
	}
	if err := runLocalDevLogin(cfg, *emailFlag, "", "development"); err != nil {
		return fmt.Errorf("failed to complete local developer login: %w", err)
	}

	// Reload config to pickup saved credentials
	cfg = LoadConfig()

	// 6. Worker initialization
	var workerCancel context.CancelFunc
	if !*noWorkerFlag {
		fmt.Println("==> Step 5/5: Enrolling and launching local worker agent...")
		_ = os.MkdirAll(*bundleDirFlag, 0755)

		credDir, _ := GetCredentialsDirectory()
		workerKeyPath := filepath.Join(credDir, "worker_dev.key")

		// Enroll worker if key doesn't exist
		if _, err := os.Stat(workerKeyPath); os.IsNotExist(err) {
			enrollArgs := []string{
				"--control-plane-url", apiURL,
				"--key-path", workerKeyPath,
				"--pool", *poolFlag,
				"--env", "development",
				"--create-token",
			}
			if err := HandleWorkerEnroll(enrollArgs); err != nil {
				fmt.Printf("Warning: automatic worker enrollment encountered: %v. Continuing without local worker\n", err)
			}
		}

		if _, err := os.Stat(workerKeyPath); err == nil {
			workerCtx, cancel := context.WithCancel(context.Background())
			workerCancel = cancel

			absBundleDir, _ := filepath.Abs(*bundleDirFlag)
			wCfg := worker.AgentConfig{
				ControlPlaneURL: apiURL,
				KeyPath:         workerKeyPath,
				BundleDir:       absBundleDir,
				Pool:            *poolFlag,
				Slots:           2,
			}

			agent, err := worker.NewAgent(wCfg)
			if err != nil {
				fmt.Printf("Warning: failed to initialize worker agent: %v\n", err)
			} else {
				go func() {
					if err := agent.Start(workerCtx); err != nil && err != context.Canceled {
						fmt.Printf("[Worker Agent Error]: %v\n", err)
					}
				}()
				fmt.Println("✓ Local worker agent active and polling for assignments.")
			}
		}
	} else {
		fmt.Println("==> Step 5/5: Skipping local worker start (--no-worker specified).")
	}

	fmt.Println()
	fmt.Println("==================================================================")
	fmt.Println(" Deadbolt Local Development Environment is Ready!")
	fmt.Println("==================================================================")
	fmt.Printf(" Control Plane URL:   %s\n", apiURL)
	fmt.Printf(" Health Endpoint:     %s/livez\n", apiURL)
	fmt.Printf(" Readiness Endpoint:  %s/readyz\n", apiURL)
	fmt.Printf(" Environment:         development\n")
	fmt.Printf(" Bundles Directory:   %s\n", *bundleDirFlag)
	fmt.Println("==================================================================")
	fmt.Println(" Developer Journey Quick Commands:")
	fmt.Println("   1. Verify environment:    runtime doctor")
	fmt.Println("   2. Build bundle:          runtime build")
	fmt.Println("   3. Deploy manifest:       runtime deploy --env development")
	fmt.Println("   4. Activate workflow:     runtime deployments activate <id> --workflow <name>")
	fmt.Println("   5. Execute run:           runtime runs create --workflow <name>")
	fmt.Println("   6. Inspect run:           runtime runs inspect <run_id>")
	fmt.Println("==================================================================")
	fmt.Println("Press Ctrl+C to shut down.")

	// Await termination signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	fmt.Println("\nReceived shutdown signal. Stopping local services gracefully...")
	if workerCancel != nil {
		workerCancel()
		time.Sleep(500 * time.Millisecond)
	}

	fmt.Println("Shutdown complete.")
	return nil
}

func checkDockerRunning() error {
	cmd := exec.Command("docker", "info")
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("docker daemon is not responding: %w", err)
	}
	return nil
}

func waitForControlPlane(apiURL string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	client := &http.Client{Timeout: 2 * time.Second}

	for time.Now().Before(deadline) {
		resp, err := client.Get(apiURL + "/readyz")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}
		time.Sleep(1 * time.Second)
	}

	return fmt.Errorf("timed out after %s waiting for %s/readyz", timeout, apiURL)
}
