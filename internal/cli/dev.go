package cli

import (
	"encoding/json"
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

	// 2. Locate docker-compose.yml. A distributed CLI resolves the canonical
	// companion asset next to the installed binary; repository-relative paths
	// remain a developer convenience only.
	composePath := *composeFileFlag
	if composePath == "" {
		if configured := os.Getenv("DEADBOLT_COMPOSE_FILE"); configured != "" {
			composePath = configured
		}
	}
	var releaseImage string
	if composePath == "" {
		if executable, err := os.Executable(); err == nil {
			assetDir := filepath.Join(filepath.Dir(executable), "..", "share", "deadbolt")
			candidate := filepath.Join(assetDir, "compose.yaml")
			if _, err := os.Stat(candidate); err == nil {
				composePath = candidate
				metadata, err := os.ReadFile(filepath.Join(assetDir, "release.json"))
				if err != nil {
					return fmt.Errorf("read installed Deadbolt release metadata: %w", err)
				}
				var release struct {
					ControlPlaneImage string `json:"controlPlaneImage"`
				}
				if err := json.Unmarshal(metadata, &release); err != nil || !strings.Contains(release.ControlPlaneImage, "@sha256:") {
					return fmt.Errorf("installed Deadbolt release metadata has no immutable control-plane image")
				}
				releaseImage = release.ControlPlaneImage
			}
		}
	}
	if composePath == "" && os.Getenv("DEADBOLT_DEV_ASSETS") == "1" {
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
		return fmt.Errorf("local Compose assets are not installed. Install the Deadbolt CLI companion assets, set DEADBOLT_COMPOSE_FILE, or for a repository checkout set DEADBOLT_DEV_ASSETS=1")
	}

	// 3. Start core services with docker compose
	fmt.Printf("==> Step 2/5: Starting local core services via Docker Compose (%s)...\n", composePath)
	composeCmd := exec.Command("docker", "compose", "-f", composePath, "--profile", "core", "up", "-d")
	if releaseImage != "" {
		composeCmd.Env = append(os.Environ(), "DEADBOLT_CONTROL_PLANE_IMAGE="+releaseImage)
	}
	composeCmd.Stdout = os.Stdout
	composeCmd.Stderr = os.Stderr
	if err := composeCmd.Run(); err != nil {
		return fmt.Errorf("failed to start core containers via docker compose: %w", err)
	}

	// A fresh database is intentionally unready before schema migration. Match
	// the production Compose lifecycle: live -> migrator -> ready.
	fmt.Printf("==> Step 3/5: Waiting for Deadbolt control plane at %s to become live...\n", apiURL)
	if err := waitForControlPlanePath(apiURL, "/livez", 60*time.Second); err != nil {
		return fmt.Errorf("control plane did not become live: %w", err)
	}
	migrateCmd := exec.Command("docker", "compose", "-f", composePath, "--profile", "core", "exec", "-T", "control-plane", "/usr/local/bin/control-plane", "--migrate")
	if releaseImage != "" {
		migrateCmd.Env = append(os.Environ(), "DEADBOLT_CONTROL_PLANE_IMAGE="+releaseImage)
	}
	migrateCmd.Stdout = os.Stdout
	migrateCmd.Stderr = os.Stderr
	if err := migrateCmd.Run(); err != nil {
		return fmt.Errorf("apply local database migrations: %w", err)
	}
	if err := waitForControlPlanePath(apiURL, "/readyz", 60*time.Second); err != nil {
		return fmt.Errorf("control plane did not become ready after migrations: %w", err)
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

	// 6. Worker initialization. Local HA is two distinct runtime worker
	// processes, not one agent with two slots.
	var workerProcesses []*exec.Cmd
	if !*noWorkerFlag {
		fmt.Println("==> Step 5/5: Enrolling and launching two local worker processes...")
		if err := os.MkdirAll(*bundleDirFlag, 0755); err != nil {
			return fmt.Errorf("create bundle directory: %w", err)
		}

		credDir, err := GetCredentialsDirectory()
		if err != nil {
			return fmt.Errorf("resolve local credentials directory: %w", err)
		}
		executable, err := os.Executable()
		if err != nil {
			return fmt.Errorf("resolve runtime executable: %w", err)
		}
		absBundleDir, err := filepath.Abs(*bundleDirFlag)
		if err != nil {
			return fmt.Errorf("resolve bundle directory: %w", err)
		}
		for _, name := range []string{"worker-a", "worker-b"} {
			workerKeyPath := filepath.Join(credDir, name+".key")
			if _, err := os.Stat(workerKeyPath); os.IsNotExist(err) {
				enrollArgs := []string{
					"--control-plane-url", apiURL,
					"--key-path", workerKeyPath,
					"--pool", *poolFlag,
					"--env", "development",
					"--create-token",
				}
				if err := HandleWorkerEnroll(enrollArgs); err != nil {
					return fmt.Errorf("enroll %s: %w", name, err)
				}
			}
			cmd := exec.Command(executable, "worker", "start", "--key-path", workerKeyPath, "--control-plane-url", apiURL, "--bundle-dir", absBundleDir, "--pool", *poolFlag, "--slots", "1")
			cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
			if err := cmd.Start(); err != nil {
				return fmt.Errorf("start %s process: %w", name, err)
			}
			workerProcesses = append(workerProcesses, cmd)
			fmt.Printf("✓ %s process started (pid %d).\n", name, cmd.Process.Pid)
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
	for _, process := range workerProcesses {
		if process.Process != nil {
			_ = process.Process.Signal(os.Interrupt)
		}
	}
	for _, process := range workerProcesses {
		_ = process.Wait()
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
	return waitForControlPlanePath(apiURL, "/readyz", timeout)
}

func waitForControlPlanePath(apiURL, path string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	client := &http.Client{Timeout: 2 * time.Second}

	for time.Now().Before(deadline) {
		resp, err := client.Get(apiURL + path)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}
		time.Sleep(1 * time.Second)
	}

	return fmt.Errorf("timed out after %s waiting for %s%s", timeout, apiURL, path)
}
