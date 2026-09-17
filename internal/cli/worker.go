package cli

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/Ryanakml/Deadbolt/internal/worker"
)

// HandleWorker processes `runtime worker <subcommand> [flags]`
func HandleWorker(args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: runtime worker <enroll|start|drain|list> [flags]")
	}

	switch args[0] {
	case "enroll":
		return handleWorkerEnroll(args[1:])
	case "start":
		return handleWorkerStart(args[1:])
	case "drain":
		return handleWorkerDrain(args[1:])
	case "list":
		return handleWorkerList(args[1:])
	default:
		return fmt.Errorf("unknown worker subcommand: %s (supported: enroll, start, drain, list)", args[0])
	}
}

// HandleWorkerEnroll runs the worker enrollment flow
func HandleWorkerEnroll(args []string) error {
	return handleWorkerEnroll(args)
}

func handleWorkerEnroll(args []string) error {
	fs := flag.NewFlagSet("runtime worker enroll", flag.ContinueOnError)
	tokenFlag := fs.String("token", "", "Single-use enrollment token")
	keyPathFlag := fs.String("key-path", "", "Path to worker private key file (default ~/.deadbolt/worker.key)")
	cpURLFlag := fs.String("control-plane-url", "", "Control plane base URL")
	poolFlag := fs.String("pool", "default", "Worker pool name")
	envFlag := fs.String("env", "", "Environment ID/name (required if using --create-token)")
	createTokenFlag := fs.Bool("create-token", false, "Generate an enrollment token from control plane API first")
	if err := fs.Parse(args); err != nil {
		return err
	}

	cfg := LoadConfig()
	cpURL := *cpURLFlag
	if cpURL == "" {
		cpURL = cfg.APIURL
	}
	cpURL = strings.TrimRight(cpURL, "/")
	cfg.APIURL = cpURL

	keyPath := *keyPathFlag
	if keyPath == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return fmt.Errorf("resolve user home dir: %w", err)
		}
		keyPath = filepath.Join(home, ".deadbolt", "worker.key")
	}

	enrollToken := strings.TrimSpace(*tokenFlag)

	// If --create-token requested, call control plane admin API to issue enrollment token
	if *createTokenFlag {
		env := *envFlag
		if env == "" {
			env = cfg.Env
		}
		if env == "" {
			return fmt.Errorf("--env is required when using --create-token")
		}

		envID, err := resolveEnvironmentID(cfg, env)
		if err != nil {
			return err
		}
		fmt.Printf("Requesting worker enrollment token for environment %s (pool: %s)...\n", env, *poolFlag)
		inPayload, _ := json.Marshal(map[string]string{"pool": *poolFlag})
		req, err := cfg.NewRequest(http.MethodPost, fmt.Sprintf("/v1/environments/%s/worker-enrollments", envID), bytes.NewReader(inPayload))
		if err != nil {
			return fmt.Errorf("create enrollment token request: %w", err)
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", fmt.Sprintf("enroll-token-%d", time.Now().UnixNano()))

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return fmt.Errorf("request enrollment token: %w", err)
		}
		defer resp.Body.Close()

		body, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
			return fmt.Errorf("failed to create enrollment token: %s", FormatAPIError(resp.StatusCode, body))
		}

		var tokenResp struct {
			Token           string `json:"token"`
			EnrollmentToken string `json:"enrollmentToken"`
			WorkerPool      string `json:"workerPool"`
			ExpiresAt       string `json:"expiresAt"`
		}
		if err := json.Unmarshal(body, &tokenResp); err != nil {
			return fmt.Errorf("parse enrollment token response: %w", err)
		}
		enrollToken = tokenResp.Token
		if enrollToken == "" {
			enrollToken = tokenResp.EnrollmentToken
		}
		fmt.Printf("Issued single-use enrollment token (expires at %s).\n", tokenResp.ExpiresAt)
	}

	if enrollToken == "" {
		return fmt.Errorf("--token is required (or pass --create-token if authenticated with admin role)")
	}

	// 1. Prepare keypair
	var privKey ed25519.PrivateKey
	var pubKey ed25519.PublicKey

	if _, err := os.Stat(keyPath); err == nil {
		p, err := worker.LoadPrivateKey(keyPath)
		if err != nil {
			return fmt.Errorf("load existing private key: %w", err)
		}
		privKey = p
		pubKey = p.Public().(ed25519.PublicKey)
	} else {
		pub, priv, err := worker.GenerateWorkerKeyPair()
		if err != nil {
			return fmt.Errorf("generate ed25519 keypair: %w", err)
		}
		if err := os.MkdirAll(filepath.Dir(keyPath), 0o700); err != nil {
			return fmt.Errorf("create key directory: %w", err)
		}
		if err := worker.SavePrivateKey(keyPath, priv); err != nil {
			return fmt.Errorf("save private key: %w", err)
		}
		privKey = priv
		pubKey = pub
	}

	// 2. Request challenge nonce from control plane
	client := &http.Client{Timeout: 10 * time.Second}
	chalPayload, _ := json.Marshal(worker.ChallengeRequestDTO{
		ProtocolVersion: worker.ProtocolVersion,
		RequestID:       fmt.Sprintf("req_chal_%d", time.Now().UnixNano()),
		PublicKey:       worker.EncodePublicKey(pubKey),
	})

	chalResp, err := client.Post(cpURL+"/worker/v1/challenge", "application/json", bytes.NewReader(chalPayload))
	if err != nil {
		return fmt.Errorf("challenge request failed: %w", err)
	}
	defer chalResp.Body.Close()

	chalBody, _ := io.ReadAll(chalResp.Body)
	if chalResp.StatusCode != http.StatusOK {
		return fmt.Errorf("challenge rejected (%d): %s", chalResp.StatusCode, string(chalBody))
	}

	var chalRes worker.ChallengeResponseDTO
	if err := json.Unmarshal(chalBody, &chalRes); err != nil {
		return fmt.Errorf("parse challenge response: %w", err)
	}

	// 3. Sign challenge nonce and execute enrollment request
	sig := worker.SignChallenge(privKey, chalRes.Nonce)
	enrollPayload, _ := json.Marshal(worker.EnrollRequestDTO{
		ProtocolVersion: worker.ProtocolVersion,
		RequestID:       fmt.Sprintf("req_enr_%d", time.Now().UnixNano()),
		EnrollmentToken: enrollToken,
		PublicKey:       worker.EncodePublicKey(pubKey),
		Nonce:           chalRes.Nonce,
		Signature:       sig,
	})

	enrollResp, err := client.Post(cpURL+"/worker/v1/enroll", "application/json", bytes.NewReader(enrollPayload))
	if err != nil {
		return fmt.Errorf("enroll request failed: %w", err)
	}
	defer enrollResp.Body.Close()

	enrollBody, _ := io.ReadAll(enrollResp.Body)
	if enrollResp.StatusCode != http.StatusOK {
		return fmt.Errorf("enrollment rejected (%d): %s", enrollResp.StatusCode, string(enrollBody))
	}

	var sessionRes worker.SessionResponseDTO
	if err := json.Unmarshal(enrollBody, &sessionRes); err != nil {
		return fmt.Errorf("parse enroll response: %w", err)
	}

	// 4. Persist worker identity
	if err := worker.SaveWorkerIdentity(keyPath, sessionRes.WorkerID); err != nil {
		return fmt.Errorf("save worker identity: %w", err)
	}

	fmt.Println("Worker Enrolled Successfully!")
	fmt.Printf("Worker ID:         %s\n", sessionRes.WorkerID)
	fmt.Printf("Private Key Path:  %s (mode 0600)\n", keyPath)
	fmt.Printf("Identity File:     %s (mode 0600)\n", worker.IdentityPath(keyPath))
	fmt.Printf("Control Plane:     %s\n", cpURL)
	fmt.Println("\nTo start this worker agent:")
	fmt.Printf("  runtime worker start --key-path %s --control-plane-url %s\n", keyPath, cpURL)

	return nil
}

func handleWorkerStart(args []string) error {
	fs := flag.NewFlagSet("runtime worker start", flag.ContinueOnError)
	keyPathFlag := fs.String("key-path", "", "Path to worker private key file (mode 0600)")
	cpURLFlag := fs.String("control-plane-url", "", "Control plane URL")
	bundleDirFlag := fs.String("bundle-dir", "./bundles", "Local bundle storage directory")
	poolFlag := fs.String("pool", "default", "Worker pool name")
	slotsFlag := fs.Int("slots", 2, "Concurrency slot capacity")
	nodePathFlag := fs.String("node-path", "node", "Path to Node.js executable")
	runnerPathFlag := fs.String("runner-path", "", "Path to installed Node runner script")
	if err := fs.Parse(args); err != nil {
		return err
	}

	cfg := LoadConfig()
	cpURL := *cpURLFlag
	if cpURL == "" {
		cpURL = cfg.APIURL
	}
	keyPath := *keyPathFlag
	if keyPath == "" {
		home, _ := os.UserHomeDir()
		keyPath = filepath.Join(home, ".deadbolt", "worker.key")
	}

	runnerPath := *runnerPathFlag
	if runnerPath == "" {
		runnerPath = os.Getenv("DEADBOLT_RUNNER_PATH")
	}
	if runnerPath == "" {
		if executable, err := os.Executable(); err == nil {
			candidate := filepath.Join(filepath.Dir(executable), "..", "share", "deadbolt", "runner", "index.js")
			if fileExists(candidate) {
				runnerPath = candidate
			}
		}
	}
	if runnerPath == "" || !fileExists(runnerPath) {
		return fmt.Errorf("Node runner is not installed. Install the Deadbolt runner companion asset or pass --runner-path /path/to/index.js (DEADBOLT_RUNNER_PATH is also supported)")
	}

	logger := log.New(os.Stdout, "[WORKER] ", log.LstdFlags|log.Lmsgprefix)

	agentCfg := worker.AgentConfig{
		ControlPlaneURL: cpURL,
		KeyPath:         keyPath,
		Pool:            *poolFlag,
		Slots:           *slotsFlag,
		NodePath:        *nodePathFlag,
		RunnerPath:      runnerPath,
		BundleDir:       *bundleDirFlag,
		Logger:          logger,
	}

	agent, err := worker.NewAgent(agentCfg)
	if err != nil {
		return fmt.Errorf("initialize worker agent: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		logger.Printf("Received termination signal; draining running tasks...")
		cancel()
	}()

	logger.Printf("Starting worker agent connected to %s (slots: %d, pool: %s, bundleDir: %s)", cpURL, *slotsFlag, *poolFlag, *bundleDirFlag)
	if err := agent.Start(ctx); err != nil && !errors.Is(err, context.Canceled) {
		return fmt.Errorf("worker agent stopped with error: %w", err)
	}

	logger.Println("Worker agent terminated cleanly.")
	return nil
}

func handleWorkerDrain(args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: runtime worker drain <worker_id>")
	}
	workerID := args[0]
	cfg := LoadConfig()

	req, err := cfg.NewRequest(http.MethodPost, fmt.Sprintf("/v1/workers/%s/drain", workerID), nil)
	if err != nil {
		return fmt.Errorf("create drain request: %w", err)
	}
	req.Header.Set("Idempotency-Key", fmt.Sprintf("worker-drain-%s-%d", workerID, time.Now().UnixNano()))

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("drain request failed: %w", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to drain worker: %s", FormatAPIError(resp.StatusCode, body))
	}

	fmt.Printf("Drain signal accepted for worker %s.\n", workerID)
	return nil
}

// resolveEnvironmentID uses only authenticated public tenant discovery routes.
func resolveEnvironmentID(cfg Config, nameOrID string) (string, error) {
	req, err := cfg.NewRequest(http.MethodGet, "/v1/projects", nil)
	if err != nil {
		return "", err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("list projects: %w", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("list projects: %s", FormatAPIError(resp.StatusCode, body))
	}
	var projects struct {
		Projects []struct {
			ID string `json:"id"`
		} `json:"projects"`
	}
	if err := json.Unmarshal(body, &projects); err != nil {
		return "", fmt.Errorf("parse projects: %w", err)
	}
	for _, project := range projects.Projects {
		req, err := cfg.NewRequest(http.MethodGet, fmt.Sprintf("/v1/projects/%s/environments", project.ID), nil)
		if err != nil {
			return "", err
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return "", fmt.Errorf("list environments: %w", err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return "", fmt.Errorf("list environments: %s", FormatAPIError(resp.StatusCode, body))
		}
		var environments struct {
			Environments []struct {
				ID   string `json:"id"`
				Name string `json:"name"`
			} `json:"environments"`
		}
		if err := json.Unmarshal(body, &environments); err != nil {
			return "", fmt.Errorf("parse environments: %w", err)
		}
		for _, environment := range environments.Environments {
			if environment.ID == nameOrID || environment.Name == nameOrID {
				return environment.ID, nil
			}
		}
	}
	return "", fmt.Errorf("environment %q was not found in the authenticated organization", nameOrID)
}

func handleWorkerList(args []string) error {
	fs := flag.NewFlagSet("runtime worker list", flag.ContinueOnError)
	envFlag := fs.String("env", "", "Environment name or ID")
	cpURLFlag := fs.String("control-plane-url", "", "Control plane base URL")
	cursorFlag := fs.String("cursor", "", "Pagination cursor")
	limitFlag := fs.Int("limit", 25, "Maximum number of items")
	jsonFlag := fs.Bool("json", false, "Output as JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}

	cfg := LoadConfig()
	if *cpURLFlag != "" {
		cfg.APIURL = strings.TrimRight(*cpURLFlag, "/")
	}
	env := *envFlag
	if env == "" {
		env = cfg.Env
	}
	if env == "" {
		return fmt.Errorf("--env is required")
	}

	q := url.Values{}
	q.Set("environment", env)
	if *cursorFlag != "" {
		q.Set("cursor", *cursorFlag)
	}
	if *limitFlag > 0 {
		q.Set("limit", fmt.Sprintf("%d", *limitFlag))
	}

	req, err := cfg.NewRequest(http.MethodGet, "/v1/workers?"+q.Encode(), nil)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("worker list request failed: %w", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("worker list failed: %s", FormatAPIError(resp.StatusCode, body))
	}

	if *jsonFlag {
		fmt.Println(string(body))
		return nil
	}

	var result struct {
		Items []struct {
			ID                string   `json:"id"`
			Pool              string   `json:"pool"`
			Status            string   `json:"status"`
			DeploymentDigests []string `json:"deploymentDigests"`
		} `json:"items"`
		NextCursor *string `json:"nextCursor"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return fmt.Errorf("parse workers response: %w", err)
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "WORKER ID\tPOOL\tSTATUS\tDEPLOYMENTS")
	for _, item := range result.Items {
		deployments := strings.Join(item.DeploymentDigests, ", ")
		if deployments == "" {
			deployments = "(none)"
		}
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\n", item.ID, item.Pool, item.Status, deployments)
	}
	w.Flush()

	if result.NextCursor != nil && *result.NextCursor != "" {
		fmt.Printf("\nNext cursor: %s\n", *result.NextCursor)
	}

	return nil
}
