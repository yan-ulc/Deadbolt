package cli

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// HandleDeployments processes `runtime deployments <subcommand> [flags]`
func HandleDeployments(args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: runtime deployments activate <deployment_id> [flags]")
	}

	switch args[0] {
	case "activate":
		return handleActivateDeployment(args[1:])
	default:
		return fmt.Errorf("unknown deployments subcommand: %s (supported: activate)", args[0])
	}
}

func handleActivateDeployment(args []string) error {
	if len(args) < 1 || strings.HasPrefix(args[0], "-") {
		return fmt.Errorf("usage: runtime deployments activate <deployment_id> --workflow <name> --env <env> [flags]")
	}

	deploymentID := args[0]
	fs := flag.NewFlagSet("runtime deployments activate", flag.ContinueOnError)
	workflowFlag := fs.String("workflow", "", "Target workflow name (required)")
	envFlag := fs.String("env", "", "Target environment (e.g. staging, development, production)")
	expectedRevFlag := fs.Int64("expected-revision", 0, "Expected current channel revision")
	allowSingleFlag := fs.Bool("allow-single-worker", false, "Allow activation with 1 worker in dev/staging (disables failover)")
	cpURLFlag := fs.String("control-plane-url", "", "Control plane base URL")
	jsonFlag := fs.Bool("json", false, "Output response as JSON")
	if err := fs.Parse(args[1:]); err != nil {
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
		return fmt.Errorf("--env is required (e.g. --env staging or --env development)")
	}

	workflowName := *workflowFlag
	if workflowName == "" {
		// Attempt to read workflow name from local config
		if prj, err := LoadProjectConfig("."); err == nil && prj != nil && prj.Workflow != "" {
			workflowName = prj.Workflow
		}
	}
	if workflowName == "" {
		return fmt.Errorf("--workflow is required (e.g. --workflow customer-onboarding)")
	}

	payload := map[string]any{
		"deploymentId":      deploymentID,
		"expectedRevision":  *expectedRevFlag,
		"allowSingleWorker": *allowSingleFlag,
	}
	reqBytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal activate payload: %w", err)
	}

	idempKey := fmt.Sprintf("dep-act-%s-%d", workflowName, time.Now().UnixNano())
	reqPath := fmt.Sprintf("/v1/workflows/%s/activate?environment=%s", workflowName, env)
	req, err := cfg.NewRequest(http.MethodPost, reqPath, bytes.NewReader(reqBytes))
	if err != nil {
		return fmt.Errorf("create activate request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", idempKey)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("activate request failed: %w", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		var envErr struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		}
		_ = json.Unmarshal(body, &envErr)

		if envErr.Code == "WORKER_PREFLIGHT_FAILED" {
			errMsg := "Activation preflight failed: Insufficient compatible workers online advertising this deployment's bundle digest.\n"
			if strings.EqualFold(env, "production") {
				errMsg += "Production strictly requires 2 compatible active workers for failover recovery."
			} else {
				errMsg += "In development or staging, you may pass --allow-single-worker if only 1 worker is available.\n" +
					"Note: with a single worker, automated failover recovery is not available."
			}
			return fmt.Errorf("%s", errMsg)
		}
		if envErr.Code == "REVISION_CONFLICT" {
			return fmt.Errorf("Revision conflict: Expected revision %d does not match current channel revision. Fetch latest revision and retry.", *expectedRevFlag)
		}

		return fmt.Errorf("%s", FormatAPIError(resp.StatusCode, body))
	}

	if *jsonFlag {
		fmt.Println(string(body))
		return nil
	}

	var res struct {
		Name               string  `json:"name"`
		ActiveDeploymentID *string `json:"activeDeploymentId"`
		Revision           int64   `json:"revision"`
		Warning            string  `json:"warning,omitempty"`
	}
	if err := json.Unmarshal(body, &res); err != nil {
		return fmt.Errorf("parse activate response: %w", err)
	}

	fmt.Println("Deployment Activated Successfully!")
	fmt.Printf("Workflow:           %s\n", res.Name)
	if res.ActiveDeploymentID != nil {
		fmt.Printf("Active Deployment:  %s\n", *res.ActiveDeploymentID)
	}
	fmt.Printf("Channel Revision:   %d\n", res.Revision)
	fmt.Printf("Environment:        %s\n", env)

	if res.Warning != "" {
		fmt.Printf("\nNotice: %s\n", res.Warning)
	}

	fmt.Println("\nNew workflow runs will now execute against this deployment.")
	fmt.Println("To start a workflow run:")
	fmt.Printf("  runtime runs create --workflow %s --env %s\n", res.Name, env)

	return nil
}
