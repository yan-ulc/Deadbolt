package cli

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"text/tabwriter"
	"time"
)

// RunRuns handles "runtime runs <create|list|inspect>"
func RunRuns(args []string) error {
	if len(args) < 1 {
		printRunsUsage()
		return fmt.Errorf("subcommand required: create, list, inspect")
	}

	cfg := LoadConfig()

	switch args[0] {
	case "create":
		return runRunsCreate(cfg, args[1:])
	case "list":
		return runRunsList(cfg, args[1:])
	case "inspect":
		return runRunsInspect(cfg, args[1:])
	case "help", "-h", "--help":
		printRunsUsage()
		return nil
	default:
		printRunsUsage()
		return fmt.Errorf("unknown subcommand %q", args[0])
	}
}

func printRunsUsage() {
	fmt.Println(`Usage:
  runtime runs create --workflow <name> [--env <env>] [--input <json|@file>] [--idempotency-key <key>] [--json]
  runtime runs list [--env <env>] [--cursor <cursor>] [--limit <limit>] [--json]
  runtime runs inspect <run_id> [--json]

Subcommands:
  create    Submit and start a workflow run
  list      List runs in an environment
  inspect   Inspect detailed run status, steps, attempts, outputs, and errors`)
}

func runRunsCreate(cfg Config, args []string) error {
	fs := flag.NewFlagSet("runs create", flag.ContinueOnError)
	workflowFlag := fs.String("workflow", "", "Name of workflow to execute")
	envFlag := fs.String("env", cfg.Env, "Target environment (e.g. development, staging, production)")
	cpURLFlag := fs.String("control-plane-url", "", "Control plane base URL")
	inputFlag := fs.String("input", "", "JSON input payload or @filename.json")
	idempKeyFlag := fs.String("idempotency-key", "", "Custom idempotency key (defaults to generated key)")
	deploymentIDFlag := fs.String("deployment-id", "", "Optional deployment ID pinning")
	jsonFlag := fs.Bool("json", false, "Output as JSON")

	if err := fs.Parse(args); err != nil {
		return err
	}

	if *cpURLFlag != "" {
		cfg.APIURL = strings.TrimRight(*cpURLFlag, "/")
	}

	workflowName := *workflowFlag
	// If workflow flag wasn't set, check if positional arg was provided
	if workflowName == "" && fs.NArg() > 0 {
		workflowName = fs.Arg(0)
	}

	// If still empty, check project config
	if workflowName == "" {
		if prj, err := LoadProjectConfig("."); err == nil && prj != nil && prj.Workflow != "" {
			workflowName = prj.Workflow
		}
	}

	if workflowName == "" {
		return fmt.Errorf("workflow name is required: use --workflow <name>")
	}

	if *envFlag == "" {
		return fmt.Errorf("environment is required: use --env <env> or set DEADBOLT_ENV")
	}

	// Parse input payload
	var inputVal any = map[string]any{}
	if *inputFlag != "" {
		rawInput := *inputFlag
		if strings.HasPrefix(rawInput, "@") {
			filePath := strings.TrimPrefix(rawInput, "@")
			fileBytes, err := os.ReadFile(filePath)
			if err != nil {
				return fmt.Errorf("failed to read input file %s: %w", filePath, err)
			}
			rawInput = string(fileBytes)
		} else if _, err := os.Stat(rawInput); err == nil {
			// If file exists with that exact name
			fileBytes, err := os.ReadFile(rawInput)
			if err == nil {
				rawInput = string(fileBytes)
			}
		}

		if err := json.Unmarshal([]byte(rawInput), &inputVal); err != nil {
			return fmt.Errorf("invalid input JSON payload: %w", err)
		}
	}

	// Idempotency key
	idempKey := *idempKeyFlag
	if idempKey == "" {
		b := make([]byte, 16)
		_, _ = rand.Read(b)
		idempKey = "cli-run-" + hex.EncodeToString(b)
	}

	// Build request payload
	reqBody := map[string]any{
		"environment": *envFlag,
		"input":       inputVal,
	}
	if *deploymentIDFlag != "" {
		reqBody["deploymentId"] = *deploymentIDFlag
	}

	bodyBytes, err := json.Marshal(reqBody)
	if err != nil {
		return fmt.Errorf("failed to encode request: %w", err)
	}

	apiPath := fmt.Sprintf("/v1/workflows/%s/runs", url.PathEscape(workflowName))
	req, err := cfg.NewRequest("POST", apiPath, bytes.NewReader(bodyBytes))
	if err != nil {
		return fmt.Errorf("failed to construct request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", idempKey)

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("request to control plane failed: %w", err)
	}
	defer resp.Body.Close()

	respBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode != http.StatusAccepted && resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to create run: %s", FormatAPIError(resp.StatusCode, respBytes))
	}

	if *jsonFlag {
		fmt.Println(string(respBytes))
		return nil
	}

	var runResp struct {
		ID           string `json:"id"`
		WorkflowName string `json:"workflowName"`
		Status       string `json:"status"`
		Revision     int64  `json:"revision"`
		DeploymentID string `json:"deploymentId"`
		CreatedAt    string `json:"createdAt"`
	}
	if err := json.Unmarshal(respBytes, &runResp); err != nil {
		return fmt.Errorf("failed to parse run response: %w", err)
	}

	fmt.Println("Workflow run initiated successfully.")
	fmt.Printf("Run ID:        %s\n", runResp.ID)
	fmt.Printf("Workflow:      %s\n", runResp.WorkflowName)
	fmt.Printf("Status:        %s\n", runResp.Status)
	fmt.Printf("Revision:      %d\n", runResp.Revision)
	fmt.Printf("Deployment ID: %s\n", runResp.DeploymentID)
	fmt.Printf("Created At:    %s\n", runResp.CreatedAt)
	fmt.Printf("\nInspect run progress:\n  runtime runs inspect %s\n", runResp.ID)
	fmt.Printf("Stream logs:\n  runtime logs %s\n", runResp.ID)

	return nil
}

func runRunsList(cfg Config, args []string) error {
	fs := flag.NewFlagSet("runs list", flag.ContinueOnError)
	envFlag := fs.String("env", cfg.Env, "Environment name or ID")
	cpURLFlag := fs.String("control-plane-url", "", "Control plane base URL")
	cursorFlag := fs.String("cursor", "", "Pagination cursor")
	limitFlag := fs.Int("limit", 25, "Maximum number of items")
	jsonFlag := fs.Bool("json", false, "Output as JSON")

	if err := fs.Parse(args); err != nil {
		return err
	}

	if *cpURLFlag != "" {
		cfg.APIURL = strings.TrimRight(*cpURLFlag, "/")
	}

	if *envFlag == "" {
		return fmt.Errorf("--env or DEADBOLT_ENV is required")
	}

	q := url.Values{}
	q.Set("environment", *envFlag)
	if *cursorFlag != "" {
		q.Set("cursor", *cursorFlag)
	}
	if *limitFlag > 0 {
		q.Set("limit", fmt.Sprintf("%d", *limitFlag))
	}

	req, err := cfg.NewRequest("GET", "/v1/runs?"+q.Encode(), nil)
	if err != nil {
		return fmt.Errorf("failed to construct request: %w", err)
	}

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to list runs: %s", FormatAPIError(resp.StatusCode, body))
	}

	if *jsonFlag {
		fmt.Println(string(body))
		return nil
	}

	var result struct {
		Items []struct {
			ID           string  `json:"id"`
			WorkflowName string  `json:"workflowName"`
			Status       string  `json:"status"`
			CreatedAt    string  `json:"createdAt"`
			ReasonCode   *string `json:"reasonCode"`
		} `json:"items"`
		NextCursor *string `json:"nextCursor"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return fmt.Errorf("failed to parse response: %w", err)
	}

	if len(result.Items) == 0 {
		fmt.Println("No runs found in environment:", *envFlag)
		return nil
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "RUN ID\tWORKFLOW\tSTATUS\tREASON\tCREATED AT")
	for _, item := range result.Items {
		reason := "-"
		if item.ReasonCode != nil {
			reason = *item.ReasonCode
		}
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n", item.ID, item.WorkflowName, item.Status, reason, item.CreatedAt)
	}
	w.Flush()

	if result.NextCursor != nil && *result.NextCursor != "" {
		fmt.Printf("\nNext cursor: %s\n", *result.NextCursor)
	}

	return nil
}

func runRunsInspect(cfg Config, args []string) error {
	fs := flag.NewFlagSet("runs inspect", flag.ContinueOnError)
	cpURLFlag := fs.String("control-plane-url", "", "Control plane base URL")
	jsonFlag := fs.Bool("json", false, "Output as JSON")

	var flagArgs []string
	var runID string
	for i := 0; i < len(args); i++ {
		if strings.HasPrefix(args[i], "-") {
			flagArgs = append(flagArgs, args[i])
			if !strings.Contains(args[i], "=") && i+1 < len(args) && !strings.HasPrefix(args[i+1], "-") {
				if args[i] != "-json" && args[i] != "--json" {
					i++
					flagArgs = append(flagArgs, args[i])
				}
			}
		} else if runID == "" {
			runID = args[i]
		}
	}

	if err := fs.Parse(flagArgs); err != nil {
		return err
	}

	if *cpURLFlag != "" {
		cfg.APIURL = strings.TrimRight(*cpURLFlag, "/")
	}

	if runID == "" && fs.NArg() > 0 {
		runID = fs.Arg(0)
	}
	if runID == "" {
		return fmt.Errorf("run ID is required: runtime runs inspect <run_id> [--json]")
	}

	req, err := cfg.NewRequest("GET", "/v1/runs/"+url.PathEscape(runID), nil)
	if err != nil {
		return fmt.Errorf("failed to construct request: %w", err)
	}

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to inspect run: %s", FormatAPIError(resp.StatusCode, body))
	}

	if *jsonFlag {
		fmt.Println(string(body))
		return nil
	}

	var snapshot struct {
		ID                string  `json:"id"`
		WorkflowName      string  `json:"workflowName"`
		Status            string  `json:"status"`
		ReasonCode        *string `json:"reasonCode"`
		Revision          int64   `json:"revision"`
		LastEventSequence int64   `json:"lastEventSequence"`
		CreatedAt         string  `json:"createdAt"`
		DeadlineAt        *string `json:"deadlineAt"`
		Steps             []struct {
			ID           string `json:"id"`
			NodeID       string `json:"nodeId"`
			Status       string `json:"status"`
			CurrentEpoch int64  `json:"currentEpoch"`
			Attempts     []struct {
				ID            string  `json:"id"`
				AttemptNumber int     `json:"attemptNumber"`
				Status        string  `json:"status"`
				StartedAt     *string `json:"startedAt"`
				CompletedAt   *string `json:"completedAt"`
			} `json:"attempts"`
		} `json:"steps"`
		Output any `json:"output"`
		Error  any `json:"error"`
	}
	if err := json.Unmarshal(body, &snapshot); err != nil {
		return fmt.Errorf("failed to parse run snapshot: %w", err)
	}

	fmt.Printf("Run ID:              %s\n", snapshot.ID)
	fmt.Printf("Workflow:            %s\n", snapshot.WorkflowName)
	fmt.Printf("Status:              %s\n", snapshot.Status)
	if snapshot.ReasonCode != nil {
		fmt.Printf("Reason Code:         %s\n", *snapshot.ReasonCode)
	}
	fmt.Printf("Revision:            %d\n", snapshot.Revision)
	fmt.Printf("Last Event Sequence: %d\n", snapshot.LastEventSequence)
	fmt.Printf("Created At:          %s\n", snapshot.CreatedAt)
	if snapshot.DeadlineAt != nil {
		fmt.Printf("Deadline At:         %s\n", *snapshot.DeadlineAt)
	}

	fmt.Println("\nSteps & Attempts:")
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "  STEP ID\tNODE\tSTATUS\tEPOCH\tATTEMPTS")
	for _, s := range snapshot.Steps {
		fmt.Fprintf(w, "  %s\t%s\t%s\t%d\t%d attempt(s)\n", s.ID, s.NodeID, s.Status, s.CurrentEpoch, len(s.Attempts))
		for _, a := range s.Attempts {
			started := "-"
			if a.StartedAt != nil {
				started = *a.StartedAt
			}
			fmt.Fprintf(w, "    └── Attempt #%d:\t[%s]\tStarted: %s\tID: %s\t\n", a.AttemptNumber, a.Status, started, a.ID)
		}
	}
	w.Flush()

	if snapshot.Error != nil {
		errBytes, _ := json.MarshalIndent(snapshot.Error, "", "  ")
		fmt.Printf("\nError:\n%s\n", string(errBytes))
	}
	if snapshot.Output != nil {
		outBytes, _ := json.MarshalIndent(snapshot.Output, "", "  ")
		fmt.Printf("\nOutput:\n%s\n", string(outBytes))
	}

	return nil
}
