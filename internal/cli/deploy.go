package cli

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

type DeploymentResponse struct {
	ID           string `json:"id"`
	ManifestHash string `json:"manifestHash"`
	BundleDigest string `json:"bundleDigest"`
	Status       string `json:"status"`
	CreatedAt    string `json:"createdAt"`
}

// HandleDeploy registers immutable deployment metadata on the control plane
func HandleDeploy(args []string) error {
	fs := flag.NewFlagSet("runtime deploy", flag.ContinueOnError)
	envFlag := fs.String("env", "", "Target environment (e.g. staging, development, production)")
	manifestFlag := fs.String("manifest", "", "Path to deployment manifest.json")
	jsonFlag := fs.Bool("json", false, "Output response as JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}

	cfg := LoadConfig()
	env := *envFlag
	if env == "" {
		env = cfg.Env
	}
	if env == "" {
		return fmt.Errorf("--env is required (e.g. --env staging or --env development)")
	}

	manifestPath := *manifestFlag
	if manifestPath == "" {
		candidates := []string{
			"dist/manifest.json",
			"bundles/manifest.json",
			"workflow.json",
		}
		for _, c := range candidates {
			if fileExists(c) {
				manifestPath = c
				break
			}
		}
	}
	if manifestPath == "" {
		return fmt.Errorf("deployment manifest not found. Run `runtime build` first or pass --manifest <path>")
	}

	manifestBytes, err := os.ReadFile(manifestPath)
	if err != nil {
		return fmt.Errorf("read manifest %s: %w", manifestPath, err)
	}

	// Validate JSON syntax locally before network call
	var parsedManifest map[string]any
	if err := json.Unmarshal(manifestBytes, &parsedManifest); err != nil {
		return fmt.Errorf("manifest is invalid JSON: %w", err)
	}

	dep, rawBody, err := RegisterDeployment(cfg, env, manifestBytes)
	if err != nil {
		return err
	}

	if *jsonFlag {
		fmt.Println(string(rawBody))
		return nil
	}

	fmt.Println("Deployment Metadata Registered Successfully!")
	fmt.Printf("Deployment ID:  %s\n", dep.ID)
	fmt.Printf("Environment:    %s\n", env)
	fmt.Printf("Status:         %s\n", dep.Status)
	fmt.Printf("Bundle Digest:  %s\n", dep.BundleDigest)
	fmt.Printf("Manifest Hash:  %s\n", dep.ManifestHash)
	fmt.Printf("Created At:     %s\n", dep.CreatedAt)

	// Extract primary workflow name for actionable activate suggestion
	workflowName := "default-workflow"
	if wfs, ok := parsedManifest["workflows"].([]any); ok && len(wfs) > 0 {
		if wf0, ok := wfs[0].(map[string]any); ok {
			if name, ok := wf0["name"].(string); ok && name != "" {
				workflowName = name
			}
		}
	}

	fmt.Println("\nArchitectural notice (Registration != Execution):")
	fmt.Println("  Customer tasks were NOT uploaded to or executed on the control plane.")
	fmt.Printf("  You must distribute bundle '%s.tar' to your workers.\n", dep.BundleDigest)
	fmt.Println("  The deployment status will become AVAILABLE once compatible workers connect.")
	fmt.Println("\nTo activate this deployment for new workflow runs:")
	fmt.Printf("  runtime deployments activate %s --workflow %s --env %s\n", dep.ID, workflowName, env)

	return nil
}

// RegisterDeployment sends manifest to control plane and returns parsed deployment response
func RegisterDeployment(cfg Config, env string, manifestBytes []byte) (*DeploymentResponse, []byte, error) {
	idempKey := fmt.Sprintf("dep-reg-%d", time.Now().UnixNano())

	req, err := cfg.NewRequest(http.MethodPost, fmt.Sprintf("/v1/deployments?environment=%s", env), bytes.NewReader(manifestBytes))
	if err != nil {
		return nil, nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", idempKey)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, nil, fmt.Errorf("deployment registration failed: %w", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return nil, body, fmt.Errorf("%s", FormatAPIError(resp.StatusCode, body))
	}

	var dep DeploymentResponse
	if err := json.Unmarshal(body, &dep); err != nil {
		return nil, body, fmt.Errorf("failed to parse deployment response: %w", err)
	}

	return &dep, body, nil
}
