package cli

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/Ryanakml/Deadbolt/internal/contracts"
	"github.com/Ryanakml/Deadbolt/internal/worker"
)

type CheckStatus string

const (
	StatusOK   CheckStatus = "OK"
	StatusWarn CheckStatus = "WARN"
	StatusFail CheckStatus = "FAIL"
)

type DoctorCheckResult struct {
	Name        string      `json:"name"`
	Status      CheckStatus `json:"status"`
	Message     string      `json:"message"`
	Remediation string      `json:"remediation,omitempty"`
}

type DoctorReport struct {
	Timestamp string              `json:"timestamp"`
	Passed    bool                `json:"passed"`
	Checks    []DoctorCheckResult `json:"checks"`
}

// HandleDoctor executes comprehensive diagnostic checks
func HandleDoctor(args []string) error {
	fs := flag.NewFlagSet("runtime doctor", flag.ContinueOnError)
	manifestFlag := fs.String("manifest", "", "Path to deployment manifest")
	bundleDirFlag := fs.String("bundle-dir", "", "Path to bundle storage directory")
	cpURLFlag := fs.String("control-plane-url", "", "Control plane base URL")
	jsonFlag := fs.Bool("json", false, "Output report as JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}

	cfg := LoadConfig()
	if *cpURLFlag != "" {
		cfg.APIURL = strings.TrimRight(*cpURLFlag, "/")
	}
	report := RunDoctorChecks(cfg, *manifestFlag, *bundleDirFlag)

	if *jsonFlag {
		b, _ := json.MarshalIndent(report, "", "  ")
		fmt.Println(string(b))
	} else {
		printDoctorReport(report)
	}

	if !report.Passed {
		return fmt.Errorf("doctor check detected critical failures")
	}
	return nil
}

// RunDoctorChecks performs all diagnostic verifications
func RunDoctorChecks(cfg Config, manifestPath, bundleDir string) DoctorReport {
	var checks []DoctorCheckResult
	allPassed := true

	// 1. Docker availability & daemon health
	dockerRes := checkDocker()
	checks = append(checks, dockerRes)
	if dockerRes.Status == StatusFail {
		allPassed = false
	}

	// 2. Node.js & toolchain versions
	nodeRes := checkNodeToolchain()
	checks = append(checks, nodeRes)
	if nodeRes.Status == StatusFail {
		allPassed = false
	}

	// 3. Control plane connectivity
	cpRes := checkControlPlane(cfg.APIURL)
	checks = append(checks, cpRes)
	if cpRes.Status == StatusFail {
		allPassed = false
	}

	// 4. Configuration and Environment validity
	cfgRes := checkConfig(cfg)
	checks = append(checks, cfgRes)
	if cfgRes.Status == StatusFail {
		allPassed = false
	}

	// 5. Bundle, Manifest, and Secret Names verification
	manifestChecks, manifestPassed := checkManifestAndBundles(manifestPath, bundleDir)
	checks = append(checks, manifestChecks...)
	if !manifestPassed {
		allPassed = false
	}

	return DoctorReport{
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Passed:    allPassed,
		Checks:    checks,
	}
}

func checkDocker() DoctorCheckResult {
	if _, err := exec.LookPath("docker"); err != nil {
		return DoctorCheckResult{
			Name:        "Docker Installation",
			Status:      StatusFail,
			Message:     "Docker CLI is not installed or not found in PATH.",
			Remediation: "Install Docker Desktop or Docker Engine, then rerun `runtime doctor`.",
		}
	}

	cmd := exec.Command("docker", "info")
	if err := cmd.Run(); err != nil {
		return DoctorCheckResult{
			Name:        "Docker Daemon",
			Status:      StatusFail,
			Message:     "Docker daemon is unavailable or not running.",
			Remediation: "Start Docker Desktop and rerun `runtime doctor`.",
		}
	}

	return DoctorCheckResult{
		Name:    "Docker Daemon",
		Status:  StatusOK,
		Message: "Docker daemon is running and responsive.",
	}
}

func checkNodeToolchain() DoctorCheckResult {
	nodePath, err := exec.LookPath("node")
	if err != nil {
		return DoctorCheckResult{
			Name:        "Node.js Runtime",
			Status:      StatusFail,
			Message:     "Node.js is not found in PATH.",
			Remediation: "Install Node.js v24.x (pinned requirement: 24.21.0) and rerun `runtime doctor`.",
		}
	}

	out, err := exec.Command(nodePath, "-v").Output()
	if err != nil {
		return DoctorCheckResult{
			Name:        "Node.js Runtime",
			Status:      StatusFail,
			Message:     "Failed to execute node -v.",
			Remediation: "Ensure Node.js is executable and rerun `runtime doctor`.",
		}
	}

	versionStr := strings.TrimSpace(string(out))
	if !strings.HasPrefix(versionStr, "v24.") {
		return DoctorCheckResult{
			Name:        "Node.js Runtime Version",
			Status:      StatusFail,
			Message:     fmt.Sprintf("Node.js version is %s, but v24.x is required by runtime contracts (pinned: 24.21.0).", versionStr),
			Remediation: "Switch to Node.js v24 using your version manager (nvm use 24 / fnm use 24) and rerun `runtime doctor`.",
		}
	}

	return DoctorCheckResult{
		Name:    "Node.js Runtime",
		Status:  StatusOK,
		Message: fmt.Sprintf("Node.js %s is compatible with runtime contracts.", versionStr),
	}
}

func checkControlPlane(apiURL string) DoctorCheckResult {
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get(apiURL + "/livez")
	if err != nil {
		return DoctorCheckResult{
			Name:        "Control Plane Connectivity",
			Status:      StatusWarn,
			Message:     fmt.Sprintf("Control plane at %s is unreachable (%v).", apiURL, err),
			Remediation: "If running locally, start the environment with `runtime dev`. In hosted mode, check DEADBOLT_API_URL and network connectivity.",
		}
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return DoctorCheckResult{
			Name:        "Control Plane Health",
			Status:      StatusWarn,
			Message:     fmt.Sprintf("Control plane /livez returned status %d.", resp.StatusCode),
			Remediation: "Check control plane logs with `docker compose logs control-plane`.",
		}
	}

	// Probe readyz for deeper database/schema checks
	readyResp, err := client.Get(apiURL + "/readyz")
	if err == nil {
		defer readyResp.Body.Close()
		if readyResp.StatusCode == http.StatusOK {
			return DoctorCheckResult{
				Name:    "Control Plane Health",
				Status:  StatusOK,
				Message: fmt.Sprintf("Control plane at %s is healthy and ready.", apiURL),
			}
		}
		return DoctorCheckResult{
			Name:        "Control Plane Health",
			Status:      StatusWarn,
			Message:     fmt.Sprintf("Control plane is live but /readyz returned %d (unready components).", readyResp.StatusCode),
			Remediation: "Verify database schema migrations have been applied: `control-plane --migrate`.",
		}
	}

	return DoctorCheckResult{
		Name:    "Control Plane Health",
		Status:  StatusOK,
		Message: fmt.Sprintf("Control plane at %s is live.", apiURL),
	}
}

func checkConfig(cfg Config) DoctorCheckResult {
	if cfg.Env == "" {
		return DoctorCheckResult{
			Name:        "Configuration",
			Status:      StatusWarn,
			Message:     "DEADBOLT_ENV is not set (defaulting to 'development').",
			Remediation: "Set DEADBOLT_ENV=staging or DEADBOLT_ENV=production when targeting non-dev environments.",
		}
	}

	validEnvs := map[string]bool{
		"development": true,
		"staging":     true,
		"production":  true,
		"local":       true,
	}
	if !validEnvs[strings.ToLower(cfg.Env)] && len(cfg.Env) < 32 {
		return DoctorCheckResult{
			Name:        "Environment Scope",
			Status:      StatusWarn,
			Message:     fmt.Sprintf("Target environment %q is unconventional (expected: development, staging, production, or UUID).", cfg.Env),
			Remediation: "Ensure target environment matches an existing environment name or UUID.",
		}
	}

	if cfg.APIKey != "" {
		if !strings.HasPrefix(cfg.APIKey, "db_") && len(cfg.APIKey) < 32 {
			return DoctorCheckResult{
				Name:        "API Key Format",
				Status:      StatusWarn,
				Message:     "Configured API key does not match canonical 'db_<env>_<8hex>_...' prefix format.",
				Remediation: "Verify DEADBOLT_API_KEY contains a valid machine API key or session token.",
			}
		}
	}

	return DoctorCheckResult{
		Name:    "Configuration",
		Status:  StatusOK,
		Message: fmt.Sprintf("Environment is %s, API URL is %s.", cfg.Env, cfg.APIURL),
	}
}

func checkManifestAndBundles(manifestPath, bundleDir string) ([]DoctorCheckResult, bool) {
	var results []DoctorCheckResult
	passed := true

	// Resolve manifest candidate paths
	if manifestPath == "" {
		candidates := []string{
			"dist/manifest.json",
			"workflow.json",
			"bundles/manifest.json",
		}
		for _, c := range candidates {
			if fileExists(c) {
				manifestPath = c
				break
			}
		}
	}

	if manifestPath == "" {
		results = append(results, DoctorCheckResult{
			Name:        "Deployment Manifest",
			Status:      StatusWarn,
			Message:     "No deployment manifest found in workspace.",
			Remediation: "Run `runtime init` to scaffold a project, or `runtime build` to produce a deployment manifest.",
		})
		return results, true
	}

	data, err := os.ReadFile(manifestPath)
	if err != nil {
		results = append(results, DoctorCheckResult{
			Name:        "Deployment Manifest",
			Status:      StatusFail,
			Message:     fmt.Sprintf("Failed to read manifest at %s: %v", manifestPath, err),
			Remediation: "Ensure manifest file exists and has read permissions.",
		})
		return results, false
	}

	parsed, err := contracts.ParseJSON(data)
	if err != nil {
		results = append(results, DoctorCheckResult{
			Name:        "Deployment Manifest",
			Status:      StatusFail,
			Message:     fmt.Sprintf("Manifest is invalid JSON: %v", err),
			Remediation: "Fix manifest JSON formatting or rerun `runtime build`.",
		})
		return results, false
	}

	if err := contracts.ValidateDeployment(parsed); err != nil {
		results = append(results, DoctorCheckResult{
			Name:        "Manifest Schema Conformance",
			Status:      StatusFail,
			Message:     fmt.Sprintf("Manifest fails deployment schema validation: %v", err),
			Remediation: "Update manifest definitions according to contracts/manifest/deployment.schema.json.",
		})
		passed = false
	} else {
		results = append(results, DoctorCheckResult{
			Name:    "Manifest Schema Conformance",
			Status:  StatusOK,
			Message: "Manifest conforms to deployment schema contract.",
		})
	}

	// Extract bundleDigest and secretNames
	var bundleDigest string
	var targetArch string
	var secretNames []string
	if m, ok := parsed.(map[string]any); ok {
		if s, ok := m["bundleDigest"].(string); ok {
			bundleDigest = s
		}
		if s, ok := m["targetArchitecture"].(string); ok {
			targetArch = s
		}
		if arr, ok := m["secretNames"].([]any); ok {
			for _, item := range arr {
				if s, ok := item.(string); ok {
					secretNames = append(secretNames, s)
				}
			}
		}
	}

	// Check bundle artifact presence and SHA-256 match
	if bundleDigest != "" {
		if bundleDir == "" {
			bundleDir = "bundles"
			if !fileExists(bundleDir) {
				bundleDir = "dist"
			}
		}

		bundleTarPath := filepath.Join(bundleDir, bundleDigest+".tar")
		if !fileExists(bundleTarPath) {
			// Check if any .tar exists
			results = append(results, DoctorCheckResult{
				Name:        "Bundle Artifact",
				Status:      StatusWarn,
				Message:     fmt.Sprintf("Bundle archive '%s.tar' not found in %s.", bundleDigest, bundleDir),
				Remediation: "Run `runtime build` to generate the immutable .tar bundle before starting workers or activating.",
			})
		} else {
			file, err := os.Open(bundleTarPath)
			if err != nil {
				results = append(results, DoctorCheckResult{
					Name:        "Bundle Artifact",
					Status:      StatusFail,
					Message:     fmt.Sprintf("Failed to open bundle archive %s: %v", bundleTarPath, err),
					Remediation: "Verify file permissions on the bundle archive.",
				})
				passed = false
			} else {
				hasher := sha256.New()
				_, _ = io.Copy(hasher, file)
				file.Close()
				computed := hex.EncodeToString(hasher.Sum(nil))

				if !strings.EqualFold(computed, bundleDigest) {
					results = append(results, DoctorCheckResult{
						Name:        "Bundle Integrity",
						Status:      StatusFail,
						Message:     fmt.Sprintf("Bundle SHA-256 mismatch: computed %s != manifest %s", computed, bundleDigest),
						Remediation: "Bundle bytes have been corrupted or modified. Rerun `runtime build` to produce a fresh bundle.",
					})
					passed = false
				} else {
					results = append(results, DoctorCheckResult{
						Name:    "Bundle Integrity",
						Status:  StatusOK,
						Message: fmt.Sprintf("Bundle archive verified (%s).", bundleDigest),
					})
				}
			}

			// Architecture compatibility
			if targetArch != "" {
				if err := worker.VerifyArchitecture(targetArch, ""); err != nil {
					results = append(results, DoctorCheckResult{
						Name:        "Architecture Compatibility",
						Status:      StatusWarn,
						Message:     fmt.Sprintf("Bundle declared targetArchitecture %q is incompatible with current host (%s).", targetArch, worker.CurrentHostArchitecture()),
						Remediation: "Compile/bundle tasks for current target architecture or execute in a compatible worker container.",
					})
				} else {
					results = append(results, DoctorCheckResult{
						Name:    "Architecture Compatibility",
						Status:  StatusOK,
						Message: fmt.Sprintf("Target architecture %q is compatible with host.", targetArch),
					})
				}
			}
		}
	}

	// Secret names verification (strictly checking presence without printing secret values!)
	envSecrets := loadLocalEnvSecretNames()
	for _, sec := range secretNames {
		if os.Getenv(sec) != "" || envSecrets[sec] {
			results = append(results, DoctorCheckResult{
				Name:    "Secret Name: " + sec,
				Status:  StatusOK,
				Message: fmt.Sprintf("Required secret name %q is configured.", sec),
			})
		} else {
			results = append(results, DoctorCheckResult{
				Name:        "Secret Name: " + sec,
				Status:      StatusFail,
				Message:     fmt.Sprintf("Missing required secret: %s", sec),
				Remediation: fmt.Sprintf("Set environment variable %s in your worker environment or .env file.", sec),
			})
			passed = false
		}
	}

	return results, passed
}

// loadLocalEnvSecretNames reads .env in working directory to check which variable names are declared
// without storing or leaking their actual values.
func loadLocalEnvSecretNames() map[string]bool {
	declared := make(map[string]bool)
	data, err := os.ReadFile(".env")
	if err != nil {
		return declared
	}

	lines := strings.Split(string(data), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.SplitN(line, "=", 2)
		if len(parts) == 2 {
			varName := strings.TrimSpace(parts[0])
			varVal := strings.TrimSpace(parts[1])
			if varName != "" && varVal != "" {
				declared[varName] = true
			}
		}
	}
	return declared
}

func printDoctorReport(report DoctorReport) {
	fmt.Println("=== Deadbolt Runtime Doctor ===")
	fmt.Println()

	for _, check := range report.Checks {
		var icon string
		switch check.Status {
		case StatusOK:
			icon = "[OK]  "
		case StatusWarn:
			icon = "[WARN]"
		case StatusFail:
			icon = "[FAIL]"
		}

		fmt.Printf("%s %s: %s\n", icon, check.Name, check.Message)
		if check.Remediation != "" && check.Status != StatusOK {
			fmt.Printf("       Remediation: %s\n", check.Remediation)
		}
	}

	fmt.Println()
	if report.Passed {
		fmt.Println("Result: All required checks passed successfully.")
	} else {
		fmt.Println("Result: Action required. Resolve the FAIL items above and rerun `runtime doctor`.")
	}
}
