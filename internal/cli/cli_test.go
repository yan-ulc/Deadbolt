package cli

import (
	"archive/tar"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestInitProject(t *testing.T) {
	tmpDir := t.TempDir()

	// Initial init should succeed
	err := InitProject(tmpDir, "sample-service", false)
	if err != nil {
		t.Fatalf("InitProject failed: %v", err)
	}

	expectedFiles := []string{
		"deadbolt.config.json",
		"package.json",
		"workflow.json",
		"tasks/validate.js",
		"tasks/provision.js",
		"tasks/notify.js",
		".env.example",
		".gitignore",
	}

	for _, f := range expectedFiles {
		p := filepath.Join(tmpDir, f)
		if _, err := os.Stat(p); os.IsNotExist(err) {
			t.Errorf("expected file %s does not exist", f)
		}
	}

	// Verify deadbolt.config.json content
	cfg, err := LoadProjectConfig(tmpDir)
	if err != nil {
		t.Fatalf("LoadProjectConfig failed: %v", err)
	}
	if cfg.Project != "sample-service" {
		t.Errorf("expected project name sample-service, got %s", cfg.Project)
	}
	if cfg.Workflow != "customer-onboarding" {
		t.Errorf("expected workflow name customer-onboarding, got %s", cfg.Workflow)
	}

	// Running init again without force should fail
	err = InitProject(tmpDir, "sample-service", false)
	if err == nil {
		t.Errorf("expected error when re-running InitProject without force, got nil")
	}

	// Running init with force should succeed
	err = InitProject(tmpDir, "sample-service", true)
	if err != nil {
		t.Errorf("expected force init to succeed, got %v", err)
	}
}

func TestBuildProjectDeterminism(t *testing.T) {
	tmpDir := t.TempDir()

	err := InitProject(tmpDir, "build-service", false)
	if err != nil {
		t.Fatalf("InitProject failed: %v", err)
	}

	buildOpts := BuildOptions{
		ProjectDir: tmpDir,
		BundleDir:  filepath.Join(tmpDir, "bundles"),
		OutputDir:  filepath.Join(tmpDir, "dist"),
		TargetArch: "amd64",
		TargetOS:   "linux",
	}

	// First build
	result1, err := BuildDeployment(buildOpts)
	if err != nil {
		t.Fatalf("First BuildDeployment failed: %v", err)
	}

	if len(result1.BundleDigest) != 64 {
		t.Errorf("expected 64-char sha256 bundleDigest, got %s", result1.BundleDigest)
	}

	// Verify bundle file on disk
	expectedBundlePath := filepath.Join(tmpDir, "bundles", result1.BundleDigest+".tar")
	bundleBytes, err := os.ReadFile(expectedBundlePath)
	if err != nil {
		t.Fatalf("failed to read bundle archive at %s: %v", expectedBundlePath, err)
	}

	// Verify digest computation matches raw bytes sha256
	h := sha256.Sum256(bundleBytes)
	actualDigest := hex.EncodeToString(h[:])
	if actualDigest != result1.BundleDigest {
		t.Errorf("manifest BundleDigest %s does not match bundle file sha256 %s", result1.BundleDigest, actualDigest)
	}

	// Second build should produce byte-for-byte identical output (determinism)
	result2, err := BuildDeployment(buildOpts)
	if err != nil {
		t.Fatalf("Second BuildDeployment failed: %v", err)
	}

	if result1.BundleDigest != result2.BundleDigest {
		t.Errorf("build is not deterministic: digest1=%s, digest2=%s", result1.BundleDigest, result2.BundleDigest)
	}
	if result1.DependencyLockDigest != result2.DependencyLockDigest {
		t.Errorf("dependencyLockDigest mismatch: %s vs %s", result1.DependencyLockDigest, result2.DependencyLockDigest)
	}
}

func TestKeychainStorage(t *testing.T) {
	tmpDir := t.TempDir()
	SetCustomCredentialsDir(tmpDir)

	service := "deadbolt-test"
	account := "api_key"
	secret := "db_live_secret1234567890abcdef"

	// Store credential
	err := StoreCredential(service, account, secret)
	if err != nil {
		t.Fatalf("StoreCredential failed: %v", err)
	}

	// Verify file permissions (0600)
	credFile := filepath.Join(tmpDir, "credentials.json")
	info, err := os.Stat(credFile)
	if err != nil {
		t.Fatalf("credentials.json not found: %v", err)
	}
	if info.Mode().Perm() != 0600 {
		t.Errorf("expected credentials.json permissions 0600, got %#o", info.Mode().Perm())
	}

	// Retrieve credential
	retrieved, err := GetCredential(service, account)
	if err != nil {
		t.Fatalf("GetCredential failed: %v", err)
	}
	if retrieved != secret {
		t.Errorf("expected secret %q, got %q", secret, retrieved)
	}

	// Delete credential
	err = DeleteCredential(service, account)
	if err != nil {
		t.Fatalf("DeleteCredential failed: %v", err)
	}

	// Retrieve after delete should be empty
	retrievedAfter, err := GetCredential(service, account)
	if err == nil && retrievedAfter != "" {
		t.Errorf("expected deleted credential to be empty, got %q", retrievedAfter)
	}
}

func TestDoctorSecretMasking(t *testing.T) {
	tmpDir := t.TempDir()

	// Create project with .env having secrets
	err := InitProject(tmpDir, "doctor-service", false)
	if err != nil {
		t.Fatalf("InitProject failed: %v", err)
	}

	superSecretValue := "super_secret_token_never_expose_998877"
	envContent := "API_SECRET=" + superSecretValue + "\nSTRIPE_KEY=sk_test_12345\n"
	_ = os.WriteFile(filepath.Join(tmpDir, ".env"), []byte(envContent), 0600)

	// Change working dir to tmpDir
	origWd, _ := os.Getwd()
	defer os.Chdir(origWd)
	_ = os.Chdir(tmpDir)

	// Build to have manifest and bundles
	buildOpts := BuildOptions{
		ProjectDir: tmpDir,
		BundleDir:  filepath.Join(tmpDir, "bundles"),
		OutputDir:  filepath.Join(tmpDir, "dist"),
		TargetArch: "amd64",
		TargetOS:   "linux",
	}
	result, err := BuildDeployment(buildOpts)
	if err != nil {
		t.Fatalf("BuildDeployment failed: %v", err)
	}

	// Run doctor checks
	cfg := Config{
		APIURL: "http://127.0.0.1:8080",
		Env:    "development",
	}
	report := RunDoctorChecks(cfg, filepath.Join(tmpDir, "dist", "manifest.json"), filepath.Join(tmpDir, "bundles"))

	// Verify doctor report:
	// 1. Bundle digest check should pass
	foundBundleCheck := false
	for _, c := range report.Checks {
		if strings.Contains(c.Name, "Bundle Integrity") {
			foundBundleCheck = true
			if c.Status != StatusOK {
				t.Errorf("expected bundle integrity to be OK, got %s: %s", c.Status, c.Message)
			}
		}
	}
	if !foundBundleCheck {
		t.Errorf("doctor report missing Bundle Integrity check")
	}

	// 2. Critical security requirement: secret values must NEVER appear in report output
	reportJSON, _ := json.Marshal(report)
	reportStr := string(reportJSON)

	if strings.Contains(reportStr, superSecretValue) {
		t.Fatalf("CRITICAL SECURITY VIOLATION: Doctor report leaked secret value %q in output!", superSecretValue)
	}
	if strings.Contains(reportStr, "sk_test_12345") {
		t.Fatalf("CRITICAL SECURITY VIOLATION: Doctor report leaked secret value 'sk_test_12345' in output!")
	}

	_ = result
}

func TestBuildArchiveStructure(t *testing.T) {
	tmpDir := t.TempDir()

	err := InitProject(tmpDir, "archive-service", false)
	if err != nil {
		t.Fatalf("InitProject failed: %v", err)
	}

	buildOpts := BuildOptions{
		ProjectDir: tmpDir,
		BundleDir:  filepath.Join(tmpDir, "bundles"),
		OutputDir:  filepath.Join(tmpDir, "dist"),
		TargetArch: "amd64",
		TargetOS:   "linux",
	}
	result, err := BuildDeployment(buildOpts)
	if err != nil {
		t.Fatalf("BuildDeployment failed: %v", err)
	}

	// Read tar archive entries
	bundleFile := filepath.Join(tmpDir, "bundles", result.BundleDigest+".tar")
	f, err := os.Open(bundleFile)
	if err != nil {
		t.Fatalf("open bundle failed: %v", err)
	}
	defer f.Close()

	tr := tar.NewReader(f)
	foundTasks := map[string]bool{}

	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("tar reading error: %v", err)
		}
		foundTasks[hdr.Name] = true
	}

	expectedEntries := []string{"tasks/validate.js", "tasks/provision.js", "tasks/notify.js"}
	for _, entry := range expectedEntries {
		if !foundTasks[entry] {
			t.Errorf("expected bundle to contain entry %s, found: %v", entry, foundTasks)
		}
	}
}

func TestLoginCIEnvironmentDetection(t *testing.T) {
	// Set CI environment variable
	origCI := os.Getenv("CI")
	defer os.Setenv("CI", origCI)
	os.Setenv("CI", "true")

	// In CI, login without --api-key and without loopback control plane must fail with actionable guidance
	err := RunLogin([]string{"--control-plane-url", "https://api.deadbolt.cloud"})
	if err == nil {
		t.Fatalf("expected login to fail in CI without api key")
	}

	errMsg := err.Error()
	if !strings.Contains(errMsg, "not supported in non-interactive / CI environments") {
		t.Errorf("expected error to explain CI non-interactive limitation, got: %s", errMsg)
	}
	if !strings.Contains(errMsg, "DEADBOLT_API_KEY") {
		t.Errorf("expected error to suggest DEADBOLT_API_KEY, got: %s", errMsg)
	}
}

func TestDoctorNodeToolchainDiagnostic(t *testing.T) {
	res := checkNodeToolchain()
	if res.Name == "" {
		t.Errorf("expected check name to be non-empty")
	}
	// Verify that if node is mismatched or missing, actionable remediation is provided
	if res.Status != StatusOK && res.Remediation == "" {
		t.Errorf("expected actionable remediation when Node check fails or warns")
	}
}
