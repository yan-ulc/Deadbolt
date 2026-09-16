package worker

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"runtime"
	"strings"
)

var (
	ErrBundleDigestMismatch = errors.New("BUNDLE_DIGEST_MISMATCH: Computed bundle SHA-256 does not match manifest")
	ErrArchitectureMismatch = errors.New("ARCHITECTURE_MISMATCH: Worker runtime architecture incompatible with bundle target")
)

// VerifyBundleDigest computes the SHA-256 digest of the bundle data from reader
// and verifies that it exactly matches the expected hex digest.
func VerifyBundleDigest(r io.Reader, expectedDigest string) (string, error) {
	hasher := sha256.New()
	if _, err := io.Copy(hasher, r); err != nil {
		return "", fmt.Errorf("failed to compute bundle digest: %w", err)
	}

	computed := hex.EncodeToString(hasher.Sum(nil))
	expected := strings.TrimPrefix(expectedDigest, "sha256:")

	if !strings.EqualFold(computed, expected) {
		return computed, ErrBundleDigestMismatch
	}

	return "sha256:" + computed, nil
}

// CurrentHostArchitecture returns the current canonical OS/arch string, e.g. "linux/amd64".
func CurrentHostArchitecture() string {
	return runtime.GOOS + "/" + runtime.GOARCH
}

// VerifyArchitecture checks whether the bundle's declared target architecture
// is compatible with the host architecture.
func VerifyArchitecture(targetArch string, hostArch string) error {
	normTarget := strings.ToLower(strings.TrimSpace(targetArch))
	normHost := strings.ToLower(strings.TrimSpace(hostArch))

	if normHost == "" {
		normHost = CurrentHostArchitecture()
	}

	// Direct match
	if normTarget == normHost {
		return nil
	}

	// Common architecture alias normalization
	aliases := map[string]string{
		"linux-x64":    "linux/amd64",
		"linux-arm64":  "linux/arm64",
		"darwin-arm64": "darwin/arm64",
		"darwin-x64":   "darwin/amd64",
		"win32-x64":    "windows/amd64",
		"windows-x64":  "windows/amd64",
	}

	if targetAliased, ok := aliases[normTarget]; ok {
		normTarget = targetAliased
	}
	if hostAliased, ok := aliases[normHost]; ok {
		normHost = hostAliased
	}

	if normTarget == normHost {
		return nil
	}

	// Allow Darwin (macOS) local worker execution for matching CPU architecture of Linux target
	if (normTarget == "linux/arm64" && normHost == "darwin/arm64") ||
		(normTarget == "linux/amd64" && normHost == "darwin/amd64") {
		return nil
	}

	return ErrArchitectureMismatch
}
