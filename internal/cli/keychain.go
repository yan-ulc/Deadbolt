package cli

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
)

var (
	ErrCredentialNotFound = errors.New("credential not found")
	customCredentialsDir  string
	credentialsMu         sync.Mutex
)

// SetCustomCredentialsDir overrides the storage path for credentials (useful for testing).
func SetCustomCredentialsDir(dir string) {
	credentialsMu.Lock()
	defer credentialsMu.Unlock()
	customCredentialsDir = dir
}

// GetCredentialsDirectory returns the path to the credentials directory
func GetCredentialsDirectory() (string, error) {
	return getCredentialsDir()
}

func getCredentialsDir() (string, error) {
	credentialsMu.Lock()
	defer credentialsMu.Unlock()
	if customCredentialsDir != "" {
		return customCredentialsDir, nil
	}
	if envDir := os.Getenv("DEADBOLT_CREDENTIALS_DIR"); envDir != "" {
		return envDir, nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, ".deadbolt"), nil
}

func getCredentialsFilePath() (string, error) {
	dir, err := getCredentialsDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, "credentials.json"), nil
}

// StoreCredential saves a credential to the native OS keychain. File storage is
// deliberately restricted to explicitly configured test/local-fixture directories.
func StoreCredential(service, account, secret string) error {
	// If custom directory or test environment is set, use secure file directly to avoid polluting system keychain
	if customCredentialsDir != "" || os.Getenv("DEADBOLT_CREDENTIALS_DIR") != "" {
		return storeFileCredential(service, account, secret)
	}

	if runtime.GOOS == "darwin" {
		cmd := exec.Command("security", "add-generic-password", "-s", service, "-a", account, "-w", secret, "-U")
		err := cmd.Run()
		if err == nil {
			return nil
		}
		return fmt.Errorf("macOS Keychain is unavailable; hosted credentials are not written to disk: %w", err)
	} else if runtime.GOOS == "linux" {
		if _, err := exec.LookPath("secret-tool"); err == nil {
			cmd := exec.Command("secret-tool", "store", "--label="+service, "service", service, "account", account)
			cmd.Stdin = strings.NewReader(secret)
			if err := cmd.Run(); err == nil {
				return nil
			}
			return fmt.Errorf("Linux Secret Service is unavailable; hosted credentials are not written to disk")
		}
		return fmt.Errorf("secret-tool is required for hosted credentials on Linux; install a Secret Service provider")
	}

	return fmt.Errorf("no supported native credential store for %s", runtime.GOOS)
}

// GetCredential retrieves a credential from the OS keychain or fallback secure storage.
func GetCredential(service, account string) (string, error) {
	if customCredentialsDir != "" || os.Getenv("DEADBOLT_CREDENTIALS_DIR") != "" {
		return getFileCredential(service, account)
	}

	if runtime.GOOS == "darwin" {
		cmd := exec.Command("security", "find-generic-password", "-s", service, "-a", account, "-w")
		out, err := cmd.Output()
		if err == nil {
			return strings.TrimSpace(string(out)), nil
		}
	} else if runtime.GOOS == "linux" {
		if _, err := exec.LookPath("secret-tool"); err == nil {
			cmd := exec.Command("secret-tool", "lookup", "service", service, "account", account)
			out, err := cmd.Output()
			if err == nil && len(out) > 0 {
				return strings.TrimSpace(string(out)), nil
			}
		}
	}

	return "", fmt.Errorf("native credential store is unavailable: %w", ErrCredentialNotFound)
}

// DeleteCredential removes a credential from the OS keychain or fallback secure storage.
func DeleteCredential(service, account string) error {
	if customCredentialsDir != "" || os.Getenv("DEADBOLT_CREDENTIALS_DIR") != "" {
		return deleteFileCredential(service, account)
	}

	if runtime.GOOS == "darwin" {
		cmd := exec.Command("security", "delete-generic-password", "-s", service, "-a", account)
		_ = cmd.Run()
	} else if runtime.GOOS == "linux" {
		if _, err := exec.LookPath("secret-tool"); err == nil {
			cmd := exec.Command("secret-tool", "clear", "service", service, "account", account)
			_ = cmd.Run()
		}
	}

	return nil
}

func storeFileCredential(service, account, secret string) error {
	dir, err := getCredentialsDir()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("create credentials dir: %w", err)
	}

	filePath, err := getCredentialsFilePath()
	if err != nil {
		return err
	}

	creds := make(map[string]string)
	if data, err := os.ReadFile(filePath); err == nil {
		_ = json.Unmarshal(data, &creds)
	}

	key := fmt.Sprintf("%s:%s", service, account)
	creds[key] = secret

	data, err := json.MarshalIndent(creds, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal credentials: %w", err)
	}

	return os.WriteFile(filePath, data, 0o600)
}

func getFileCredential(service, account string) (string, error) {
	filePath, err := getCredentialsFilePath()
	if err != nil {
		return "", err
	}

	data, err := os.ReadFile(filePath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "", ErrCredentialNotFound
		}
		return "", err
	}

	// Validate permissions on POSIX
	if runtime.GOOS != "windows" {
		if fi, err := os.Stat(filePath); err == nil {
			if fi.Mode().Perm()&0o077 != 0 {
				_ = os.Chmod(filePath, 0o600)
			}
		}
	}

	var creds map[string]string
	if err := json.Unmarshal(data, &creds); err != nil {
		return "", fmt.Errorf("unmarshal credentials: %w", err)
	}

	key := fmt.Sprintf("%s:%s", service, account)
	val, ok := creds[key]
	if !ok || val == "" {
		return "", ErrCredentialNotFound
	}

	return val, nil
}

func deleteFileCredential(service, account string) error {
	filePath, err := getCredentialsFilePath()
	if err != nil {
		return err
	}

	data, err := os.ReadFile(filePath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}

	var creds map[string]string
	if err := json.Unmarshal(data, &creds); err != nil {
		return nil
	}

	key := fmt.Sprintf("%s:%s", service, account)
	delete(creds, key)

	updated, err := json.MarshalIndent(creds, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(filePath, updated, 0o600)
}
