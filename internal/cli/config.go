package cli

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
)

// Config represents CLI runtime configuration
type Config struct {
	APIURL string
	APIKey string
	OrgID  string
	Env    string
	JSON   bool
}

type profile struct {
	APIURL string `json:"apiURL"`
}

// ProjectConfig captures deadbolt.config.json options
type ProjectConfig struct {
	Project    string   `json:"project,omitempty"`
	Workflow   string   `json:"workflow,omitempty"`
	BundleDir  string   `json:"bundleDir,omitempty"`
	TargetArch string   `json:"targetArch,omitempty"`
	Secrets    []string `json:"secrets,omitempty"`
}

// LoadConfig resolves CLI configuration in priority order:
// 1. Environment variables
// 2. Local deadbolt.config.json
// 3. OS Keychain / stored credentials
// 4. Baseline defaults
func LoadConfig() Config {
	apiURL := os.Getenv("DEADBOLT_API_URL")
	if apiURL == "" {
		if saved, err := loadProfile(); err == nil {
			apiURL = saved.APIURL
		}
	}
	if apiURL == "" {
		apiURL = "http://localhost:8080"
	}
	apiURL = strings.TrimRight(apiURL, "/")

	apiKey := os.Getenv("DEADBOLT_API_KEY")
	orgID := os.Getenv("DEADBOLT_ORG_ID")
	env := os.Getenv("DEADBOLT_ENV")

	// If missing, check local project config
	if prj, err := LoadProjectConfig("."); err == nil && prj != nil {
		if env == "" && prj.Workflow != "" {
			// keep env check fallback
		}
	}

	// If still missing, check OS keychain / stored credentials
	if apiKey == "" {
		if storedKey, err := GetCredential("deadbolt", "api_key"); err == nil && storedKey != "" {
			apiKey = storedKey
		} else if storedToken, err := GetCredential("deadbolt", "session_token"); err == nil && storedToken != "" {
			apiKey = storedToken
		}
	}
	if orgID == "" {
		if storedOrg, err := GetCredential("deadbolt", "org_id"); err == nil && storedOrg != "" {
			orgID = storedOrg
		}
	}
	if env == "" {
		if storedEnv, err := GetCredential("deadbolt", "env"); err == nil && storedEnv != "" {
			env = storedEnv
		} else {
			env = "development"
		}
	}

	return Config{
		APIURL: apiURL,
		APIKey: apiKey,
		OrgID:  orgID,
		Env:    env,
	}
}

// StoreControlPlaneURL persists the non-secret endpoint selected at login. Tokens
// remain exclusively in the platform credential store.
func StoreControlPlaneURL(rawURL string) error {
	u, err := url.ParseRequestURI(rawURL)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return fmt.Errorf("invalid control plane URL %q", rawURL)
	}
	dir, err := getCredentialsDir()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("create CLI profile directory: %w", err)
	}
	b, err := json.Marshal(profile{APIURL: strings.TrimRight(rawURL, "/")})
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, "config.json"), b, 0o600)
}

func loadProfile() (profile, error) {
	dir, err := getCredentialsDir()
	if err != nil {
		return profile{}, err
	}
	b, err := os.ReadFile(filepath.Join(dir, "config.json"))
	if err != nil {
		return profile{}, err
	}
	var p profile
	if err := json.Unmarshal(b, &p); err != nil {
		return profile{}, err
	}
	return p, nil
}

// LoadProjectConfig reads deadbolt.config.json if present
func LoadProjectConfig(dir string) (*ProjectConfig, error) {
	configPath := filepath.Join(dir, "deadbolt.config.json")
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, err
	}

	var prj ProjectConfig
	if err := json.Unmarshal(data, &prj); err != nil {
		return nil, fmt.Errorf("invalid deadbolt.config.json: %w", err)
	}

	return &prj, nil
}

// NewRequest creates an authenticated HTTP request with canonical headers
func (c *Config) NewRequest(method, path string, body io.Reader) (*http.Request, error) {
	reqURL := fmt.Sprintf("%s%s", c.APIURL, path)
	req, err := http.NewRequest(method, reqURL, body)
	if err != nil {
		return nil, err
	}
	if c.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.APIKey)
	}
	if c.OrgID != "" {
		req.Header.Set("X-Organization-ID", c.OrgID)
	}
	req.Header.Set("Accept", "application/json")
	return req, nil
}

// FormatAPIError formats a control plane error response with actionable context
func FormatAPIError(statusCode int, body []byte) string {
	var env struct {
		Code      string         `json:"code"`
		Message   string         `json:"message"`
		RequestID string         `json:"requestId"`
		Details   map[string]any `json:"details"`
	}
	if err := json.Unmarshal(body, &env); err == nil && env.Code != "" {
		return fmt.Sprintf("API Error (%d [%s]): %s (Request ID: %s)", statusCode, env.Code, env.Message, env.RequestID)
	}
	return fmt.Sprintf("API Error (%d): %s", statusCode, strings.TrimSpace(string(body)))
}
