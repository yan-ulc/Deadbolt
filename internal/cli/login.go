package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"time"

	"github.com/Ryanakml/Deadbolt/internal/auth"
)

// RunLogin handles "runtime login [--local] [--api-key <key>] [--org <org_id>] [--env <env>] [--control-plane-url <url>]"
func RunLogin(args []string) error {
	fs := flag.NewFlagSet("login", flag.ContinueOnError)
	localFlag := fs.Bool("local", false, "Use local developer authentication (loopback only)")
	apiKeyFlag := fs.String("api-key", "", "Directly configure an API key into secure credentials store")
	orgFlag := fs.String("org", "", "Set default organization ID")
	envFlag := fs.String("env", "", "Set default environment (e.g. development, staging, production)")
	cpURLFlag := fs.String("control-plane-url", "", "Control plane base URL (defaults to DEADBOLT_API_URL or http://localhost:8080)")
	emailFlag := fs.String("email", "dev-admin@deadbolt.local", "Email address for local developer session")

	if err := fs.Parse(args); err != nil {
		return err
	}

	cfg := LoadConfig()
	if *cpURLFlag != "" {
		cfg.APIURL = strings.TrimRight(*cpURLFlag, "/")
	}

	// 1. Direct API key provisioning
	if *apiKeyFlag != "" {
		if err := StoreCredential("deadbolt", "api_key", *apiKeyFlag); err != nil {
			return fmt.Errorf("failed to save API key to secure credentials store: %w", err)
		}
		if *orgFlag != "" {
			_ = StoreCredential("deadbolt", "org_id", *orgFlag)
		}
		if *envFlag != "" {
			_ = StoreCredential("deadbolt", "env", *envFlag)
		}
		fmt.Println("✓ API key securely saved to credentials store.")
		if *orgFlag != "" {
			fmt.Printf("  Default Organization: %s\n", *orgFlag)
		}
		if *envFlag != "" {
			fmt.Printf("  Default Environment:  %s\n", *envFlag)
		}
		return nil
	}

	// Check if local dev mode
	isLocal := *localFlag || isLocalURL(cfg.APIURL)

	// 2. CI / Non-interactive check (Blueprint §22 / Issue #15)
	if !isLocal && isCIOrNonInteractive() {
		return fmt.Errorf("interactive browser login is not supported in non-interactive / CI environments. Configure the DEADBOLT_API_KEY environment variable or pass --api-key <key>")
	}

	if isLocal {
		return runLocalDevLogin(cfg, *emailFlag, *orgFlag, *envFlag)
	}

	return runHostedBrowserLogin(cfg, *orgFlag, *envFlag)
}

func isCIOrNonInteractive() bool {
	if os.Getenv("CI") != "" || os.Getenv("CONTINUOUS_INTEGRATION") != "" || os.Getenv("GITHUB_ACTIONS") != "" {
		return true
	}
	// Check if stdin is a character device
	fileInfo, err := os.Stdin.Stat()
	if err != nil {
		return false
	}
	return (fileInfo.Mode() & os.ModeCharDevice) == 0
}

func isLocalURL(rawURL string) bool {
	u, err := url.Parse(rawURL)
	if err != nil {
		return false
	}
	host := u.Hostname()
	return host == "localhost" || host == "127.0.0.1" || host == "::1" || strings.HasPrefix(host, "127.")
}

// runLocalDevLogin handles local dev login via /api/auth/dev-login and bootstraps local dev entities via public HTTP endpoints
func runLocalDevLogin(cfg Config, email string, customOrg string, customEnv string) error {
	fmt.Printf("Authenticating with local Deadbolt control plane at %s...\n", cfg.APIURL)

	loginURL := fmt.Sprintf("%s/api/auth/dev-login", cfg.APIURL)
	reqBody, _ := json.Marshal(map[string]string{
		"email": email,
		"name":  "Local Development Admin",
	})

	httpReq, err := http.NewRequest("POST", loginURL, bytes.NewReader(reqBody))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Origin", cfg.APIURL)

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(httpReq)
	if err != nil {
		return fmt.Errorf("failed to connect to local control plane at %s: %w. Ensure control plane is running", cfg.APIURL, err)
	}
	defer resp.Body.Close()

	bodyBytes, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("local dev login failed: %s", FormatAPIError(resp.StatusCode, bodyBytes))
	}

	// Extract session cookie & CSRF token
	var sessionCookie string
	for _, c := range resp.Cookies() {
		if strings.Contains(c.Name, "deadbolt_session") {
			sessionCookie = c.Value
		}
	}

	var devResp struct {
		Mode                 string  `json:"mode"`
		SessionID            string  `json:"session_id"`
		ActiveOrganizationID *string `json:"active_organization_id"`
		CSRFToken            string  `json:"csrf_token"`
	}
	if err := json.Unmarshal(bodyBytes, &devResp); err != nil {
		return fmt.Errorf("failed to parse dev login response: %w", err)
	}

	if sessionCookie != "" {
		_ = StoreCredential("deadbolt", "session_token", sessionCookie)
	}
	if devResp.CSRFToken != "" {
		_ = StoreCredential("deadbolt", "csrf_token", devResp.CSRFToken)
	}

	// Helper for authenticated requests using session cookie & CSRF
	doAuthReq := func(method, path string, payload any) (*http.Response, []byte, error) {
		var bodyReader io.Reader
		if payload != nil {
			data, _ := json.Marshal(payload)
			bodyReader = bytes.NewReader(data)
		}
		r, err := http.NewRequest(method, cfg.APIURL+path, bodyReader)
		if err != nil {
			return nil, nil, err
		}
		if sessionCookie != "" {
			r.AddCookie(&http.Cookie{Name: "deadbolt_session", Value: sessionCookie})
		}
		if devResp.CSRFToken != "" {
			r.Header.Set("X-CSRF-Token", devResp.CSRFToken)
		}
		r.Header.Set("Origin", cfg.APIURL)
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("Accept", "application/json")

		res, err := client.Do(r)
		if err != nil {
			return nil, nil, err
		}
		defer res.Body.Close()
		b, err := io.ReadAll(res.Body)
		return res, b, err
	}

	orgID := ""
	if devResp.ActiveOrganizationID != nil && *devResp.ActiveOrganizationID != "" {
		orgID = *devResp.ActiveOrganizationID
	}
	if customOrg != "" {
		orgID = customOrg
	}

	// If user has no active organization, create one
	if orgID == "" {
		res, resBody, err := doAuthReq("POST", "/organizations", map[string]string{"name": "Local Development Org"})
		if err == nil && (res.StatusCode == http.StatusOK || res.StatusCode == http.StatusCreated) {
			var newOrg struct {
				ID string `json:"id"`
			}
			if err := json.Unmarshal(resBody, &newOrg); err == nil && newOrg.ID != "" {
				orgID = newOrg.ID
				// Switch session to new org
				_, _, _ = doAuthReq("POST", "/api/auth/switch-org", map[string]string{"organization_id": orgID})
			}
		}
	}

	targetEnv := "development"
	if customEnv != "" {
		targetEnv = customEnv
	}

	// Try to ensure project, environment, and an API key for CLI operations
	if orgID != "" {
		_ = StoreCredential("deadbolt", "org_id", orgID)

		// List or create project
		var projectID string
		res, resBody, err := doAuthReq("GET", "/projects", nil)
		if err == nil && res.StatusCode == http.StatusOK {
			var prjList struct {
				Projects []struct {
					ID   string `json:"id"`
					Name string `json:"name"`
				} `json:"projects"`
			}
			if err := json.Unmarshal(resBody, &prjList); err == nil && len(prjList.Projects) > 0 {
				projectID = prjList.Projects[0].ID
			}
		}

		if projectID == "" {
			res, resBody, err := doAuthReq("POST", "/projects", map[string]string{"name": "default"})
			if err == nil && (res.StatusCode == http.StatusOK || res.StatusCode == http.StatusCreated) {
				var newPrj struct {
					ID string `json:"id"`
				}
				if err := json.Unmarshal(resBody, &newPrj); err == nil {
					projectID = newPrj.ID
				}
			}
		}

		if projectID != "" {
			// Find or create environment
			var envID string
			res, resBody, err := doAuthReq("GET", fmt.Sprintf("/projects/%s/environments", projectID), nil)
			if err == nil && res.StatusCode == http.StatusOK {
				var envList struct {
					Environments []struct {
						ID   string `json:"id"`
						Name string `json:"name"`
					} `json:"environments"`
				}
				if err := json.Unmarshal(resBody, &envList); err == nil {
					for _, e := range envList.Environments {
						if e.Name == targetEnv {
							envID = e.ID
							break
						}
					}
				}
			}

			if envID == "" {
				res, resBody, err := doAuthReq("POST", fmt.Sprintf("/projects/%s/environments", projectID), map[string]any{
					"name":            targetEnv,
					"max_concurrency": 10,
				})
				if err == nil && (res.StatusCode == http.StatusOK || res.StatusCode == http.StatusCreated) {
					var newEnv struct {
						ID string `json:"id"`
					}
					if err := json.Unmarshal(resBody, &newEnv); err == nil {
						envID = newEnv.ID
					}
				}
			}

			// Generate an API key for CLI commands if env exists
			if envID != "" {
				res, resBody, err := doAuthReq("POST", fmt.Sprintf("/environments/%s/api-keys", envID), map[string]any{
					"capabilities": []string{"*"},
					"expiry_days":  365,
				})
				if err == nil && (res.StatusCode == http.StatusOK || res.StatusCode == http.StatusCreated) {
					var keyResp struct {
						PlaintextKey string `json:"plaintext_key"`
					}
					if err := json.Unmarshal(resBody, &keyResp); err == nil && keyResp.PlaintextKey != "" {
						_ = StoreCredential("deadbolt", "api_key", keyResp.PlaintextKey)
					}
				}
			}
		}
	}

	_ = StoreCredential("deadbolt", "env", targetEnv)

	fmt.Println("✓ Successfully authenticated to local Deadbolt environment.")
	fmt.Printf("  User:         %s\n", email)
	if orgID != "" {
		fmt.Printf("  Organization: %s\n", orgID)
	}
	fmt.Printf("  Environment:  %s\n", targetEnv)
	fmt.Println("  Credentials stored securely in OS credentials store.")
	return nil
}

// runHostedBrowserLogin executes PKCE authorization code flow with loopback redirect
func runHostedBrowserLogin(cfg Config, customOrg string, customEnv string) error {
	pkce, err := auth.GeneratePKCE()
	if err != nil {
		return fmt.Errorf("failed to initialize PKCE security parameters: %w", err)
	}

	// Bind loopback listener on random available port
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return fmt.Errorf("failed to start local callback server: %w", err)
	}
	defer listener.Close()

	port := listener.Addr().(*net.TCPAddr).Port
	redirectURI := fmt.Sprintf("http://127.0.0.1:%d/callback", port)

	authURL := fmt.Sprintf("%s/api/auth/login?response_type=code&client_id=deadbolt-cli&redirect_uri=%s&code_challenge=%s&code_challenge_method=%s&state=%s",
		cfg.APIURL,
		url.QueryEscape(redirectURI),
		url.QueryEscape(pkce.CodeChallenge),
		url.QueryEscape(pkce.CodeChallengeMethod),
		url.QueryEscape(pkce.State),
	)

	fmt.Println("Opening your browser for Deadbolt authentication...")
	fmt.Printf("If the browser does not open automatically, visit:\n\n  %s\n\n", authURL)

	// Attempt to open browser
	_ = openBrowser(authURL)

	codeChan := make(chan string, 1)
	errChan := make(chan error, 1)

	server := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/callback" {
				http.NotFound(w, r)
				return
			}

			q := r.URL.Query()
			state := q.Get("state")
			if state != pkce.State {
				errChan <- fmt.Errorf("state parameter mismatch; possible CSRF attack")
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write([]byte("Authentication failed: state parameter mismatch."))
				return
			}

			code := q.Get("code")
			if code == "" {
				errChan <- fmt.Errorf("no authorization code returned from identity provider")
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write([]byte("Authentication failed: missing authorization code."))
				return
			}

			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`<!DOCTYPE html><html><body style="font-family:sans-serif;text-align:center;padding-top:50px;">
<h2>Authentication Successful</h2>
<p>You may close this tab and return to your terminal.</p>
</body></html>`))

			codeChan <- code
		}),
	}

	go func() {
		if err := server.Serve(listener); err != nil && err != http.ErrServerClosed {
			errChan <- err
		}
	}()

	var code string
	select {
	case code = <-codeChan:
		// Succeeded
	case err := <-errChan:
		return err
	case <-time.After(2 * time.Minute):
		return fmt.Errorf("authentication timed out waiting for browser callback (2 minutes)")
	}

	_ = server.Shutdown(context.Background())

	// Exchange code for session / token
	tokenURL := fmt.Sprintf("%s/api/auth/token", cfg.APIURL)
	tokenData := url.Values{}
	tokenData.Set("grant_type", "authorization_code")
	tokenData.Set("code", code)
	tokenData.Set("code_verifier", pkce.CodeVerifier)
	tokenData.Set("redirect_uri", redirectURI)
	tokenData.Set("client_id", "deadbolt-cli")

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.PostForm(tokenURL, tokenData)
	if err != nil {
		return fmt.Errorf("failed to exchange authorization code: %w", err)
	}
	defer resp.Body.Close()

	respBytes, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("token exchange failed: %s", FormatAPIError(resp.StatusCode, respBytes))
	}

	var tokenResp struct {
		AccessToken    string `json:"access_token"`
		APIKey         string `json:"api_key"`
		OrganizationID string `json:"organization_id"`
		Environment    string `json:"environment"`
	}
	if err := json.Unmarshal(respBytes, &tokenResp); err != nil {
		return fmt.Errorf("failed to parse token exchange response: %w", err)
	}

	tokenToStore := tokenResp.APIKey
	if tokenToStore == "" {
		tokenToStore = tokenResp.AccessToken
	}
	if tokenToStore != "" {
		_ = StoreCredential("deadbolt", "api_key", tokenToStore)
	}

	orgID := tokenResp.OrganizationID
	if customOrg != "" {
		orgID = customOrg
	}
	if orgID != "" {
		_ = StoreCredential("deadbolt", "org_id", orgID)
	}

	envName := tokenResp.Environment
	if customEnv != "" {
		envName = customEnv
	}
	if envName != "" {
		_ = StoreCredential("deadbolt", "env", envName)
	}

	fmt.Println("✓ Successfully authenticated via browser.")
	if orgID != "" {
		fmt.Printf("  Organization: %s\n", orgID)
	}
	if envName != "" {
		fmt.Printf("  Environment:  %s\n", envName)
	}
	fmt.Println("  Credentials saved securely in OS credentials store.")

	return nil
}

func openBrowser(targetURL string) error {
	var cmd *exec.Cmd
	switch runtime.GOOS {
	case "darwin":
		cmd = exec.Command("open", targetURL)
	case "windows":
		cmd = exec.Command("cmd", "/c", "start", targetURL)
	default:
		cmd = exec.Command("xdg-open", targetURL)
	}
	return cmd.Start()
}
