package auth

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"strings"
	"time"
)

const (
	// SessionCookieName is the exact __Host- cookie mandated by Blueprint §24.1 for secure hosted mode
	SessionCookieName = "__Host-runtime_session"

	// CSRFCookieName is the readable __Host- cookie for SPA CSRF token bootstrap in secure hosted mode
	CSRFCookieName = "__Host-csrf_token"

	// LocalSessionCookieName is used when CookieSecure=false in local development (since browsers reject non-secure __Host-)
	LocalSessionCookieName = "deadbolt_local_session"

	// LocalCSRFCookieName is used when CookieSecure=false in local development
	LocalCSRFCookieName = "deadbolt_local_csrf"

	// DefaultSessionIdleTimeout is 12 hours (Blueprint §24.1)
	DefaultSessionIdleTimeout = 12 * time.Hour

	// DefaultSessionAbsoluteTimeout is 7 days (Blueprint §24.1)
	DefaultSessionAbsoluteTimeout = 7 * 24 * time.Hour

	// ModeHosted represents production/staging hosted control plane
	ModeHosted = "hosted"

	// ModeLocal represents local workstation development
	ModeLocal = "local"
)

// OIDCConfig defines configuration for standard OpenID Connect provider
type OIDCConfig struct {
	Issuer       string
	ClientID     string
	ClientSecret string
	RedirectURL  string
	CLIClientID  string
}

// Config represents authentication and Go BFF configuration
type Config struct {
	RuntimeMode            string
	OIDC                   OIDCConfig
	AllowedOrigins         []string
	DevAuthEnabled         bool
	ContainerLocal         bool   // Explicitly permits container-local binding (e.g. Docker Compose) in local mode
	DevKey                 string // Explicit development key for local workstation mode (Blueprint §24.4)
	DevKeyPath             string // Path to gitignored local development key secret file (Blueprint §24.4)
	CookieSecure           bool
	SessionIdleTimeout     time.Duration
	SessionAbsoluteTimeout time.Duration
}

// SessionCookieName returns the appropriate session cookie name based on CookieSecure
func (c *Config) SessionCookieName() string {
	if c.CookieSecure {
		return SessionCookieName
	}
	return LocalSessionCookieName
}

// CSRFCookieName returns the appropriate CSRF cookie name based on CookieSecure
func (c *Config) CSRFCookieName() string {
	if c.CookieSecure {
		return CSRFCookieName
	}
	return LocalCSRFCookieName
}

// DefaultConfig returns default authentication configuration
func DefaultConfig() Config {
	return Config{
		RuntimeMode:            ModeHosted,
		CookieSecure:           true,
		SessionIdleTimeout:     DefaultSessionIdleTimeout,
		SessionAbsoluteTimeout: DefaultSessionAbsoluteTimeout,
	}
}

// Validate verifies configuration boundaries according to Blueprint §22.2, §24.1 & §24.4.
// In hosted mode, dev auth, development keys, and container-local modes are strictly rejected, and CookieSecure must be true.
// In local mode, dev auth is permitted when bound to loopback or when explicitly configured as ContainerLocal.
func (c *Config) Validate(listenHost string) error {
	if c.RuntimeMode == "" {
		return errors.New("RuntimeMode must be configured ('hosted' or 'local')")
	}

	if c.SessionIdleTimeout <= 0 {
		c.SessionIdleTimeout = DefaultSessionIdleTimeout
	}
	if c.SessionAbsoluteTimeout <= 0 {
		c.SessionAbsoluteTimeout = DefaultSessionAbsoluteTimeout
	}

	if c.RuntimeMode == ModeHosted {
		// Blueprint §24.1: hosted mode strictly requires Secure cookies for __Host- enforcement
		if !c.CookieSecure {
			return errors.New("hosted mode requires CookieSecure=true to enforce __Host- cookie policy")
		}
		// Blueprint §22.2 & §24.4: "hosted-mode startup rejects dev auth and development keys"
		if c.DevAuthEnabled {
			return errors.New("hosted startup rejects dev auth and development keys")
		}
		if c.ContainerLocal || os.Getenv("DEADBOLT_CONTAINER_LOCAL") == "true" {
			return errors.New("hosted startup rejects ContainerLocal configuration")
		}
		if c.DevKey != "" || c.DevKeyPath != "" || os.Getenv("DEADBOLT_DEV_KEY") != "" || os.Getenv("DEADBOLT_DEV_KEY_PATH") != "" {
			return errors.New("hosted startup rejects dev auth and development keys")
		}
		if c.OIDC.Issuer == "" {
			return errors.New("DEADBOLT_OIDC_ISSUER is required in hosted mode")
		}
		if c.OIDC.ClientID == "" {
			return errors.New("DEADBOLT_OIDC_CLIENT_ID is required in hosted mode")
		}
		if len(c.AllowedOrigins) == 0 {
			return errors.New("at least one allowed origin must be configured in hosted mode")
		}
		return nil
	}

	if c.RuntimeMode == ModeLocal {
		if c.DevAuthEnabled {
			trimmedHost := strings.TrimSpace(listenHost)
			if trimmedHost == "" {
				return errors.New("dev auth requires an explicit loopback listen host (e.g. '127.0.0.1' or 'localhost')")
			}
			if !c.ContainerLocal && !isLoopbackHost(trimmedHost) {
				return fmt.Errorf("dev auth is restricted strictly to loopback binding (got listen host %q)", listenHost)
			}
		}
		if c.DevKeyPath != "" {
			if _, err := os.Stat(c.DevKeyPath); err != nil {
				return fmt.Errorf("invalid DevKeyPath: %w", err)
			}
		}
		return nil
	}

	return fmt.Errorf("unsupported RuntimeMode %q (must be 'hosted' or 'local')", c.RuntimeMode)
}

// ReadDevKey resolves the development key from DevKey, DevKeyPath, or environment variables.
// Per Blueprint §24.4, development keys are strictly prohibited in hosted mode.
func (c *Config) ReadDevKey() (string, error) {
	if c.RuntimeMode == ModeHosted {
		return "", errors.New("development keys are strictly prohibited in hosted mode")
	}
	if c.DevKey != "" {
		return strings.TrimSpace(c.DevKey), nil
	}
	path := c.DevKeyPath
	if path == "" {
		path = os.Getenv("DEADBOLT_DEV_KEY_PATH")
	}
	if path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			return "", fmt.Errorf("failed to read development key from %q: %w", path, err)
		}
		return strings.TrimSpace(string(data)), nil
	}
	if envKey := os.Getenv("DEADBOLT_DEV_KEY"); envKey != "" {
		return strings.TrimSpace(envKey), nil
	}
	return "", nil
}

// IsOriginAllowed checks whether the specified origin is in the allowed origins list
func (c *Config) IsOriginAllowed(origin string) bool {
	if origin == "" {
		return false
	}
	parsedOrigin, err := url.Parse(origin)
	if err != nil {
		return false
	}
	normalizedOrigin := fmt.Sprintf("%s://%s", parsedOrigin.Scheme, parsedOrigin.Host)

	for _, allowed := range c.AllowedOrigins {
		allowed = strings.TrimRight(allowed, "/")
		if strings.EqualFold(normalizedOrigin, allowed) {
			return true
		}
	}
	return false
}

// isLoopbackHost verifies whether the host/IP represents loopback
func isLoopbackHost(host string) bool {
	host = strings.TrimSpace(host)
	if host == "" {
		return false
	}
	if host == "localhost" || host == "127.0.0.1" || host == "::1" {
		return true
	}

	// Try parsing host without port if present
	h, _, err := net.SplitHostPort(host)
	if err == nil {
		host = h
	}

	if host == "localhost" {
		return true
	}

	ip := net.ParseIP(host)
	if ip != nil && ip.IsLoopback() {
		return true
	}

	return false
}

// isPrivateNetworkHost verifies whether the host/IP represents a private network or loopback address
func isPrivateNetworkHost(host string) bool {
	host = strings.TrimSpace(host)
	if host == "" {
		return false
	}
	if isLoopbackHost(host) {
		return true
	}

	h, _, err := net.SplitHostPort(host)
	if err == nil {
		host = h
	}

	ip := net.ParseIP(host)
	if ip != nil && (ip.IsPrivate() || ip.IsLoopback()) {
		return true
	}

	return false
}
