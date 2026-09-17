package auth

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/Ryanakml/Deadbolt/internal/storage"
	"github.com/jackc/pgx/v5/pgxpool"
)

type contextKey string

const (
	SessionContextKey contextKey = "deadbolt.auth.session"
	PKCECookieName               = "__Host-deadbolt_pkce"
)

// SessionFromContext extracts the authenticated Session from request context
func SessionFromContext(ctx context.Context) (*Session, bool) {
	s, ok := ctx.Value(SessionContextKey).(*Session)
	return s, ok
}

// ContextWithSession stores the authenticated Session in request context
func ContextWithSession(ctx context.Context, s *Session) context.Context {
	return context.WithValue(ctx, SessionContextKey, s)
}

// BFFHandler manages the Go BFF auth routes and middleware
type BFFHandler struct {
	cfg    Config
	oidc   *OIDCClient
	store  *SessionStore
	pool   *pgxpool.Pool
	logger *log.Logger
}

// NewBFFHandler initializes a new BFFHandler
func NewBFFHandler(cfg Config, oidc *OIDCClient, store *SessionStore, pool *pgxpool.Pool) *BFFHandler {
	if cfg.SessionIdleTimeout <= 0 {
		cfg.SessionIdleTimeout = DefaultSessionIdleTimeout
	}
	if cfg.SessionAbsoluteTimeout <= 0 {
		cfg.SessionAbsoluteTimeout = DefaultSessionAbsoluteTimeout
	}
	return &BFFHandler{
		cfg:   cfg,
		oidc:  oidc,
		store: store,
		pool:  pool,
	}
}

// SetLogger attaches a logger for security event auditing
func (h *BFFHandler) SetLogger(l *log.Logger) {
	h.logger = l
}

func (h *BFFHandler) logSecurityEvent(event string, r *http.Request, reason SecurityReason) {
	LogSecurityEvent(h.logger, event, r, reason)
}

// SetSessionCookies sets both the HttpOnly session cookie and the readable CSRF bootstrap cookie
func (h *BFFHandler) SetSessionCookies(w http.ResponseWriter, rawSessionToken, rawCSRFToken string, maxAge int) {
	// 1. Session cookie: HttpOnly, Secure (in hosted mode), SameSite=Lax
	http.SetCookie(w, &http.Cookie{
		Name:     h.cfg.SessionCookieName(),
		Value:    rawSessionToken,
		Path:     "/",
		MaxAge:   maxAge,
		HttpOnly: true,
		Secure:   h.cfg.CookieSecure,
		SameSite: http.SameSiteLaxMode,
	})

	// 2. CSRF cookie: readable by SPA JS (HttpOnly: false), Secure (in hosted mode), SameSite=Lax
	if rawCSRFToken != "" {
		http.SetCookie(w, &http.Cookie{
			Name:     h.cfg.CSRFCookieName(),
			Value:    rawCSRFToken,
			Path:     "/",
			MaxAge:   maxAge,
			HttpOnly: false,
			Secure:   h.cfg.CookieSecure,
			SameSite: http.SameSiteLaxMode,
		})
	}
}

// SetSessionCookie is a backwards-compatible helper setting the session cookie
func (h *BFFHandler) SetSessionCookie(w http.ResponseWriter, rawSessionToken string, maxAge int) {
	h.SetSessionCookies(w, rawSessionToken, "", maxAge)
}

// Routes returns an http.Handler with all auth endpoints wired with CORS and appropriate security middleware
func (h *BFFHandler) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/auth/login", h.HandleLogin)
	mux.HandleFunc("/api/auth/callback", h.HandleCallback)
	mux.HandleFunc("/api/auth/cli/config", h.HandleCLIConfig)
	mux.HandleFunc("/api/auth/cli/token", h.HandleCLIToken)
	mux.Handle("/api/auth/session", h.RequireAuth(http.HandlerFunc(h.HandleGetSession)))
	mux.Handle("/api/auth/switch-org", h.RequireAuth(h.RequireCSRFAndOrigin(http.HandlerFunc(h.HandleSwitchOrg))))
	mux.Handle("/api/auth/logout", h.RequireAuth(h.RequireCSRFAndOrigin(http.HandlerFunc(h.HandleLogout))))

	return h.CORSMiddleware(mux)
}

// HandleCLIConfig exposes only public OAuth metadata for the separately
// registered native CLI client. Its absence must never affect dashboard BFF.
func (h *BFFHandler) HandleCLIConfig(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		WriteSanitizedError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "Method not allowed")
		return
	}
	if h.cfg.OIDC.CLIClientID == "" {
		WriteSanitizedError(w, http.StatusServiceUnavailable, "CLI_OIDC_NOT_CONFIGURED", "Hosted CLI login is not configured")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"issuer": h.cfg.OIDC.Issuer, "clientId": h.cfg.OIDC.CLIClientID})
}

// HandleCLIToken exchanges a PKCE-bound public-client code and returns a
// namespaced human CLI bearer session, never a browser cookie or API key.
func (h *BFFHandler) HandleCLIToken(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		WriteSanitizedError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "Method not allowed")
		return
	}
	if h.cfg.OIDC.CLIClientID == "" {
		WriteSanitizedError(w, http.StatusServiceUnavailable, "CLI_OIDC_NOT_CONFIGURED", "Hosted CLI login is not configured")
		return
	}
	var in struct {
		Code         string `json:"code"`
		CodeVerifier string `json:"code_verifier"`
		RedirectURI  string `json:"redirect_uri"`
		Nonce        string `json:"nonce"`
	}
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil || in.Code == "" || in.CodeVerifier == "" || in.Nonce == "" || !validCLILoopbackRedirect(in.RedirectURI) {
		WriteSanitizedError(w, http.StatusBadRequest, "INVALID_CLI_OIDC_REQUEST", "A PKCE code, verifier, nonce, and loopback redirect URI are required")
		return
	}
	identity, err := h.oidc.ExchangePublicCode(r.Context(), in.Code, in.CodeVerifier, in.RedirectURI, in.Nonce, h.cfg.OIDC.CLIClientID)
	if err != nil {
		h.logSecurityEvent("CLI_OIDC_EXCHANGE_FAILED", r, ReasonTokenVerificationFailed)
		WriteSanitizedError(w, http.StatusUnauthorized, "OIDC_VERIFICATION_FAILED", "CLI authorization code exchange failed")
		return
	}
	user, err := h.store.GetOrCreateUserFromOIDC(r.Context(), identity)
	if err != nil {
		WriteSanitizedError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to persist user identity")
		return
	}
	memberships, err := storage.DiscoverUserMemberships(r.Context(), h.pool, user.ID)
	if err != nil {
		WriteSanitizedError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to discover user memberships")
		return
	}
	var orgID *string
	if len(memberships) > 0 {
		orgID = &memberships[0].OrganizationID
	}
	_, token, err := h.store.CreateCLISession(r.Context(), user.ID, orgID, r.RemoteAddr, r.UserAgent(), h.cfg.SessionIdleTimeout, h.cfg.SessionAbsoluteTimeout)
	if err != nil {
		WriteSanitizedError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to create CLI session")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"access_token": token, "token_type": "Bearer", "organization_id": valueOrEmpty(orgID)})
}

func valueOrEmpty(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func validCLILoopbackRedirect(raw string) bool {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "http" || u.Path != "/callback" || u.RawQuery != "" || u.Fragment != "" {
		return false
	}
	host, port, err := net.SplitHostPort(u.Host)
	if err != nil || host != "127.0.0.1" {
		return false
	}
	p, err := strconv.Atoi(port)
	return err == nil && p >= 1024 && p <= 65535
}

// CORSMiddleware enforces strict allowlisted CORS for cross-origin requests
func (h *BFFHandler) CORSMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		if origin == "" {
			next.ServeHTTP(w, r)
			return
		}

		if !h.cfg.IsOriginAllowed(origin) {
			if r.Method == http.MethodOptions {
				h.logSecurityEvent("CORS_PREFLIGHT_FORBIDDEN", r, ReasonOriginNotAllowlisted)
				WriteSanitizedError(w, http.StatusForbidden, "ORIGIN_FORBIDDEN", fmt.Sprintf("Origin %q is not allowlisted", origin))
				return
			}
			// For non-OPTIONS requests with untrusted origin, pass through without CORS headers
			next.ServeHTTP(w, r)
			return
		}

		// Allowlisted origin: attach strict CORS headers (never wildcard *)
		w.Header().Set("Access-Control-Allow-Origin", origin)
		w.Header().Set("Access-Control-Allow-Credentials", "true")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, PATCH, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, X-CSRF-Token, Idempotency-Key")
		w.Header().Set("Access-Control-Expose-Headers", "X-CSRF-Token")
		w.Header().Set("Access-Control-Max-Age", "86400")

		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}

		next.ServeHTTP(w, r)
	})
}

// HandleLogin initiates the OIDC authorization code flow with PKCE
func (h *BFFHandler) HandleLogin(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		WriteSanitizedError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "Method not allowed")
		return
	}

	pkce, err := GeneratePKCE()
	if err != nil {
		h.logSecurityEvent("LOGIN_INIT_FAILED", r, ReasonPKCEGenerationFailed)
		WriteSanitizedError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to initialize auth request")
		return
	}

	// Store PKCE in temporary short-lived HttpOnly cookie (10 minutes)
	pkcePayload, _ := json.Marshal(pkce)
	encodedPKCE := base64.RawURLEncoding.EncodeToString(pkcePayload)
	http.SetCookie(w, &http.Cookie{
		Name:     PKCECookieName,
		Value:    encodedPKCE,
		Path:     "/",
		MaxAge:   600,
		HttpOnly: true,
		Secure:   h.cfg.CookieSecure,
		SameSite: http.SameSiteLaxMode,
	})

	authURL, err := h.oidc.BuildAuthorizationURL(r.Context(), pkce)
	if err != nil {
		h.logSecurityEvent("OIDC_AUTH_URL_FAILED", r, ReasonAuthURLBuildFailed)
		WriteSanitizedError(w, http.StatusInternalServerError, "OIDC_ERROR", "Failed to build authorization URL")
		return
	}

	http.Redirect(w, r, authURL, http.StatusFound)
}

// HandleCallback processes the OIDC authorization callback
func (h *BFFHandler) HandleCallback(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		WriteSanitizedError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "Method not allowed")
		return
	}

	code := r.URL.Query().Get("code")
	state := r.URL.Query().Get("state")
	if code == "" || state == "" {
		h.logSecurityEvent("CALLBACK_INVALID_PARAMS", r, ReasonMissingCodeOrState)
		WriteSanitizedError(w, http.StatusBadRequest, "INVALID_REQUEST", "Missing code or state parameter")
		return
	}

	// Retrieve and clear PKCE cookie
	pkceCookie, err := r.Cookie(PKCECookieName)
	if err != nil || pkceCookie.Value == "" {
		h.logSecurityEvent("CALLBACK_PKCE_MISSING", r, ReasonPKCEMissingOrExpired)
		WriteSanitizedError(w, http.StatusBadRequest, "OIDC_STATE_MISMATCH", "Missing or expired PKCE state")
		return
	}
	// Clear PKCE cookie
	http.SetCookie(w, &http.Cookie{
		Name:     PKCECookieName,
		Value:    "",
		Path:     "/",
		MaxAge:   -1,
		HttpOnly: true,
		Secure:   h.cfg.CookieSecure,
		SameSite: http.SameSiteLaxMode,
	})

	rawPKCE, err := base64.RawURLEncoding.DecodeString(pkceCookie.Value)
	if err != nil {
		h.logSecurityEvent("CALLBACK_PKCE_DECODE_FAILED", r, ReasonPKCEDecodeFailed)
		WriteSanitizedError(w, http.StatusBadRequest, "OIDC_STATE_MISMATCH", "Invalid authorization cookie format")
		return
	}

	var pkce PKCEParams
	if err := json.Unmarshal(rawPKCE, &pkce); err != nil || pkce.State != state {
		h.logSecurityEvent("CALLBACK_STATE_MISMATCH", r, ReasonPKCEStateMismatch)
		WriteSanitizedError(w, http.StatusBadRequest, "OIDC_STATE_MISMATCH", "Invalid authorization state")
		return
	}

	// Exchange code + verifier with OIDC provider
	identity, err := h.oidc.ExchangeAndVerify(r.Context(), code, pkce.CodeVerifier, pkce.Nonce)
	if err != nil {
		h.logSecurityEvent("OIDC_EXCHANGE_FAILED", r, ReasonTokenVerificationFailed)
		WriteSanitizedError(w, http.StatusUnauthorized, "OIDC_VERIFICATION_FAILED", "Identity token verification failed")
		return
	}

	// Upsert user and oidc_identities
	user, err := h.store.GetOrCreateUserFromOIDC(r.Context(), identity)
	if err != nil {
		h.logSecurityEvent("USER_UPSERT_FAILED", r, ReasonUserPersistenceFailed)
		WriteSanitizedError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to persist user identity")
		return
	}

	// Discover user memberships across organizations
	memberships, err := storage.DiscoverUserMemberships(r.Context(), h.pool, user.ID)
	if err != nil {
		h.logSecurityEvent("MEMBERSHIP_DISCOVERY_FAILED", r, ReasonMembershipDiscoveryFailed)
		WriteSanitizedError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to discover user memberships")
		return
	}

	var defaultOrgID *string
	if len(memberships) > 0 {
		defaultOrgID = &memberships[0].OrganizationID
	}

	ip := r.RemoteAddr
	userAgent := r.UserAgent()

	// Create new session
	_, rawToken, rawCSRF, err := h.store.CreateSession(
		r.Context(),
		user.ID,
		defaultOrgID,
		ip,
		userAgent,
		h.cfg.SessionIdleTimeout,
		h.cfg.SessionAbsoluteTimeout,
	)
	if err != nil {
		h.logSecurityEvent("SESSION_CREATE_FAILED", r, ReasonSessionCreationFailed)
		WriteSanitizedError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to establish session")
		return
	}

	// Issue session cookie + readable CSRF cookie
	maxAge := int(h.cfg.SessionAbsoluteTimeout.Seconds())
	h.SetSessionCookies(w, rawToken, rawCSRF, maxAge)

	// In response header provide CSRF token for single-page dashboard apps
	w.Header().Set("X-CSRF-Token", rawCSRF)

	h.logSecurityEvent("LOGIN_SUCCESS", r, ReasonLoginSuccess)

	// Redirect to application root or dashboard
	http.Redirect(w, r, "/", http.StatusFound)
}

// HandleLogout terminates the user session in the database and clears session & CSRF cookies
func (h *BFFHandler) HandleLogout(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		WriteSanitizedError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "Method not allowed")
		return
	}

	sess, ok := SessionFromContext(r.Context())
	if !ok || sess == nil {
		h.logSecurityEvent("LOGOUT_UNAUTHENTICATED", r, ReasonSessionMissing)
		WriteSanitizedError(w, http.StatusUnauthorized, "UNAUTHENTICATED", "Active session required for logout")
		return
	}

	// Revoke session in DB
	if err := h.store.RevokeSession(r.Context(), sess.ID, "LOGOUT"); err != nil {
		h.logSecurityEvent("LOGOUT_REVOKE_FAILED", r, ReasonSessionRevocationFailed)
		WriteSanitizedError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to revoke session")
		return
	}

	h.logSecurityEvent("LOGOUT_SUCCESS", r, ReasonLogoutSuccess)

	// Clear session & CSRF cookies cleanly
	ClearSessionCookies(w, h.cfg)
	w.WriteHeader(http.StatusNoContent)
}

// HandleGetSession returns current authenticated user and session metadata
func (h *BFFHandler) HandleGetSession(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		WriteSanitizedError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "Method not allowed")
		return
	}

	sess, ok := SessionFromContext(r.Context())
	if !ok || sess == nil {
		WriteSanitizedError(w, http.StatusUnauthorized, "UNAUTHENTICATED", "No active session")
		return
	}

	// Discover user memberships
	memberships, err := storage.DiscoverUserMemberships(r.Context(), h.pool, sess.UserID)
	if err != nil {
		memberships = []storage.UserMembership{}
	}

	var u User
	err = h.pool.QueryRow(r.Context(), `
		SELECT id, COALESCE(email, ''), COALESCE(name, ''), created_at, updated_at
		FROM users WHERE id = $1
	`, sess.UserID).Scan(&u.ID, &u.Email, &u.Name, &u.CreatedAt, &u.UpdatedAt)
	if err != nil {
		WriteSanitizedError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to load user info")
		return
	}

	resp := map[string]interface{}{
		"user":                   u,
		"active_organization_id": sess.ActiveOrganizationID,
		"memberships":            memberships,
		"created_at":             sess.CreatedAt,
		"last_seen_at":           sess.LastSeenAt,
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

// HandleSwitchOrg rotates session upon changing active organization
func (h *BFFHandler) HandleSwitchOrg(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		WriteSanitizedError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "Method not allowed")
		return
	}

	sess, ok := SessionFromContext(r.Context())
	if !ok || sess == nil {
		WriteSanitizedError(w, http.StatusUnauthorized, "UNAUTHENTICATED", "No active session")
		return
	}

	var req struct {
		OrganizationID string `json:"organization_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.OrganizationID == "" {
		WriteSanitizedError(w, http.StatusBadRequest, "INVALID_REQUEST", "Invalid organization_id payload")
		return
	}

	// Verify user is an active member of requested organization
	memberships, err := storage.DiscoverUserMemberships(r.Context(), h.pool, sess.UserID)
	if err != nil {
		WriteSanitizedError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to verify organization membership")
		return
	}

	var targetFound bool
	for _, m := range memberships {
		if m.OrganizationID == req.OrganizationID && m.Status == "ACTIVE" {
			targetFound = true
			break
		}
	}
	if !targetFound {
		h.logSecurityEvent("ORG_SWITCH_DENIED", r, ReasonUnauthorizedOrgMembership)
		WriteSanitizedError(w, http.StatusForbidden, "FORBIDDEN_ORGANIZATION_MEMBERSHIP", "User is not an active member of the requested organization")
		return
	}

	ip := r.RemoteAddr
	userAgent := r.UserAgent()

	// Rotate session with new organization ID
	newSess, rawSessionToken, rawCSRFToken, err := h.store.RotateSession(
		r.Context(),
		sess.ID,
		sess.UserID,
		&req.OrganizationID,
		ip,
		userAgent,
		h.cfg.SessionIdleTimeout,
		h.cfg.SessionAbsoluteTimeout,
	)
	if err != nil {
		h.logSecurityEvent("SESSION_ROTATE_FAILED", r, ReasonSessionRotationFailed)
		WriteSanitizedError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to rotate session")
		return
	}

	// Set rotated cookies
	maxAge := int(h.cfg.SessionAbsoluteTimeout.Seconds())
	h.SetSessionCookies(w, rawSessionToken, rawCSRFToken, maxAge)

	h.logSecurityEvent("ORG_SWITCH_SUCCESS", r, ReasonOrgSwitchSuccess)

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-CSRF-Token", rawCSRFToken)
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"session_id":             newSess.ID,
		"active_organization_id": newSess.ActiveOrganizationID,
		"csrf_token":             rawCSRFToken,
	})
}

// RequireAuth middleware extracts the session cookie and validates against DB revocation & timeouts
func (h *BFFHandler) RequireAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cookie, err := r.Cookie(h.cfg.SessionCookieName())
		if err != nil || cookie.Value == "" {
			h.logSecurityEvent("AUTH_MISSING_COOKIE", r, ReasonCookieMissing)
			WriteSanitizedError(w, http.StatusUnauthorized, "UNAUTHENTICATED", "Missing session cookie")
			return
		}

		sess, err := h.store.ValidateSession(r.Context(), cookie.Value, h.cfg.SessionIdleTimeout)
		if err != nil {
			// Clear invalid cookies
			ClearSessionCookies(w, h.cfg)

			if errors.Is(err, ErrSessionRevoked) {
				h.logSecurityEvent("AUTH_SESSION_REVOKED", r, ReasonSessionRevoked)
				WriteSanitizedError(w, http.StatusUnauthorized, "SESSION_REVOKED", "Session has been revoked")
				return
			}
			if errors.Is(err, ErrSessionIdleTimeout) {
				h.logSecurityEvent("AUTH_SESSION_IDLE_TIMEOUT", r, ReasonSessionIdleTimeout)
				WriteSanitizedError(w, http.StatusUnauthorized, "SESSION_IDLE_TIMEOUT", "Session idle timeout exceeded")
				return
			}
			if errors.Is(err, ErrSessionExpired) {
				h.logSecurityEvent("AUTH_SESSION_EXPIRED", r, ReasonSessionAbsoluteTimeout)
				WriteSanitizedError(w, http.StatusUnauthorized, "SESSION_EXPIRED", "Session absolute expiration reached")
				return
			}
			h.logSecurityEvent("AUTH_INVALID_SESSION", r, ReasonUnrecognizedSession)
			WriteSanitizedError(w, http.StatusUnauthorized, "UNAUTHENTICATED", "Invalid session")
			return
		}

		ctx := ContextWithSession(r.Context(), sess)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// RequireCSRFAndOrigin middleware verifies allowed Origin and X-CSRF-Token on mutating requests
func (h *BFFHandler) RequireCSRFAndOrigin(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Only check mutating methods: POST, PUT, PATCH, DELETE
		method := strings.ToUpper(r.Method)
		if method == http.MethodGet || method == http.MethodHead || method == http.MethodOptions {
			next.ServeHTTP(w, r)
			return
		}

		sess, ok := SessionFromContext(r.Context())
		if !ok || sess == nil {
			h.logSecurityEvent("MUTATION_AUTH_REQUIRED", r, ReasonUnauthenticatedMutation)
			WriteSanitizedError(w, http.StatusUnauthorized, "UNAUTHENTICATED", "Authentication required")
			return
		}

		// 1. Origin check
		origin := r.Header.Get("Origin")
		if origin == "" {
			origin = r.Header.Get("Referer")
		}
		if origin == "" || !h.cfg.IsOriginAllowed(origin) {
			h.logSecurityEvent("ORIGIN_FORBIDDEN", r, ReasonOriginNotAllowlisted)
			WriteSanitizedError(w, http.StatusForbidden, "ORIGIN_FORBIDDEN", fmt.Sprintf("Origin %q is not allowlisted", origin))
			return
		}

		// 2. CSRF Token check
		csrfToken := r.Header.Get("X-CSRF-Token")
		if csrfToken == "" || !ValidateCSRFToken(sess, csrfToken) {
			h.logSecurityEvent("CSRF_VALIDATION_FAILED", r, ReasonCSRFValidationFailed)
			WriteSanitizedError(w, http.StatusForbidden, "CSRF_VALIDATION_FAILED", "Missing or invalid X-CSRF-Token header")
			return
		}

		next.ServeHTTP(w, r)
	})
}
