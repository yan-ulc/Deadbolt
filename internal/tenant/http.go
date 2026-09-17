package tenant

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/Ryanakml/Deadbolt/internal/auth"
	"github.com/Ryanakml/Deadbolt/internal/storage"
	"github.com/jackc/pgx/v5/pgxpool"
)

type contextKey string

const (
	callerIdentityKey contextKey = "deadbolt.tenant.caller_identity"
	requestIDKey      contextKey = "deadbolt.tenant.request_id"
)

// CallerIdentity captures the verified identity and authorized scope for a request.
type CallerIdentity struct {
	Type            IdentityType `json:"type"`
	UserID          string       `json:"user_id,omitempty"`
	KeyID           string       `json:"key_id,omitempty"`
	Role            string       `json:"role,omitempty"`
	OrganizationID  string       `json:"organization_id"`
	EnvironmentID   string       `json:"environment_id,omitempty"`
	EnvironmentName string       `json:"environment_name,omitempty"`
	Capabilities    []string     `json:"capabilities"`
}

// CallerFromContext extracts the authenticated CallerIdentity from context.
func CallerFromContext(ctx context.Context) (*CallerIdentity, bool) {
	id, ok := ctx.Value(callerIdentityKey).(*CallerIdentity)
	return id, ok && id != nil
}

// ContextWithCaller sets the authenticated CallerIdentity into context.
func ContextWithCaller(ctx context.Context, id *CallerIdentity) context.Context {
	return context.WithValue(ctx, callerIdentityKey, id)
}

// RequestIDFromContext extracts the request ID from context or returns an empty string.
func RequestIDFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	if id, ok := ctx.Value(requestIDKey).(string); ok && id != "" {
		return id
	}
	return ""
}

// HTTPHandler provides REST endpoints for tenant, project, environment, and API key management.
type HTTPHandler struct {
	service      *Service
	pool         *pgxpool.Pool
	sessionStore *auth.SessionStore
	authCfg      auth.Config
}

// NewHTTPHandler constructs a new HTTPHandler.
func NewHTTPHandler(service *Service, pool *pgxpool.Pool, sessionStore *auth.SessionStore, authCfg auth.Config) *HTTPHandler {
	return &HTTPHandler{
		service:      service,
		pool:         pool,
		sessionStore: sessionStore,
		authCfg:      authCfg,
	}
}

// WithRequestID injects or generates an X-Request-ID header and stores it in context.
func (h *HTTPHandler) WithRequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqID := r.Header.Get("X-Request-ID")
		if reqID == "" {
			var err error
			reqID, err = NewUUID()
			if err != nil {
				reqID = fmt.Sprintf("req-%d", time.Now().UnixNano())
			}
		}
		w.Header().Set("X-Request-ID", reqID)
		ctx := context.WithValue(r.Context(), requestIDKey, reqID)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// RegisterRoutes mounts all tenant management routes onto the provided mux with dual /api/v1 and /v1 prefixes.
func (h *HTTPHandler) RegisterRoutes(mux *http.ServeMux) {
	register := func(pattern string, handler http.HandlerFunc) {
		parts := strings.SplitN(pattern, " ", 2)
		method, path := parts[0], parts[1]
		wrapped := h.WithRequestID(handler)
		mux.Handle(method+" /api/v1"+path, wrapped)
		mux.Handle(method+" /v1"+path, wrapped)
	}

	// Organization CRUD
	register("POST /organizations", h.RequireAuth(h.HandleCreateOrganization))
	register("GET /organizations", h.RequireAuth(h.HandleListOrganizations))
	register("GET /organizations/{id}", h.RequireAuth(h.RequireOrgScope(CapOrgRead, h.HandleGetOrganization)))
	register("PATCH /organizations/{id}", h.RequireAuth(h.RequireOrgScope(CapOrgUpdate, h.HandleUpdateOrganization)))
	register("DELETE /organizations/{id}", h.RequireAuth(h.RequireOrgScope(CapOrgDelete, h.HandleDeleteOrganization)))

	// Organization Members
	register("GET /organizations/{id}/members", h.RequireAuth(h.RequireOrgScope(CapOrgRead, h.HandleListMembers)))
	register("POST /organizations/{id}/members", h.RequireAuth(h.RequireOrgScope(CapAdminMember, h.HandleAddMember)))
	register("PATCH /organizations/{id}/members/{userId}", h.RequireAuth(h.RequireOrgScope(CapAdminMember, h.HandleUpdateMemberRole)))
	register("PATCH /organizations/{id}/members/{userId}/status", h.RequireAuth(h.RequireOrgScope(CapAdminMember, h.HandleUpdateMemberStatus)))
	register("DELETE /organizations/{id}/members/{userId}", h.RequireAuth(h.RequireOrgScope(CapAdminMember, h.HandleRemoveMember)))

	// Projects
	register("GET /projects", h.RequireAuth(h.RequireOrgScope(CapOrgRead, h.HandleListProjects)))
	register("POST /projects", h.RequireAuth(h.RequireOrgScope(CapAdminProject, h.HandleCreateProject)))

	// Environments
	register("GET /projects/{projectId}/environments", h.RequireAuth(h.RequireOrgScope(CapOrgRead, h.HandleListEnvironments)))
	register("POST /projects/{projectId}/environments", h.RequireAuth(h.RequireOrgScope(CapAdminProject, h.HandleCreateEnvironment)))

	// API Keys
	register("POST /environments/{envId}/api-keys", h.RequireAuth(h.RequireOrgScope(CapAdminKey, h.HandleCreateAPIKey)))
	register("GET /environments/{envId}/api-keys", h.RequireAuth(h.RequireOrgScope(CapAdminKey, h.HandleListAPIKeys)))
	register("POST /api-keys/{id}/rotate", h.RequireAuth(h.RequireOrgScope(CapAdminKey, h.HandleRotateAPIKey)))
	register("DELETE /api-keys/{id}", h.RequireAuth(h.RequireOrgScope(CapAdminKey, h.HandleRevokeAPIKey)))

	// Viewer Payload Protection Demonstration / Enforcement Endpoint
	register("GET /environments/{envId}/payload-preview", h.RequireAuth(h.RequireOrgScope(CapPayloadRead, h.HandlePayloadPreview)))
}

// Routes mounts all tenant management routes onto an http.Handler with dual /api/v1 and /v1 prefixes.
func (h *HTTPHandler) Routes() http.Handler {
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)
	return mux
}

// enforceIdempotency only validates the header. Claiming and replay happen inside
// the service transaction together with the mutation; middleware must not preflight.
func (h *HTTPHandler) enforceIdempotency(w http.ResponseWriter, r *http.Request, orgID string) (string, bool) {
	method := strings.ToUpper(r.Method)
	if method == http.MethodGet || method == http.MethodHead || method == http.MethodOptions {
		return "", true
	}
	idempKey := r.Header.Get("Idempotency-Key")
	if idempKey == "" {
		writeJSONError(w, r, http.StatusBadRequest, "MISSING_IDEMPOTENCY_KEY", "Idempotency-Key header is required for mutating requests")
		return "", false
	}
	return idempKey, true
}

func commandRequest(r *http.Request, scope string) (*http.Request, error) {
	key := r.Header.Get("Idempotency-Key")
	var body []byte
	if r.Body != nil {
		var err error
		body, err = io.ReadAll(r.Body)
		if err != nil {
			return nil, err
		}
		r.Body = io.NopCloser(bytes.NewReader(body))
	}
	path := strings.TrimPrefix(strings.TrimPrefix(r.URL.Path, "/api/v1"), "/v1")
	return r.WithContext(ContextWithCommand(r.Context(), Command{Scope: scope, Key: key, Operation: r.Method + " " + path, Fingerprint: RequestFingerprint(r.Method, path, body)})), nil
}

// isSelfInvalidatingKeyReplay identifies the only mutations a revoked machine
// key may replay: the command that rotated or revoked that exact key. A revoked
// credential must not regain authority to replay unrelated old mutations.
func isSelfInvalidatingKeyReplay(r *http.Request, keyID string) bool {
	path := strings.TrimPrefix(strings.TrimPrefix(r.URL.Path, "/api/v1"), "/v1")
	if r.Method == http.MethodPost {
		return path == "/api-keys/"+keyID+"/rotate"
	}
	return r.Method == http.MethodDelete && path == "/api-keys/"+keyID
}

// RequireAuth authenticates the caller via Bearer API Key or Session Cookie.
// Enforces CSRF & Origin validation for human session mutations (Blueprint §24.1).
func (h *HTTPHandler) RequireAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		// 1. Check Bearer API Key first
		authHeader := r.Header.Get("Authorization")
		if strings.HasPrefix(authHeader, "Bearer ") {
			rawKey := strings.TrimPrefix(authHeader, "Bearer ")
			if strings.HasPrefix(rawKey, "dbcli_") {
				sess, err := h.sessionStore.ValidateCLISession(ctx, rawKey, h.authCfg.SessionIdleTimeout)
				if err != nil || sess == nil {
					writeJSONError(w, r, http.StatusUnauthorized, "UNAUTHENTICATED", "Invalid or expired CLI session")
					return
				}
				caller := &CallerIdentity{Type: IdentityTypeHuman, UserID: sess.UserID}
				if sess.ActiveOrganizationID != nil {
					caller.OrganizationID = *sess.ActiveOrganizationID
				}
				next.ServeHTTP(w, r.WithContext(ContextWithCaller(ctx, caller)))
				return
			}
			apiKey, err := h.service.AuthenticateAPIKey(ctx, rawKey)
			if err != nil {
				var revokedErr *RevokedKeyError
				if errors.As(err, &revokedErr) {
					idempKey := r.Header.Get("Idempotency-Key")
					if idempKey != "" && isSelfInvalidatingKeyReplay(r, revokedErr.Key.ID) {
						cmdScope := "org:" + revokedErr.Key.OrganizationID + ":key:" + revokedErr.Key.ID
						var body []byte
						if r.Body != nil {
							body, _ = io.ReadAll(r.Body)
							r.Body = io.NopCloser(bytes.NewReader(body))
						}
						path := strings.TrimPrefix(strings.TrimPrefix(r.URL.Path, "/api/v1"), "/v1")
						fp := RequestFingerprint(r.Method, path, body)
						op := r.Method + " " + path

						completedCmd, checkErr := h.service.GetCompletedCommand(ctx, revokedErr.Key.OrganizationID, cmdScope, idempKey)
						if checkErr == nil && completedCmd != nil {
							if completedCmd.Fingerprint != fp || completedCmd.Operation != op {
								writeJSONError(w, r, http.StatusConflict, "IDEMPOTENCY_CONFLICT", "Idempotency key has already been used with different request parameters")
								return
							}
							if completedCmd.ResponseCode == http.StatusNoContent {
								w.WriteHeader(http.StatusNoContent)
								return
							}
							w.Header().Set("Content-Type", "application/json")
							w.WriteHeader(completedCmd.ResponseCode)
							_, _ = w.Write(completedCmd.Outcome)
							return
						}
					}
					writeJSONError(w, r, http.StatusUnauthorized, "API_KEY_REVOKED", err.Error())
					return
				}
				if errors.Is(err, ErrKeyExpired) {
					writeJSONError(w, r, http.StatusUnauthorized, "API_KEY_EXPIRED", err.Error())
					return
				}
				writeJSONError(w, r, http.StatusUnauthorized, "UNAUTHENTICATED", "Invalid or unrecognized API key")
				return
			}

			caller := &CallerIdentity{
				Type:            IdentityTypeMachine,
				KeyID:           apiKey.ID,
				OrganizationID:  apiKey.OrganizationID,
				EnvironmentID:   apiKey.EnvironmentID,
				EnvironmentName: apiKey.EnvironmentName,
				Capabilities:    apiKey.Capabilities,
			}
			next.ServeHTTP(w, r.WithContext(ContextWithCaller(ctx, caller)))
			return
		}

		// 2. Check BFF Session Cookie
		cookie, err := r.Cookie(h.authCfg.SessionCookieName())
		if err == nil && cookie.Value != "" && h.sessionStore != nil {
			sess, err := h.sessionStore.ValidateSession(ctx, cookie.Value, h.authCfg.SessionIdleTimeout)
			if err == nil && sess != nil {
				// Enforce CSRF token and Origin verification for mutating HTTP methods per Blueprint §24.1
				method := strings.ToUpper(r.Method)
				if method != http.MethodGet && method != http.MethodHead && method != http.MethodOptions {
					origin := r.Header.Get("Origin")
					if origin == "" {
						origin = r.Header.Get("Referer")
					}
					if origin == "" || !h.authCfg.IsOriginAllowed(origin) {
						writeJSONError(w, r, http.StatusForbidden, "ORIGIN_FORBIDDEN", fmt.Sprintf("Origin %q is not allowlisted", origin))
						return
					}

					csrfToken := r.Header.Get("X-CSRF-Token")
					if csrfToken == "" || !auth.ValidateCSRFToken(sess, csrfToken) {
						writeJSONError(w, r, http.StatusForbidden, "CSRF_VALIDATION_FAILED", "Missing or invalid X-CSRF-Token header")
						return
					}
				}

				caller := &CallerIdentity{
					Type:   IdentityTypeHuman,
					UserID: sess.UserID,
				}
				if sess.ActiveOrganizationID != nil {
					caller.OrganizationID = *sess.ActiveOrganizationID
				}
				next.ServeHTTP(w, r.WithContext(ContextWithCaller(ctx, caller)))
				return
			}
		}

		writeJSONError(w, r, http.StatusUnauthorized, "UNAUTHENTICATED", "Authentication required")
	}
}

// RequireOrgScope resolves the active organization ID, checks tenant membership, and enforces RBAC capability.
// Also strictly enforces that API key environment scope matches the request (Blueprint §20.1 & §24.3).
func (h *HTTPHandler) RequireOrgScope(requiredCap string, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		caller, ok := CallerFromContext(r.Context())
		if !ok || caller == nil {
			writeJSONError(w, r, http.StatusUnauthorized, "UNAUTHENTICATED", "Caller identity not found")
			return
		}

		var targetOrgID string
		cleanPath := strings.TrimPrefix(r.URL.Path, "/api/v1")
		cleanPath = strings.TrimPrefix(cleanPath, "/v1")
		if strings.HasPrefix(cleanPath, "/organizations/") {
			parts := strings.Split(strings.TrimPrefix(cleanPath, "/organizations/"), "/")
			if len(parts) > 0 && parts[0] != "" {
				targetOrgID = parts[0]
			}
		}
		if targetOrgID == "" {
			targetOrgID = r.Header.Get("X-Organization-ID")
		}
		if targetOrgID == "" {
			targetOrgID = r.URL.Query().Get("org_id")
		}
		if targetOrgID == "" && caller.OrganizationID != "" {
			targetOrgID = caller.OrganizationID
		}

		if targetOrgID == "" {
			writeJSONError(w, r, http.StatusBadRequest, "MISSING_ORGANIZATION_ID", "Organization context is required")
			return
		}

		if _, ok := h.enforceIdempotency(w, r, targetOrgID); !ok {
			return
		}

		actorScope := "user:" + caller.UserID
		if caller.Type == IdentityTypeMachine {
			actorScope = "key:" + caller.KeyID
		}
		cmdScope := "org:" + targetOrgID + ":" + actorScope
		idempKey := r.Header.Get("Idempotency-Key")

		if r.Method != http.MethodGet && r.Method != http.MethodHead && r.Method != http.MethodOptions {
			var body []byte
			if r.Body != nil {
				var err error
				body, err = io.ReadAll(r.Body)
				if err != nil {
					writeInternalError(w, r, fmt.Errorf("read idempotency request: %w", err))
					return
				}
				r.Body = io.NopCloser(bytes.NewReader(body))
			}
			path := strings.TrimPrefix(strings.TrimPrefix(r.URL.Path, "/api/v1"), "/v1")
			// Query selectors such as deployment environment are part of a command's
			// semantic identity; never replay an accepted mutation across scopes.
			if query := r.URL.Query().Encode(); query != "" {
				path += "?" + query
			}
			op := r.Method + " " + path
			fp := RequestFingerprint(r.Method, path, body)

			// 1. Check for deterministic replay of an already completed command issued by this actor
			// (Blueprint §20.1: Replays succeed even if the accepted command deleted the organization or membership)
			if idempKey != "" {
				completedCmd, err := h.service.GetCompletedCommand(r.Context(), targetOrgID, cmdScope, idempKey)
				if err != nil {
					writeInternalError(w, r, fmt.Errorf("lookup idempotency: %w", err))
					return
				}
				if completedCmd != nil {
					if completedCmd.Fingerprint != fp || completedCmd.Operation != op {
						writeJSONError(w, r, http.StatusConflict, "IDEMPOTENCY_CONFLICT", "Idempotency key has already been used with different request parameters")
						return
					}
					if completedCmd.ResponseCode == http.StatusNoContent {
						w.WriteHeader(http.StatusNoContent)
						return
					}
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(completedCmd.ResponseCode)
					_, _ = w.Write(completedCmd.Outcome)
					return
				}
			}

			// Attach command context for atomic withCommandTx execution
			r = r.WithContext(ContextWithCommand(r.Context(), Command{
				Scope:       cmdScope,
				Key:         idempKey,
				Operation:   op,
				Fingerprint: fp,
			}))
		}

		// Machine identity cross-tenant, environment mismatch, and capability check
		if caller.Type == IdentityTypeMachine {
			if caller.OrganizationID != targetOrgID {
				writeJSONError(w, r, http.StatusForbidden, "CROSS_TENANT_ACCESS_DENIED", "API key does not belong to target organization")
				return
			}

			// Strict Environment Mismatch Enforcement (Blueprint §20.1)
			routeEnvID := r.PathValue("envId")
			if routeEnvID != "" && caller.EnvironmentID != "" && caller.EnvironmentID != routeEnvID {
				writeJSONError(w, r, http.StatusForbidden, "ENVIRONMENT_MISMATCH", "API key does not belong to target environment")
				return
			}

			queryEnv := r.URL.Query().Get("environment")
			queryEnvID := r.URL.Query().Get("env_id")
			if queryEnvID == "" {
				queryEnvID = r.URL.Query().Get("envId")
			}
			if queryEnvID != "" && queryEnvID != caller.EnvironmentID {
				writeJSONError(w, r, http.StatusForbidden, "ENVIRONMENT_MISMATCH", "Environment query parameter does not match the API key's scoped environment")
				return
			}
			if queryEnv != "" && queryEnv != caller.EnvironmentName && queryEnv != caller.EnvironmentID {
				writeJSONError(w, r, http.StatusForbidden, "ENVIRONMENT_MISMATCH", "Environment query parameter does not match the API key's scoped environment")
				return
			}

			hdrEnvID := r.Header.Get("X-Environment-ID")
			hdrEnv := r.Header.Get("X-Environment")
			if hdrEnvID != "" && hdrEnvID != caller.EnvironmentID {
				writeJSONError(w, r, http.StatusForbidden, "ENVIRONMENT_MISMATCH", "Environment header does not match the API key's scoped environment")
				return
			}
			if hdrEnv != "" && hdrEnv != caller.EnvironmentName && hdrEnv != caller.EnvironmentID {
				writeJSONError(w, r, http.StatusForbidden, "ENVIRONMENT_MISMATCH", "Environment header does not match the API key's scoped environment")
				return
			}

			if requiredCap != "" && !CanAPIKeyPerform(caller.Capabilities, requiredCap) {
				writeJSONError(w, r, http.StatusForbidden, "FORBIDDEN", fmt.Sprintf("API key lacks required capability %q", requiredCap))
				return
			}

			next.ServeHTTP(w, r)
			return
		}

		// Human identity role and capability check
		member, err := h.service.GetMember(r.Context(), targetOrgID, caller.UserID)
		if err != nil || member.Status != StatusActive {
			writeJSONError(w, r, http.StatusForbidden, "FORBIDDEN_ORGANIZATION_MEMBERSHIP", "User is not an active member of the target organization")
			return
		}

		caller.OrganizationID = targetOrgID
		caller.Role = member.Role
		caller.Capabilities = RoleCapabilities(member.Role)

		if requiredCap != "" && !CanRolePerform(caller.Role, requiredCap) {
			writeJSONError(w, r, http.StatusForbidden, "FORBIDDEN", fmt.Sprintf("Role %q lacks required capability %q", caller.Role, requiredCap))
			return
		}

		next.ServeHTTP(w, r.WithContext(ContextWithCaller(r.Context(), caller)))
	}
}

// -------------------------------------------------------------------------
// Organization Handlers
// -------------------------------------------------------------------------

func (h *HTTPHandler) HandleCreateOrganization(w http.ResponseWriter, r *http.Request) {
	if _, ok := h.enforceIdempotency(w, r, ""); !ok {
		return
	}
	caller, ok := CallerFromContext(r.Context())
	if !ok || caller == nil || caller.UserID == "" {
		writeJSONError(w, r, http.StatusUnauthorized, "UNAUTHORIZED", "Authentication is required")
		return
	}
	if caller.Type != IdentityTypeHuman {
		writeJSONError(w, r, http.StatusForbidden, "MACHINE_CREATION_FORBIDDEN", "Only human users can create organizations")
		return
	}
	idempKey := r.Header.Get("Idempotency-Key")
	cmdScope := "user:" + caller.UserID

	var body []byte
	if r.Body != nil {
		var err error
		body, err = io.ReadAll(r.Body)
		if err != nil {
			writeInternalError(w, r, fmt.Errorf("read idempotency request: %w", err))
			return
		}
		r.Body = io.NopCloser(bytes.NewReader(body))
	}
	path := strings.TrimPrefix(strings.TrimPrefix(r.URL.Path, "/api/v1"), "/v1")
	op := r.Method + " " + path
	fp := RequestFingerprint(r.Method, path, body)

	// Check completed command replay
	if idempKey != "" {
		orgID := commandOrganizationID(caller.UserID, idempKey)
		completedCmd, err := h.service.GetCompletedCommand(r.Context(), orgID, cmdScope, idempKey)
		if err == nil && completedCmd != nil {
			if completedCmd.Fingerprint != fp || completedCmd.Operation != op {
				writeJSONError(w, r, http.StatusConflict, "IDEMPOTENCY_CONFLICT", "Idempotency key has already been used with different request parameters")
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(completedCmd.ResponseCode)
			_, _ = w.Write(completedCmd.Outcome)
			return
		}
	}

	r = r.WithContext(ContextWithCommand(r.Context(), Command{
		Scope:       cmdScope,
		Key:         idempKey,
		Operation:   op,
		Fingerprint: fp,
	}))

	var req struct {
		Name string `json:"name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Name == "" {
		writeJSONError(w, r, http.StatusBadRequest, "INVALID_REQUEST", "Organization name is required")
		return
	}

	org, err := h.service.CreateOrganization(r.Context(), caller.UserID, req.Name)
	if err != nil {
		writeInternalError(w, r, err)
		return
	}

	writeJSON(w, http.StatusCreated, org)
}

func (h *HTTPHandler) HandleListOrganizations(w http.ResponseWriter, r *http.Request) {
	caller, _ := CallerFromContext(r.Context())
	if caller.Type != IdentityTypeHuman {
		writeJSONError(w, r, http.StatusForbidden, "MACHINE_LIST_FORBIDDEN", "Only human users can enumerate organizations")
		return
	}

	memberships, err := storage.DiscoverUserMemberships(r.Context(), h.pool, caller.UserID)
	if err != nil {
		writeInternalError(w, r, err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"organizations": memberships,
	})
}

func (h *HTTPHandler) HandleGetOrganization(w http.ResponseWriter, r *http.Request) {
	orgID := r.PathValue("id")
	org, err := h.service.GetOrganization(r.Context(), orgID)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			writeJSONError(w, r, http.StatusNotFound, "NOT_FOUND", "Organization not found")
			return
		}
		writeInternalError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, org)
}

func (h *HTTPHandler) HandleUpdateOrganization(w http.ResponseWriter, r *http.Request) {
	orgID := r.PathValue("id")
	var req struct {
		Name string `json:"name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Name == "" {
		writeJSONError(w, r, http.StatusBadRequest, "INVALID_REQUEST", "Organization name is required")
		return
	}

	org, err := h.service.UpdateOrganization(r.Context(), orgID, req.Name)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			writeJSONError(w, r, http.StatusNotFound, "NOT_FOUND", "Organization not found")
			return
		}
		writeInternalError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, org)
}

func (h *HTTPHandler) HandleDeleteOrganization(w http.ResponseWriter, r *http.Request) {
	orgID := r.PathValue("id")
	err := h.service.DeleteOrganization(r.Context(), orgID)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			writeJSONError(w, r, http.StatusNotFound, "NOT_FOUND", "Organization not found")
			return
		}
		writeInternalError(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// -------------------------------------------------------------------------
// Member Handlers
// -------------------------------------------------------------------------

func (h *HTTPHandler) HandleListMembers(w http.ResponseWriter, r *http.Request) {
	caller, _ := CallerFromContext(r.Context())
	if caller.Type == IdentityTypeMachine {
		writeJSONError(w, r, http.StatusForbidden, "MACHINE_KEY_UNAUTHORIZED", "Machine API keys are not permitted to manage organization members")
		return
	}
	orgID := r.PathValue("id")
	members, err := h.service.ListMembers(r.Context(), orgID)
	if err != nil {
		writeInternalError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"members": members})
}

func (h *HTTPHandler) HandleAddMember(w http.ResponseWriter, r *http.Request) {
	caller, _ := CallerFromContext(r.Context())
	if caller.Type == IdentityTypeMachine {
		writeJSONError(w, r, http.StatusForbidden, "MACHINE_KEY_UNAUTHORIZED", "Machine API keys are not permitted to manage organization members")
		return
	}
	orgID := r.PathValue("id")
	var req struct {
		UserID string `json:"user_id"`
		Role   string `json:"role"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" || req.Role == "" {
		writeJSONError(w, r, http.StatusBadRequest, "INVALID_REQUEST", "user_id and role are required")
		return
	}

	member, err := h.service.AddMember(r.Context(), orgID, req.UserID, req.Role)
	if err != nil {
		if errors.Is(err, ErrInvalidRole) {
			writeJSONError(w, r, http.StatusBadRequest, "INVALID_ROLE", err.Error())
			return
		}
		writeInternalError(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, member)
}

func (h *HTTPHandler) HandleUpdateMemberRole(w http.ResponseWriter, r *http.Request) {
	caller, _ := CallerFromContext(r.Context())
	if caller.Type == IdentityTypeMachine {
		writeJSONError(w, r, http.StatusForbidden, "MACHINE_KEY_UNAUTHORIZED", "Machine API keys are not permitted to manage organization members")
		return
	}
	orgID := r.PathValue("id")
	targetUserID := r.PathValue("userId")
	var req struct {
		Role string `json:"role"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Role == "" {
		writeJSONError(w, r, http.StatusBadRequest, "INVALID_REQUEST", "role is required")
		return
	}

	err := h.service.UpdateMemberRole(r.Context(), orgID, targetUserID, req.Role)
	if err != nil {
		if errors.Is(err, ErrLastOwnerDemotion) {
			writeJSONError(w, r, http.StatusForbidden, "LAST_OWNER_DEMOTION_FORBIDDEN", err.Error())
			return
		}
		if errors.Is(err, ErrInvalidRole) {
			writeJSONError(w, r, http.StatusBadRequest, "INVALID_ROLE", err.Error())
			return
		}
		if errors.Is(err, ErrNotFound) {
			writeJSONError(w, r, http.StatusNotFound, "NOT_FOUND", "Member not found")
			return
		}
		writeInternalError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "updated"})
}

func (h *HTTPHandler) HandleUpdateMemberStatus(w http.ResponseWriter, r *http.Request) {
	caller, _ := CallerFromContext(r.Context())
	if caller.Type == IdentityTypeMachine {
		writeJSONError(w, r, http.StatusForbidden, "MACHINE_KEY_UNAUTHORIZED", "Machine API keys are not permitted to manage organization members")
		return
	}
	orgID := r.PathValue("id")
	targetUserID := r.PathValue("userId")
	var req struct {
		Status string `json:"status"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Status == "" {
		writeJSONError(w, r, http.StatusBadRequest, "INVALID_REQUEST", "status is required")
		return
	}

	err := h.service.UpdateMemberStatus(r.Context(), orgID, targetUserID, req.Status)
	if err != nil {
		if errors.Is(err, ErrLastOwnerSuspension) {
			writeJSONError(w, r, http.StatusForbidden, "LAST_OWNER_SUSPENSION_FORBIDDEN", err.Error())
			return
		}
		if errors.Is(err, ErrInvalidStatus) {
			writeJSONError(w, r, http.StatusBadRequest, "INVALID_STATUS", err.Error())
			return
		}
		if errors.Is(err, ErrNotFound) {
			writeJSONError(w, r, http.StatusNotFound, "NOT_FOUND", "Member not found")
			return
		}
		writeInternalError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "updated"})
}

func (h *HTTPHandler) HandleRemoveMember(w http.ResponseWriter, r *http.Request) {
	caller, _ := CallerFromContext(r.Context())
	if caller.Type == IdentityTypeMachine {
		writeJSONError(w, r, http.StatusForbidden, "MACHINE_KEY_UNAUTHORIZED", "Machine API keys are not permitted to manage organization members")
		return
	}
	orgID := r.PathValue("id")
	targetUserID := r.PathValue("userId")

	err := h.service.RemoveMember(r.Context(), orgID, targetUserID)
	if err != nil {
		if errors.Is(err, ErrLastOwnerRemoval) {
			writeJSONError(w, r, http.StatusForbidden, "LAST_OWNER_REMOVAL_FORBIDDEN", err.Error())
			return
		}
		if errors.Is(err, ErrNotFound) {
			writeJSONError(w, r, http.StatusNotFound, "NOT_FOUND", "Member not found")
			return
		}
		writeInternalError(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// -------------------------------------------------------------------------
// Project Handlers
// -------------------------------------------------------------------------

func (h *HTTPHandler) HandleListProjects(w http.ResponseWriter, r *http.Request) {
	caller, _ := CallerFromContext(r.Context())
	if caller.Type == IdentityTypeMachine {
		proj, err := h.service.GetProjectForEnvironment(r.Context(), caller.OrganizationID, caller.EnvironmentID)
		if err != nil {
			if errors.Is(err, ErrNotFound) {
				writeJSON(w, http.StatusOK, map[string]any{"projects": []Project{}})
				return
			}
			writeInternalError(w, r, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"projects": []Project{*proj}})
		return
	}
	projects, err := h.service.ListProjects(r.Context(), caller.OrganizationID)
	if err != nil {
		writeInternalError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"projects": projects})
}

func (h *HTTPHandler) HandleCreateProject(w http.ResponseWriter, r *http.Request) {
	caller, _ := CallerFromContext(r.Context())
	if caller.Type == IdentityTypeMachine {
		writeJSONError(w, r, http.StatusForbidden, "MACHINE_KEY_UNAUTHORIZED", "Machine API keys are not permitted to create projects")
		return
	}
	var req struct {
		Name string `json:"name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Name == "" {
		writeJSONError(w, r, http.StatusBadRequest, "INVALID_REQUEST", "Project name is required")
		return
	}

	project, err := h.service.CreateProject(r.Context(), caller.OrganizationID, req.Name)
	if err != nil {
		writeInternalError(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, project)
}

// -------------------------------------------------------------------------
// Environment Handlers
// -------------------------------------------------------------------------

func (h *HTTPHandler) HandleListEnvironments(w http.ResponseWriter, r *http.Request) {
	caller, _ := CallerFromContext(r.Context())
	projectID := r.PathValue("projectId")

	if caller.Type == IdentityTypeMachine {
		proj, err := h.service.GetProjectForEnvironment(r.Context(), caller.OrganizationID, caller.EnvironmentID)
		if err != nil {
			if errors.Is(err, ErrNotFound) {
				writeJSONError(w, r, http.StatusNotFound, "NOT_FOUND", "Project not found")
				return
			}
			writeInternalError(w, r, err)
			return
		}
		if proj.ID != projectID {
			writeJSONError(w, r, http.StatusForbidden, "ENVIRONMENT_MISMATCH", "API key cannot access environments of another project")
			return
		}
		env, err := h.service.GetEnvironment(r.Context(), caller.OrganizationID, caller.EnvironmentID)
		if err != nil {
			writeInternalError(w, r, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"environments": []Environment{*env}})
		return
	}

	envs, err := h.service.ListEnvironments(r.Context(), caller.OrganizationID, projectID)
	if err != nil {
		writeInternalError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"environments": envs})
}

func (h *HTTPHandler) HandleCreateEnvironment(w http.ResponseWriter, r *http.Request) {
	caller, _ := CallerFromContext(r.Context())
	if caller.Type == IdentityTypeMachine {
		writeJSONError(w, r, http.StatusForbidden, "MACHINE_KEY_UNAUTHORIZED", "Machine API keys are not permitted to create environments")
		return
	}
	projectID := r.PathValue("projectId")
	var req struct {
		Name           string `json:"name"`
		MaxConcurrency int    `json:"max_concurrency"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Name == "" {
		writeJSONError(w, r, http.StatusBadRequest, "INVALID_REQUEST", "Environment name is required")
		return
	}

	env, err := h.service.CreateEnvironment(r.Context(), caller.OrganizationID, projectID, req.Name, req.MaxConcurrency)
	if err != nil {
		if errors.Is(err, ErrInvalidEnvironment) {
			writeJSONError(w, r, http.StatusBadRequest, "INVALID_ENVIRONMENT", err.Error())
			return
		}
		if errors.Is(err, ErrNotFound) {
			writeJSONError(w, r, http.StatusNotFound, "NOT_FOUND", "Project not found")
			return
		}
		writeInternalError(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, env)
}

// -------------------------------------------------------------------------
// API Key Handlers
// -------------------------------------------------------------------------

func (h *HTTPHandler) HandleCreateAPIKey(w http.ResponseWriter, r *http.Request) {
	caller, _ := CallerFromContext(r.Context())
	envID := r.PathValue("envId")

	var req struct {
		Capabilities []string `json:"capabilities"`
		ExpiryDays   int      `json:"expiry_days"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, r, http.StatusBadRequest, "INVALID_REQUEST", "Invalid JSON payload")
		return
	}

	reqID := RequestIDFromContext(r.Context())
	idempKey := r.Header.Get("Idempotency-Key")
	if idempKey == "" {
		idempKey = reqID
	}

	var actorID *string
	var creatorCaps []string
	if caller.Type == IdentityTypeMachine {
		actorID = &caller.KeyID
		creatorCaps = caller.Capabilities
	} else {
		if caller.UserID != "" {
			actorID = &caller.UserID
		}
		if len(caller.Capabilities) > 0 {
			creatorCaps = caller.Capabilities
		} else if caller.Role != "" {
			creatorCaps = RoleCapabilities(caller.Role)
		}
	}

	audit := &AuditContext{
		ActorID:       actorID,
		ActorType:     caller.Type,
		Role:          caller.Role,
		Capabilities:  creatorCaps,
		CorrelationID: idempKey,
		Reason:        r.Header.Get("X-Audit-Reason"),
	}

	key, err := h.service.CreateAPIKey(r.Context(), caller.OrganizationID, envID, req.Capabilities, req.ExpiryDays, creatorCaps, audit)
	if err != nil {
		if errors.Is(err, ErrCapabilityElevation) {
			writeJSONError(w, r, http.StatusForbidden, "CAPABILITY_ELEVATION_FORBIDDEN", err.Error())
			return
		}
		if errors.Is(err, ErrMachineKeyRestricted) {
			writeJSONError(w, r, http.StatusForbidden, "MACHINE_KEY_UNAUTHORIZED", err.Error())
			return
		}
		if errors.Is(err, ErrNotFound) {
			writeJSONError(w, r, http.StatusNotFound, "NOT_FOUND", "Environment not found")
			return
		}
		writeInternalError(w, r, err)
		return
	}

	writeJSON(w, http.StatusCreated, key)
}

func (h *HTTPHandler) HandleListAPIKeys(w http.ResponseWriter, r *http.Request) {
	caller, _ := CallerFromContext(r.Context())
	envID := r.PathValue("envId")

	keys, err := h.service.ListAPIKeys(r.Context(), caller.OrganizationID, envID)
	if err != nil {
		writeInternalError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"api_keys": keys})
}

func (h *HTTPHandler) HandleRotateAPIKey(w http.ResponseWriter, r *http.Request) {
	caller, _ := CallerFromContext(r.Context())
	keyID := r.PathValue("id")

	var req struct {
		ExpiryDays int `json:"expiry_days"`
	}
	if r.Body != nil && r.ContentLength != 0 {
		dec := json.NewDecoder(r.Body)
		dec.DisallowUnknownFields()
		if err := dec.Decode(&req); err != nil && !errors.Is(err, io.EOF) {
			writeJSONError(w, r, http.StatusBadRequest, "MALFORMED_JSON", "Request body contains invalid JSON: "+err.Error())
			return
		}
	}

	var callerEnvID string
	var actorID *string
	var callerCaps []string
	if caller.Type == IdentityTypeMachine {
		callerEnvID = caller.EnvironmentID
		actorID = &caller.KeyID
		callerCaps = caller.Capabilities
	} else {
		if caller.UserID != "" {
			actorID = &caller.UserID
		}
		if len(caller.Capabilities) > 0 {
			callerCaps = caller.Capabilities
		} else if caller.Role != "" {
			callerCaps = RoleCapabilities(caller.Role)
		}
	}

	reqID := RequestIDFromContext(r.Context())
	idempKey := r.Header.Get("Idempotency-Key")
	if idempKey == "" {
		idempKey = reqID
	}
	audit := &AuditContext{
		ActorID:       actorID,
		ActorType:     caller.Type,
		Role:          caller.Role,
		Capabilities:  callerCaps,
		CorrelationID: idempKey,
		Reason:        r.Header.Get("X-Audit-Reason"),
	}

	key, err := h.service.RotateAPIKey(r.Context(), caller.OrganizationID, keyID, req.ExpiryDays, callerCaps, callerEnvID, audit)
	if err != nil {
		if errors.Is(err, ErrCapabilityElevation) {
			writeJSONError(w, r, http.StatusForbidden, "CAPABILITY_ELEVATION_FORBIDDEN", err.Error())
			return
		}
		if errors.Is(err, ErrEnvironmentMismatch) {
			writeJSONError(w, r, http.StatusForbidden, "ENVIRONMENT_MISMATCH", "API key cannot rotate keys outside its scoped environment")
			return
		}
		if errors.Is(err, ErrKeyRevoked) {
			writeJSONError(w, r, http.StatusBadRequest, "API_KEY_REVOKED", "Cannot rotate an already revoked API key")
			return
		}
		if errors.Is(err, ErrNotFound) {
			writeJSONError(w, r, http.StatusNotFound, "NOT_FOUND", "API key not found")
			return
		}
		writeInternalError(w, r, err)
		return
	}

	writeJSON(w, http.StatusOK, key)
}

func (h *HTTPHandler) HandleRevokeAPIKey(w http.ResponseWriter, r *http.Request) {
	caller, _ := CallerFromContext(r.Context())
	keyID := r.PathValue("id")

	var callerEnvID string
	var actorID *string
	var callerCaps []string
	if caller.Type == IdentityTypeMachine {
		callerEnvID = caller.EnvironmentID
		actorID = &caller.KeyID
		callerCaps = caller.Capabilities
	} else {
		if caller.UserID != "" {
			actorID = &caller.UserID
		}
		if len(caller.Capabilities) > 0 {
			callerCaps = caller.Capabilities
		} else if caller.Role != "" {
			callerCaps = RoleCapabilities(caller.Role)
		}
	}

	reqID := RequestIDFromContext(r.Context())
	idempKey := r.Header.Get("Idempotency-Key")
	if idempKey == "" {
		idempKey = reqID
	}
	audit := &AuditContext{
		ActorID:       actorID,
		ActorType:     caller.Type,
		Role:          caller.Role,
		Capabilities:  callerCaps,
		CorrelationID: idempKey,
		Reason:        r.Header.Get("X-Audit-Reason"),
	}

	err := h.service.RevokeAPIKey(r.Context(), caller.OrganizationID, keyID, callerEnvID, audit)
	if err != nil {
		if errors.Is(err, ErrEnvironmentMismatch) {
			writeJSONError(w, r, http.StatusForbidden, "ENVIRONMENT_MISMATCH", "API key cannot revoke keys outside its scoped environment")
			return
		}
		if errors.Is(err, ErrNotFound) {
			writeJSONError(w, r, http.StatusNotFound, "NOT_FOUND", "API key not found")
			return
		}
		writeInternalError(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *HTTPHandler) HandlePayloadPreview(w http.ResponseWriter, r *http.Request) {
	// Reached only if caller has CapPayloadRead capability
	writeJSON(w, http.StatusOK, map[string]any{
		"status":  "ok",
		"payload": "confidential_task_output_data",
	})
}

// -------------------------------------------------------------------------
// JSON Helpers
// -------------------------------------------------------------------------

func writeJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(data)
}

func writeJSONError(w http.ResponseWriter, r *http.Request, status int, code, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)

	reqID := ""
	if r != nil {
		if id, ok := r.Context().Value(requestIDKey).(string); ok && id != "" {
			reqID = id
		} else {
			reqID = r.Header.Get("X-Request-ID")
		}
	}
	if reqID == "" {
		reqID = "req-unknown"
	}

	retryable := status >= 500 || status == http.StatusTooManyRequests

	_ = json.NewEncoder(w).Encode(ErrorEnvelope{
		Code:      code,
		Message:   message,
		RequestID: reqID,
		Details:   map[string]any{},
		Retryable: retryable,
	})
}

func writeInternalError(w http.ResponseWriter, r *http.Request, err error) {
	if errors.Is(err, ErrIdempotencyConflict) {
		writeJSONError(w, r, http.StatusConflict, "IDEMPOTENCY_CONFLICT", "Idempotency-Key was already used with different request content")
		return
	}
	reqID := ""
	if r != nil {
		reqID = RequestIDFromContext(r.Context())
		if reqID == "" {
			reqID = r.Header.Get("X-Request-ID")
		}
	}
	if reqID == "" {
		reqID = "req-unknown"
	}
	log.Printf("[ERROR] [request_id=%s] internal server error: %v", reqID, err)
	writeJSONError(w, r, http.StatusInternalServerError, "INTERNAL_ERROR", "An internal server error occurred")
}
