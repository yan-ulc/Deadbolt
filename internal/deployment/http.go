package deployment

import (
	"encoding/json"
	"errors"
	"github.com/Ryanakml/Deadbolt/internal/contracts"
	"github.com/Ryanakml/Deadbolt/internal/tenant"
	"io"
	"net/http"
)

type HTTPHandler struct {
	service *Service
	tenants *tenant.Service
}

func NewHTTPHandler(s *Service, tenants *tenant.Service) *HTTPHandler {
	return &HTTPHandler{service: s, tenants: tenants}
}
func (h *HTTPHandler) Register(w http.ResponseWriter, r *http.Request) {
	caller, ok := tenant.CallerFromContext(r.Context())
	if !ok {
		errJSON(w, r, 401, "UNAUTHENTICATED", "Authentication required")
		return
	}
	env := r.URL.Query().Get("environment")
	if env == "" {
		errJSON(w, r, 400, "MISSING_ENVIRONMENT", "Environment is required")
		return
	}
	if caller.Type == tenant.IdentityTypeMachine && (env == caller.EnvironmentName || env == caller.EnvironmentID) {
		env = caller.EnvironmentID
	}
	if !h.allowed(r, caller, env, tenant.CapDeploymentsRegister) {
		errJSON(w, r, 403, "FORBIDDEN", "Deployment registration is not permitted")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 2<<20)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			errJSON(w, r, http.StatusRequestEntityTooLarge, "PAYLOAD_TOO_LARGE", "Manifest exceeds the request limit")
			return
		}
		errJSON(w, r, 400, "INVALID_MANIFEST", "Invalid manifest")
		return
	}
	d, statusCode, err := h.service.Register(r.Context(), caller.OrganizationID, env, body, auditFromCaller(caller, r))
	if err != nil {
		writeServiceErr(w, r, err)
		return
	}
	writeJSON(w, statusCode, d)
}
func (h *HTTPHandler) Activate(w http.ResponseWriter, r *http.Request) {
	caller, ok := tenant.CallerFromContext(r.Context())
	if !ok {
		errJSON(w, r, 401, "UNAUTHENTICATED", "Authentication required")
		return
	}
	env := r.URL.Query().Get("environment")
	if caller.Type == tenant.IdentityTypeMachine && (env == caller.EnvironmentName || env == caller.EnvironmentID) {
		env = caller.EnvironmentID
	}
	var in struct {
		DeploymentID      string `json:"deploymentId"`
		ExpectedRevision  *int64 `json:"expectedRevision"`
		AllowSingleWorker bool   `json:"allowSingleWorker"`
	}
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if env == "" || dec.Decode(&in) != nil || in.DeploymentID == "" || in.ExpectedRevision == nil || dec.Decode(&struct{}{}) != io.EOF {
		errJSON(w, r, 400, "INVALID_REQUEST", "deploymentId, expectedRevision, and environment are required")
		return
	}
	environment, err := h.tenants.GetEnvironment(r.Context(), caller.OrganizationID, env)
	if err != nil {
		errJSON(w, r, 404, "NOT_FOUND", "Environment not found")
		return
	}
	cap := tenant.CapDeploymentsActivateStaging
	if environment.Name == tenant.EnvProduction {
		cap = tenant.CapDeploymentsActivateProd
	}
	if !h.allowed(r, caller, env, cap) {
		errJSON(w, r, 403, "FORBIDDEN", "Deployment activation is not permitted")
		return
	}
	out, err := h.service.Activate(r.Context(), caller.OrganizationID, env, r.PathValue("name"), in.DeploymentID, *in.ExpectedRevision, in.AllowSingleWorker, auditFromCaller(caller, r))
	if err != nil {
		writeServiceErr(w, r, err)
		return
	}
	writeJSON(w, 200, out)
}
func (h *HTTPHandler) allowed(r *http.Request, c *tenant.CallerIdentity, env, cap string) bool {
	if c.OrganizationID == "" {
		return false
	}
	if c.Type == tenant.IdentityTypeMachine {
		return c.EnvironmentID == env && tenant.CanAPIKeyPerform(c.Capabilities, cap)
	}
	m, err := h.tenants.GetMember(r.Context(), c.OrganizationID, c.UserID)
	return err == nil && m.Status == tenant.StatusActive && tenant.CanRolePerform(m.Role, cap)
}
func writeServiceErr(w http.ResponseWriter, r *http.Request, e error) {
	code, status := "INTERNAL_ERROR", 500
	if _, ok := e.(*contracts.Error); ok {
		code, status = e.(*contracts.Error).Code, 422
	}
	if errors.Is(e, ErrImmutable) {
		code, status = "IMMUTABLE_CONTENT_CONFLICT", 409
	}
	if errors.Is(e, ErrConflict) {
		code, status = "REVISION_CONFLICT", 409
	}
	if errors.Is(e, ErrPreflight) {
		code, status = "WORKER_PREFLIGHT_FAILED", 409
	}
	if errors.Is(e, ErrNotFound) {
		code, status = "NOT_FOUND", 404
	}
	errJSON(w, r, status, code, "Request could not be completed")
}
func auditFromCaller(c *tenant.CallerIdentity, r *http.Request) *tenant.AuditContext {
	var id *string
	if c.Type == tenant.IdentityTypeMachine {
		id = &c.KeyID
	} else {
		id = &c.UserID
	}
	return &tenant.AuditContext{ActorID: id, ActorType: c.Type, Role: c.Role, Capabilities: c.Capabilities, CorrelationID: tenant.RequestIDFromContext(r.Context())}
}
func writeJSON(w http.ResponseWriter, s int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(s)
	_ = json.NewEncoder(w).Encode(v)
}
func errJSON(w http.ResponseWriter, r *http.Request, s int, c, m string) {
	writeJSON(w, s, map[string]any{"code": c, "message": m, "requestId": tenant.RequestIDFromContext(r.Context()), "details": map[string]any{}, "retryable": s >= 500})
}
