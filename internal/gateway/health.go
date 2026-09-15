package gateway

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// DefaultSchedulerTimeout is the maximum acceptable lag for scheduler heartbeats
const DefaultSchedulerTimeout = 60 * time.Second

// VersionInfo holds runtime version and build provenance
type VersionInfo struct {
	Version     string `json:"version"`
	CommitSHA   string `json:"commit"`
	BuildTime   string `json:"build_time"`
	ImageDigest string `json:"image_digest"`
	RuntimeMode string `json:"runtime_mode"`
}

// LiveResponse defines the response payload for GET /livez
type LiveResponse struct {
	Status    string `json:"status"`
	Timestamp string `json:"timestamp"`
}

// ReadyResponse defines the sanitized response payload for GET /readyz.
// Per Blueprint §25.2, internal topology, hostnames, and credentials are strictly redacted.
type ReadyResponse struct {
	Status    string `json:"status"`
	Timestamp string `json:"timestamp"`
	Database  string `json:"database"`
	Schema    string `json:"schema"`
	Scheduler string `json:"scheduler"`
	NATS      string `json:"nats"`
}

// NATSStatusChecker abstracts checking connection health of NATS JetStream
type NATSStatusChecker interface {
	IsConnected() bool
}

// TCPNATSChecker implements NATSStatusChecker via active TCP probe to the NATS server address
type TCPNATSChecker struct {
	addr string
}

// NewTCPNATSChecker creates a new TCPNATSChecker
func NewTCPNATSChecker(addr string) *TCPNATSChecker {
	if strings.Contains(addr, "://") {
		if parsed, err := url.Parse(addr); err == nil && parsed.Host != "" {
			addr = parsed.Host
		}
	}
	return &TCPNATSChecker{addr: addr}
}

// IsConnected performs an active TCP dial probe with a short timeout
func (c *TCPNATSChecker) IsConnected() bool {
	if c == nil || c.addr == "" {
		return false
	}
	conn, err := net.DialTimeout("tcp", c.addr, 2*time.Second)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

// HealthChecker manages health and version reporting for the control plane
type HealthChecker struct {
	versionInfo      VersionInfo
	pool             *pgxpool.Pool
	natsConn         NATSStatusChecker
	expectedVersion  int64
	schedulerTick    *atomic.Int64
	schedulerTimeout time.Duration
}

// NewHealthChecker creates a configured HealthChecker
func NewHealthChecker(versionInfo VersionInfo, pool *pgxpool.Pool, natsConn NATSStatusChecker, expectedVersion int64) *HealthChecker {
	return &HealthChecker{
		versionInfo:      versionInfo,
		pool:             pool,
		natsConn:         natsConn,
		expectedVersion:  expectedVersion,
		schedulerTimeout: DefaultSchedulerTimeout,
	}
}

// SetSchedulerTicker attaches an atomic timestamp for tracking scheduler loop freshness
func (h *HealthChecker) SetSchedulerTicker(ticker *atomic.Int64, timeout time.Duration) {
	h.schedulerTick = ticker
	if timeout > 0 {
		h.schedulerTimeout = timeout
	}
}

// HandleLivez handles GET /livez (fast liveness check)
func (h *HealthChecker) HandleLivez(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(LiveResponse{
		Status:    "alive",
		Timestamp: time.Now().UTC().Format(time.RFC3339),
	})
}

// HandleVersion handles GET /version (runtime version & commit provenance)
func (h *HealthChecker) HandleVersion(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(h.versionInfo)
}

// HandleReadyz handles GET /readyz (deep readiness probe)
func (h *HealthChecker) HandleReadyz(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	now := time.Now().UTC().Format(time.RFC3339)
	dbStatus := "unreachable"
	schemaStatus := "unreachable"
	schedulerStatus := "unobserved"
	natsStatus := "unobserved"

	// 1. Check database connectivity
	if h.pool != nil {
		if err := h.pool.Ping(ctx); err == nil {
			dbStatus = "healthy"

			// 2. Check schema migration compatibility (fail closed: must be verified and current)
			var appliedVersion int64
			err := h.pool.QueryRow(ctx, `
				SELECT COALESCE(MAX(version_id), 0)
				FROM goose_db_version
				WHERE is_applied = true
			`).Scan(&appliedVersion)
			if err != nil {
				schemaStatus = "unverified"
			} else if h.expectedVersion > 0 && appliedVersion < h.expectedVersion {
				schemaStatus = "outdated"
			} else {
				schemaStatus = "current"
			}
		}
	}

	// 3. Check scheduler freshness (fail closed: unobserved or stale is not ready)
	if h.schedulerTick != nil {
		lastNano := h.schedulerTick.Load()
		if lastNano == 0 || time.Since(time.Unix(0, lastNano)) > h.schedulerTimeout {
			schedulerStatus = "stale"
		} else {
			schedulerStatus = "active"
		}
	}

	// 4. Check NATS connectivity
	// Blueprint §25.2: "degraded NATS/telemetry is reported separately because DB fallback remains valid"
	if h.natsConn != nil {
		if h.natsConn.IsConnected() {
			natsStatus = "connected"
		} else {
			natsStatus = "degraded"
		}
	}

	// Determine overall readiness
	// Readiness strictly requires healthy DB, current schema, and active scheduler.
	// Fail closed: "unverified" schema and "unobserved" scheduler are NOT ready.
	// NATS degradation is reported separately and does NOT fail the readiness gate.
	isReady := (dbStatus == "healthy") &&
		(schemaStatus == "current") &&
		(schedulerStatus == "active")

	resp := ReadyResponse{
		Timestamp: now,
		Database:  dbStatus,
		Schema:    schemaStatus,
		Scheduler: schedulerStatus,
		NATS:      natsStatus,
	}

	w.Header().Set("Content-Type", "application/json")
	if isReady {
		resp.Status = "ready"
		w.WriteHeader(http.StatusOK)
	} else {
		resp.Status = "not_ready"
		w.WriteHeader(http.StatusServiceUnavailable)
	}
	_ = json.NewEncoder(w).Encode(resp)
}

// Routes registers the health check endpoints on an http.ServeMux
func (h *HealthChecker) Routes(mux *http.ServeMux) {
	mux.HandleFunc("/livez", h.HandleLivez)
	mux.HandleFunc("/readyz", h.HandleReadyz)
	mux.HandleFunc("/version", h.HandleVersion)
}
