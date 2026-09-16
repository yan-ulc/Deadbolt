package worker

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
)

var (
	ErrHTTPSRequired    = errors.New("HTTPS_REQUIRED: Worker connections outside loopback must use HTTPS")
	ErrEnrollmentFailed = errors.New("ENROLLMENT_FAILED: Worker enrollment was rejected by control plane")
)

type AgentConfig struct {
	ControlPlaneURL   string
	KeyPath           string
	EnrollmentToken   string
	Pool              string
	Slots             int
	PollTimeout       time.Duration
	HeartbeatInterval time.Duration
	NodePath          string
	RunnerPath        string
	BundleDir         string
	TaskEnvAllowlist  []string
	DrainGracePeriod  time.Duration
	Logger            *log.Logger
	// HTTPClient is optional. A nil client preserves the production default.
	HTTPClient *http.Client
	// OnTaskProcessStart is an optional observation hook for integration tests.
	OnTaskProcessStart func()
}

type activeAttempt struct {
	attemptID string
	epoch     int64
	pid       int
	cancel    context.CancelFunc
}

type Agent struct {
	cfg        AgentConfig
	client     *http.Client
	baseURL    *url.URL
	privKey    ed25519.PrivateKey
	pubKey     ed25519.PublicKey
	workerID   string
	sessionID  string
	sessionTok string
	expiresAt  time.Time
	poolName   string
	supervisor *ProcessSupervisor

	mu              sync.RWMutex
	runningAttempts map[string]*activeAttempt
	draining        bool
	revoked         bool
	stopPoll        chan struct{}
	stopOnce        sync.Once
	slotsChan       chan struct{}
}

func NewAgent(cfg AgentConfig) (*Agent, error) {
	if cfg.ControlPlaneURL == "" {
		return nil, errors.New("control plane URL is required")
	}
	parsedURL, err := url.Parse(strings.TrimRight(cfg.ControlPlaneURL, "/"))
	if err != nil {
		return nil, fmt.Errorf("invalid control plane URL: %w", err)
	}

	// Outbound HTTPS only outside loopback per Blueprint §12.1
	isLoopback := isLoopbackHost(parsedURL.Hostname())
	if parsedURL.Scheme != "https" && !isLoopback {
		return nil, ErrHTTPSRequired
	}

	if cfg.Slots <= 0 {
		cfg.Slots = DefaultSlots
	}
	if cfg.PollTimeout <= 0 {
		cfg.PollTimeout = DefaultPollTimeout
	}
	if cfg.HeartbeatInterval <= 0 {
		cfg.HeartbeatInterval = HeartbeatInterval
	}
	if cfg.DrainGracePeriod <= 0 {
		cfg.DrainGracePeriod = DrainGracePeriod
	}
	if cfg.Pool == "" {
		cfg.Pool = "default"
	}
	if cfg.NodePath == "" {
		cfg.NodePath = "node"
	}
	if cfg.KeyPath == "" {
		home, _ := os.UserHomeDir()
		cfg.KeyPath = filepath.Join(home, ".deadbolt", "worker.key")
	}
	if cfg.Logger == nil {
		cfg.Logger = log.New(os.Stdout, "[WORKER_AGENT] ", log.LstdFlags|log.Lmsgprefix)
	}

	supervisor := NewProcessSupervisor(cfg.NodePath, cfg.RunnerPath)
	supervisor.TaskEnvAllowlist = cfg.TaskEnvAllowlist

	client := cfg.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}

	return &Agent{
		cfg:             cfg,
		client:          client,
		baseURL:         parsedURL,
		poolName:        cfg.Pool,
		supervisor:      supervisor,
		runningAttempts: make(map[string]*activeAttempt),
		stopPoll:        make(chan struct{}),
		slotsChan:       make(chan struct{}, cfg.Slots),
	}, nil
}

func isLoopbackHost(host string) bool {
	if host == "localhost" || host == "127.0.0.1" || host == "::1" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// Start boots the agent, establishes identity, and runs polling until context is canceled.
func (a *Agent) Start(ctx context.Context) error {
	if err := a.ensureIdentity(ctx); err != nil {
		return err
	}

	a.cfg.Logger.Printf("Worker online: workerId=%s sessionId=%s pool=%s slots=%d",
		a.workerID, a.sessionID, a.poolName, a.cfg.Slots)

	// Graceful shutdown trap
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		select {
		case <-sigChan:
			a.cfg.Logger.Printf("Shutdown signal received; draining workers...")
			a.Drain(ctx)
		case <-ctx.Done():
			a.Drain(ctx)
		}
	}()

	return a.pollLoop(ctx)
}

func (a *Agent) ensureIdentity(ctx context.Context) error {
	// 1. Check if private key exists
	if _, err := os.Stat(a.cfg.KeyPath); err == nil {
		priv, err := LoadPrivateKey(a.cfg.KeyPath)
		if err != nil {
			return fmt.Errorf("load private key: %w", err)
		}
		a.privKey = priv
		a.pubKey = priv.Public().(ed25519.PublicKey)
		workerID, identityErr := LoadWorkerIdentity(a.cfg.KeyPath)
		if identityErr == nil {
			a.workerID = workerID
		} else if errors.Is(identityErr, os.ErrNotExist) {
			return fmt.Errorf("worker identity metadata is missing; refusing unauthenticated reconnect")
		} else {
			return fmt.Errorf("load worker identity: %w", identityErr)
		}
	} else {
		// Key does not exist: generate keypair and save with 0600
		if a.cfg.EnrollmentToken == "" {
			return errors.New("ENROLLMENT_TOKEN_REQUIRED: Private key not found and no enrollment token provided")
		}
		pub, priv, err := GenerateWorkerKeyPair()
		if err != nil {
			return err
		}
		if err := os.MkdirAll(filepath.Dir(a.cfg.KeyPath), 0o700); err != nil {
			return fmt.Errorf("create key directory: %w", err)
		}
		if err := SavePrivateKey(a.cfg.KeyPath, priv); err != nil {
			return fmt.Errorf("save private key: %w", err)
		}
		a.privKey = priv
		a.pubKey = pub
	}

	// 2. Obtain challenge nonce
	challengeReq := &ChallengeRequestDTO{
		ProtocolVersion: ProtocolVersion,
		RequestID:       fmt.Sprintf("req_chal_%d", time.Now().UnixNano()),
	}
	if a.workerID != "" {
		challengeReq.WorkerID = a.workerID
	} else {
		challengeReq.PublicKey = EncodePublicKey(a.pubKey)
	}
	challengeRes, err := a.requestChallenge(ctx, challengeReq)
	if err != nil {
		return fmt.Errorf("challenge request: %w", err)
	}

	// 3. Enroll or re-authenticate session
	sig := SignChallenge(a.privKey, challengeRes.Nonce)
	if a.workerID == "" && a.cfg.EnrollmentToken != "" {
		enrollReq := &EnrollRequestDTO{
			ProtocolVersion: ProtocolVersion,
			RequestID:       fmt.Sprintf("req_enr_%d", time.Now().UnixNano()),
			EnrollmentToken: a.cfg.EnrollmentToken,
			PublicKey:       EncodePublicKey(a.pubKey),
			Nonce:           challengeRes.Nonce,
			Signature:       sig,
		}
		sessionRes, err := a.enroll(ctx, enrollReq)
		if err != nil {
			return fmt.Errorf("enrollment: %w", err)
		}
		a.workerID = sessionRes.WorkerID
		if err := SaveWorkerIdentity(a.cfg.KeyPath, a.workerID); err != nil {
			return fmt.Errorf("save worker identity: %w", err)
		}
		a.sessionID = sessionRes.SessionID
		a.sessionTok = sessionRes.SessionToken
		t, _ := time.Parse(time.RFC3339, sessionRes.ExpiresAt)
		a.expiresAt = t
	} else {
		// Session re-authentication
		sessionReq := &SessionRequestDTO{
			ProtocolVersion: ProtocolVersion,
			RequestID:       fmt.Sprintf("req_sess_%d", time.Now().UnixNano()),
			WorkerID:        a.workerID,
			Nonce:           challengeRes.Nonce,
			Signature:       sig,
		}
		sessionRes, err := a.createSession(ctx, sessionReq)
		if err != nil {
			return fmt.Errorf("session re-auth: %w", err)
		}
		a.sessionID = sessionRes.SessionID
		a.sessionTok = sessionRes.SessionToken
		t, _ := time.Parse(time.RFC3339, sessionRes.ExpiresAt)
		a.expiresAt = t

		// Reconnecting stops old runners per Blueprint §12.1
		a.stopAllRunners()
	}

	return nil
}

func (a *Agent) stopAllRunners() {
	a.mu.Lock()
	defer a.mu.Unlock()
	for id, att := range a.runningAttempts {
		a.cfg.Logger.Printf("Reconnecting: stopping old runner for attempt %s (PID %d)", id, att.pid)
		att.cancel()
		if att.pid > 0 {
			_ = a.supervisor.TerminateProcessGroup(att.pid)
		}
		delete(a.runningAttempts, id)
	}
}

func (a *Agent) pollLoop(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-a.stopPoll:
			if a.isRevoked() {
				return ErrWorkerRevoked
			}
			return nil
		default:
		}

		a.mu.RLock()
		isDraining := a.draining
		a.mu.RUnlock()
		if isDraining {
			return nil
		}

		// Re-authenticate session before 15m expiration
		if time.Now().Add(2 * time.Minute).After(a.expiresAt) {
			if err := a.refreshSession(ctx); err != nil {
				a.cfg.Logger.Printf("Failed to refresh session: %v", err)
			}
		}

		availableSlots := a.cfg.Slots - len(a.slotsChan)
		if availableSlots <= 0 {
			select {
			case <-ctx.Done():
				return nil
			case <-a.stopPoll:
				if a.isRevoked() {
					return ErrWorkerRevoked
				}
				return nil
			case <-time.After(200 * time.Millisecond):
			}
			continue
		}

		pollReq := &PollRequestDTO{
			ProtocolVersion:   ProtocolVersion,
			RequestID:         fmt.Sprintf("req_poll_%d", time.Now().UnixNano()),
			WorkerID:          a.workerID,
			SessionID:         a.sessionID,
			AvailableSlots:    availableSlots,
			DeploymentDigests: a.listAvailableDigests(),
			Pool:              a.poolName,
		}

		pollCtx, cancelPoll := context.WithTimeout(ctx, a.cfg.PollTimeout)
		pollRes, err := a.poll(pollCtx, pollReq)
		cancelPoll()
		if err != nil {
			if errors.Is(err, ErrWorkerRevoked) {
				a.handleWorkerRevoked()
				return ErrWorkerRevoked
			}
			time.Sleep(2 * time.Second)
			continue
		}

		for _, assignment := range pollRes.Assignments {
			a.slotsChan <- struct{}{}
			go a.executeAssignment(ctx, assignment)
		}

		// The control plane holds the request until work arrives or PollTimeout.
	}
}

func (a *Agent) executeAssignment(parentCtx context.Context, assignment AssignmentDTO) {
	defer func() { <-a.slotsChan }()

	attCtx, cancel := context.WithCancel(parentCtx)
	defer cancel()

	active := &activeAttempt{
		attemptID: assignment.AttemptID,
		epoch:     assignment.OwnershipEpoch,
		cancel:    cancel,
	}

	a.mu.Lock()
	a.runningAttempts[assignment.AttemptID] = active
	a.mu.Unlock()

	defer func() {
		a.mu.Lock()
		delete(a.runningAttempts, assignment.AttemptID)
		a.mu.Unlock()
	}()

	// 1. Initialize lease tracker
	leaseExpiresAt, _ := time.Parse(time.RFC3339, assignment.LeaseExpiresAt)
	leaseTracker := NewLeaseTracker(leaseExpiresAt, 500*time.Millisecond, 2*time.Second)

	// 2. Start attempt with retry on network error
	startReq := &StartRequestDTO{
		ProtocolVersion: ProtocolVersion,
		RequestID:       fmt.Sprintf("req_start_%d", time.Now().UnixNano()),
		WorkerID:        a.workerID,
		SessionID:       a.sessionID,
		AttemptID:       assignment.AttemptID,
		OwnershipEpoch:  assignment.OwnershipEpoch,
	}

	startRes, err := a.startWithRetry(attCtx, startReq)
	if err != nil {
		if errors.Is(err, ErrWorkerRevoked) {
			a.handleWorkerRevoked()
			return
		}
		a.cfg.Logger.Printf("Start rejected for attempt %s: %v", assignment.AttemptID, err)
		return
	}
	if !startRes.Accepted {
		a.cfg.Logger.Printf("Start not accepted for attempt %s", assignment.AttemptID)
		return
	}

	// 3. Start background heartbeat loop
	heartbeatDone := make(chan struct{})
	defer close(heartbeatDone)

	go a.heartbeatLoop(attCtx, assignment.AttemptID, assignment.OwnershipEpoch, leaseTracker, heartbeatDone)

	// 4. Configure supervisor callbacks and execute
	bundleSpec := &BundleSpec{
		Path:       filepath.Join(a.cfg.BundleDir, assignment.BundleDigest+".tar"),
		SHA256:     assignment.BundleDigest,
		TargetArch: assignment.TargetArchitecture,
		Entrypoint: assignment.TaskEntrypoint,
	}
	if bundleSpec.TargetArch == "" {
		bundleSpec.TargetArch = CurrentHostArchitecture()
	}

	secretEnv, secretErr := resolveTaskSecrets(assignment.SecretNames)
	if secretErr != nil {
		a.cfg.Logger.Printf("Refusing attempt %s: %v", assignment.AttemptID, secretErr)
		digest := sha256.Sum256([]byte(secretErr.Error()))
		preflight := &CompleteRequestDTO{
			ProtocolVersion: ProtocolVersion,
			RequestID:       fmt.Sprintf("req_comp_preflight_%d", time.Now().UnixNano()),
			WorkerID:        a.workerID,
			SessionID:       a.sessionID,
			AttemptID:       assignment.AttemptID,
			OwnershipEpoch:  assignment.OwnershipEpoch,
			Outcome:         "FAILED",
			ResultDigest:    hex.EncodeToString(digest[:]),
			Error: &TaskErrorDTO{
				Code:         "MISSING_REQUIRED_SECRET",
				Message:      secretErr.Error(),
				Retryable:    false,
				EffectStatus: "NOT_APPLIED",
			},
		}
		preflight.ResultDigest, _ = CanonicalCompletionDigest(preflight)
		_, _ = a.completeWithRetry(attCtx, preflight)
		return
	}
	taskInput := &TaskInput{
		AttemptID:   assignment.AttemptID,
		OperationID: assignment.OperationID,
		StepID:      assignment.StepID,
		TaskName:    assignment.TaskEntrypoint,
		Entrypoint:  assignment.TaskEntrypoint,
		Input:       assignment.Input,
		TimeoutMs:   assignment.AttemptTimeoutMs,
		Bundle:      bundleSpec,
		Env:         secretEnv,
	}

	sup := NewProcessSupervisor(a.cfg.NodePath, a.cfg.RunnerPath)
	sup.LeaseTracker = leaseTracker
	sup.StartAuthorization = &StartAuthorization{
		AttemptID: assignment.AttemptID,
		Epoch:     assignment.OwnershipEpoch,
		Decision:  StartAccepted,
	}
	sup.TaskEnvAllowlist = assignment.SecretNames
	sup.OnProcessStart = func(pid int) {
		a.mu.Lock()
		active.pid = pid
		a.mu.Unlock()
		if a.cfg.OnTaskProcessStart != nil {
			a.cfg.OnTaskProcessStart()
		}
	}

	completion, logs, execErr := sup.ExecuteAttempt(attCtx, taskInput, assignment.OwnershipEpoch)
	if a.isRevoked() {
		return
	}

	// 5. Send logs to control plane
	if logs != nil && (logs.Stdout != "" || logs.Stderr != "") {
		if err := a.sendLogBatch(attCtx, assignment.AttemptID, logs, secretEnv); errors.Is(err, ErrWorkerRevoked) {
			a.handleWorkerRevoked()
			return
		}
	}

	// 6. Complete request
	outcome := "SUCCEEDED"
	if execErr != nil || completion == nil || completion.Status == "FAILED" {
		outcome = "FAILED"
	}

	compReq := &CompleteRequestDTO{
		ProtocolVersion: ProtocolVersion,
		RequestID:       fmt.Sprintf("req_comp_%d", time.Now().UnixNano()),
		WorkerID:        a.workerID,
		SessionID:       a.sessionID,
		AttemptID:       assignment.AttemptID,
		OwnershipEpoch:  assignment.OwnershipEpoch,
		Outcome:         outcome,
	}
	if outcome == "SUCCEEDED" && completion != nil {
		compReq.Output = completion.Output
	} else if completion != nil && completion.Error != nil {
		compReq.Error = &TaskErrorDTO{
			Code:         completion.Error.Code,
			Message:      completion.Error.Message,
			Retryable:    completion.Error.Retryable,
			Details:      completion.Error.Details,
			EffectStatus: "NOT_APPLIED",
		}
	} else if execErr != nil {
		compReq.Error = &TaskErrorDTO{
			Code:         "WORKER_PREFLIGHT_FAILED",
			Message:      execErr.Error(),
			Retryable:    false,
			EffectStatus: "NOT_APPLIED",
		}
	}
	resDigest, digestErr := CanonicalCompletionDigest(compReq)
	if digestErr != nil {
		return
	}
	compReq.ResultDigest = resDigest

	if _, err := a.completeWithRetry(attCtx, compReq); errors.Is(err, ErrWorkerRevoked) {
		a.handleWorkerRevoked()
	}
}

func resolveTaskSecrets(names []string) (map[string]string, error) {
	values := make(map[string]string, len(names))
	for _, name := range names {
		if value, ok := os.LookupEnv(name); ok {
			values[name] = value
		} else {
			return nil, fmt.Errorf("required task secret %q is not available", name)
		}
	}
	return values, nil
}

func (a *Agent) startWithRetry(ctx context.Context, req *StartRequestDTO) (*StartResponseDTO, error) {
	var lastErr error
	for tries := 0; tries < 2; tries++ {
		res, err := a.start(ctx, req)
		if err == nil {
			return res, nil
		}
		if errors.Is(err, ErrWorkerRevoked) {
			return nil, err
		}
		lastErr = err
		time.Sleep(100 * time.Millisecond)
	}
	return nil, lastErr
}

func (a *Agent) heartbeatLoop(ctx context.Context, attemptID string, epoch int64, lt *LeaseTracker, done chan struct{}) {
	ticker := time.NewTicker(a.cfg.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-done:
			return
		case <-ticker.C:
			// Check monotonic lease safety
			if lt.IsExpired(time.Time{}) {
				a.cfg.Logger.Printf("Monotonic lease safety boundary expired for attempt %s", attemptID)
				a.mu.RLock()
				att := a.runningAttempts[attemptID]
				a.mu.RUnlock()
				if att != nil && att.pid > 0 {
					_ = a.supervisor.TerminateProcessGroup(att.pid)
				}
				return
			}

			hbReq := &HeartbeatRequestDTO{
				ProtocolVersion: ProtocolVersion,
				RequestID:       fmt.Sprintf("req_hb_%d", time.Now().UnixNano()),
				WorkerID:        a.workerID,
				SessionID:       a.sessionID,
				Attempts: []HeartbeatAttemptDTO{
					{AttemptID: attemptID, OwnershipEpoch: epoch},
				},
			}

			startT := time.Now()
			hbRes, err := a.heartbeat(ctx, hbReq)
			rtt := time.Since(startT)
			if err != nil {
				if errors.Is(err, ErrWorkerRevoked) {
					a.handleWorkerRevoked()
					return
				}
				continue
			}

			// Process renewals
			for _, ren := range hbRes.Renewals {
				if ren.AttemptID == attemptID {
					t, _ := time.Parse(time.RFC3339, ren.LeaseExpiresAt)
					lt.Renew(t, rtt)
				}
			}

			// Process stop commands
			for _, stop := range hbRes.Stops {
				if stop.AttemptID == attemptID {
					a.cfg.Logger.Printf("Stop command received for attempt %s: %s", attemptID, stop.Reason)
					a.mu.RLock()
					att := a.runningAttempts[attemptID]
					a.mu.RUnlock()
					if att != nil && att.pid > 0 {
						_ = a.supervisor.TerminateProcessGroup(att.pid)
						stopAck := &StopAckRequestDTO{
							ProtocolVersion: ProtocolVersion,
							RequestID:       fmt.Sprintf("req_stopack_%d", time.Now().UnixNano()),
							WorkerID:        a.workerID,
							SessionID:       a.sessionID,
							AttemptID:       attemptID,
							OwnershipEpoch:  epoch,
							ProcessStopped:  true,
						}
						if _, err := a.stopAck(ctx, stopAck); errors.Is(err, ErrWorkerRevoked) {
							a.handleWorkerRevoked()
						}
					}
					return
				}
			}
		}
	}
}

func (a *Agent) sendLogBatch(ctx context.Context, attemptID string, logs *ExecutionLogs, resolvedSecrets map[string]string) error {
	logs = RedactExecutionLogs(logs, resolvedSecrets)
	records := make([]LogRecordDTO, 0)
	now := time.Now().UTC().Format(time.RFC3339)
	var seq int64 = 1

	for _, line := range strings.Split(logs.Stdout, "\n") {
		if strings.TrimSpace(line) != "" {
			records = append(records, LogRecordDTO{Sequence: seq, Timestamp: now, Level: "info", Message: line})
			seq++
		}
	}
	for _, line := range strings.Split(logs.Stderr, "\n") {
		if strings.TrimSpace(line) != "" {
			records = append(records, LogRecordDTO{Sequence: seq, Timestamp: now, Level: "warn", Message: line})
			seq++
		}
	}

	if len(records) > 0 {
		req := &LogBatchRequestDTO{
			ProtocolVersion: ProtocolVersion,
			RequestID:       fmt.Sprintf("req_log_%d", time.Now().UnixNano()),
			WorkerID:        a.workerID,
			SessionID:       a.sessionID,
			AttemptID:       attemptID,
			Records:         records,
		}
		_, err := a.sendLogs(ctx, req)
		return err
	}
	return nil
}

func (a *Agent) isRevoked() bool {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.revoked
}

// handleWorkerRevoked is terminal: a revoked session has no remaining
// execution authority, so every local child is cancelled before the agent exits.
func (a *Agent) handleWorkerRevoked() {
	a.mu.Lock()
	a.revoked = true
	a.draining = true
	a.mu.Unlock()
	a.cfg.Logger.Printf("Worker revoked by control plane. Stopping immediately.")
	a.stopOnce.Do(func() { close(a.stopPoll) })
	// Process-group shutdown can wait through its grace period. Start it now but
	// never hold the terminal revocation signal behind that wait.
	go a.stopAllRunners()
}

func (a *Agent) listAvailableDigests() []string {
	if a.cfg.BundleDir == "" {
		return []string{}
	}
	entries, err := os.ReadDir(a.cfg.BundleDir)
	if err != nil {
		return []string{}
	}
	digests := make([]string, 0)
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".tar") {
			digests = append(digests, strings.TrimSuffix(e.Name(), ".tar"))
		}
	}
	return digests
}

func (a *Agent) refreshSession(ctx context.Context) error {
	chal, err := a.requestChallenge(ctx, &ChallengeRequestDTO{
		ProtocolVersion: ProtocolVersion,
		RequestID:       fmt.Sprintf("req_chal_%d", time.Now().UnixNano()),
		WorkerID:        a.workerID,
	})
	if err != nil {
		return err
	}
	sig := SignChallenge(a.privKey, chal.Nonce)
	res, err := a.createSession(ctx, &SessionRequestDTO{
		ProtocolVersion: ProtocolVersion,
		RequestID:       fmt.Sprintf("req_sess_%d", time.Now().UnixNano()),
		WorkerID:        a.workerID,
		Nonce:           chal.Nonce,
		Signature:       sig,
	})
	if err != nil {
		return err
	}
	a.stopAllRunners()
	a.sessionID = res.SessionID
	a.sessionTok = res.SessionToken
	t, _ := time.Parse(time.RFC3339, res.ExpiresAt)
	a.expiresAt = t
	return nil
}

// Drain marks the agent as draining and waits for active attempts to complete.
func (a *Agent) Drain(ctx context.Context) {
	a.mu.Lock()
	a.draining = true
	a.mu.Unlock()

	a.stopOnce.Do(func() { close(a.stopPoll) })

	graceTimer := time.NewTimer(a.cfg.DrainGracePeriod)
	defer graceTimer.Stop()
	tick := time.NewTicker(200 * time.Millisecond)
	defer tick.Stop()

	for {
		select {
		case <-graceTimer.C:
			a.cfg.Logger.Printf("Drain grace exceeded; stopping remaining runners")
			a.stopAllRunners()
			return
		case <-tick.C:
			a.mu.RLock()
			count := len(a.runningAttempts)
			a.mu.RUnlock()
			if count == 0 {
				a.cfg.Logger.Printf("All runners drained cleanly")
				return
			}
		case <-ctx.Done():
			a.stopAllRunners()
			return
		}
	}
}

// Low-level HTTP request helpers

func (a *Agent) postJSON(ctx context.Context, path string, reqBody any, resBody any, authenticated bool) error {
	u := a.baseURL.ResolveReference(&url.URL{Path: path})
	data, err := json.Marshal(reqBody)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, "POST", u.String(), bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if authenticated && a.sessionTok != "" {
		req.Header.Set("Authorization", "Bearer "+a.sessionTok)
	}

	resp, err := a.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	if resp.StatusCode >= 400 {
		var env ErrorEnvelopeDTO
		_ = json.Unmarshal(bodyBytes, &env)
		if resp.StatusCode == 403 && env.Code == "WORKER_REVOKED" {
			return ErrWorkerRevoked
		}
		if resp.StatusCode == 409 && env.Code == "STALE_OWNERSHIP" {
			return ErrStaleOwnership
		}
		return fmt.Errorf("HTTP %d [%s]: %s", resp.StatusCode, env.Code, env.Message)
	}

	if resBody != nil {
		return json.Unmarshal(bodyBytes, resBody)
	}
	return nil
}

func (a *Agent) requestChallenge(ctx context.Context, req *ChallengeRequestDTO) (*ChallengeResponseDTO, error) {
	var res ChallengeResponseDTO
	err := a.postJSON(ctx, "/worker/v1/challenge", req, &res, false)
	return &res, err
}

func (a *Agent) enroll(ctx context.Context, req *EnrollRequestDTO) (*SessionResponseDTO, error) {
	var res SessionResponseDTO
	err := a.postJSON(ctx, "/worker/v1/enroll", req, &res, false)
	return &res, err
}

func (a *Agent) createSession(ctx context.Context, req *SessionRequestDTO) (*SessionResponseDTO, error) {
	var res SessionResponseDTO
	err := a.postJSON(ctx, "/worker/v1/session", req, &res, false)
	return &res, err
}

func (a *Agent) poll(ctx context.Context, req *PollRequestDTO) (*PollResponseDTO, error) {
	var res PollResponseDTO
	err := a.postJSON(ctx, "/worker/v1/poll", req, &res, true)
	return &res, err
}

func (a *Agent) start(ctx context.Context, req *StartRequestDTO) (*StartResponseDTO, error) {
	var res StartResponseDTO
	err := a.postJSON(ctx, "/worker/v1/start", req, &res, true)
	return &res, err
}

func (a *Agent) heartbeat(ctx context.Context, req *HeartbeatRequestDTO) (*HeartbeatResponseDTO, error) {
	var res HeartbeatResponseDTO
	err := a.postJSON(ctx, "/worker/v1/heartbeat", req, &res, true)
	return &res, err
}

func (a *Agent) complete(ctx context.Context, req *CompleteRequestDTO) (*CompleteResponseDTO, error) {
	var res CompleteResponseDTO
	err := a.postJSON(ctx, "/worker/v1/complete", req, &res, true)
	return &res, err
}

// completeWithRetry replays the exact canonical result identity after an
// ambiguous response. The control plane fences the owner and ACKs an identical
// committed digest, so this never starts a second task execution.
func (a *Agent) completeWithRetry(ctx context.Context, req *CompleteRequestDTO) (*CompleteResponseDTO, error) {
	var lastErr error
	for tries := 0; tries < 2; tries++ {
		res, err := a.complete(ctx, req)
		if err == nil || errors.Is(err, ErrWorkerRevoked) {
			return res, err
		}
		lastErr = err
	}
	return nil, lastErr
}

func (a *Agent) stopAck(ctx context.Context, req *StopAckRequestDTO) (*AckResponseDTO, error) {
	var res AckResponseDTO
	err := a.postJSON(ctx, "/worker/v1/stop-ack", req, &res, true)
	return &res, err
}

func (a *Agent) sendLogs(ctx context.Context, req *LogBatchRequestDTO) (*AckResponseDTO, error) {
	var res AckResponseDTO
	err := a.postJSON(ctx, "/worker/v1/logs", req, &res, true)
	return &res, err
}
