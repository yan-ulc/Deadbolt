package execution

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Ryanakml/Deadbolt/internal/artifacts"
	"github.com/Ryanakml/Deadbolt/internal/contracts"
	"github.com/Ryanakml/Deadbolt/internal/storage"
	"github.com/Ryanakml/Deadbolt/internal/tenant"
	"github.com/Ryanakml/Deadbolt/internal/worker"
	"github.com/jackc/pgx/v5"
)

const defaultAttemptTimeout = 5 * time.Minute

// WorkerEngine is the PostgreSQL execution authority used by the worker HTTP
// adapter. State transitions, leases, history, and wake-up hints are committed
// together here instead of in the transport layer.
type WorkerEngine struct {
	pool                 *storage.Pool
	hub                  *EventHub
	commands             *tenant.Service
	artifacts            *artifacts.Service
	beforeCompleteCommit func() error
	afterCompleteCommit  func() error
}

func NewWorkerEngine(pool *storage.Pool, hub ...*EventHub) *WorkerEngine {
	var h *EventHub
	if len(hub) > 0 && hub[0] != nil {
		h = hub[0]
	}
	return &WorkerEngine{pool: pool, hub: h}
}

func (e *WorkerEngine) SetHub(hub *EventHub) {
	e.hub = hub
}

// SetCommands attaches the canonical tenant command service so mutations
// record idempotent command outcomes atomically with state transitions.
// Engines without it (unit-style construction) execute bare transactions.
func (e *WorkerEngine) SetCommands(commands *tenant.Service) {
	e.commands = commands
}

// SetArtifacts attaches the scoped artifact service for result association
// and consumer integrity admission. Engines without it fail artifact paths
// closed.
func (e *WorkerEngine) SetArtifacts(svc *artifacts.Service) {
	e.artifacts = svc
}

// SetBeforeCompleteCommitHookForTest injects a deterministic failure after all
// completion writes have been staged but before the transaction is allowed to
// commit. Production constructors leave it nil.
func (e *WorkerEngine) SetBeforeCompleteCommitHookForTest(hook func() error) {
	e.beforeCompleteCommit = hook
}

// SetAfterCompleteCommitHookForTest injects a caller-visible failure only after
// Complete has committed. Production constructors leave it nil.
func (e *WorkerEngine) SetAfterCompleteCommitHookForTest(hook func() error) {
	e.afterCompleteCommit = hook
}

type workflowNode struct {
	ID    string   `json:"id"`
	Type  string   `json:"type"`
	Task  string   `json:"task"`
	After []string `json:"after"`
	Input any      `json:"input"`
}

type workflowManifest struct {
	Name         string         `json:"name"`
	InputSchema  any            `json:"inputSchema"`
	OutputSchema any            `json:"outputSchema"`
	Nodes        []workflowNode `json:"nodes"`
	Output       any            `json:"output"`
}

type deploymentManifest struct {
	TargetOS           string   `json:"targetOS"`
	TargetArchitecture string   `json:"targetArchitecture"`
	SecretNames        []string `json:"secretNames"`
	Tasks              []struct {
		Name                string `json:"name"`
		Entrypoint          string `json:"entrypoint"`
		Recovery            string `json:"recovery"`
		TimeoutMs           int64  `json:"timeoutMs"`
		InputSchema         any    `json:"inputSchema"`
		OutputSchema        any    `json:"outputSchema"`
		IdempotencyWindowMs *int64 `json:"idempotencyWindowMs"`
		Retry               struct {
			MaxAttempts    int   `json:"maxAttempts"`
			InitialDelayMs int64 `json:"initialDelayMs"`
			MaxDelayMs     int64 `json:"maxDelayMs"`
		} `json:"retry"`
	} `json:"tasks"`
	Workflows []workflowManifest `json:"workflows"`
}

func (m deploymentManifest) taskRetryPolicy(workflowName, nodeID string) RetryPolicy {
	taskName := nodeID
	for _, wf := range m.Workflows {
		if wf.Name == workflowName {
			for _, n := range wf.Nodes {
				if n.ID == nodeID && n.Task != "" {
					taskName = n.Task
					break
				}
			}
			break
		}
	}
	for _, task := range m.Tasks {
		if task.Name == taskName {
			return NormalizeRetryPolicy(task.Retry.MaxAttempts, task.Retry.InitialDelayMs, task.Retry.MaxDelayMs, task.TimeoutMs, task.Recovery, task.IdempotencyWindowMs)
		}
	}
	// Unknown task reference: never fall back to a retryable policy. The
	// empty recovery fails closed downstream (no automatic retry).
	return NormalizeRetryPolicy(0, 0, 0, 0, "", nil)
}

func (m deploymentManifest) taskRecoveryPolicy(workflowName, nodeID string) (string, int) {
	p := m.taskRetryPolicy(workflowName, nodeID)
	return p.Recovery, p.MaxAttempts
}

func (m deploymentManifest) taskPolicy(workflowName, nodeID string) (string, int64) {
	taskName := nodeID
	for _, workflowDefinition := range m.Workflows {
		if workflowDefinition.Name != workflowName {
			continue
		}
		for _, node := range workflowDefinition.Nodes {
			if node.ID == nodeID && node.Task != "" {
				taskName = node.Task
				break
			}
		}
	}
	for _, task := range m.Tasks {
		if task.Name == taskName {
			timeout := task.TimeoutMs
			if timeout <= 0 {
				timeout = defaultAttemptTimeout.Milliseconds()
			}
			if timeout > MaxAttemptTimeoutMs {
				timeout = MaxAttemptTimeoutMs
			}
			return task.Entrypoint, timeout
		}
	}
	return nodeID, defaultAttemptTimeout.Milliseconds()
}

func (m deploymentManifest) taskSchemas(workflowName, nodeID string) (any, any) {
	taskName := nodeID
	for _, wf := range m.Workflows {
		if wf.Name != workflowName {
			continue
		}
		for _, node := range wf.Nodes {
			if node.ID == nodeID && node.Task != "" {
				taskName = node.Task
				break
			}
		}
	}
	for _, task := range m.Tasks {
		if task.Name == taskName {
			return task.InputSchema, task.OutputSchema
		}
	}
	return nil, nil
}

func stableOperationID(environmentID, runID, nodeID string) string {
	digest := sha256.Sum256([]byte("deadbolt-operation:v1:" + environmentID + ":" + runID + ":" + nodeID))
	return "op_" + hex.EncodeToString(digest[:])
}

func targetArchitecture(os, architecture string) string {
	os = strings.ToLower(strings.TrimSpace(os))
	architecture = strings.ToLower(strings.TrimSpace(architecture))
	if os == "" {
		os = "linux"
	}
	switch architecture {
	case "":
		return ""
	case "amd64", "x64":
		return os + "/amd64"
	case "arm64":
		return os + "/arm64"
	default:
		return architecture
	}
}

// ensureIdempotencyWindowTx sets run_steps.idempotency_valid_until on first
// claim (first-claim + window) and never extends it afterwards.
// Returns the authoritative valid_until (nil when policy has no window).
func ensureIdempotencyWindowTx(ctx context.Context, tx storage.Tx, organizationID, stepID string, policy RetryPolicy) (*time.Time, error) {
	if strings.ToLower(strings.TrimSpace(policy.Recovery)) != "idempotent" || policy.IdempotencyWindowMs == nil {
		return nil, nil
	}
	windowMs := *policy.IdempotencyWindowMs
	var validUntil *time.Time
	if err := tx.QueryRow(ctx, `SELECT idempotency_valid_until FROM run_steps
		WHERE id=$1::uuid AND organization_id=$2::uuid`, stepID, organizationID).Scan(&validUntil); err != nil {
		return nil, err
	}
	if validUntil != nil {
		return validUntil, nil
	}
	var set time.Time
	if err := tx.QueryRow(ctx, `UPDATE run_steps SET idempotency_valid_until=
		clock_timestamp()+($1::bigint*INTERVAL '1 millisecond'), updated_at=clock_timestamp()
		WHERE id=$2::uuid AND organization_id=$3::uuid AND idempotency_valid_until IS NULL
		RETURNING idempotency_valid_until`, windowMs, stepID, organizationID).Scan(&set); err != nil {
		return nil, err
	}
	return &set, nil
}

// claimIdempotencyAdmissionTx resolves the idempotency deadline for a READY
// step about to be claimed. It returns (deadline, holdReason, err):
//   - non-idempotent policy: (nil, "", nil), no admission applies;
//   - true first claim (no prior attempts, no persisted deadline): the window
//     is minted exactly once via ensureIdempotencyWindowTx;
//   - legacy step with prior attempts but NULL deadline: the ORIGINAL
//     first-claim deadline is derived from attempt history, never now+window;
//   - holdReason != "": the deadline cannot be established, so the caller must
//     route to reconciliation without creating an attempt.
func claimIdempotencyAdmissionTx(ctx context.Context, tx storage.Tx, organizationID, stepID string, policy RetryPolicy) (*time.Time, string, error) {
	if strings.ToLower(strings.TrimSpace(policy.Recovery)) != "idempotent" {
		return nil, "", nil
	}
	var valid *time.Time
	var next int
	if err := tx.QueryRow(ctx, `SELECT idempotency_valid_until, next_attempt_number FROM run_steps
		WHERE id=$1::uuid AND organization_id=$2::uuid`, stepID, organizationID).Scan(&valid, &next); err != nil {
		return nil, "", err
	}
	if valid != nil {
		return valid, "", nil
	}
	if policy.IdempotencyWindowMs == nil {
		// No declared dedup bound. A first execution repeats nothing, so it
		// may proceed; any later retry is held by the scheduler instead.
		if next > 1 {
			return nil, "IDEMPOTENCY_WINDOW_UNKNOWN", nil
		}
		return nil, "", nil
	}
	if next == 1 {
		minted, err := ensureIdempotencyWindowTx(ctx, tx, organizationID, stepID, policy)
		if err != nil {
			return nil, "", err
		}
		return minted, "", nil
	}
	derived, err := deriveIdempotencyDeadlineTx(ctx, tx, organizationID, stepID, *policy.IdempotencyWindowMs)
	if err != nil {
		return nil, "", err
	}
	if derived == nil {
		return nil, "IDEMPOTENCY_WINDOW_UNKNOWN", nil
	}
	return derived, "", nil
}

func insertReconciliationCaseTx(ctx context.Context, tx storage.Tx, organizationID, environmentID, stepID string, attemptID *string, reason string, evidence map[string]any) error {
	encoded, err := json.Marshal(evidence)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `INSERT INTO reconciliation_cases
		(organization_id, environment_id, step_id, attempt_id, reason, evidence, status)
		VALUES ($1::uuid,$2::uuid,$3::uuid,$4::uuid,$5,$6::jsonb,'OPEN')`,
		organizationID, environmentID, stepID, attemptID, reason, string(encoded))
	return err
}

// routeStepToReconciliationTx moves a step into WAITING/RECONCILIATION with
// an OPEN reconciliation case. It never creates an attempt and never extends
// the idempotency window. The run parks in WAITING/RECONCILIATION only once
// sibling attempts drain; while siblings are still active the run stays
// RUNNING and the hold is enforced by the run-wide claim guard instead.
func routeStepToReconciliationTx(ctx context.Context, tx storage.Tx, organizationID, environmentID, runID, stepID, nodeID string, attemptID *string, reason string, evidence map[string]any) error {
	if _, err := tx.Exec(ctx, `UPDATE run_steps SET state='WAITING', wait_reason='RECONCILIATION', updated_at=clock_timestamp()
		WHERE id=$1::uuid AND organization_id=$2::uuid`, stepID, organizationID); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `UPDATE runs SET status='WAITING', reason_code='RECONCILIATION', updated_at=clock_timestamp()
		WHERE id=$1::uuid AND organization_id=$2::uuid AND status IN ('QUEUED','RUNNING','WAITING')
		AND NOT EXISTS (SELECT 1 FROM task_attempts a
			JOIN run_steps rs ON rs.id=a.step_id AND rs.organization_id=a.organization_id
			WHERE rs.run_id=$1::uuid AND a.organization_id=$2::uuid
				AND a.status IN ('CLAIMED','RUNNING'))`, runID, organizationID); err != nil {
		return err
	}
	if err := insertReconciliationCaseTx(ctx, tx, organizationID, environmentID, stepID, attemptID, reason, evidence); err != nil {
		return err
	}
	return appendRunEvent(ctx, tx, organizationID, runID, "STEP_WAITING", map[string]any{
		"stepId": stepID, "nodeId": nodeID, "reason": "RECONCILIATION", "holdReason": reason,
	})
}

// deriveIdempotencyDeadlineTx reconstructs the original first-claim + window
// deadline for a legacy step that has attempt history but no persisted
// idempotency_valid_until, and backfills it (only where still NULL).
// It returns (nil, nil) when no attempt history exists. It never mints a
// fresh now+window deadline.
func deriveIdempotencyDeadlineTx(ctx context.Context, tx storage.Tx, organizationID, stepID string, windowMs int64) (*time.Time, error) {
	var firstClaim time.Time
	if err := tx.QueryRow(ctx, `SELECT created_at FROM task_attempts
		WHERE step_id=$1::uuid AND organization_id=$2::uuid
		ORDER BY attempt_number ASC LIMIT 1`, stepID, organizationID).Scan(&firstClaim); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	derived := firstClaim.Add(time.Duration(windowMs) * time.Millisecond)
	var stored time.Time
	if err := tx.QueryRow(ctx, `UPDATE run_steps SET idempotency_valid_until=$1, updated_at=clock_timestamp()
		WHERE id=$2::uuid AND organization_id=$3::uuid AND idempotency_valid_until IS NULL
		RETURNING idempotency_valid_until`, derived, stepID, organizationID).Scan(&stored); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			// A concurrent backfill won the race; re-read the authoritative value.
			if err := tx.QueryRow(ctx, `SELECT idempotency_valid_until FROM run_steps
				WHERE id=$1::uuid AND organization_id=$2::uuid`, stepID, organizationID).Scan(&stored); err != nil {
				return nil, err
			}
			return &stored, nil
		}
		return nil, err
	}
	return &stored, nil
}

// scheduleRetryOrHoldTx persists a retry intent (WAITING/RETRY_BACKOFF +
// PENDING timer with a once-chosen due_at) or routes to reconciliation /
// terminal failure when guards forbid retry. It returns "retry", "hold", or
// "fail". The caller has already closed the failed attempt and deleted its
// lease; this function owns step/run/timer/case transitions atomically.
func scheduleRetryOrHoldTx(ctx context.Context, tx storage.Tx, organizationID, environmentID, runID, stepID, nodeID, failedAttemptID string, failedAttemptNumber int, policy RetryPolicy, errorCode string, retryable bool, effectStatus string, retryAfterRaw string, runStatus, runReason string, runDeadline *time.Time, environmentIDForOp string) (string, string, error) {
	var dbNow time.Time
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&dbNow); err != nil {
		return "", "", err
	}
	// Lock run and step in canonical order (run -> step) before timer work.
	var lockedRunStatus, lockedRunReason string
	var lockedRunDeadline *time.Time
	if err := tx.QueryRow(ctx, `SELECT status, COALESCE(reason_code,''), deadline_at FROM runs
		WHERE id=$1::uuid AND organization_id=$2::uuid FOR UPDATE`, runID, organizationID).Scan(&lockedRunStatus, &lockedRunReason, &lockedRunDeadline); err != nil {
		return "", "", err
	}
	var nextAttemptNumber int
	var currentWaitReason *string
	var validUntil *time.Time
	if err := tx.QueryRow(ctx, `SELECT next_attempt_number, wait_reason, idempotency_valid_until FROM run_steps
		WHERE id=$1::uuid AND organization_id=$2::uuid FOR UPDATE`, stepID, organizationID).Scan(&nextAttemptNumber, &currentWaitReason, &validUntil); err != nil {
		return "", "", err
	}

	// Terminal states never reopen (INV-09). If the run already terminalized
	// (e.g. sibling fail-fast), do not schedule.
	switch lockedRunStatus {
	case "SUCCEEDED", "FAILED", "CANCELLED", "CANCELLING", "PAUSING", "PAUSED":
		return "fail", lockedRunStatus, nil
	}
	if !CanScheduleRetry(lockedRunStatus, lockedRunReason) {
		return "fail", errorCode, nil
	}
	// Deterministic terminal failures never consume budget.
	if IsNonRetryableCode(errorCode) {
		return "fail", errorCode, nil
	}
	// Absent/invalid recovery policy fails closed: malformed persisted
	// manifests must never become permission to repeat customer work.
	if !IsValidRecoveryPolicy(policy.Recovery) {
		return "fail", "INVALID_RECOVERY_POLICY", nil
	}
	// Reconcile policy with ambiguous outcome => hold, never blind retry.
	if RequiresReconciliation(policy.Recovery, effectStatus) {
		evidence := map[string]any{
			"operationId": stableOperationID(environmentIDForOp, runID, nodeID),
			"attemptId":   failedAttemptID, "errorCode": errorCode, "effectStatus": effectStatus,
		}
		if err := routeStepToReconciliationTx(ctx, tx, organizationID, environmentID, runID, stepID, nodeID, &failedAttemptID, "AMBIGUOUS_OUTCOME", evidence); err != nil {
			return "", "", err
		}
		return "hold", "", nil
	}
	// Worker-declared non-retryable (and not ambiguous-hold) => fail.
	if !retryable {
		return "fail", errorCode, nil
	}
	// Budget includes the first attempt.
	if !HasRetryBudget(nextAttemptNumber, policy.MaxAttempts) {
		return "fail", "MAX_ATTEMPTS_EXCEEDED", nil
	}
	// Idempotent window admission: the deadline is first-claim + window and is
	// never extended or reset by retry, restart, or migration compatibility.
	// Insufficient window => hold, never blind retry.
	if strings.ToLower(strings.TrimSpace(policy.Recovery)) == "idempotent" {
		if policy.IdempotencyWindowMs == nil {
			// No declared dedup bound: a retry cannot be proven covered.
			evidence := map[string]any{
				"operationId": stableOperationID(environmentIDForOp, runID, nodeID),
				"attemptId":   failedAttemptID, "errorCode": errorCode,
			}
			if err := routeStepToReconciliationTx(ctx, tx, organizationID, environmentID, runID, stepID, nodeID, &failedAttemptID, "IDEMPOTENCY_WINDOW_UNKNOWN", evidence); err != nil {
				return "", "", err
			}
			return "hold", "", nil
		}
		if validUntil == nil {
			// Legacy/migrated row without a persisted first-claim deadline.
			// Derive the ORIGINAL deadline from the earliest persisted attempt
			// (first claim time + window). Never mint now+window here.
			derived, err := deriveIdempotencyDeadlineTx(ctx, tx, organizationID, stepID, *policy.IdempotencyWindowMs)
			if err != nil {
				return "", "", err
			}
			if derived == nil {
				// No authoritative history to derive from: fail closed.
				evidence := map[string]any{
					"operationId": stableOperationID(environmentIDForOp, runID, nodeID),
					"attemptId":   failedAttemptID, "errorCode": errorCode,
				}
				if err := routeStepToReconciliationTx(ctx, tx, organizationID, environmentID, runID, stepID, nodeID, &failedAttemptID, "IDEMPOTENCY_WINDOW_UNKNOWN", evidence); err != nil {
					return "", "", err
				}
				return "hold", "", nil
			}
			validUntil = derived
		}
		if validUntil != nil && InsufficientIdempotencyWindow(dbNow, *validUntil, policy.TimeoutMs) {
			evidence := map[string]any{
				"operationId": stableOperationID(environmentIDForOp, runID, nodeID),
				"attemptId":   failedAttemptID, "errorCode": errorCode,
				"idempotencyValidUntil": validUntil.UTC().Format(time.RFC3339Nano),
			}
			if err := routeStepToReconciliationTx(ctx, tx, organizationID, environmentID, runID, stepID, nodeID, &failedAttemptID, "IDEMPOTENCY_WINDOW_INSUFFICIENT", evidence); err != nil {
				return "", "", err
			}
			return "hold", "", nil
		}
	}

	// Compute once-chosen due_at with full jitter + capped Retry-After.
	var retryAfterMs *int64
	if strings.TrimSpace(retryAfterRaw) != "" {
		if ms, ok := ParseRetryAfter(retryAfterRaw, dbNow); ok {
			retryAfterMs = &ms
		}
	}
	delayMs := ComputeRetryDelayMs(failedAttemptNumber, policy.InitialDelayMs, policy.MaxDelayMs, SampleJitter(), retryAfterMs)
	dueAt := dbNow.Add(time.Duration(delayMs) * time.Millisecond)

	// Run deadline guard: not enough time left => fail, never schedule.
	deadlineRef := runDeadline
	if deadlineRef == nil {
		deadlineRef = lockedRunDeadline
	}
	if InsufficientRunDeadline(dueAt, policy.TimeoutMs, deadlineRef) {
		return "fail", "RUN_DEADLINE_EXCEEDED", nil
	}

	operationID := stableOperationID(environmentIDForOp, runID, nodeID)
	// Persist durable timer first; partial unique index guarantees one PENDING
	// timer per (org, kind, reference=step). Duplicate schedule is idempotent.
	var timerID string
	err := tx.QueryRow(ctx, `INSERT INTO timers
		(organization_id, environment_id, kind, reference_id, run_id, step_id, attempt_number, operation_id, reason, due_at, state)
		VALUES ($1::uuid,$2::uuid,'RETRY_BACKOFF',$3::uuid,$4::uuid,$3::uuid,$5,$6,$7,$8,'PENDING')
		ON CONFLICT DO NOTHING
		RETURNING id::text`,
		organizationID, environmentID, stepID, runID, nextAttemptNumber, operationID, errorCode, dueAt).Scan(&timerID)
	if err != nil {
		// ON CONFLICT DO NOTHING with RETURNING yields NoRows when the pending
		// timer already exists: restart-safe idempotency, do not redraw.
		if errors.Is(err, pgx.ErrNoRows) {
			var existingDue time.Time
			if qErr := tx.QueryRow(ctx, `SELECT due_at FROM timers
				WHERE organization_id=$1::uuid AND kind='RETRY_BACKOFF' AND reference_id=$2::uuid AND state='PENDING'`,
				organizationID, stepID).Scan(&existingDue); qErr == nil {
				dueAt = existingDue
			}
		} else {
			return "", "", err
		}
	}

	if _, err := tx.Exec(ctx, `UPDATE run_steps SET state='WAITING', wait_reason='RETRY_BACKOFF',
		eligible_at=$1, updated_at=clock_timestamp()
		WHERE id=$2::uuid AND organization_id=$3::uuid`, dueAt, stepID, organizationID); err != nil {
		return "", "", err
	}
	if _, err := tx.Exec(ctx, `UPDATE runs SET status='WAITING', reason_code='RETRY_BACKOFF', updated_at=clock_timestamp()
		WHERE id=$1::uuid AND organization_id=$2::uuid AND status IN ('QUEUED','RUNNING')`, runID, organizationID); err != nil {
		return "", "", err
	}
	if err := appendRunEvent(ctx, tx, organizationID, runID, "STEP_WAITING", map[string]any{
		"stepId": stepID, "nodeId": nodeID, "reason": "RETRY_BACKOFF",
		"dueAt": dueAt.UTC().Format(time.RFC3339Nano), "attemptNumber": nextAttemptNumber,
		"operationId": operationID,
	}); err != nil {
		return "", "", err
	}
	return "retry", "", nil
}

func failRunForStepTx(ctx context.Context, tx storage.Tx, organizationID, runID, stepID, reasonCode string, actorID *string) error {
	// Fail-fast is a single durable settlement: preserve successful siblings,
	// but revoke every other live owner and leave a stop command for workers
	// that may still be executing customer code.  The run is terminal even if
	// a worker is late to observe its stop (the stop record is the durable
	// hand-off, not an implicit rollback claim).
	type liveFailureAttempt struct {
		attemptID string
		stepID    string
		started   bool
	}
	rows, err := tx.Query(ctx, `SELECT a.id::text, rs.id::text,
			(a.status='RUNNING' OR a.started_at IS NOT NULL)
		FROM runs r
		JOIN run_steps rs ON rs.run_id=r.id AND rs.organization_id=r.organization_id
		JOIN task_attempts a ON a.step_id=rs.id AND a.organization_id=rs.organization_id
		WHERE r.id=$1::uuid AND r.organization_id=$2::uuid
		  AND a.status IN ('CLAIMED','RUNNING')
		ORDER BY rs.id, a.id
		FOR UPDATE OF r, rs, a`, runID, organizationID)
	if err != nil {
		return err
	}
	live := make([]liveFailureAttempt, 0)
	for rows.Next() {
		var item liveFailureAttempt
		if err := rows.Scan(&item.attemptID, &item.stepID, &item.started); err != nil {
			rows.Close()
			return err
		}
		live = append(live, item)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()
	var stopCount int
	for _, item := range live {
		effectStatus := "NOT_APPLIED"
		if item.started {
			effectStatus = "UNKNOWN"
		}
		if _, err := tx.Exec(ctx, `UPDATE task_attempts SET status='CANCELLED', completed_at=clock_timestamp(),
			error=jsonb_build_object('code','SIBLING_FAILED','message','A dependency failed and the run was failed fast','retryable',false,'effectStatus',$1::text)
			WHERE id=$2::uuid AND organization_id=$3::uuid AND status IN ('CLAIMED','RUNNING')`, effectStatus, item.attemptID, organizationID); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `DELETE FROM task_leases WHERE attempt_id=$1::uuid AND organization_id=$2::uuid`, item.attemptID, organizationID); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO stop_commands
			(organization_id, attempt_id, reason, deadline_at)
			VALUES ($1::uuid, $2::uuid, 'SIBLING_FAILED', clock_timestamp()+INTERVAL '10 seconds')`, organizationID, item.attemptID); err != nil {
			return err
		}
		stopCount++
	}
	if _, err := tx.Exec(ctx, `UPDATE run_steps SET state='FAILED', wait_reason=$1, updated_at=clock_timestamp()
		WHERE id=$2::uuid AND organization_id=$3::uuid`, reasonCode, stepID, organizationID); err != nil {
		return err
	}
	// Holds mooted by run failure close with the deciding actor (or NULL for
	// system terminalization) so no OPEN case survives a terminal run.
	if err := closeOpenCasesTx(ctx, tx, organizationID, runID, actorID, reasonCode, "FAIL"); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `UPDATE runs SET status='FAILED', reason_code=$1, updated_at=clock_timestamp()
		WHERE id=$2::uuid AND organization_id=$3::uuid`, reasonCode, runID, organizationID); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `UPDATE run_steps SET state='CANCELLED', updated_at=clock_timestamp()
		WHERE run_id=$1::uuid AND organization_id=$2::uuid AND state IN ('BLOCKED','READY','WAITING')`, runID, organizationID); err != nil {
		return err
	}
	// Pending timers die with the run so a later firing cannot resurrect it.
	if _, err := tx.Exec(ctx, `UPDATE timers SET state='CANCELLED', updated_at=clock_timestamp()
		WHERE run_id=$1::uuid AND organization_id=$2::uuid AND state='PENDING'`, runID, organizationID); err != nil {
		return err
	}
	return appendRunEvent(ctx, tx, organizationID, runID, "RUN_FAILED", map[string]any{
		"status": "FAILED", "reason": reasonCode, "stepId": stepID, "stopCount": stopCount,
	})
}

// FireDueRetryTimers fires due RETRY_BACKOFF timers: due firing creates READY,
// claim alone creates the next attempt. Duplicate firing is safe: the
// Timer FIRED transition is conditional on PENDING and the step transition on
// WAITING/RETRY_BACKOFF, so only one firer wins. Guard failures (paused, hold,
// cancelling, terminal) keep the timer PENDING with its original due_at.
func (e *WorkerEngine) FireDueRetryTimers(ctx context.Context, organizationID string) (int, error) {
	var fired int
	var affected []string
	err := e.pool.WithTenantTx(ctx, organizationID, func(ctx context.Context, tx storage.Tx) error {
		n, runs, err := fireDueRetryTimersTx(ctx, tx, organizationID)
		if err != nil {
			return err
		}
		fired = n
		affected = runs
		return nil
	})
	if err != nil {
		return 0, err
	}
	if e.hub != nil {
		for _, rID := range affected {
			e.hub.Publish(rID)
		}
	}
	return fired, nil
}

type retryTimerCandidate struct {
	timerID   string
	stepID    string
	runID     string
	dueAt     time.Time
	nodeID    string
	runStatus string
	runReason string
}

func fireDueRetryTimersTx(ctx context.Context, tx storage.Tx, organizationID string) (int, []string, error) {
	// Candidate scan without locks; ownership is granted only after acquiring
	// run -> step -> timer locks in order and revalidating under those locks.
	rows, err := tx.Query(ctx, `SELECT t.id::text, t.step_id::text, t.run_id::text, t.due_at,
			rs.node_id, r.status, COALESCE(r.reason_code,'')
		FROM timers t
		JOIN runs r ON r.id=t.run_id AND r.organization_id=t.organization_id
		JOIN run_steps rs ON rs.id=t.step_id AND rs.organization_id=t.organization_id
		WHERE t.organization_id=$1::uuid AND t.kind='RETRY_BACKOFF' AND t.state='PENDING'
			AND t.due_at <= clock_timestamp()
		ORDER BY t.due_at, t.id
		LIMIT 50`, organizationID)
	if err != nil {
		return 0, nil, fmt.Errorf("query due retry timers: %w", err)
	}
	candidates := make([]retryTimerCandidate, 0)
	for rows.Next() {
		var c retryTimerCandidate
		if err := rows.Scan(&c.timerID, &c.stepID, &c.runID, &c.dueAt, &c.nodeID, &c.runStatus, &c.runReason); err != nil {
			rows.Close()
			return 0, nil, err
		}
		candidates = append(candidates, c)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return 0, nil, err
	}
	rows.Close()
	if len(candidates) == 0 {
		return 0, nil, nil
	}
	var dbNow time.Time
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&dbNow); err != nil {
		return 0, nil, err
	}
	fired := 0
	affectedMap := make(map[string]struct{})
	for _, c := range candidates {
		// Lock order: run -> step -> timer (Blueprint §11.2).
		var runStatus, runReason string
		var runDeadline *time.Time
		if err := tx.QueryRow(ctx, `SELECT status, COALESCE(reason_code,''), deadline_at FROM runs
			WHERE id=$1::uuid AND organization_id=$2::uuid FOR UPDATE`, c.runID, organizationID).Scan(&runStatus, &runReason, &runDeadline); err != nil {
			continue
		}
		var stepState, waitReason string
		var eligibleAt *time.Time
		if err := tx.QueryRow(ctx, `SELECT state, COALESCE(wait_reason,''), eligible_at FROM run_steps
			WHERE id=$1::uuid AND organization_id=$2::uuid FOR UPDATE`, c.stepID, organizationID).Scan(&stepState, &waitReason, &eligibleAt); err != nil {
			continue
		}
		var timerState string
		var timerDue time.Time
		if err := tx.QueryRow(ctx, `SELECT state, due_at FROM timers
			WHERE id=$1::uuid AND organization_id=$2::uuid FOR UPDATE`, c.timerID, organizationID).Scan(&timerState, &timerDue); err != nil {
			continue
		}
		// Revalidate under locks: timer must still be pending and due.
		if timerState != "PENDING" || timerDue.After(dbNow) {
			continue
		}
		if stepState != "WAITING" || waitReason != "RETRY_BACKOFF" {
			continue
		}
		if !CanFireRetry(runStatus, runReason) {
			// Guard holds: keep waiting with original due_at (no reset).
			continue
		}
		if runDeadline != nil && !dbNow.Before(*runDeadline) {
			// Run deadline passed while parked: never resurrect. The
			// overdue sweep terminalizes the run; the timer stays pending.
			continue
		}
		// Conditional fire: only one scheduler wins.
		tag, err := tx.Exec(ctx, `UPDATE timers SET state='FIRED', fired_at=clock_timestamp(), updated_at=clock_timestamp()
			WHERE id=$1::uuid AND organization_id=$2::uuid AND state='PENDING'`, c.timerID, organizationID)
		if err != nil {
			return 0, nil, err
		}
		if tag.RowsAffected() == 0 {
			continue
		}
		if _, err := tx.Exec(ctx, `UPDATE run_steps SET state='READY', wait_reason=NULL,
			eligible_at=$1, updated_at=clock_timestamp()
			WHERE id=$2::uuid AND organization_id=$3::uuid AND state='WAITING' AND wait_reason='RETRY_BACKOFF'`,
			timerDue, c.stepID, organizationID); err != nil {
			return 0, nil, err
		}
		// Release run from retry wait; preserve started/never-started distinction.
		if _, err := tx.Exec(ctx, `UPDATE runs SET status=CASE
				WHEN EXISTS (SELECT 1 FROM task_attempts ta JOIN run_steps rst ON rst.id=ta.step_id WHERE rst.run_id=$1::uuid AND ta.started_at IS NOT NULL) THEN 'RUNNING'
				ELSE 'QUEUED'
			END, reason_code=NULL, updated_at=clock_timestamp()
			WHERE id=$1::uuid AND organization_id=$2::uuid AND status='WAITING' AND reason_code='RETRY_BACKOFF'`,
			c.runID, organizationID); err != nil {
			return 0, nil, err
		}
		if err := appendRunEvent(ctx, tx, organizationID, c.runID, "STEP_READY", map[string]any{
			"stepId": c.stepID, "nodeId": c.nodeID, "reason": "RETRY_DUE",
			"dueAt": timerDue.UTC().Format(time.RFC3339Nano),
		}); err != nil {
			return 0, nil, err
		}
		fired++
		affectedMap[c.runID] = struct{}{}
	}
	affected := make([]string, 0, len(affectedMap))
	for rID := range affectedMap {
		affected = append(affected, rID)
	}
	return fired, affected, nil
}

type claimMatch struct {
	stepID        string
	runID         string
	nodeID        string
	input         any
	bundle        string
	manifest      []byte
	workflowName  string
	runDeadline   *time.Time
	environmentID string
}

// pendingClaim snapshots one candidate plus its immutable artifact
// requirements while TX A holds locks. Provider verification runs after TX A
// commits (no authoritative Claim transaction open); TX B revalidates before
// claiming so stale snapshots are never claimed.
type pendingClaim struct {
	match           claimMatch
	assignmentInput any
	refs            []string
	manifest        deploymentManifest
	entrypoint      string
	timeoutMs       int64
	policy          RetryPolicy
	verified        bool
}

func equalRefSets(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	counts := make(map[string]int, len(a))
	for _, id := range a {
		counts[id]++
	}
	for _, id := range b {
		counts[id]--
		if counts[id] < 0 {
			return false
		}
	}
	for _, c := range counts {
		if c != 0 {
			return false
		}
	}
	return true
}

func (e *WorkerEngine) verifyPendingClaimsOutsideTx(ctx context.Context, orgID string, pending []pendingClaim) error {
	for i := range pending {
		if len(pending[i].refs) == 0 {
			pending[i].verified = true
			continue
		}
		if e.artifacts == nil {
			pending[i].verified = false
			continue
		}
		err := e.artifacts.VerifyReferences(ctx, orgID, pending[i].refs)
		if err == nil {
			pending[i].verified = true
			continue
		}
		if errors.Is(err, artifacts.ErrArtifactNotFound) ||
			errors.Is(err, artifacts.ErrArtifactNotReady) ||
			errors.Is(err, artifacts.ErrObjectNotFound) ||
			errors.Is(err, artifacts.ErrSizeMismatch) ||
			errors.Is(err, artifacts.ErrChecksumMismatch) {
			pending[i].verified = false
			continue
		}
		return err
	}
	return nil
}

func (e *WorkerEngine) Claim(ctx context.Context, session *worker.WorkerSessionContext, req *worker.PollRequestDTO) (*worker.PollResponseDTO, error) {
	if req.WorkerID != session.WorkerID || req.SessionID != session.SessionID {
		return nil, worker.ErrUnauthorized
	}
	assignments := make([]worker.AssignmentDTO, 0)
	if req.AvailableSlots <= 0 {
		return &worker.PollResponseDTO{ProtocolVersion: worker.ProtocolVersion, RequestID: req.RequestID, Assignments: assignments}, nil
	}

	// Disaster recovery dispatch gate (Blueprint §27.3). Fail closed: a
	// missing or unreadable controls row must not grant new task ownership.
	var dispatchEnabled bool
	var recMode string
	if err := e.pool.QueryRow(ctx, `SELECT dispatch_enabled, mode FROM system_recovery_controls WHERE id = 1`).Scan(&dispatchEnabled, &recMode); err != nil {
		return nil, worker.ErrRecoveryControlsUnavailable
	}
	if !dispatchEnabled || recMode == "READ_ONLY" || recMode == "DISASTER_RECOVERY" {
		return &worker.PollResponseDTO{ProtocolVersion: worker.ProtocolVersion, RequestID: req.RequestID, Assignments: assignments}, nil
	}

	var reconciledRuns []string

	// TX A snapshots candidates plus immutable artifact requirements and
	// commits before any provider I/O. No authoritative Claim transaction
	// remains open while S3 HEAD/GET/hash runs below.
	effectivePool := session.PoolName
	if effectivePool == "" {
		effectivePool = "default"
	}
	var pending []pendingClaim
	// Workers are environment-bound: every authenticated session carries its
	// environment (see app.authenticate_worker_session). A worker must never
	// execute work from another environment, so candidate selection is
	// strictly env-scoped FIFO (eligible_at, id) per Blueprint 19.3.
	// Cross-environment fairness lives in the control-plane scheduler
	// (ReconcileReadyWork), which is org-scoped and may consider all
	// environments without granting any worker foreign authority.
	if session.EnvironmentID == "" {
		return &worker.PollResponseDTO{ProtocolVersion: worker.ProtocolVersion, RequestID: req.RequestID, Assignments: assignments}, nil
	}
	snapshotErr := e.pool.WithTenantTx(ctx, session.OrganizationID, func(ctx context.Context, tx storage.Tx) error {
		query := `SELECT rs.id::text, rs.run_id::text, rs.node_id, r.input,
					d.bundle_digest, d.manifest, r.workflow_name, r.deadline_at, rs.environment_id::text
				FROM run_steps rs
				JOIN runs r ON r.id=rs.run_id AND r.organization_id=rs.organization_id
				JOIN deployments d ON d.id=r.deployment_id AND d.organization_id=r.organization_id
				JOIN worker_deployments wd ON wd.session_id=$1::uuid AND wd.bundle_digest=d.bundle_digest
				JOIN worker_sessions candidate_ws ON candidate_ws.id=wd.session_id
				JOIN workers candidate_w ON candidate_w.id=candidate_ws.worker_id
				WHERE rs.organization_id=$2::uuid AND rs.environment_id=$3::uuid
					AND (candidate_w.pool_name=$5 OR $5 = '' OR candidate_w.pool_name='default')
					AND rs.state='READY' AND rs.eligible_at <= clock_timestamp()
					AND (r.status IN ('QUEUED','RUNNING') OR (r.status='WAITING' AND r.reason_code='QUOTA_WAIT'))
					AND (r.deadline_at IS NULL OR clock_timestamp() < r.deadline_at)
					AND NOT EXISTS (SELECT 1 FROM reconciliation_cases rc
						JOIN run_steps hrs ON hrs.id=rc.step_id AND hrs.organization_id=rc.organization_id
						WHERE hrs.run_id=rs.run_id AND hrs.organization_id=rs.organization_id
							AND rc.organization_id=$2::uuid AND rc.status='OPEN')
				ORDER BY rs.eligible_at, rs.id
				LIMIT $4
				FOR UPDATE OF r, rs SKIP LOCKED`
		args := []any{session.SessionID, session.OrganizationID, session.EnvironmentID, req.AvailableSlots, effectivePool}
		rows, err := tx.Query(ctx, query, args...)
		if err != nil {
			return fmt.Errorf("snapshot claim candidates: %w", err)
		}
		var matches []claimMatch
		for rows.Next() {
			var m claimMatch
			var inputJSON []byte
			if err := rows.Scan(&m.stepID, &m.runID, &m.nodeID, &inputJSON, &m.bundle, &m.manifest, &m.workflowName, &m.runDeadline, &m.environmentID); err != nil {
				rows.Close()
				return err
			}
			if len(inputJSON) > 0 {
				if err := json.Unmarshal(inputJSON, &m.input); err != nil {
					rows.Close()
					return fmt.Errorf("decode run input: %w", err)
				}
			}
			matches = append(matches, m)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return err
		}
		rows.Close()
		for _, m := range matches {
			var manifest deploymentManifest
			if err := json.Unmarshal(m.manifest, &manifest); err != nil {
				return fmt.Errorf("decode deployment manifest: %w", err)
			}
			entrypoint, timeoutMs := manifest.taskPolicy(m.workflowName, m.nodeID)
			policy := manifest.taskRetryPolicy(m.workflowName, m.nodeID)
			if policy.TimeoutMs > 0 {
				timeoutMs = policy.TimeoutMs
			}
			assignmentInput := m.input
			var targetNode *workflowNode
			for _, wf := range manifest.Workflows {
				if wf.Name != m.workflowName {
					continue
				}
				for i := range wf.Nodes {
					if wf.Nodes[i].ID == m.nodeID {
						targetNode = &wf.Nodes[i]
						break
					}
				}
				break
			}
			if targetNode != nil && targetNode.Input != nil {
				outRows, err := tx.Query(ctx, `SELECT node_id, output FROM run_steps
					WHERE run_id=$1::uuid AND organization_id=$2::uuid AND output IS NOT NULL`,
					m.runID, session.OrganizationID)
				if err != nil {
					return fmt.Errorf("snapshot mapped outputs: %w", err)
				}
				outputsMap := make(map[string]any)
				for outRows.Next() {
					var nodeID string
					var rawOutput []byte
					if err := outRows.Scan(&nodeID, &rawOutput); err != nil {
						outRows.Close()
						return err
					}
					var output any
					if err := json.Unmarshal(rawOutput, &output); err != nil {
						outRows.Close()
						return fmt.Errorf("decode mapped step output: %w", err)
					}
					outputsMap[nodeID] = output
				}
				if err := outRows.Err(); err != nil {
					outRows.Close()
					return err
				}
				outRows.Close()
				mapped, err := contracts.MapInput(targetNode.Input, m.input, outputsMap)
				if err != nil {
					return fmt.Errorf("map task input: %w", err)
				}
				inputSchema, _ := manifest.taskSchemas(m.workflowName, m.nodeID)
				if inputSchema != nil && contracts.ValidatePayload(inputSchema, mapped) != nil {
					return fmt.Errorf("map task input: %w", ErrSchemaViolation)
				}
				assignmentInput = mapped
			}
			pending = append(pending, pendingClaim{
				match: m, assignmentInput: assignmentInput,
				refs:     artifacts.CollectArtifactRefs(assignmentInput),
				manifest: manifest, entrypoint: entrypoint,
				timeoutMs: timeoutMs, policy: policy,
			})
		}
		return nil
	})
	if snapshotErr != nil {
		return nil, snapshotErr
	}
	// Provider verification with no authoritative Claim DB transaction open.
	// A slow or blocked object store must not stall execution coordination.
	if err := e.verifyPendingClaimsOutsideTx(ctx, session.OrganizationID, pending); err != nil {
		return nil, err
	}

	err := e.pool.WithTenantTx(ctx, session.OrganizationID, func(ctx context.Context, tx storage.Tx) error {
		var workerStatus string
		var revokedAt *time.Time
		var sessionExpiry, dbNow time.Time
		if err := tx.QueryRow(ctx, `SELECT w.status,ws.revoked_at,ws.expires_at
			FROM workers w JOIN worker_sessions ws ON ws.worker_id=w.id AND ws.organization_id=w.organization_id
			WHERE w.id=$1::uuid AND w.organization_id=$2::uuid AND ws.id=$3::uuid
			FOR UPDATE OF w,ws`, session.WorkerID, session.OrganizationID, session.SessionID).Scan(&workerStatus, &revokedAt, &sessionExpiry); err != nil {
			return err
		}
		if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&dbNow); err != nil {
			return err
		}
		if revokedAt != nil {
			return worker.ErrSessionRevoked
		}
		if !dbNow.Before(sessionExpiry) {
			return worker.ErrSessionExpired
		}
		if workerStatus == "REVOKED" {
			return worker.ErrWorkerRevoked
		}
		if workerStatus != "ACTIVE" {
			return nil
		}

		// Serialize admission before locking runs and their steps, then derive
		// capacity from live leases while holding that admission row.
		// The admission scope is always the worker's own environment:
		// sessions are environment-bound, so cross-environment admission is
		// never evaluated at the worker claim boundary.
		var environmentID string
		var maxConcurrency int = 10
		if err := tx.QueryRow(ctx, `SELECT environment_id::text,max_concurrency FROM environment_admissions
			WHERE environment_id=$1::uuid AND organization_id=$2::uuid FOR UPDATE`, session.EnvironmentID, session.OrganizationID).Scan(&environmentID, &maxConcurrency); err != nil {
			return err
		}
		if maxConcurrency > 10 {
			maxConcurrency = 10
		}

		// Reconcile expired leases and unstarted claims before evaluating capacity and ready steps
		if _, runs, err := e.reconcileExpiredLeasesTx(ctx, tx, session.OrganizationID); err != nil {
			return err
		} else {
			reconciledRuns = append(reconciledRuns, runs...)
		}

		// Fire due retry timers in the same admission transaction so a poll
		// observes newly READY retries without an extra round-trip. Firing is
		// idempotent and preserves the original due_at across restarts.
		if _, runs, err := fireDueRetryTimersTx(ctx, tx, session.OrganizationID); err != nil {
			return err
		} else {
			reconciledRuns = append(reconciledRuns, runs...)
		}

		var activeLeases int
		if environmentID != "" {
			if err := tx.QueryRow(ctx, `SELECT count(*) FROM task_leases l
				JOIN run_steps rs ON rs.id=l.step_id AND rs.organization_id=l.organization_id
				WHERE rs.environment_id=$1::uuid AND rs.organization_id=$2::uuid
					AND clock_timestamp() < l.expires_at`, environmentID, session.OrganizationID).Scan(&activeLeases); err != nil {
				return err
			}
		}
		// The MVP worker/task boundary is two concurrent assignments per
		// session. The worker may advertise fewer free slots, but cannot raise
		// the server-side cap by sending an arbitrary value.
		claimLimit := req.AvailableSlots
		if claimLimit > worker.DefaultSlots {
			claimLimit = worker.DefaultSlots
		}
		if remaining := maxConcurrency - activeLeases; remaining < claimLimit {
			claimLimit = remaining
		}
		var poolActiveLeases int
		if environmentID != "" {
			if err := tx.QueryRow(ctx, `SELECT count(*)
				FROM task_leases l
				JOIN run_steps rs ON rs.id=l.step_id AND rs.organization_id=l.organization_id
				JOIN worker_sessions leased_ws ON leased_ws.id=l.session_id
				JOIN workers leased_w ON leased_w.id=leased_ws.worker_id
				WHERE rs.environment_id=$1::uuid AND rs.organization_id=$2::uuid
				  AND leased_w.pool_name=$3 AND clock_timestamp() < l.expires_at`,
				environmentID, session.OrganizationID, effectivePool).Scan(&poolActiveLeases); err != nil {
				return err
			}
			if remaining := maxConcurrency - poolActiveLeases; remaining < claimLimit {
				claimLimit = remaining
			}
		}
		if claimLimit <= 0 {
			// Keep accepted work queued with an explicit durable reason. The
			// candidate query above includes this state, so capacity becoming
			// available is enough to resume claiming without a separate queue.
			if environmentID != "" {
				if _, err := tx.Exec(ctx, `UPDATE runs r
					SET status='WAITING', reason_code='QUOTA_WAIT', updated_at=clock_timestamp()
					WHERE r.organization_id=$1::uuid AND r.environment_id=$2::uuid
					  AND r.status IN ('QUEUED','RUNNING')
					  AND EXISTS (
						SELECT 1 FROM run_steps rs
						WHERE rs.run_id=r.id AND rs.organization_id=r.organization_id
						  AND rs.environment_id=r.environment_id AND rs.state='READY'
						  AND rs.eligible_at <= clock_timestamp()
					  )`, session.OrganizationID, environmentID); err != nil {
					return err
				}
			}
			return nil
		}

		// TX B reuses TX A snapshots and never runs provider I/O. Candidates
		// that became READY after the snapshot wait for the next poll so the
		// authoritative claim never waits for S3 while holding locks.
		if len(pending) > claimLimit {
			pending = pending[:claimLimit]
		}

		for _, pc := range pending {
			match := pc.match
			// Defense in depth: never transition work outside the worker's
			// own environment, even if a snapshot row somehow disagrees.
			if match.environmentID != session.EnvironmentID {
				continue
			}
			var manifest deploymentManifest
			if err := json.Unmarshal(match.manifest, &manifest); err != nil {
				return fmt.Errorf("decode deployment manifest: %w", err)
			}
			entrypoint, timeoutMs := manifest.taskPolicy(match.workflowName, match.nodeID)
			policy := manifest.taskRetryPolicy(match.workflowName, match.nodeID)
			// Prefer normalized timeout for window/deadline guards.
			if policy.TimeoutMs > 0 {
				timeoutMs = policy.TimeoutMs
			}
			// First-claim idempotency expiry: set once, never extend (Blueprint §14.1).
			// Legacy steps that already have attempts but no persisted deadline
			// resolve the ORIGINAL first-claim deadline instead of minting
			// now+window; unresolvable steps hold without creating an attempt.
			validUntil, holdReason, err := claimIdempotencyAdmissionTx(ctx, tx, session.OrganizationID, match.stepID, policy)
			if err != nil {
				return err
			}
			if holdReason != "" {
				evidence := map[string]any{
					"operationId": stableOperationID(session.EnvironmentID, match.runID, match.nodeID),
					"timeoutMs":   timeoutMs,
				}
				if err := routeStepToReconciliationTx(ctx, tx, session.OrganizationID, environmentID, match.runID, match.stepID, match.nodeID, nil, holdReason, evidence); err != nil {
					return err
				}
				reconciledRuns = append(reconciledRuns, match.runID)
				continue
			}
			if validUntil != nil && InsufficientIdempotencyWindow(dbNow, *validUntil, timeoutMs) {
				// Insufficient window routes to reconciliation; no attempt is
				// created and no budget is spent (Blueprint §14.1).
				evidence := map[string]any{
					"operationId":           stableOperationID(session.EnvironmentID, match.runID, match.nodeID),
					"timeoutMs":             timeoutMs,
					"idempotencyValidUntil": validUntil.UTC().Format(time.RFC3339Nano),
				}
				if err := routeStepToReconciliationTx(ctx, tx, session.OrganizationID, environmentID, match.runID, match.stepID, match.nodeID, nil, "IDEMPOTENCY_WINDOW_INSUFFICIENT", evidence); err != nil {
					return err
				}
				reconciledRuns = append(reconciledRuns, match.runID)
				continue
			}
			assignmentInput := match.input
			var targetNode *workflowNode
			for _, wf := range manifest.Workflows {
				if wf.Name != match.workflowName {
					continue
				}
				for i := range wf.Nodes {
					if wf.Nodes[i].ID == match.nodeID {
						targetNode = &wf.Nodes[i]
						break
					}
				}
				break
			}
			if targetNode != nil && targetNode.Input != nil {
				outRows, err := tx.Query(ctx, `SELECT node_id, output FROM run_steps
					WHERE run_id=$1::uuid AND organization_id=$2::uuid AND output IS NOT NULL`,
					match.runID, session.OrganizationID)
				if err != nil {
					return fmt.Errorf("load mapped step outputs: %w", err)
				}
				outputsMap := make(map[string]any)
				for outRows.Next() {
					var nodeID string
					var rawOutput []byte
					if err := outRows.Scan(&nodeID, &rawOutput); err != nil {
						outRows.Close()
						return err
					}
					var output any
					if err := json.Unmarshal(rawOutput, &output); err != nil {
						outRows.Close()
						return fmt.Errorf("decode mapped step output: %w", err)
					}
					outputsMap[nodeID] = output
				}
				if err := outRows.Err(); err != nil {
					outRows.Close()
					return err
				}
				outRows.Close()
				mapped, err := contracts.MapInput(targetNode.Input, match.input, outputsMap)
				if err != nil {
					return fmt.Errorf("map task input: %w", err)
				}
				inputSchema, _ := manifest.taskSchemas(match.workflowName, match.nodeID)
				if inputSchema != nil && contracts.ValidatePayload(inputSchema, mapped) != nil {
					return fmt.Errorf("map task input: %w", ErrSchemaViolation)
				}
				assignmentInput = mapped
			}

			// TX B revalidation: the snapshot's artifact requirements must still
			// apply. Mapping is recomputed under canonical locks; a drift
			// means the pre-verification is stale, so this candidate waits for
			// the next poll instead of claiming stale data.
			currentRefs := artifacts.CollectArtifactRefs(assignmentInput)
			if !equalRefSets(currentRefs, pc.refs) {
				continue
			}
			// Consumer integrity admission uses the provider verification that
			// ran after TX A committed (no Claim transaction open). Here we
			// only revalidate durable DB state and never touch S3.
			if len(currentRefs) > 0 {
				if !pc.verified {
					missing := currentRefs
					if _, uErr := tx.Exec(ctx, `UPDATE run_steps SET state='FAILED',wait_reason='ARTIFACT_UNAVAILABLE',updated_at=clock_timestamp()
						WHERE id=$1::uuid AND organization_id=$2::uuid AND state='READY'`, match.stepID, session.OrganizationID); uErr != nil {
						return uErr
					}
					// Only terminalize the run when this candidate still owns
					// the READY slot; a concurrent claim winner keeps its work.
					var failed bool
					if err := tx.QueryRow(ctx, `SELECT state='FAILED' FROM run_steps WHERE id=$1::uuid AND organization_id=$2::uuid`, match.stepID, session.OrganizationID).Scan(&failed); err == nil && failed {
						if _, uErr := tx.Exec(ctx, `UPDATE runs SET status='FAILED',reason_code='ARTIFACT_UNAVAILABLE',updated_at=clock_timestamp()
							WHERE id=$1::uuid AND organization_id=$2::uuid`, match.runID, session.OrganizationID); uErr != nil {
							return uErr
						}
						if _, uErr := tx.Exec(ctx, `UPDATE run_steps SET state='CANCELLED',updated_at=clock_timestamp()
							WHERE run_id=$1::uuid AND organization_id=$2::uuid AND state IN ('BLOCKED','READY','WAITING')`,
							match.runID, session.OrganizationID); uErr != nil {
							return uErr
						}
						if err := appendRunEvent(ctx, tx, session.OrganizationID, match.runID, "RUN_FAILED", map[string]any{
							"reason": "ARTIFACT_UNAVAILABLE", "nodeId": match.nodeID, "artifactIds": missing,
						}); err != nil {
							return err
						}
					}
					continue
				}
				// Verified outside, but the DB rows must still be READY under
				// this transaction's locks; otherwise the association would
				// dangle.
				ready := map[string]bool{}
				if e.artifacts != nil {
					got, err := e.artifacts.ReadyArtifactIDsTx(ctx, tx, session.OrganizationID, currentRefs)
					if err != nil {
						return err
					}
					ready = got
				}
				allReady := len(currentRefs) > 0
				for _, id := range currentRefs {
					if !ready[id] {
						allReady = false
						break
					}
				}
				if !allReady {
					missing := currentRefs
					if _, uErr := tx.Exec(ctx, `UPDATE run_steps SET state='FAILED',wait_reason='ARTIFACT_UNAVAILABLE',updated_at=clock_timestamp()
						WHERE id=$1::uuid AND organization_id=$2::uuid AND state='READY'`, match.stepID, session.OrganizationID); uErr != nil {
						return uErr
					}
					var failed bool
					if err := tx.QueryRow(ctx, `SELECT state='FAILED' FROM run_steps WHERE id=$1::uuid AND organization_id=$2::uuid`, match.stepID, session.OrganizationID).Scan(&failed); err == nil && failed {
						if _, uErr := tx.Exec(ctx, `UPDATE runs SET status='FAILED',reason_code='ARTIFACT_UNAVAILABLE',updated_at=clock_timestamp()
							WHERE id=$1::uuid AND organization_id=$2::uuid`, match.runID, session.OrganizationID); uErr != nil {
							return uErr
						}
						if _, uErr := tx.Exec(ctx, `UPDATE run_steps SET state='CANCELLED',updated_at=clock_timestamp()
							WHERE run_id=$1::uuid AND organization_id=$2::uuid AND state IN ('BLOCKED','READY','WAITING')`,
							match.runID, session.OrganizationID); uErr != nil {
							return uErr
						}
						if err := appendRunEvent(ctx, tx, session.OrganizationID, match.runID, "RUN_FAILED", map[string]any{
							"reason": "ARTIFACT_UNAVAILABLE", "nodeId": match.nodeID, "artifactIds": missing,
						}); err != nil {
							return err
						}
					}
					continue
				}
			}

			var epoch int64
			var attemptNumber int
			if err := tx.QueryRow(ctx, `UPDATE run_steps
				SET state='RUNNING', wait_reason=NULL, current_epoch=current_epoch+1,
					next_attempt_number=next_attempt_number+1, updated_at=clock_timestamp()
				WHERE id=$1::uuid AND organization_id=$2::uuid AND state='READY'
				RETURNING current_epoch, next_attempt_number-1`, match.stepID, session.OrganizationID).Scan(&epoch, &attemptNumber); err != nil {
				if errors.Is(err, pgx.ErrNoRows) {
					// Lost the race after the snapshot: another worker
					// claimed first. Skip without failing the run.
					continue
				}
				return err
			}

			var attemptID string
			var claimDeadline time.Time
			if err := tx.QueryRow(ctx, `INSERT INTO task_attempts
				(organization_id,step_id,attempt_number,session_id,epoch,status,claim_start_deadline_at,attempt_timeout_ms)
				VALUES ($1::uuid,$2::uuid,$3,$4::uuid,$5,'CLAIMED',clock_timestamp()+INTERVAL '5 seconds',$6)
				RETURNING id::text,claim_start_deadline_at`, session.OrganizationID, match.stepID, attemptNumber, session.SessionID, epoch, timeoutMs).Scan(&attemptID, &claimDeadline); err != nil {
				return fmt.Errorf("insert claimed attempt: %w", err)
			}

			var leaseExpiry time.Time
			if err := tx.QueryRow(ctx, `INSERT INTO task_leases
				(step_id,organization_id,attempt_id,session_id,epoch,expires_at)
				VALUES ($1::uuid,$2::uuid,$3::uuid,$4::uuid,$5,clock_timestamp()+INTERVAL '30 seconds')
				RETURNING expires_at`, match.stepID, session.OrganizationID, attemptID, session.SessionID, epoch).Scan(&leaseExpiry); err != nil {
				return fmt.Errorf("insert attempt lease: %w", err)
			}
			if err := appendRunEvent(ctx, tx, session.OrganizationID, match.runID, "TASK_CLAIMED", map[string]any{
				"attemptId": attemptID, "stepId": match.stepID, "epoch": epoch, "sessionId": session.SessionID,
			}); err != nil {
				return err
			}

			runDeadline := ""
			if match.runDeadline != nil {
				runDeadline = match.runDeadline.UTC().Format(time.RFC3339Nano)
			}

			assignments = append(assignments, worker.AssignmentDTO{
				RunID: match.runID, StepID: match.stepID, AttemptID: attemptID, OwnershipEpoch: epoch,
				TaskEntrypoint: entrypoint, Input: assignmentInput, DeploymentDigest: match.bundle, BundleDigest: match.bundle,
				OperationID: stableOperationID(session.EnvironmentID, match.runID, match.nodeID),
				LeaseTTLMs:  worker.DefaultLeaseTTL.Milliseconds(), LeaseExpiresAt: leaseExpiry.UTC().Format(time.RFC3339Nano),
				ClaimStartDeadlineAt: claimDeadline.UTC().Format(time.RFC3339Nano), AttemptTimeoutMs: timeoutMs,
				RunDeadlineAt:      runDeadline,
				TraceContext:       worker.TraceContextDTO{Traceparent: "00-00000000000000000000000000000000-0000000000000000-01"},
				TargetArchitecture: targetArchitecture(manifest.TargetOS, manifest.TargetArchitecture), SecretNames: manifest.SecretNames,
				TargetOS: manifest.TargetOS,
			})
			if _, err := tx.Exec(ctx, `UPDATE runs
				SET status='RUNNING', reason_code=NULL, updated_at=clock_timestamp()
				WHERE id=$1::uuid AND organization_id=$2::uuid AND reason_code='QUOTA_WAIT'`,
				match.runID, session.OrganizationID); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if e.hub != nil {
		published := make(map[string]struct{})
		for _, a := range assignments {
			published[a.RunID] = struct{}{}
			e.hub.Publish(a.RunID)
		}
		for _, rID := range reconciledRuns {
			if _, ok := published[rID]; !ok {
				published[rID] = struct{}{}
				e.hub.Publish(rID)
			}
		}
	}
	return &worker.PollResponseDTO{ProtocolVersion: worker.ProtocolVersion, RequestID: req.RequestID, Assignments: assignments}, nil
}

func (e *WorkerEngine) Start(ctx context.Context, session *worker.WorkerSessionContext, req *worker.StartRequestDTO) (*worker.StartResponseDTO, error) {
	if req.WorkerID != session.WorkerID || req.SessionID != session.SessionID {
		return nil, worker.ErrUnauthorized
	}
	var deadline time.Time
	var runID string
	err := e.pool.WithTenantTx(ctx, session.OrganizationID, func(ctx context.Context, tx storage.Tx) error {
		var status, ownerSession string
		var epoch int64
		var claimDeadline, storedDeadline, runDeadline *time.Time
		var timeoutMs int64
		err := tx.QueryRow(ctx, `SELECT r.id::text,a.status,a.epoch,a.session_id::text,
				a.claim_start_deadline_at,a.deadline_at,a.attempt_timeout_ms,r.deadline_at
			FROM runs r JOIN run_steps rs ON rs.run_id=r.id AND rs.organization_id=r.organization_id
			JOIN task_attempts a ON a.step_id=rs.id AND a.organization_id=rs.organization_id
			WHERE a.id=$1::uuid AND a.organization_id=$2::uuid
			FOR UPDATE OF r,rs,a`, req.AttemptID, session.OrganizationID).Scan(
			&runID, &status, &epoch, &ownerSession, &claimDeadline, &storedDeadline, &timeoutMs, &runDeadline)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return worker.ErrAttemptNotFound
			}
			return err
		}
		var leaseExpiry time.Time
		var leaseEpoch int64
		var leaseSession string
		if err := tx.QueryRow(ctx, `SELECT epoch,session_id::text,expires_at FROM task_leases
			WHERE attempt_id=$1::uuid AND organization_id=$2::uuid FOR UPDATE`, req.AttemptID, session.OrganizationID).Scan(
			&leaseEpoch, &leaseSession, &leaseExpiry); err != nil {
			return worker.ErrStaleOwnership
		}
		var dbNow time.Time
		if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&dbNow); err != nil {
			return err
		}
		if epoch != req.OwnershipEpoch || ownerSession != session.SessionID || leaseEpoch != req.OwnershipEpoch || leaseSession != session.SessionID || !dbNow.Before(leaseExpiry) {
			return worker.ErrStaleOwnership
		}
		if status == "RUNNING" {
			if storedDeadline == nil || !dbNow.Before(*storedDeadline) {
				return worker.ErrStaleOwnership
			}
			deadline = *storedDeadline
			return nil
		}
		if status != "CLAIMED" {
			return worker.ErrStaleOwnership
		}
		if claimDeadline == nil || !dbNow.Before(*claimDeadline) || (runDeadline != nil && !dbNow.Before(*runDeadline)) {
			return worker.ErrStartDeadlineExceeded
		}

		if err := tx.QueryRow(ctx, `UPDATE task_attempts a SET status='RUNNING',started_at=clock_timestamp(),
				deadline_at=LEAST(clock_timestamp()+a.attempt_timeout_ms*INTERVAL '1 millisecond',
					COALESCE(r.deadline_at,'infinity'::timestamptz))
			FROM run_steps rs JOIN runs r ON r.id=rs.run_id AND r.organization_id=rs.organization_id
			WHERE a.id=$1::uuid AND a.organization_id=$2::uuid AND rs.id=a.step_id
			RETURNING a.deadline_at`, req.AttemptID, session.OrganizationID).Scan(&deadline); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE runs SET status='RUNNING',reason_code=NULL,updated_at=clock_timestamp()
			WHERE id=$1::uuid AND organization_id=$2::uuid AND status='QUEUED'`, runID, session.OrganizationID); err != nil {
			return err
		}
		return appendRunEvent(ctx, tx, session.OrganizationID, runID, "TASK_STARTED", map[string]any{
			"attemptId": req.AttemptID, "epoch": req.OwnershipEpoch, "deadlineAt": deadline.UTC().Format(time.RFC3339Nano), "timeoutMs": timeoutMs,
		})
	})
	if err != nil {
		if errors.Is(err, ErrHistoryLimitExceeded) {
			_ = e.TerminalizeHistoryLimitExceeded(ctx, session.OrganizationID, runID)
		}
		return nil, err
	}
	if e.hub != nil {
		e.hub.Publish(runID)
	}
	return &worker.StartResponseDTO{ProtocolVersion: worker.ProtocolVersion, RequestID: req.RequestID,
		AttemptID: req.AttemptID, OwnershipEpoch: req.OwnershipEpoch, Accepted: true,
		AttemptDeadlineAt: deadline.UTC().Format(time.RFC3339Nano), LeaseTTLMs: worker.DefaultLeaseTTL.Milliseconds()}, nil
}

func stop(item worker.HeartbeatAttemptDTO, reason string) worker.StopCommandDTO {
	return worker.StopCommandDTO{AttemptID: item.AttemptID, OwnershipEpoch: item.OwnershipEpoch, Reason: reason, GraceTimeoutMs: 10000}
}

func (e *WorkerEngine) Heartbeat(ctx context.Context, session *worker.WorkerSessionContext, req *worker.HeartbeatRequestDTO) (*worker.HeartbeatResponseDTO, error) {
	if req.WorkerID != session.WorkerID || req.SessionID != session.SessionID {
		return nil, worker.ErrUnauthorized
	}
	renewals := make([]worker.LeaseRenewalDTO, 0, len(req.Attempts))
	stops := make([]worker.StopCommandDTO, 0)
	err := e.pool.WithTenantTx(ctx, session.OrganizationID, func(ctx context.Context, tx storage.Tx) error {
		if _, err := tx.Exec(ctx, `UPDATE workers SET last_seen_at=clock_timestamp()
			WHERE id=$1::uuid AND organization_id=$2::uuid`, session.WorkerID, session.OrganizationID); err != nil {
			return err
		}
		for _, item := range req.Attempts {
			var status, ownerSession string
			var epoch int64
			var leaseExpiry time.Time
			var attemptDeadline, runDeadline *time.Time
			err := tx.QueryRow(ctx, `SELECT a.status,a.epoch,a.session_id::text,l.expires_at,a.deadline_at,r.deadline_at
				FROM runs r JOIN run_steps rs ON rs.run_id=r.id AND rs.organization_id=r.organization_id
				JOIN task_attempts a ON a.step_id=rs.id AND a.organization_id=rs.organization_id
				JOIN task_leases l ON l.attempt_id=a.id AND l.organization_id=a.organization_id
				WHERE a.id=$1::uuid AND a.organization_id=$2::uuid
				FOR UPDATE OF r,rs,a,l`, item.AttemptID, session.OrganizationID).Scan(
				&status, &epoch, &ownerSession, &leaseExpiry, &attemptDeadline, &runDeadline)
			if err != nil {
				if errors.Is(err, pgx.ErrNoRows) {
					stops = append(stops, stop(item, "LEASE_NOT_FOUND"))
					continue
				}
				return err
			}
			var dbNow time.Time
			if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&dbNow); err != nil {
				return err
			}
			if epoch != item.OwnershipEpoch || ownerSession != session.SessionID {
				stops = append(stops, stop(item, "STALE_OWNERSHIP"))
				continue
			}
			if status != "RUNNING" || attemptDeadline == nil {
				stops = append(stops, stop(item, "ATTEMPT_NOT_RUNNING"))
				continue
			}
			if !dbNow.Before(leaseExpiry) || !dbNow.Before(*attemptDeadline) || (runDeadline != nil && !dbNow.Before(*runDeadline)) {
				stops = append(stops, stop(item, "DEADLINE_EXPIRED"))
				continue
			}

			var stopReason string
			err = tx.QueryRow(ctx, `SELECT reason FROM stop_commands
				WHERE attempt_id=$1::uuid AND organization_id=$2::uuid AND acked_at IS NULL LIMIT 1`, item.AttemptID, session.OrganizationID).Scan(&stopReason)
			if err == nil {
				stops = append(stops, stop(item, stopReason))
				continue
			}
			if !errors.Is(err, pgx.ErrNoRows) {
				return err
			}

			var newExpiry time.Time
			if err := tx.QueryRow(ctx, `UPDATE task_leases l SET expires_at=LEAST(
				clock_timestamp()+INTERVAL '30 seconds',a.deadline_at,COALESCE(r.deadline_at,'infinity'::timestamptz))
				FROM task_attempts a JOIN run_steps rs ON rs.id=a.step_id AND rs.organization_id=a.organization_id
				JOIN runs r ON r.id=rs.run_id AND r.organization_id=rs.organization_id
				WHERE l.attempt_id=a.id AND l.attempt_id=$1::uuid AND l.organization_id=$2::uuid
				RETURNING l.expires_at`, item.AttemptID, session.OrganizationID).Scan(&newExpiry); err != nil {
				return err
			}
			renewals = append(renewals, worker.LeaseRenewalDTO{AttemptID: item.AttemptID, OwnershipEpoch: item.OwnershipEpoch,
				LeaseTTLMs: worker.DefaultLeaseTTL.Milliseconds(), LeaseExpiresAt: newExpiry.UTC().Format(time.RFC3339Nano)})
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &worker.HeartbeatResponseDTO{ProtocolVersion: worker.ProtocolVersion, RequestID: req.RequestID, Renewals: renewals, Stops: stops}, nil
}

func validOutcome(outcome string) bool {
	switch outcome {
	case "SUCCEEDED", "FAILED", "TIMED_OUT", "CANCELLED":
		return true
	default:
		return false
	}
}

func terminalAttempt(status string) bool {
	switch status {
	case "SUCCEEDED", "FAILED", "TIMED_OUT", "LOST", "CANCELLED":
		return true
	default:
		return false
	}
}

// artifactsReady enforces consumer integrity: referenced artifacts must be
// READY with intact objects. Integrity absences fail closed as unverified;
// transport failures propagate. A missing service fails closed.
func (e *WorkerEngine) artifactsReady(ctx context.Context, orgID string, ids []string) (bool, error) {
	if e.artifacts == nil {
		return false, nil
	}
	if err := e.artifacts.VerifyReferences(ctx, orgID, ids); err != nil {
		if errors.Is(err, artifacts.ErrArtifactNotFound) ||
			errors.Is(err, artifacts.ErrObjectNotFound) ||
			errors.Is(err, artifacts.ErrSizeMismatch) ||
			errors.Is(err, artifacts.ErrChecksumMismatch) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// artifactFailureCode maps association failures to terminal codes. Every
// mapping is a deterministic failure: the producer is never silently rerun.
func artifactFailureCode(err error) string {
	switch {
	case errors.Is(err, artifacts.ErrArtifactNotFound):
		return "ARTIFACT_NOT_FOUND"
	case errors.Is(err, artifacts.ErrArtifactNotReady):
		return "ARTIFACT_NOT_READY"
	case errors.Is(err, artifacts.ErrNotOwned):
		return "ARTIFACT_NOT_OWNED"
	default:
		return "ARTIFACT_UNAVAILABLE"
	}
}

// advanceAfterStepSuccessTx unblocks dependency-satisfied steps and
// terminalizes the run when every node is terminal. It is shared by worker
// completion and audited reconciliation success so downstream scheduling
// cannot diverge between the two paths.
func advanceAfterStepSuccessTx(ctx context.Context, tx storage.Tx, organizationID, runID string, defaultOutput any) error {
	var manifestBytes []byte
	var workflowName string
	var rawRunInput []byte
	if err := tx.QueryRow(ctx, `SELECT d.manifest, r.workflow_name, r.input
		FROM runs r JOIN deployments d ON d.id=r.deployment_id AND d.organization_id=r.organization_id
		WHERE r.id=$1::uuid AND r.organization_id=$2::uuid`, runID, organizationID).Scan(&manifestBytes, &workflowName, &rawRunInput); err != nil {
		return err
	}
	var manifest deploymentManifest
	if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
		return fmt.Errorf("decode manifest: %w", err)
	}
	var targetWorkflow *workflowManifest
	for i := range manifest.Workflows {
		if manifest.Workflows[i].Name == workflowName {
			targetWorkflow = &manifest.Workflows[i]
			break
		}
	}
	var runInput any
	if len(rawRunInput) > 0 {
		_ = json.Unmarshal(rawRunInput, &runInput)
	}

	type stepData struct {
		id     string
		nodeID string
		state  string
		output any
	}
	stepRows, err := tx.Query(ctx, `SELECT id::text, node_id, state, output
		FROM run_steps WHERE run_id=$1::uuid AND organization_id=$2::uuid
		FOR UPDATE`, runID, organizationID)
	if err != nil {
		return err
	}
	stepsByNode := make(map[string]*stepData)
	outputsMap := make(map[string]any)
	for stepRows.Next() {
		var s stepData
		var rawOut []byte
		if err := stepRows.Scan(&s.id, &s.nodeID, &s.state, &rawOut); err != nil {
			stepRows.Close()
			return err
		}
		if len(rawOut) > 0 && string(rawOut) != "null" {
			_ = json.Unmarshal(rawOut, &s.output)
			outputsMap[s.nodeID] = s.output
		}
		stepsByNode[s.nodeID] = &s
	}
	stepRows.Close()

	if targetWorkflow != nil {
		// Re-evaluate until stable because manifests need not be topologically
		// ordered; skipped propagation must not leave a downstream join stuck.
		for changed := true; changed; {
			changed = false
			for _, node := range targetWorkflow.Nodes {
				st, ok := stepsByNode[node.ID]
				if !ok || st.state != "BLOCKED" {
					continue
				}
				allDepsMet := true
				dependencySkipped := false
				for _, depID := range node.After {
					depStep, depExists := stepsByNode[depID]
					if !depExists {
						allDepsMet = false
						break
					}
					if depStep.state == "SKIPPED" {
						dependencySkipped = true
					}
					if depStep.state != "SUCCEEDED" && depStep.state != "SKIPPED" {
						allDepsMet = false
					}
				}
				if allDepsMet {
					if dependencySkipped {
						if _, err := tx.Exec(ctx, `UPDATE run_steps SET state='SKIPPED',wait_reason='DEPENDENCY_SKIPPED',updated_at=clock_timestamp()
						WHERE id=$1::uuid AND organization_id=$2::uuid AND state='BLOCKED'`, st.id, organizationID); err != nil {
							return err
						}
						st.state = "SKIPPED"
						changed = true
						if err := appendRunEvent(ctx, tx, organizationID, runID, "STEP_SKIPPED", map[string]any{
							"stepId": st.id, "nodeId": node.ID, "reason": "DEPENDENCY_SKIPPED",
						}); err != nil {
							return err
						}
						continue
					}
					// Evaluate input mapping if present
					if node.Input != nil {
						mapped, mapErr := contracts.MapInput(node.Input, runInput, outputsMap)
						inputSchema, _ := manifest.taskSchemas(workflowName, node.ID)
						if mapErr != nil || (inputSchema != nil && contracts.ValidatePayload(inputSchema, mapped) != nil) {
							// Non-retryable mapping error per Blueprint §10.4 & §14.2
							return failRunForStepTx(ctx, tx, organizationID, runID, st.id, "INPUT_MAPPING_ERROR", nil)
						}
					}
					// Unblock to READY
					if _, err := tx.Exec(ctx, `UPDATE run_steps SET state='READY',wait_reason=NULL,eligible_at=clock_timestamp(),updated_at=clock_timestamp()
					WHERE id=$1::uuid AND organization_id=$2::uuid`, st.id, organizationID); err != nil {
						return err
					}
					st.state = "READY"
					changed = true
					if err := appendRunEvent(ctx, tx, organizationID, runID, "STEP_READY", map[string]any{
						"stepId": st.id, "nodeId": node.ID,
					}); err != nil {
						return err
					}
				}
			}
		}

		// Check if all nodes are terminal
		allTerminal := true
		allSucceeded := true
		for _, node := range targetWorkflow.Nodes {
			st, ok := stepsByNode[node.ID]
			if !ok || (st.state != "SUCCEEDED" && st.state != "SKIPPED" && st.state != "FAILED" && st.state != "CANCELLED") {
				allTerminal = false
				break
			}
			if st.state != "SUCCEEDED" && st.state != "SKIPPED" {
				allSucceeded = false
			}
		}

		if allTerminal {
			if allSucceeded {
				var finalOutput any = defaultOutput
				if targetWorkflow.Output != nil {
					mappedOut, outErr := contracts.MapInput(targetWorkflow.Output, runInput, outputsMap)
					if outErr != nil {
						// Output mapping error -> fail run
						if _, uErr := tx.Exec(ctx, `UPDATE runs SET status='FAILED',reason_code='OUTPUT_MAPPING_ERROR',updated_at=clock_timestamp()
							WHERE id=$1::uuid AND organization_id=$2::uuid`, runID, organizationID); uErr != nil {
							return uErr
						}
						return appendRunEvent(ctx, tx, organizationID, runID, "RUN_FAILED", map[string]any{
							"reason": "OUTPUT_MAPPING_ERROR",
						})
					}
					finalOutput = mappedOut
				}
				if targetWorkflow.OutputSchema != nil && contracts.ValidatePayload(targetWorkflow.OutputSchema, finalOutput) != nil {
					if _, err := tx.Exec(ctx, `UPDATE runs SET status='FAILED',reason_code='OUTPUT_SCHEMA_VIOLATION',updated_at=clock_timestamp()
						WHERE id=$1::uuid AND organization_id=$2::uuid`, runID, organizationID); err != nil {
						return err
					}
					return appendRunEvent(ctx, tx, organizationID, runID, "RUN_FAILED", map[string]any{"reason": "OUTPUT_SCHEMA_VIOLATION"})
				}
				finalJSON, err := contracts.CanonicalizeGeneric(finalOutput)
				if err != nil || len(finalJSON) > worker.MaxInlinePayloadBytes {
					return worker.ErrPayloadTooLarge
				}
				if _, err := tx.Exec(ctx, `UPDATE runs SET status='SUCCEEDED',output=$1::jsonb,reason_code=NULL,updated_at=clock_timestamp()
					WHERE id=$2::uuid AND organization_id=$3::uuid`, string(finalJSON), runID, organizationID); err != nil {
					return err
				}
				if err := appendRunEvent(ctx, tx, organizationID, runID, "RUN_COMPLETED", map[string]any{
					"status": "SUCCEEDED",
				}); err != nil {
					return err
				}
			} else {
				if _, err := tx.Exec(ctx, `UPDATE runs SET status='FAILED',reason_code='STEP_FAILED',updated_at=clock_timestamp()
					WHERE id=$1::uuid AND organization_id=$2::uuid`, runID, organizationID); err != nil {
					return err
				}
				if err := appendRunEvent(ctx, tx, organizationID, runID, "RUN_FAILED", map[string]any{
					"status": "FAILED",
				}); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func (e *WorkerEngine) Complete(ctx context.Context, session *worker.WorkerSessionContext, req *worker.CompleteRequestDTO) (*worker.CompleteResponseDTO, error) {
	if req.WorkerID != session.WorkerID || req.SessionID != session.SessionID {
		return nil, worker.ErrUnauthorized
	}
	if !validOutcome(req.Outcome) {
		return nil, worker.ErrInvalidOutcome
	}
	if req.Output != nil {
		canonicalOutput, err := contracts.CanonicalizeGeneric(req.Output)
		if err != nil || len(canonicalOutput) > worker.MaxInlinePayloadBytes {
			return nil, worker.ErrPayloadTooLarge
		}
	}
	computedDigest, err := worker.CanonicalCompletionDigest(req)
	if err != nil || req.ResultDigest != computedDigest {
		return nil, worker.ErrResultConflict
	}
	var runID string
	err = e.pool.WithTenantTx(ctx, session.OrganizationID, func(ctx context.Context, tx storage.Tx) error {
		var stepID, nodeID, status, ownerSession, workflowName string
		var manifestBytes []byte
		var epoch int64
		var storedDigest *string
		var attemptDeadline *time.Time
		var attemptNumber int
		var runEnvID, runStatus, runReason string
		var runDeadlineVal *time.Time
		err := tx.QueryRow(ctx, `SELECT r.id::text,rs.id::text,rs.node_id,a.status,a.epoch,a.session_id::text,a.outcome_digest,a.deadline_at,d.manifest,r.workflow_name,
			a.attempt_number,r.environment_id::text,r.status,COALESCE(r.reason_code,''),r.deadline_at
			FROM runs r JOIN run_steps rs ON rs.run_id=r.id AND rs.organization_id=r.organization_id
			JOIN task_attempts a ON a.step_id=rs.id AND a.organization_id=rs.organization_id
			JOIN deployments d ON d.id=r.deployment_id AND d.organization_id=r.organization_id
			WHERE a.id=$1::uuid AND a.organization_id=$2::uuid
			FOR UPDATE OF r,rs,a`, req.AttemptID, session.OrganizationID).Scan(
			&runID, &stepID, &nodeID, &status, &epoch, &ownerSession, &storedDigest, &attemptDeadline, &manifestBytes, &workflowName,
			&attemptNumber, &runEnvID, &runStatus, &runReason, &runDeadlineVal)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return worker.ErrAttemptNotFound
			}
			return err
		}
		// A duplicate ACK is safe only for the same authenticated ownership.
		if epoch != req.OwnershipEpoch || ownerSession != session.SessionID {
			return worker.ErrStaleOwnership
		}
		if terminalAttempt(status) {
			// Stored digest names the submitted result, while status names the
			// engine decision after schema validation. A deterministic validation
			// failure must still ACK an identical authorized delivery.
			if storedDigest != nil && *storedDigest == req.ResultDigest {
				// Cancel-first rejects even identical results: revoking
				// ownership ends the identity, so there is nothing to ACK
				// against (Blueprint §15.4, F-12).
				if status == "CANCELLED" && (runStatus == "CANCELLING" || runStatus == "CANCELLED") {
					return worker.ErrStaleOwnership
				}
				return nil
			}
			return worker.ErrResultConflict
		}
		if status != "RUNNING" || attemptDeadline == nil {
			return worker.ErrStaleOwnership
		}

		var leaseEpoch int64
		var leaseSession string
		var leaseExpiry time.Time
		if err := tx.QueryRow(ctx, `SELECT epoch,session_id::text,expires_at FROM task_leases
			WHERE attempt_id=$1::uuid AND organization_id=$2::uuid FOR UPDATE`, req.AttemptID, session.OrganizationID).Scan(
			&leaseEpoch, &leaseSession, &leaseExpiry); err != nil {
			return worker.ErrStaleOwnership
		}
		var dbNow time.Time
		if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&dbNow); err != nil {
			return err
		}
		if leaseEpoch != req.OwnershipEpoch || leaseSession != session.SessionID || !dbNow.Before(leaseExpiry) || !dbNow.Before(*attemptDeadline) {
			return worker.ErrStaleOwnership
		}
		// Validate a successful worker result before it can change durable state.
		// Contract/schema failures are deterministic terminal failures, never retryable
		// successes with a bad payload.
		if req.Outcome == "SUCCEEDED" {
			artifactResult := false
			if req.ArtifactID != "" {
				// Large results travel as typed references: the artifact must
				// be READY and bound to this attempt by current ownership.
				// Anything else fails closed without touching run state.
				// Artifact bytes are opaque to the control plane, so the
				// reference bypasses output-schema validation.
				if req.Output != nil {
					req.Outcome = "FAILED"
					req.Output = nil
					req.ArtifactID = ""
					req.Error = &worker.TaskErrorDTO{Code: "OUTPUT_SCHEMA_VIOLATION", Message: "Result carries both inline output and an artifact reference", Retryable: false, EffectStatus: "NOT_APPLIED"}
				} else if e.artifacts == nil {
					return fmt.Errorf("artifact association unavailable")
				} else if _, aerr := e.artifacts.LookupForCompletionTx(
					ctx, tx, session.OrganizationID, stepID, req.AttemptID, req.OwnershipEpoch, req.ArtifactID); aerr != nil {
					req.Outcome = "FAILED"
					req.Output = nil
					req.ArtifactID = ""
					req.Error = &worker.TaskErrorDTO{Code: artifactFailureCode(aerr), Message: "Artifact result cannot be associated", Retryable: false, EffectStatus: "UNKNOWN"}
				} else {
					req.Output = map[string]any{artifacts.ArtifactRefKey: req.ArtifactID}
					artifactResult = true
				}
			}
			if req.Outcome == "SUCCEEDED" && !artifactResult {
				var manifest deploymentManifest
				if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
					return fmt.Errorf("decode manifest: %w", err)
				}
				_, outputSchema := manifest.taskSchemas(workflowName, nodeID)
				if outputSchema != nil && contracts.ValidatePayload(outputSchema, req.Output) != nil {
					req.Outcome = "FAILED"
					req.Output = nil
					req.Error = &worker.TaskErrorDTO{Code: "OUTPUT_SCHEMA_VIOLATION", Message: "Task output does not conform to output schema", Retryable: false, EffectStatus: "NOT_APPLIED"}
				}
			}
		}

		errorJSON := "null"
		if req.Error != nil {
			encoded, err := json.Marshal(req.Error)
			if err != nil {
				return err
			}
			errorJSON = string(encoded)
		}
		if _, err := tx.Exec(ctx, `UPDATE task_attempts SET status=$1,outcome_digest=$2,error=$3::jsonb,
			completed_at=clock_timestamp() WHERE id=$4::uuid AND organization_id=$5::uuid`,
			req.Outcome, req.ResultDigest, errorJSON, req.AttemptID, session.OrganizationID); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `DELETE FROM task_leases WHERE attempt_id=$1::uuid AND organization_id=$2::uuid`, req.AttemptID, session.OrganizationID); err != nil {
			return err
		}
		stepState := "FAILED"
		if req.Outcome == "SUCCEEDED" {
			stepState = "SUCCEEDED"
		} else if req.Outcome == "CANCELLED" {
			stepState = "CANCELLED"
		}
		outputJSON := "null"
		if req.Output != nil {
			encoded, err := json.Marshal(req.Output)
			if err != nil {
				return err
			}
			outputJSON = string(encoded)
		}
		if _, err := tx.Exec(ctx, `UPDATE run_steps SET state=$1,output=$2::jsonb,wait_reason=NULL,updated_at=clock_timestamp()
			WHERE id=$3::uuid AND organization_id=$4::uuid`, stepState, outputJSON, stepID, session.OrganizationID); err != nil {
			return err
		}
		if err := appendRunEvent(ctx, tx, session.OrganizationID, runID, "TASK_COMPLETED", map[string]any{
			"attemptId": req.AttemptID, "stepId": stepID, "epoch": req.OwnershipEpoch, "outcome": req.Outcome, "resultDigest": req.ResultDigest,
		}); err != nil {
			return err
		}

		if req.Outcome == "SUCCEEDED" {
			if err := advanceAfterStepSuccessTx(ctx, tx, session.OrganizationID, runID, req.Output); err != nil {
				return err
			}
		} else {
			if req.Outcome == "CANCELLED" {
				// Cancelled attempts never retry; preserve terminal semantics.
				errorCode := "TASK_CANCELLED"
				if req.Error != nil && req.Error.Code != "" {
					errorCode = req.Error.Code
				}
				if err := failRunForStepTx(ctx, tx, session.OrganizationID, runID, stepID, errorCode, nil); err != nil {
					return err
				}
			} else {
				// FAILED/TIMED_OUT: policy-based retry intent with durable timer,
				// budget/deadline/hold guards, and idempotency-window admission.
				// Blueprint §14-15: safe/idempotent/definitive NOT_APPLIED may
				// retry; reconcile UNKNOWN and insufficient windows hold.
				var manifest deploymentManifest
				if len(manifestBytes) > 0 {
					_ = json.Unmarshal(manifestBytes, &manifest)
				}
				policy := manifest.taskRetryPolicy(workflowName, nodeID)
				errorCode := "TASK_FAILED"
				retryable := true
				effectStatus := "UNKNOWN"
				retryAfterRaw := ""
				if req.Error != nil {
					if req.Error.Code != "" {
						errorCode = req.Error.Code
					}
					retryable = req.Error.Retryable
					if req.Error.EffectStatus != "" {
						effectStatus = req.Error.EffectStatus
					}
					retryAfterRaw = req.Error.RetryAfter
				} else if req.Outcome == "TIMED_OUT" {
					errorCode = "ATTEMPT_TIMED_OUT"
					retryable = true
					effectStatus = "UNKNOWN"
				}
				envForTimer := runEnvID
				if envForTimer == "" {
					envForTimer = session.EnvironmentID
				}
				decision, failCode, err := scheduleRetryOrHoldTx(ctx, tx, session.OrganizationID, envForTimer, runID, stepID, nodeID, req.AttemptID, attemptNumber, policy, errorCode, retryable, effectStatus, retryAfterRaw, runStatus, runReason, runDeadlineVal, session.EnvironmentID)
				if err != nil {
					return err
				}
				if decision == "retry" || decision == "hold" {
					// Durable intent persisted; run/step now WAITING with timer or hold.
					// No further fail-fast; attempts are created only by claim.
				} else {
					if failCode == "" {
						failCode = errorCode
					}
					if err := failRunForStepTx(ctx, tx, session.OrganizationID, runID, stepID, failCode, nil); err != nil {
						return err
					}
				}
			}
		}
		// Park the run in WAITING/RECONCILIATION once sibling attempts drain
		// while a hold is open. Terminal runs and runs with live work are
		// left untouched.
		if err := settleHoldAfterCompletionTx(ctx, tx, session.OrganizationID, runID); err != nil {
			return err
		}
		if e.beforeCompleteCommit != nil {
			return e.beforeCompleteCommit()
		}
		return nil
	})
	if err != nil {
		if errors.Is(err, ErrHistoryLimitExceeded) {
			_ = e.TerminalizeHistoryLimitExceeded(ctx, session.OrganizationID, runID)
		}
		return nil, err
	}
	if e.hub != nil {
		e.hub.Publish(runID)
	}
	if e.afterCompleteCommit != nil {
		if err := e.afterCompleteCommit(); err != nil {
			return nil, err
		}
	}
	return &worker.CompleteResponseDTO{ProtocolVersion: worker.ProtocolVersion, RequestID: req.RequestID,
		AttemptID: req.AttemptID, OwnershipEpoch: req.OwnershipEpoch, Accepted: true, ResultDigest: req.ResultDigest}, nil
}

func (e *WorkerEngine) StopAck(ctx context.Context, session *worker.WorkerSessionContext, req *worker.StopAckRequestDTO) (*worker.AckResponseDTO, error) {
	if req.WorkerID != session.WorkerID || req.SessionID != session.SessionID {
		return nil, worker.ErrUnauthorized
	}
	err := e.pool.WithTenantTx(ctx, session.OrganizationID, func(ctx context.Context, tx storage.Tx) error {
		// Resolve the run without locks first; the ordered locks below
		// follow run → attempt (Blueprint §11.2).
		var runID string
		if err := tx.QueryRow(ctx, `SELECT rs.run_id::text FROM run_steps rs
			JOIN task_attempts a ON a.step_id=rs.id AND a.organization_id=rs.organization_id
			WHERE a.id=$1::uuid AND a.organization_id=$2::uuid`,
			req.AttemptID, session.OrganizationID).Scan(&runID); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return nil
			}
			return err
		}
		if _, err := tx.Exec(ctx, `SELECT 1 FROM runs
			WHERE id=$1::uuid AND organization_id=$2::uuid FOR UPDATE`, runID, session.OrganizationID); err != nil {
			return err
		}
		// Fence the ACK to the attempt's original session and epoch:
		// cancellation revokes the lease, but stop confirmation stays bound
		// to the worker/session/epoch that owned the physical process.
		// Session and epoch are immutable after claim, so this read cannot
		// race ownership changes.
		var ownerSession string
		var ownerEpoch int64
		if err := tx.QueryRow(ctx, `SELECT session_id::text, epoch FROM task_attempts
			WHERE id=$1::uuid AND organization_id=$2::uuid FOR UPDATE`,
			req.AttemptID, session.OrganizationID).Scan(&ownerSession, &ownerEpoch); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return nil
			}
			return err
		}
		if ownerSession != session.SessionID || ownerEpoch != req.OwnershipEpoch {
			return worker.ErrStaleOwnership
		}
		_, err := tx.Exec(ctx, `UPDATE stop_commands SET acked_at=COALESCE(acked_at,clock_timestamp()),
			termination_confirmed_at=CASE WHEN $1 THEN clock_timestamp() ELSE termination_confirmed_at END
			WHERE attempt_id=$2::uuid AND organization_id=$3::uuid`,
			req.ProcessStopped, req.AttemptID, session.OrganizationID)
		if err != nil {
			return err
		}
		// An ACK may complete grace settlement; the sweeper covers the rest.
		_, err = settleCancellingRunTx(ctx, tx, session.OrganizationID, runID)
		return err
	})
	if err != nil {
		return nil, err
	}
	return &worker.AckResponseDTO{ProtocolVersion: worker.ProtocolVersion, RequestID: req.RequestID, Accepted: true}, nil
}

type lostAttempt struct {
	attemptID string
	stepID    string
	runID     string
	epoch     int64
}

// FenceWorkerSessions records ownership loss and emits a durable recovery
// handoff before the caller revokes old sessions. It intentionally accepts the
// caller transaction so reconnect/revoke cannot commit half of the transition.
func (e *WorkerEngine) FenceWorkerSessions(ctx context.Context, tx storage.Tx, organizationID, workerID, reason string) error {
	var lockedWorkerID string
	if err := tx.QueryRow(ctx, `SELECT id::text FROM workers
		WHERE id=$1::uuid AND organization_id=$2::uuid FOR UPDATE`, workerID, organizationID).Scan(&lockedWorkerID); err != nil {
		return err
	}
	rows, err := tx.Query(ctx, `SELECT a.id::text,rs.id::text,r.id::text,a.epoch
		FROM runs r JOIN run_steps rs ON rs.run_id=r.id AND rs.organization_id=r.organization_id
		JOIN task_attempts a ON a.step_id=rs.id AND a.organization_id=rs.organization_id
		JOIN worker_sessions ws ON ws.id=a.session_id AND ws.organization_id=a.organization_id
		WHERE ws.worker_id=$1::uuid AND ws.organization_id=$2::uuid
			AND ws.revoked_at IS NULL AND a.status IN ('CLAIMED','RUNNING')
		ORDER BY r.id,rs.id,a.id FOR UPDATE OF r,rs,a`, workerID, organizationID)
	if err != nil {
		return err
	}
	lost := make([]lostAttempt, 0)
	for rows.Next() {
		var item lostAttempt
		if err := rows.Scan(&item.attemptID, &item.stepID, &item.runID, &item.epoch); err != nil {
			rows.Close()
			return err
		}
		lost = append(lost, item)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()

	for _, item := range lost {
		if _, err := tx.Exec(ctx, `UPDATE task_attempts SET status='LOST',completed_at=clock_timestamp(),
			error=jsonb_build_object('code','OWNERSHIP_LOST','reason',$1::text)
			WHERE id=$2::uuid AND organization_id=$3::uuid AND status IN ('CLAIMED','RUNNING')`, reason, item.attemptID, organizationID); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `DELETE FROM task_leases WHERE attempt_id=$1::uuid AND organization_id=$2::uuid`, item.attemptID, organizationID); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE run_steps SET state='WAITING',wait_reason='RECOVERY_HANDOFF',updated_at=clock_timestamp()
			WHERE id=$1::uuid AND organization_id=$2::uuid AND current_epoch=$3 AND state='RUNNING'`, item.stepID, organizationID, item.epoch); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE runs SET status='WAITING',reason_code='RECOVERY_HANDOFF',updated_at=clock_timestamp()
			WHERE id=$1::uuid AND organization_id=$2::uuid AND status IN ('QUEUED','RUNNING')`, item.runID, organizationID); err != nil {
			return err
		}
		if err := appendRunEvent(ctx, tx, organizationID, item.runID, "TASK_LOST", map[string]any{
			"attemptId": item.attemptID, "stepId": item.stepID, "epoch": item.epoch, "reason": reason, "recovery": "HANDOFF_REQUIRED",
		}); err != nil {
			return err
		}
	}
	return nil
}

type expiredAttemptCandidate struct {
	runID                string
	stepID               string
	attemptID            string
	attemptStatus        string
	attemptEpoch         int64
	startedAt            *time.Time
	claimStartDeadlineAt *time.Time
	attemptDeadlineAt    *time.Time
	leaseExpiresAt       *time.Time
	nextAttemptNumber    int
	currentEpoch         int64
	nodeID               string
	workflowName         string
	manifestBytes        []byte
	runStatus            string
	runDeadlineAt        *time.Time
	environmentID        string
	attemptNumber        int
}

// ReconcileExpiredLeases checks for expired leases and unstarted claims across the
// organization, closes them authoritatively as LOST/TIMED_OUT, and restores schedulability
// per Blueprint §13.3.
func (e *WorkerEngine) ReconcileExpiredLeases(ctx context.Context, organizationID string) (int, error) {
	var totalReclaimed int
	var affectedRuns []string
	err := e.pool.WithTenantTx(ctx, organizationID, func(ctx context.Context, tx storage.Tx) error {
		reclaimed, runs, err := e.reconcileExpiredLeasesTx(ctx, tx, organizationID)
		if err != nil {
			return err
		}
		totalReclaimed = reclaimed
		affectedRuns = runs
		fired, firedRuns, err := fireDueRetryTimersTx(ctx, tx, organizationID)
		if err != nil {
			return err
		}
		totalReclaimed += fired
		affectedRuns = append(affectedRuns, firedRuns...)
		// The run deadline stays active while held: terminalize overdue held
		// runs that have no live work left to settle them.
		overdue, overdueRuns, err := failOverdueRunsTx(ctx, tx, organizationID)
		if err != nil {
			return err
		}
		totalReclaimed += overdue
		affectedRuns = append(affectedRuns, overdueRuns...)
		// Durable grace settlement: CANCELLING runs settle once stops are
		// acknowledged or past grace, surviving control-plane restarts.
		settled, settledRuns, err := settleCancellingRunsTx(ctx, tx, organizationID)
		if err != nil {
			return err
		}
		totalReclaimed += settled
		affectedRuns = append(affectedRuns, settledRuns...)
		return nil
	})
	if err != nil {
		return 0, err
	}
	if e.hub != nil {
		for _, runID := range affectedRuns {
			e.hub.Publish(runID)
		}
	}
	return totalReclaimed, nil
}

// ReconcileReadyWork repairs the durable graph after a completion-to-wakeup
// gap (for example, a broker loss or scheduler restart). It deliberately uses
// the same transaction boundary as normal execution: a state transition is
// accompanied by its history event and outbox intent through appendRunEvent.
// The operation is idempotent because only BLOCKED steps are eligible and the
// run/step locks serialize competing workers.
func (e *WorkerEngine) ReconcileReadyWork(ctx context.Context, organizationID string) (int, error) {
	if e == nil || e.pool == nil {
		return 0, fmt.Errorf("ready-work reconciliation requires a database pool")
	}
	var repaired int
	var affectedRuns []string
	err := e.pool.WithTenantTx(ctx, organizationID, func(ctx context.Context, tx storage.Tx) error {
		type readyWorkCandidate struct {
			runID, workflowName string
			manifestBytes       []byte
		}
		// Round-robin candidate selection across environments (Blueprint
		// 19.3): interleave runs so that every environment with eligible work
		// is represented in each bounded batch. The per-environment position
		// is computed in a CTE (window functions cannot appear with FOR
		// UPDATE in the same query level); the outer query orders by that
		// position so no environment starves behind another's backlog.
		// This runs in the org-scoped control-plane scheduler, which may
		// consider all environments; workers remain environment-bound and
		// only ever claim within their own environment.
		rows, err := tx.Query(ctx, `
			WITH candidates AS (
				SELECT r.id AS run_id,
					ROW_NUMBER() OVER (PARTITION BY r.environment_id ORDER BY r.reconciliation_checked_at NULLS FIRST, r.id) AS env_posn,
					r.reconciliation_checked_at AS checked_at
				FROM runs r
				JOIN deployments d ON d.id=r.deployment_id AND d.organization_id=r.organization_id
				WHERE r.organization_id=$1::uuid AND r.status IN ('QUEUED','RUNNING')
					AND EXISTS (
						SELECT 1 FROM run_steps blocked
						WHERE blocked.run_id=r.id AND blocked.organization_id=r.organization_id
							AND blocked.state='BLOCKED'
					)
			)
			SELECT r.id::text, r.workflow_name, d.manifest
			FROM candidates c
			JOIN runs r ON r.id=c.run_id AND r.organization_id=$1::uuid
			JOIN deployments d ON d.id=r.deployment_id AND d.organization_id=r.organization_id
			ORDER BY c.env_posn, c.checked_at NULLS FIRST, r.id
			LIMIT 50
			FOR UPDATE OF r SKIP LOCKED`, organizationID)
		if err != nil {
			return fmt.Errorf("query ready-work candidates: %w", err)
		}
		candidates := make([]readyWorkCandidate, 0, 50)
		for rows.Next() {
			var runID, workflowName string
			var manifestBytes []byte
			if err := rows.Scan(&runID, &workflowName, &manifestBytes); err != nil {
				rows.Close()
				return fmt.Errorf("scan ready-work candidate: %w", err)
			}
			candidates = append(candidates, readyWorkCandidate{runID: runID, workflowName: workflowName, manifestBytes: manifestBytes})
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return err
		}
		rows.Close()

		for _, candidate := range candidates {
			runID := candidate.runID
			workflowName := candidate.workflowName
			manifestBytes := candidate.manifestBytes
			var manifest deploymentManifest
			if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
				return fmt.Errorf("decode deployment manifest for run %s: %w", runID, err)
			}
			var workflow *workflowManifest
			for i := range manifest.Workflows {
				if manifest.Workflows[i].Name == workflowName {
					workflow = &manifest.Workflows[i]
					break
				}
			}
			if workflow == nil {
				continue
			}

			stepRows, err := tx.Query(ctx, `SELECT id::text, node_id, state FROM run_steps
				WHERE run_id=$1::uuid AND organization_id=$2::uuid ORDER BY id FOR UPDATE`, runID, organizationID)
			if err != nil {
				return fmt.Errorf("query steps for run %s: %w", runID, err)
			}
			type readyStep struct{ id, state string }
			steps := make(map[string]readyStep, len(workflow.Nodes))
			for stepRows.Next() {
				var id, nodeID, state string
				if err := stepRows.Scan(&id, &nodeID, &state); err != nil {
					stepRows.Close()
					return err
				}
				steps[nodeID] = readyStep{id: id, state: state}
			}
			if err := stepRows.Err(); err != nil {
				stepRows.Close()
				return err
			}
			stepRows.Close()

			for _, node := range workflow.Nodes {
				step, ok := steps[node.ID]
				if !ok || step.state != "BLOCKED" {
					continue
				}
				depsReady := true
				for _, dependency := range node.After {
					dep, exists := steps[dependency]
					if !exists || (dep.state != "SUCCEEDED" && dep.state != "SKIPPED") {
						depsReady = false
						break
					}
				}
				if !depsReady {
					continue
				}
				result, err := tx.Exec(ctx, `UPDATE run_steps
					SET state='READY', wait_reason=NULL, eligible_at=clock_timestamp(), updated_at=clock_timestamp()
					WHERE id=$1::uuid AND organization_id=$2::uuid AND state='BLOCKED'`, step.id, organizationID)
				if err != nil {
					return fmt.Errorf("repair blocked step %s: %w", step.id, err)
				}
				if result.RowsAffected() == 0 {
					continue
				}
				if err := appendRunEvent(ctx, tx, organizationID, runID, "STEP_READY", map[string]any{
					"stepId": step.id, "nodeId": node.ID, "reason": "RECONCILIATION",
				}); err != nil {
					return fmt.Errorf("record repaired step %s: %w", step.id, err)
				}
				repaired++
				steps[node.ID] = readyStep{id: step.id, state: "READY"}
				affectedRuns = append(affectedRuns, runID)
			}
			// A run with unmet dependencies must yield its place to the next
			// bounded batch. This observation does not alter workflow state and
			// intentionally has no execution event; any actual transition above
			// still uses appendRunEvent and its outbox intent.
			if _, err := tx.Exec(ctx, `UPDATE runs SET reconciliation_checked_at=clock_timestamp()
				WHERE id=$1::uuid AND organization_id=$2::uuid`, runID, organizationID); err != nil {
				return fmt.Errorf("record reconciliation observation for run %s: %w", runID, err)
			}
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	if e.hub != nil {
		for _, runID := range affectedRuns {
			e.hub.Publish(runID)
		}
	}
	return repaired, nil
}

func (e *WorkerEngine) reconcileExpiredLeasesTx(ctx context.Context, tx storage.Tx, organizationID string) (int, []string, error) {
	rows, err := tx.Query(ctx, `SELECT
			r.id::text, rs.id::text, a.id::text, a.status, a.epoch, a.started_at,
			a.claim_start_deadline_at, a.deadline_at, l.expires_at,
			rs.next_attempt_number, rs.current_epoch, rs.node_id,
			r.workflow_name, d.manifest, r.status, r.deadline_at,
			r.environment_id::text, a.attempt_number
		FROM runs r
		JOIN run_steps rs ON rs.run_id=r.id AND rs.organization_id=r.organization_id
		JOIN task_attempts a ON a.step_id=rs.id AND a.organization_id=rs.organization_id
		LEFT JOIN task_leases l ON l.attempt_id=a.id AND l.organization_id=a.organization_id
		JOIN deployments d ON d.id=r.deployment_id AND d.organization_id=r.organization_id
		WHERE r.organization_id=$1::uuid
			AND (r.status IN ('QUEUED','RUNNING') OR (r.status='WAITING' AND r.reason_code='RECOVERY_HANDOFF'))
			AND a.status IN ('CLAIMED','RUNNING')
			AND (
				(a.status='CLAIMED' AND (a.claim_start_deadline_at <= clock_timestamp() OR (l.expires_at IS NOT NULL AND l.expires_at <= clock_timestamp()) OR l.step_id IS NULL))
				OR (a.status='RUNNING' AND l.expires_at IS NOT NULL AND l.expires_at <= clock_timestamp())
				OR (a.status='RUNNING' AND a.deadline_at IS NOT NULL AND a.deadline_at <= clock_timestamp())
				OR (r.deadline_at IS NOT NULL AND r.deadline_at <= clock_timestamp())
			)
		ORDER BY r.id, rs.id, a.id
		LIMIT 50
		FOR UPDATE OF r, rs, a SKIP LOCKED`, organizationID)
	if err != nil {
		return 0, nil, fmt.Errorf("query expired attempts: %w", err)
	}

	candidates := make([]expiredAttemptCandidate, 0)
	for rows.Next() {
		var c expiredAttemptCandidate
		if err := rows.Scan(
			&c.runID, &c.stepID, &c.attemptID, &c.attemptStatus, &c.attemptEpoch, &c.startedAt,
			&c.claimStartDeadlineAt, &c.attemptDeadlineAt, &c.leaseExpiresAt,
			&c.nextAttemptNumber, &c.currentEpoch, &c.nodeID,
			&c.workflowName, &c.manifestBytes, &c.runStatus, &c.runDeadlineAt,
			&c.environmentID, &c.attemptNumber,
		); err != nil {
			rows.Close()
			return 0, nil, fmt.Errorf("scan expired attempt candidate: %w", err)
		}
		candidates = append(candidates, c)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return 0, nil, err
	}
	rows.Close()

	type handoffCandidate struct {
		runID             string
		stepID            string
		nextAttemptNumber int
		currentEpoch      int64
		nodeID            string
		workflowName      string
		manifestBytes     []byte
		runStatus         string
		runDeadlineAt     *time.Time
		hasStartedAttempt bool
	}

	// Also sweep steps in WAITING with wait_reason='RECOVERY_HANDOFF'
	handoffRows, err := tx.Query(ctx, `SELECT
			r.id::text, rs.id::text, rs.next_attempt_number, rs.current_epoch, rs.node_id,
			r.workflow_name, d.manifest, r.status, r.deadline_at,
			EXISTS (SELECT 1 FROM task_attempts ta JOIN run_steps rst ON rst.id=ta.step_id WHERE rst.run_id=r.id AND ta.started_at IS NOT NULL)
		FROM runs r
		JOIN run_steps rs ON rs.run_id=r.id AND rs.organization_id=r.organization_id
		JOIN deployments d ON d.id=r.deployment_id AND d.organization_id=r.organization_id
		WHERE r.organization_id=$1::uuid
			AND rs.state='WAITING' AND rs.wait_reason='RECOVERY_HANDOFF'
			AND (r.status IN ('QUEUED','RUNNING') OR (r.status='WAITING' AND r.reason_code='RECOVERY_HANDOFF'))
		ORDER BY r.id, rs.id
		LIMIT 50
		FOR UPDATE OF r, rs SKIP LOCKED`, organizationID)
	if err != nil {
		return 0, nil, fmt.Errorf("query recovery handoff steps: %w", err)
	}

	handoffs := make([]handoffCandidate, 0)
	for handoffRows.Next() {
		var h handoffCandidate
		if err := handoffRows.Scan(
			&h.runID, &h.stepID, &h.nextAttemptNumber, &h.currentEpoch, &h.nodeID,
			&h.workflowName, &h.manifestBytes, &h.runStatus, &h.runDeadlineAt,
			&h.hasStartedAttempt,
		); err != nil {
			handoffRows.Close()
			return 0, nil, fmt.Errorf("scan recovery handoff candidate: %w", err)
		}
		handoffs = append(handoffs, h)
	}
	if err := handoffRows.Err(); err != nil {
		handoffRows.Close()
		return 0, nil, err
	}
	handoffRows.Close()

	if len(candidates) == 0 && len(handoffs) == 0 {
		return 0, nil, nil
	}

	var dbNow time.Time
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&dbNow); err != nil {
		return 0, nil, err
	}

	reclaimedRunsMap := make(map[string]struct{})
	reclaimedCount := 0

	for _, c := range candidates {
		reclaimedRunsMap[c.runID] = struct{}{}
		reclaimedCount++

		var manifest deploymentManifest
		if len(c.manifestBytes) > 0 {
			_ = json.Unmarshal(c.manifestBytes, &manifest)
		}
		fullPolicy := manifest.taskRetryPolicy(c.workflowName, c.nodeID)
		policy := fullPolicy.Recovery

		// 1. Overall run deadline exceeded
		if c.runDeadlineAt != nil && !dbNow.Before(*c.runDeadlineAt) {
			if _, err := tx.Exec(ctx, `UPDATE task_attempts SET status='LOST', completed_at=clock_timestamp(),
				error=jsonb_build_object('code','RUN_DEADLINE_EXCEEDED','message','Run deadline exceeded','retryable',false,'effectStatus','UNKNOWN')
				WHERE id=$1::uuid AND organization_id=$2::uuid`, c.attemptID, organizationID); err != nil {
				return 0, nil, err
			}
			if _, err := tx.Exec(ctx, `DELETE FROM task_leases WHERE step_id=$1::uuid AND organization_id=$2::uuid`, c.stepID, organizationID); err != nil {
				return 0, nil, err
			}
			if _, err := tx.Exec(ctx, `UPDATE run_steps SET state='CANCELLED', updated_at=clock_timestamp()
				WHERE run_id=$1::uuid AND organization_id=$2::uuid AND state IN ('BLOCKED','READY','WAITING','RUNNING')`, c.runID, organizationID); err != nil {
				return 0, nil, err
			}
			if _, err := tx.Exec(ctx, `UPDATE runs SET status='FAILED', reason_code='RUN_DEADLINE_EXCEEDED', updated_at=clock_timestamp()
				WHERE id=$1::uuid AND organization_id=$2::uuid`, c.runID, organizationID); err != nil {
				return 0, nil, err
			}
			if err := appendRunEvent(ctx, tx, organizationID, c.runID, "TASK_LOST", map[string]any{
				"attemptId": c.attemptID, "stepId": c.stepID, "epoch": c.attemptEpoch, "reason": "RUN_DEADLINE_EXCEEDED", "recovery": "TERMINAL",
			}); err != nil {
				return 0, nil, err
			}
			if err := appendRunEvent(ctx, tx, organizationID, c.runID, "RUN_FAILED", map[string]any{
				"status": "FAILED", "reason": "RUN_DEADLINE_EXCEEDED", "stepId": c.stepID,
			}); err != nil {
				return 0, nil, err
			}
			continue
		}

		// 2. Unstarted claim (status == CLAIMED or started_at == nil)
		if c.attemptStatus == "CLAIMED" || c.startedAt == nil {
			if _, err := tx.Exec(ctx, `UPDATE task_attempts SET status='LOST', completed_at=clock_timestamp(),
				error=jsonb_build_object('code','START_DEADLINE_EXCEEDED','message','Claim start deadline exceeded without Start','retryable',true,'effectStatus','NOT_APPLIED')
				WHERE id=$1::uuid AND organization_id=$2::uuid`, c.attemptID, organizationID); err != nil {
				return 0, nil, err
			}
			if _, err := tx.Exec(ctx, `DELETE FROM task_leases WHERE step_id=$1::uuid AND organization_id=$2::uuid`, c.stepID, organizationID); err != nil {
				return 0, nil, err
			}
			if err := appendRunEvent(ctx, tx, organizationID, c.runID, "TASK_LOST", map[string]any{
				"attemptId": c.attemptID, "stepId": c.stepID, "epoch": c.attemptEpoch, "reason": "START_DEADLINE_EXCEEDED", "recovery": "RETRY",
			}); err != nil {
				return 0, nil, err
			}

			// Unstarted claims never executed side effects (NOT_APPLIED): schedule a
			// durable backoff timer instead of immediate READY so restart never
			// redraws jitter or resets the timer (Blueprint §15.1, F-09).
			{
				var m deploymentManifest
				if len(c.manifestBytes) > 0 {
					_ = json.Unmarshal(c.manifestBytes, &m)
				}
				pol := m.taskRetryPolicy(c.workflowName, c.nodeID)
				envForTimer := c.environmentID
				decision, failCode, err := scheduleRetryOrHoldTx(ctx, tx, organizationID, envForTimer, c.runID, c.stepID, c.nodeID, c.attemptID, c.attemptNumber, pol, "START_DEADLINE_EXCEEDED", true, "NOT_APPLIED", "", c.runStatus, "", c.runDeadlineAt, envForTimer)
				if err != nil {
					return 0, nil, err
				}
				if decision == "retry" || decision == "hold" {
					continue
				}
				if failCode == "" {
					failCode = "START_DEADLINE_EXCEEDED"
				}
				if err := failRunForStepTx(ctx, tx, organizationID, c.runID, c.stepID, failCode, nil); err != nil {
					return 0, nil, err
				}
			}
			continue
		}

		// 3. Running attempt (started_at != nil): either attempt deadline exceeded or lease expired.
		// Durable retry uses a persisted backoff timer (Blueprint §15.1, F-09);
		// ambiguous reconcile outcomes and insufficient idempotent windows hold.
		isTimeout := c.attemptDeadlineAt != nil && !dbNow.Before(*c.attemptDeadlineAt)
		outcome := "LOST"
		reasonCode := "LEASE_EXPIRED"
		message := "Task lease expired without renewal"
		retryable := true
		if isTimeout {
			outcome = "TIMED_OUT"
			reasonCode = "ATTEMPT_TIMED_OUT"
			message = "Attempt execution deadline exceeded"
			retryable = true
		}

		if _, err := tx.Exec(ctx, `UPDATE task_attempts SET status=$1, completed_at=clock_timestamp(),
			error=jsonb_build_object('code',$2::text,'message',$3::text,'retryable',$4::boolean,'effectStatus','UNKNOWN')
			WHERE id=$5::uuid AND organization_id=$6::uuid`, outcome, reasonCode, message, retryable, c.attemptID, organizationID); err != nil {
			return 0, nil, err
		}
		if _, err := tx.Exec(ctx, `DELETE FROM task_leases WHERE step_id=$1::uuid AND organization_id=$2::uuid`, c.stepID, organizationID); err != nil {
			return 0, nil, err
		}
		if err := appendRunEvent(ctx, tx, organizationID, c.runID, "TASK_LOST", map[string]any{
			"attemptId": c.attemptID, "stepId": c.stepID, "epoch": c.attemptEpoch, "reason": reasonCode, "recovery": policy,
		}); err != nil {
			return 0, nil, err
		}

		{
			envForTimer := c.environmentID
			decision, failCode, err := scheduleRetryOrHoldTx(ctx, tx, organizationID, envForTimer, c.runID, c.stepID, c.nodeID, c.attemptID, c.attemptNumber, fullPolicy, reasonCode, retryable, "UNKNOWN", "", c.runStatus, "", c.runDeadlineAt, envForTimer)
			if err != nil {
				return 0, nil, err
			}
			if decision == "retry" || decision == "hold" {
				// Timer or hold persisted; firing creates READY, claim creates attempt.
			} else {
				if failCode == "" {
					failCode = reasonCode
				}
				if err := failRunForStepTx(ctx, tx, organizationID, c.runID, c.stepID, failCode, nil); err != nil {
					return 0, nil, err
				}
			}
		}
	}

	for _, h := range handoffs {
		reclaimedRunsMap[h.runID] = struct{}{}
		reclaimedCount++

		var manifest deploymentManifest
		if len(h.manifestBytes) > 0 {
			_ = json.Unmarshal(h.manifestBytes, &manifest)
		}
		_, maxAttempts := manifest.taskRecoveryPolicy(h.workflowName, h.nodeID)

		if h.runDeadlineAt != nil && !dbNow.Before(*h.runDeadlineAt) {
			if _, err := tx.Exec(ctx, `UPDATE run_steps SET state='CANCELLED', updated_at=clock_timestamp()
				WHERE run_id=$1::uuid AND organization_id=$2::uuid AND state IN ('BLOCKED','READY','WAITING','RUNNING')`, h.runID, organizationID); err != nil {
				return 0, nil, err
			}
			if _, err := tx.Exec(ctx, `UPDATE runs SET status='FAILED', reason_code='RUN_DEADLINE_EXCEEDED', updated_at=clock_timestamp()
				WHERE id=$1::uuid AND organization_id=$2::uuid`, h.runID, organizationID); err != nil {
				return 0, nil, err
			}
			if err := appendRunEvent(ctx, tx, organizationID, h.runID, "RUN_FAILED", map[string]any{
				"status": "FAILED", "reason": "RUN_DEADLINE_EXCEEDED", "stepId": h.stepID,
			}); err != nil {
				return 0, nil, err
			}
			continue
		}

		if h.nextAttemptNumber <= maxAttempts {
			if _, err := tx.Exec(ctx, `UPDATE run_steps SET state='READY', wait_reason=NULL, eligible_at=clock_timestamp(), updated_at=clock_timestamp()
				WHERE id=$1::uuid AND organization_id=$2::uuid AND state='WAITING' AND wait_reason='RECOVERY_HANDOFF'`, h.stepID, organizationID); err != nil {
				return 0, nil, err
			}

			targetRunStatus := "QUEUED"
			if h.hasStartedAttempt {
				targetRunStatus = "RUNNING"
			}
			if _, err := tx.Exec(ctx, `UPDATE runs SET status=$1, reason_code=NULL, updated_at=clock_timestamp()
				WHERE id=$2::uuid AND organization_id=$3::uuid AND status='WAITING' AND reason_code='RECOVERY_HANDOFF'`, targetRunStatus, h.runID, organizationID); err != nil {
				return 0, nil, err
			}

			if err := appendRunEvent(ctx, tx, organizationID, h.runID, "STEP_READY", map[string]any{
				"stepId": h.stepID, "nodeId": h.nodeID, "reason": "RECOVERY_HANDOFF_RESOLVED",
			}); err != nil {
				return 0, nil, err
			}
		} else {
			if _, err := tx.Exec(ctx, `UPDATE run_steps SET state='FAILED', wait_reason='MAX_ATTEMPTS_EXCEEDED', updated_at=clock_timestamp()
				WHERE id=$1::uuid AND organization_id=$2::uuid`, h.stepID, organizationID); err != nil {
				return 0, nil, err
			}
			if _, err := tx.Exec(ctx, `UPDATE runs SET status='FAILED', reason_code='MAX_ATTEMPTS_EXCEEDED', updated_at=clock_timestamp()
				WHERE id=$1::uuid AND organization_id=$2::uuid`, h.runID, organizationID); err != nil {
				return 0, nil, err
			}
			if _, err := tx.Exec(ctx, `UPDATE run_steps SET state='CANCELLED', updated_at=clock_timestamp()
				WHERE run_id=$1::uuid AND organization_id=$2::uuid AND state IN ('BLOCKED','READY','WAITING')`, h.runID, organizationID); err != nil {
				return 0, nil, err
			}
			if err := appendRunEvent(ctx, tx, organizationID, h.runID, "RUN_FAILED", map[string]any{
				"status": "FAILED", "reason": "MAX_ATTEMPTS_EXCEEDED", "stepId": h.stepID,
			}); err != nil {
				return 0, nil, err
			}
		}
	}

	affectedRuns := make([]string, 0, len(reclaimedRunsMap))
	for rID := range reclaimedRunsMap {
		affectedRuns = append(affectedRuns, rID)
	}
	return reclaimedCount, affectedRuns, nil
}

// TerminalizeHistoryLimitExceeded atomically marks a run FAILED with reason
// HISTORY_LIMIT_EXCEEDED, cancels all open steps, and writes the reserved
// terminal event into run_events and outbox.
func (e *WorkerEngine) TerminalizeHistoryLimitExceeded(ctx context.Context, organizationID, runID string) error {
	return e.pool.WithTenantTx(ctx, organizationID, func(ctx context.Context, tx storage.Tx) error {
		var status string
		var lastSeq int64
		if err := tx.QueryRow(ctx, `SELECT status, last_event_sequence
			FROM runs
			WHERE id=$1::uuid AND organization_id=$2::uuid
			FOR UPDATE`, runID, organizationID).Scan(&status, &lastSeq); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrRunNotFound
			}
			return err
		}
		if status == "SUCCEEDED" || status == "FAILED" || status == "CANCELLED" {
			return nil
		}
		if _, err := tx.Exec(ctx, `UPDATE run_steps
			SET state='CANCELLED', updated_at=clock_timestamp()
			WHERE run_id=$1::uuid AND organization_id=$2::uuid
			  AND state IN ('BLOCKED','READY','WAITING')`, runID, organizationID); err != nil {
			return err
		}
		if err := appendRunEvent(ctx, tx, organizationID, runID, "RUN_FAILED", map[string]any{
			"reason": "HISTORY_LIMIT_EXCEEDED",
		}); err != nil && !errors.Is(err, ErrHistoryLimitExceeded) {
			return err
		}
		_, err := tx.Exec(ctx, `UPDATE runs
			SET status='FAILED', reason_code='HISTORY_LIMIT_EXCEEDED', updated_at=clock_timestamp()
			WHERE id=$1::uuid AND organization_id=$2::uuid AND status NOT IN ('SUCCEEDED','FAILED','CANCELLED')`,
			runID, organizationID)
		return err
	})
}

func appendRunEvent(ctx context.Context, tx storage.Tx, organizationID, runID, eventType string, payload any) error {
	encoded, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	var sequence int64
	// Reserve the final history slot for a terminal event. This keeps a noisy
	// run from exhausting its event budget before the control plane can record
	// the terminal outcome. The run transition that hit the limit is rolled
	// back by the caller, so no correctness event is silently dropped.
	terminal := eventType == "RUN_COMPLETED" || eventType == "RUN_FAILED" || eventType == "RUN_CANCELLED"
	whereBudget := "last_event_sequence < $3"
	if terminal {
		whereBudget = "last_event_sequence < $3 OR (last_event_sequence = $3 AND $4)"
	}
	query := fmt.Sprintf(`UPDATE runs SET last_event_sequence=last_event_sequence+1,updated_at=clock_timestamp()
		WHERE id=$1::uuid AND organization_id=$2::uuid AND (%s)
		RETURNING last_event_sequence`, whereBudget)
	args := []any{runID, organizationID, int64(maxRunEvents - 1)}
	if terminal {
		args = append(args, terminal)
	}
	if err := tx.QueryRow(ctx, query, args...).Scan(&sequence); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrHistoryLimitExceeded
		}
		return err
	}
	var eventID string
	if err := tx.QueryRow(ctx, `INSERT INTO run_events (organization_id,run_id,sequence,event_type,payload)
		VALUES ($1::uuid,$2::uuid,$3,$4,$5::jsonb) RETURNING id::text`, organizationID, runID, sequence, eventType, string(encoded)).Scan(&eventID); err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `INSERT INTO outbox_events (organization_id,event_id,subject,payload)
		VALUES ($1::uuid,$2::uuid,'execution.state_changed',jsonb_build_object(
			'runId',$3::text,'sequence',$4::bigint,'eventType',$5::text))`, organizationID, eventID, runID, sequence, eventType)
	return err
}
