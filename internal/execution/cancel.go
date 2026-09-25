package execution

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/Ryanakml/Deadbolt/internal/storage"
	"github.com/Ryanakml/Deadbolt/internal/tenant"
	"github.com/jackc/pgx/v5"
)

// Durable cancellation with stop confirmation (Blueprint §10.4, §15.4).
//
// A cancel commit revokes all leases and nonterminal work immediately,
// preserves already-succeeded steps, cancels pending timers, moots open
// reconciliation holds, and records one stop command per live attempt with a
// 10s grace deadline. The run waits in CANCELLING until every stop is
// acknowledged or the grace expires, then settles to CANCELLED with an
// explicit termination_confirmed flag. A cancelled run never promises
// external rollback: termination_confirmed only reports whether worker
// processes acknowledged the stop.
//
// State priority (§10.2) is applied centrally here: a committed terminal
// state or CANCELLING is never overwritten, and completion-first wins — a
// result that committed before cancel stands, while results arriving after
// the cancel commit are rejected.

// CancelRunRequest mirrors RevisionCommand in
// contracts/openapi/control-plane.yaml.
type CancelRunRequest struct {
	ExpectedRevision int64 `json:"expectedRevision"`
}

// CancelRun durably cancels an active run through the canonical tenant
// command path when available, so retried command identities replay the
// recorded outcome instead of re-entering the engine.
func (e *WorkerEngine) CancelRun(
	ctx context.Context,
	orgID, runID string,
	req CancelRunRequest,
	audit *tenant.AuditContext,
) (*RunDTO, error) {
	if audit == nil {
		return nil, tenant.ErrAuditRequired
	}
	var resp *RunDTO
	mutate := func(ctx context.Context, tx storage.Tx) error {
		r, err := e.cancelRunTx(ctx, tx, orgID, runID, req.ExpectedRevision, audit)
		if err != nil {
			return err
		}
		resp = r
		return nil
	}
	if e.commands != nil {
		_, err := e.commands.WithCommandTx(ctx, orgID, "", http.StatusOK, mutate,
			func() any { return resp },
			func(raw json.RawMessage) error { return json.Unmarshal(raw, &resp) })
		if err != nil {
			return nil, err
		}
		return resp, nil
	}
	if err := e.pool.WithTenantTx(ctx, orgID, mutate); err != nil {
		return nil, err
	}
	return resp, nil
}

func (e *WorkerEngine) cancelRunTx(
	ctx context.Context, tx storage.Tx,
	orgID, runID string, expectedRevision int64,
	audit *tenant.AuditContext,
) (*RunDTO, error) {
	var status, reason string
	var revision int64
	var environmentID string
	if err := tx.QueryRow(ctx, `SELECT status, COALESCE(reason_code,''), revision, environment_id::text FROM runs
		WHERE id=$1::uuid AND organization_id=$2::uuid FOR UPDATE`,
		runID, orgID).Scan(&status, &reason, &revision, &environmentID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrRunNotFound
		}
		return nil, err
	}
	switch status {
	case "SUCCEEDED", "FAILED", "CANCELLED":
		return nil, ErrRunTerminal
	}
	if revision != expectedRevision {
		return nil, ErrRevisionConflict
	}
	if status == "CANCELLING" {
		// Duplicate cancel while settling: idempotent, no new transition.
		return queryRunDTO(ctx, tx, orgID, runID)
	}

	// Lock live attempts in run → step → attempt order.
	type liveAttempt struct {
		attemptID string
		stepID    string
		started   bool
	}
	rows, err := tx.Query(ctx, `SELECT a.id::text, rs.id::text,
			(a.status='RUNNING' OR a.started_at IS NOT NULL)
		FROM task_attempts a JOIN run_steps rs ON rs.id=a.step_id AND rs.organization_id=a.organization_id
		WHERE rs.run_id=$1::uuid AND a.organization_id=$2::uuid
			AND a.status IN ('CLAIMED','RUNNING')
		ORDER BY rs.id, a.id
		FOR UPDATE OF rs, a`, runID, orgID)
	if err != nil {
		return nil, err
	}
	live := make([]liveAttempt, 0)
	for rows.Next() {
		var la liveAttempt
		if err := rows.Scan(&la.attemptID, &la.stepID, &la.started); err != nil {
			rows.Close()
			return nil, err
		}
		live = append(live, la)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()

	var dbNow time.Time
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&dbNow); err != nil {
		return nil, err
	}
	graceUntil := dbNow.Add(time.Duration(CancelGraceMs) * time.Millisecond)

	for _, la := range live {
		// Cancellation proves nothing about external effects of an attempt
		// that already started: only a demonstrably unstarted claim may
		// record NOT_APPLIED. Process termination is not rollback evidence.
		effectStatus := "NOT_APPLIED"
		if la.started {
			effectStatus = "UNKNOWN"
		}
		if _, err := tx.Exec(ctx, `UPDATE task_attempts SET status='CANCELLED', completed_at=clock_timestamp(),
			error=jsonb_build_object('code','CANCEL_REQUESTED','message','Run cancellation requested','retryable',false,'effectStatus',$1::text)
			WHERE id=$2::uuid AND organization_id=$3::uuid`, effectStatus, la.attemptID, orgID); err != nil {
			return nil, err
		}
		if _, err := tx.Exec(ctx, `DELETE FROM task_leases WHERE step_id=$1::uuid AND organization_id=$2::uuid`, la.stepID, orgID); err != nil {
			return nil, err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO stop_commands
			(organization_id, attempt_id, reason, deadline_at)
			VALUES ($1::uuid, $2::uuid, 'CANCEL_REQUESTED', $3)`,
			orgID, la.attemptID, graceUntil); err != nil {
			return nil, err
		}
	}
	// Nonterminal work stops now; SUCCEEDED/FAILED/CANCELLED/SKIPPED remain.
	if _, err := tx.Exec(ctx, `UPDATE run_steps SET state='CANCELLED', wait_reason='CANCEL_REQUESTED', updated_at=clock_timestamp()
		WHERE run_id=$1::uuid AND organization_id=$2::uuid AND state IN ('BLOCKED','READY','RUNNING','WAITING')`,
		runID, orgID); err != nil {
		return nil, err
	}
	if _, err := tx.Exec(ctx, `UPDATE timers SET state='CANCELLED', updated_at=clock_timestamp()
		WHERE run_id=$1::uuid AND organization_id=$2::uuid AND state='PENDING'`, runID, orgID); err != nil {
		return nil, err
	}
	var actorID *string
	if audit.ActorID != nil && strings.TrimSpace(*audit.ActorID) != "" {
		actorID = audit.ActorID
	}
	if err := closeOpenCasesTx(ctx, tx, orgID, runID, actorID, "CANCEL_REQUESTED", "CANCEL"); err != nil {
		return nil, err
	}

	if len(live) == 0 {
		// Nothing to stop: settle immediately as confirmed.
		confirmed := true
		if _, err := tx.Exec(ctx, `UPDATE runs SET status='CANCELLED', reason_code='CANCEL_REQUESTED',
			revision=revision+1, termination_confirmed=$1, updated_at=clock_timestamp()
			WHERE id=$2::uuid AND organization_id=$3::uuid`, confirmed, runID, orgID); err != nil {
			return nil, err
		}
		if err := appendRunEvent(ctx, tx, orgID, runID, "RUN_CANCELLED", map[string]any{
			"reason": "CANCEL_REQUESTED", "terminationConfirmed": confirmed,
		}); err != nil {
			return nil, err
		}
	} else {
		if _, err := tx.Exec(ctx, `UPDATE runs SET status='CANCELLING', reason_code='CANCEL_REQUESTED',
			revision=revision+1, updated_at=clock_timestamp()
			WHERE id=$1::uuid AND organization_id=$2::uuid`, runID, orgID); err != nil {
			return nil, err
		}
		if err := appendRunEvent(ctx, tx, orgID, runID, "RUN_CANCELLING", map[string]any{
			"reason": "CANCEL_REQUESTED", "stopCount": len(live),
		}); err != nil {
			return nil, err
		}
	}
	if err := appendCancelAuditTx(ctx, tx, orgID, runID, audit, len(live)); err != nil {
		return nil, err
	}
	return queryRunDTO(ctx, tx, orgID, runID)
}

func appendCancelAuditTx(ctx context.Context, tx storage.Tx, orgID, runID string, audit *tenant.AuditContext, stopCount int) error {
	meta, err := json.Marshal(map[string]any{
		"actor_type":   audit.ActorType,
		"role":         audit.Role,
		"capabilities": audit.Capabilities,
		"stopCount":    stopCount,
	})
	if err != nil {
		return err
	}
	var actorID *string
	if audit.ActorID != nil && strings.TrimSpace(*audit.ActorID) != "" {
		actorID = audit.ActorID
	}
	_, err = tx.Exec(ctx, `INSERT INTO audit_events
		(organization_id, actor_id, action, target_type, target_id, correlation_id, reason, metadata)
		VALUES ($1::uuid, $2, 'run.cancel', 'run', $3::uuid, $4, 'CANCEL_REQUESTED', $5::jsonb)`,
		orgID, actorID, runID, audit.CorrelationID, string(meta))
	return err
}

// settleCancellingRunTx settles a CANCELLING run once every stop is
// acknowledged or the grace has expired for all outstanding stops.
// termination_confirmed is true only when no stop went unacknowledged.
func settleCancellingRunTx(ctx context.Context, tx storage.Tx, orgID, runID string) (bool, error) {
	var status string
	if err := tx.QueryRow(ctx, `SELECT status FROM runs
		WHERE id=$1::uuid AND organization_id=$2::uuid FOR UPDATE`, runID, orgID).Scan(&status); err != nil {
		return false, err
	}
	if status != "CANCELLING" {
		return false, nil
	}
	var liveGrace int
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM stop_commands sc
		JOIN task_attempts a ON a.id=sc.attempt_id AND a.organization_id=sc.organization_id
		JOIN run_steps rs ON rs.id=a.step_id AND rs.organization_id=a.organization_id
		WHERE rs.run_id=$1::uuid AND sc.organization_id=$2::uuid
			AND sc.acked_at IS NULL AND sc.deadline_at > clock_timestamp()`,
		runID, orgID).Scan(&liveGrace); err != nil {
		return false, err
	}
	if liveGrace > 0 {
		return false, nil
	}
	var unacked int
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM stop_commands sc
		JOIN task_attempts a ON a.id=sc.attempt_id AND a.organization_id=sc.organization_id
		JOIN run_steps rs ON rs.id=a.step_id AND rs.organization_id=a.organization_id
		WHERE rs.run_id=$1::uuid AND sc.organization_id=$2::uuid AND sc.acked_at IS NULL`,
		runID, orgID).Scan(&unacked); err != nil {
		return false, err
	}
	// Confirmation is durable process-stop proof, not mere acknowledgement:
	// an ACK with ProcessStopped=false leaves termination_confirmed_at NULL
	// and settles the run as unconfirmed.
	var unconfirmed int
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM stop_commands sc
		JOIN task_attempts a ON a.id=sc.attempt_id AND a.organization_id=sc.organization_id
		JOIN run_steps rs ON rs.id=a.step_id AND rs.organization_id=a.organization_id
		WHERE rs.run_id=$1::uuid AND sc.organization_id=$2::uuid
			AND sc.termination_confirmed_at IS NULL`,
		runID, orgID).Scan(&unconfirmed); err != nil {
		return false, err
	}
	confirmed := unconfirmed == 0
	if _, err := tx.Exec(ctx, `UPDATE runs SET status='CANCELLED', termination_confirmed=$1,
		updated_at=clock_timestamp() WHERE id=$2::uuid AND organization_id=$3::uuid`,
		confirmed, runID, orgID); err != nil {
		return false, err
	}
	if err := appendRunEvent(ctx, tx, orgID, runID, "RUN_CANCELLED", map[string]any{
		"reason": "CANCEL_REQUESTED", "terminationConfirmed": confirmed,
	}); err != nil {
		return false, err
	}
	return true, nil
}

// settleCancellingRunsTx settles every CANCELLING run whose stops are all
// acknowledged or past grace. It is restart-safe: all state is read from the
// database, so a new scheduler continues grace settlement after a restart.
func settleCancellingRunsTx(ctx context.Context, tx storage.Tx, orgID string) (int, []string, error) {
	rows, err := tx.Query(ctx, `SELECT DISTINCT r.id::text FROM runs r
		WHERE r.organization_id=$1::uuid AND r.status='CANCELLING'
		ORDER BY 1 LIMIT 50`, orgID)
	if err != nil {
		return 0, nil, err
	}
	ids := make([]string, 0)
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return 0, nil, err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return 0, nil, err
	}
	rows.Close()
	settled := 0
	affected := make([]string, 0, len(ids))
	for _, id := range ids {
		done, err := settleCancellingRunTx(ctx, tx, orgID, id)
		if err != nil {
			return 0, nil, err
		}
		if done {
			settled++
			affected = append(affected, id)
		}
	}
	return settled, affected, nil
}

// failOverdueRunsTx terminalizes runs whose deadline passed with no live
// work left to settle them: held runs, retry waits, and never-claimed queue.
// Runs with live attempts are left alone; their attempt deadlines already
// incorporate the run deadline and drive recovery through policy.
func failOverdueRunsTx(ctx context.Context, tx storage.Tx, orgID string) (int, []string, error) {
	rows, err := tx.Query(ctx, `SELECT DISTINCT r.id::text
		FROM runs r
		WHERE r.organization_id=$1::uuid
			AND r.status IN ('QUEUED','RUNNING','WAITING')
			AND r.deadline_at IS NOT NULL AND r.deadline_at <= clock_timestamp()
			AND NOT EXISTS (SELECT 1 FROM task_attempts a
				JOIN run_steps rs ON rs.id=a.step_id AND rs.organization_id=a.organization_id
				WHERE rs.run_id=r.id AND a.organization_id=r.organization_id
					AND a.status IN ('CLAIMED','RUNNING'))
		ORDER BY 1
		LIMIT 50`, orgID)
	if err != nil {
		return 0, nil, fmt.Errorf("query overdue runs: %w", err)
	}
	runIDs := make([]string, 0)
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return 0, nil, err
		}
		runIDs = append(runIDs, id)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return 0, nil, err
	}
	rows.Close()
	affected := make([]string, 0, len(runIDs))
	for _, runID := range runIDs {
		// Lock order: run → step (Blueprint §11.2).
		var runStatus string
		if err := tx.QueryRow(ctx, `SELECT status FROM runs
			WHERE id=$1::uuid AND organization_id=$2::uuid FOR UPDATE`, runID, orgID).Scan(&runStatus); err != nil {
			continue
		}
		switch runStatus {
		case "SUCCEEDED", "FAILED", "CANCELLED", "CANCELLING", "PAUSING", "PAUSED":
			continue
		}
		var stepID string
		if err := tx.QueryRow(ctx, `SELECT rs.id::text FROM run_steps rs
			WHERE rs.run_id=$1::uuid AND rs.organization_id=$2::uuid
				AND rs.state IN ('BLOCKED','READY','RUNNING','WAITING')
			ORDER BY rs.id LIMIT 1 FOR UPDATE OF rs`, runID, orgID).Scan(&stepID); err != nil {
			continue
		}
		if err := failRunForStepTx(ctx, tx, orgID, runID, stepID, "RUN_DEADLINE_EXCEEDED", nil); err != nil {
			return 0, nil, err
		}
		affected = append(affected, runID)
	}
	return len(affected), affected, nil
}
