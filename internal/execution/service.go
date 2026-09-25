package execution

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Ryanakml/Deadbolt/internal/contracts"
	"github.com/Ryanakml/Deadbolt/internal/recovery"
	"github.com/Ryanakml/Deadbolt/internal/storage"
	"github.com/Ryanakml/Deadbolt/internal/tenant"
	"github.com/Ryanakml/Deadbolt/internal/worker"
	"github.com/jackc/pgx/v5"
)

var (
	ErrMissingIdempotencyKey = errors.New("MISSING_IDEMPOTENCY_KEY: Idempotency-Key header is required")
	ErrIdempotencyConflict   = errors.New("IDEMPOTENCY_CONFLICT: Idempotency key already used with different payload")
	ErrNoActiveDeployment    = errors.New("NO_ACTIVE_DEPLOYMENT: No active deployment found for workflow")
	ErrDeploymentNotFound    = errors.New("DEPLOYMENT_NOT_FOUND: Deployment not found")
	ErrWorkflowNotFound      = errors.New("WORKFLOW_NOT_FOUND: Workflow not found in deployment")
	ErrRunNotFound           = errors.New("RUN_NOT_FOUND: Run not found")
	ErrSchemaViolation       = errors.New("SCHEMA_VIOLATION: Input does not conform to workflow schema")
	ErrEnvironmentNotFound   = errors.New("ENVIRONMENT_NOT_FOUND: Environment not found")
	ErrPayloadTooLarge       = errors.New("PAYLOAD_TOO_LARGE: Inline JSON payload exceeds 256 KiB")
	ErrRunQuotaExceeded      = errors.New("RUN_QUOTA_EXCEEDED: Environment has reached its nonterminal run limit")
	ErrCreateRateLimited     = errors.New("CREATE_RUN_RATE_LIMITED: Run creation rate exceeded")
	ErrWorkflowTooLarge      = errors.New("WORKFLOW_TOO_LARGE: Workflow exceeds the maximum node limit")
	ErrHistoryLimitExceeded  = errors.New("HISTORY_LIMIT_EXCEEDED: Run event history budget exhausted")
	ErrInvalidCursor         = errors.New("INVALID_CURSOR: Cursor is invalid")
)

const (
	maxInlinePayloadBytes = worker.MaxInlinePayloadBytes
	maxWorkflowNodes      = 50
	maxNonterminalRuns    = 100
	maxRunEvents          = 10000
)

type Service struct {
	pool               *storage.Pool
	tenants            *tenant.Service
	hub                *EventHub
	beforeCreateCommit func() error
	afterCreateCommit  func() error
}

func NewService(pool *storage.Pool, tenants *tenant.Service, hub ...*EventHub) *Service {
	var h *EventHub
	if len(hub) > 0 && hub[0] != nil {
		h = hub[0]
	} else {
		h = NewEventHub()
	}
	return &Service{
		pool:    pool,
		tenants: tenants,
		hub:     h,
	}
}

func (s *Service) Hub() *EventHub {
	return s.hub
}

// SetBeforeCreateCommitHookForTest injects a deterministic failure after all
// create writes have been staged but before the transaction is allowed to
// commit. Production constructors leave it nil.
func (s *Service) SetBeforeCreateCommitHookForTest(hook func() error) {
	s.beforeCreateCommit = hook
}

// SetAfterCreateCommitHookForTest injects a caller-visible failure only after
// CreateRun has committed. Production constructors leave it nil.
func (s *Service) SetAfterCreateCommitHookForTest(hook func() error) {
	s.afterCreateCommit = hook
}

type payloadHashModel struct {
	Workflow     string  `json:"workflow"`
	DeploymentID *string `json:"deploymentId,omitempty"`
	Input        any     `json:"input"`
}

func (s *Service) resolveEnvironment(ctx context.Context, orgID, envParam string) (*tenant.Environment, error) {
	env, err := s.tenants.GetEnvironment(ctx, orgID, envParam)
	if err == nil {
		return env, nil
	}
	// Fallback to query by name. Several projects in one organization may
	// each own an environment with the same name, so every match is
	// collected: zero matches fail, exactly one wins, and several fail with
	// an explicit ambiguity error instead of silently picking one.
	var matches []tenant.Environment
	err = s.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		query := `
			SELECT e.id, e.organization_id, e.project_id, e.name, COALESCE(ea.max_concurrency, 10), e.created_at, e.updated_at
			FROM environments e
			LEFT JOIN environment_admissions ea ON ea.environment_id = e.id AND ea.organization_id = e.organization_id
			WHERE e.organization_id = $1 AND e.name = $2
		`
		rows, err := tx.Query(ctx, query, orgID, envParam)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var out tenant.Environment
			if err := rows.Scan(&out.ID, &out.OrganizationID, &out.ProjectID, &out.Name, &out.MaxConcurrency, &out.CreatedAt, &out.UpdatedAt); err != nil {
				return err
			}
			matches = append(matches, out)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, ErrEnvironmentNotFound
	}
	if len(matches) == 0 {
		return nil, ErrEnvironmentNotFound
	}
	if len(matches) > 1 {
		return nil, tenant.ErrEnvironmentAmbiguous
	}
	return &matches[0], nil
}

// CreateRun implements Blueprint §8 & §14.2 & §20.1:
//  1. Authenticates & resolves environment
//  2. Computes JCS canonical payload hash
//  3. In single transaction: checks idempotency, resolves deployment once,
//     inserts run, run_steps (root READY, others BLOCKED), idempotency_record,
//     run_events, and outbox_events.
//
// Returns (run, isReplay, error).
func (s *Service) CreateRun(
	ctx context.Context,
	orgID, envParam, workflowName, idempotencyKey string,
	explicitDeploymentID *string,
	input any,
	audit *tenant.AuditContext,
) (*RunDTO, bool, error) {
	if audit == nil {
		return nil, false, tenant.ErrAuditRequired
	}
	if idempotencyKey == "" {
		return nil, false, ErrMissingIdempotencyKey
	}

	env, err := s.resolveEnvironment(ctx, orgID, envParam)
	if err != nil {
		if errors.Is(err, tenant.ErrEnvironmentAmbiguous) {
			return nil, false, err
		}
		return nil, false, ErrEnvironmentNotFound
	}
	envID := env.ID

	hashPayload := map[string]any{
		"workflow": workflowName,
		"input":    input,
	}
	if explicitDeploymentID != nil {
		hashPayload["deploymentId"] = *explicitDeploymentID
	}
	canonicalPayload, err := contracts.CanonicalizeGeneric(hashPayload)
	if err != nil {
		return nil, false, fmt.Errorf("canonicalize request: %w", err)
	}
	requestHash := contracts.SHA256Hex(canonicalPayload)
	keyHash := contracts.SHA256Hex([]byte(idempotencyKey))

	canonicalInput, err := contracts.CanonicalizeGeneric(input)
	if err != nil {
		return nil, false, fmt.Errorf("canonicalize input: %w", err)
	}
	if len(canonicalInput) > maxInlinePayloadBytes {
		return nil, false, ErrPayloadTooLarge
	}

	var resultRun *RunDTO
	var isReplay bool

	err = s.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		// 1. Check idempotency record under lock
		// PostgreSQL advisory transaction locks serialize a previously unseen key
		// too; SELECT FOR UPDATE alone cannot lock a missing row.
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, envID+":CREATE_RUN:"+keyHash); err != nil {
			return fmt.Errorf("lock idempotency identity: %w", err)
		}
		var storedReqHash, storedRunID string
		err := tx.QueryRow(ctx, `SELECT request_hash, response_identity::text
			FROM idempotency_records
			WHERE environment_id = $1::uuid AND operation_type='CREATE_RUN' AND key_hash = $2
			FOR UPDATE`, envID, keyHash).Scan(&storedReqHash, &storedRunID)
		if err == nil {
			if storedReqHash != requestHash {
				return ErrIdempotencyConflict
			}
			// Exact replay: load and return original run
			isReplay = true
			resultRun, err = queryRunDTO(ctx, tx, orgID, storedRunID)
			return err
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("query idempotency: %w", err)
		}

		// Disaster recovery admission gate (Blueprint §27.3). Fail closed:
		// a missing or unreadable controls row must not admit new runs.
		var admissionEnabled bool
		var recMode string
		if err := tx.QueryRow(ctx, `SELECT admission_enabled, mode FROM system_recovery_controls WHERE id = 1`).Scan(&admissionEnabled, &recMode); err != nil {
			return fmt.Errorf("%w: read system recovery controls: %v", recovery.ErrRecoveryControlsUnavailable, err)
		}
		if !admissionEnabled || recMode == "READ_ONLY" || recMode == "DISASTER_RECOVERY" {
			return recovery.ErrAdmissionDisabled
		}

		// 2. Serialize all environment admission decisions. The lock must be
		// acquired before counting nonterminal runs or recent creates so
		// concurrent CreateRun requests cannot pass the same cap together.
		var admissionEnvironmentID string
		if err := tx.QueryRow(ctx, `SELECT environment_id::text
			FROM environment_admissions
			WHERE environment_id = $1::uuid AND organization_id = $2::uuid
			FOR UPDATE`, envID, orgID).Scan(&admissionEnvironmentID); err != nil {
			return fmt.Errorf("lock environment admission: %w", err)
		}
		var remainingTokens float64
		if err := tx.QueryRow(ctx, `UPDATE environment_admissions
			SET create_rate_tokens = LEAST(10::double precision,
				create_rate_tokens + EXTRACT(EPOCH FROM (clock_timestamp() - create_rate_updated_at)) * 5) - 1,
				create_rate_updated_at = clock_timestamp(), updated_at = clock_timestamp()
			WHERE environment_id = $1::uuid AND organization_id = $2::uuid
			  AND LEAST(10::double precision,
				create_rate_tokens + EXTRACT(EPOCH FROM (clock_timestamp() - create_rate_updated_at)) * 5) >= 1
			RETURNING create_rate_tokens`, envID, orgID).Scan(&remainingTokens); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrCreateRateLimited
			}
			return fmt.Errorf("debit create-run rate bucket: %w", err)
		}
		var nonterminalRuns int
		if err := tx.QueryRow(ctx, `SELECT count(*)
			FROM runs
			WHERE environment_id = $1::uuid AND organization_id = $2::uuid
			  AND status IN ('QUEUED','RUNNING','WAITING','PAUSING','PAUSED','CANCELLING')`, envID, orgID).Scan(&nonterminalRuns); err != nil {
			return fmt.Errorf("count nonterminal runs: %w", err)
		}
		if nonterminalRuns >= maxNonterminalRuns {
			return ErrRunQuotaExceeded
		}
		// 3. Resolve deployment once
		var deploymentID string
		var manifestJSON []byte
		if explicitDeploymentID != nil && *explicitDeploymentID != "" {
			err = tx.QueryRow(ctx, `SELECT id::text, manifest FROM deployments
				WHERE id = $1::uuid AND organization_id = $2::uuid AND environment_id = $3::uuid`,
				*explicitDeploymentID, orgID, envID).Scan(&deploymentID, &manifestJSON)
			if err != nil {
				if errors.Is(err, pgx.ErrNoRows) {
					return ErrDeploymentNotFound
				}
				return fmt.Errorf("query explicit deployment: %w", err)
			}
		} else {
			err = tx.QueryRow(ctx, `SELECT d.id::text, d.manifest
				FROM workflow_channels c
				JOIN deployments d ON d.id = c.active_deployment_id AND d.organization_id = c.organization_id
				WHERE c.environment_id = $1::uuid AND c.organization_id = $2::uuid AND c.workflow_name = $3`,
				envID, orgID, workflowName).Scan(&deploymentID, &manifestJSON)
			if err != nil {
				if errors.Is(err, pgx.ErrNoRows) {
					return ErrNoActiveDeployment
				}
				return fmt.Errorf("query active deployment: %w", err)
			}
		}

		// 3. Parse manifest and validate input against workflow inputSchema
		var manifest deploymentManifest
		if err := json.Unmarshal(manifestJSON, &manifest); err != nil {
			return fmt.Errorf("decode manifest: %w", err)
		}
		var targetWorkflow *workflowManifest
		for i := range manifest.Workflows {
			if manifest.Workflows[i].Name == workflowName {
				targetWorkflow = &manifest.Workflows[i]
				break
			}
		}
		if targetWorkflow == nil {
			return ErrWorkflowNotFound
		}
		if len(targetWorkflow.Nodes) > maxWorkflowNodes {
			return ErrWorkflowTooLarge
		}

		if targetWorkflow.InputSchema != nil {
			if err := contracts.ValidatePayload(targetWorkflow.InputSchema, input); err != nil {
				return ErrSchemaViolation
			}
		}

		// 4. Create Run with the MVP lifetime default: 24h from acceptance.
		var runID string
		var createdAt time.Time
		var deadlineAt *time.Time
		runQuery := `INSERT INTO runs (
			organization_id, environment_id, deployment_id, workflow_name,
			idempotency_key, status, revision, input, last_event_sequence,
			deadline_at, created_at, updated_at
		) VALUES (
			$1::uuid, $2::uuid, $3::uuid, $4,
			$5, 'QUEUED', 1, $6::jsonb, 1,
			clock_timestamp()+($7::bigint*INTERVAL '1 millisecond'),
			clock_timestamp(), clock_timestamp()
		) RETURNING id::text, created_at, deadline_at`
		if err := tx.QueryRow(ctx, runQuery, orgID, envID, deploymentID, workflowName, idempotencyKey, canonicalInput, RunLifetimeMs).
			Scan(&runID, &createdAt, &deadlineAt); err != nil {
			return fmt.Errorf("insert run: %w", err)
		}

		// 5. Create run_steps from graph
		for _, node := range targetWorkflow.Nodes {
			kind := node.Type
			if kind == "" {
				kind = "task"
			}
			initState := "BLOCKED"
			var eligibleAt any = nil
			if len(node.After) == 0 {
				initState = "READY"
				eligibleAt = time.Now()
			}
			stepQuery := `INSERT INTO run_steps (
				organization_id, environment_id, run_id, node_id,
				kind, state, eligible_at, created_at, updated_at
			) VALUES (
				$1::uuid, $2::uuid, $3::uuid, $4,
				$5, $6, $7, clock_timestamp(), clock_timestamp()
			)`
			if _, err := tx.Exec(ctx, stepQuery, orgID, envID, runID, node.ID, kind, initState, eligibleAt); err != nil {
				return fmt.Errorf("insert step %s: %w", node.ID, err)
			}
		}

		// 6. Record idempotency
		idempotencyInsert := `INSERT INTO idempotency_records (
			organization_id, environment_id, operation_type, key_hash, request_hash,
			response_identity, expires_at, created_at
		) VALUES (
			$1::uuid, $2::uuid, 'CREATE_RUN', $3, $4,
			$5::uuid, 'infinity', clock_timestamp()
		)`
		if _, err := tx.Exec(ctx, idempotencyInsert, orgID, envID, keyHash, requestHash, runID); err != nil {
			return fmt.Errorf("insert idempotency: %w", err)
		}

		// 7. Initial run_event
		eventInsert := `INSERT INTO run_events (
			organization_id, run_id, sequence, event_type, payload
		) VALUES (
			$1::uuid, $2::uuid, 1, 'RUN_CREATED',
			jsonb_build_object('runId', $2::uuid, 'workflowName', $3::text, 'deploymentId', $4::uuid)
		)`
		if _, err := tx.Exec(ctx, eventInsert, orgID, runID, workflowName, deploymentID); err != nil {
			return fmt.Errorf("insert run event: %w", err)
		}

		// 8. Outbox hint
		outboxInsert := `INSERT INTO outbox_events (
			organization_id, subject, payload
		) VALUES (
			$1::uuid, 'execution.state_changed',
			jsonb_build_object('runId', $2::uuid, 'eventType', 'RUN_CREATED')
		)`
		if _, err := tx.Exec(ctx, outboxInsert, orgID, runID); err != nil {
			return fmt.Errorf("insert outbox: %w", err)
		}

		// 9. Audit event
		var actorID *string
		if audit.ActorID != nil && *audit.ActorID != "" {
			actorID = audit.ActorID
		}
		auditMeta, err := json.Marshal(map[string]any{
			"actor_type":     audit.ActorType,
			"role":           audit.Role,
			"capabilities":   audit.Capabilities,
			"environment_id": envID,
			"workflow_name":  workflowName,
			"deployment_id":  deploymentID,
		})
		if err != nil {
			return fmt.Errorf("marshal audit metadata: %w", err)
		}
		auditInsert := `INSERT INTO audit_events (
			organization_id, actor_id, action, target_type, target_id, correlation_id, reason, metadata
		) VALUES (
			$1::uuid, $2, 'run.create', 'run', $3::uuid, $4, $5, $6::jsonb
		)`
		if _, err := tx.Exec(ctx, auditInsert, orgID, actorID, runID, audit.CorrelationID, audit.Reason, auditMeta); err != nil {
			return fmt.Errorf("insert audit: %w", err)
		}

		var deadlineStr *string
		if deadlineAt != nil {
			d := deadlineAt.UTC().Format(time.RFC3339)
			deadlineStr = &d
		}

		resultRun = &RunDTO{
			ID:             runID,
			OrganizationID: orgID,
			ProjectID:      env.ProjectID,
			EnvironmentID:  envID,
			WorkflowName:   workflowName,
			DeploymentID:   deploymentID,
			Status:         contracts.RunStatusQUEUED,
			Revision:       1,
			CreatedAt:      createdAt.UTC().Format(time.RFC3339),
			DeadlineAt:     deadlineStr,
		}
		if s.beforeCreateCommit != nil {
			return s.beforeCreateCommit()
		}
		return nil
	})

	if err != nil {
		return nil, false, err
	}
	if s.afterCreateCommit != nil {
		if err := s.afterCreateCommit(); err != nil {
			return nil, false, err
		}
	}
	if s.hub != nil {
		s.hub.Publish(resultRun.ID)
	}
	return resultRun, isReplay, nil
}

func (s *Service) GetRun(ctx context.Context, orgID, runID string) (*RunSnapshotDTO, error) {
	var snapshot *RunSnapshotDTO
	err := s.pool.WithTenantReadOnlyRepeatableReadTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		run, err := queryRunDTO(ctx, tx, orgID, runID)
		if err != nil {
			return err
		}

		var lastEventSeq int64
		var rawOutput, rawError []byte
		err = tx.QueryRow(ctx, `SELECT last_event_sequence, output, reason_code FROM runs
			WHERE id = $1::uuid AND organization_id = $2::uuid`, runID, orgID).Scan(&lastEventSeq, &rawOutput, &run.ReasonCode)
		if err != nil {
			return err
		}

		var output any
		if len(rawOutput) > 0 && string(rawOutput) != "null" {
			_ = json.Unmarshal(rawOutput, &output)
		}

		var runError any
		if len(rawError) > 0 && string(rawError) != "null" {
			_ = json.Unmarshal(rawError, &runError)
		}

		rows, err := tx.Query(ctx, `SELECT id::text, node_id, state, current_epoch, completion_source
			FROM run_steps
			WHERE run_id = $1::uuid AND organization_id = $2::uuid
			ORDER BY created_at ASC, id ASC`, runID, orgID)
		if err != nil {
			return fmt.Errorf("query steps: %w", err)
		}
		defer rows.Close()

		steps := make([]RunStepDTO, 0)
		for rows.Next() {
			var st RunStepDTO
			var stateStr string
			if err := rows.Scan(&st.ID, &st.NodeID, &stateStr, &st.CurrentEpoch, &st.CompletionSource); err != nil {
				return err
			}
			st.Status = contracts.StepStatus(stateStr)
			st.Attempts = []AttemptSummaryDTO{}
			steps = append(steps, st)
		}
		if err := rows.Err(); err != nil {
			return err
		}

		if len(steps) > 0 {
			stepIDs := make([]string, len(steps))
			for i, st := range steps {
				stepIDs[i] = st.ID
			}
			attemptRows, err := tx.Query(ctx, `
				SELECT id::text, step_id::text, attempt_number, status, session_id::text,
					epoch, started_at, completed_at, error
				FROM task_attempts
				WHERE organization_id = $1::uuid AND step_id = ANY($2::uuid[])
				ORDER BY attempt_number ASC
			`, orgID, stepIDs)
			if err != nil {
				return fmt.Errorf("query attempts: %w", err)
			}
			defer attemptRows.Close()

			attemptsByStep := make(map[string][]AttemptSummaryDTO)
			for attemptRows.Next() {
				var a AttemptSummaryDTO
				var stepID string
				var sessID *string
				var startedAt, completedAt *time.Time
				var rawErr []byte
				if err := attemptRows.Scan(&a.ID, &stepID, &a.AttemptNumber, &a.Status, &sessID, &a.OwnershipEpoch, &startedAt, &completedAt, &rawErr); err != nil {
					return err
				}
				a.WorkerSessionID = sessID
				if startedAt != nil {
					s := startedAt.UTC().Format(time.RFC3339Nano)
					a.StartedAt = &s
				}
				if completedAt != nil {
					c := completedAt.UTC().Format(time.RFC3339Nano)
					a.CompletedAt = &c
				}
				if len(rawErr) > 0 && string(rawErr) != "null" {
					_ = json.Unmarshal(rawErr, &a.Error)
				}
				attemptsByStep[stepID] = append(attemptsByStep[stepID], a)
			}
			if err := attemptRows.Err(); err != nil {
				return err
			}

			for i := range steps {
				if atts, ok := attemptsByStep[steps[i].ID]; ok {
					steps[i].Attempts = atts
				}
			}
		}

		var activeWorkers int
		if run.DeploymentID != "" {
			err := tx.QueryRow(ctx, `
				SELECT COUNT(DISTINCT ws.id)
				FROM worker_sessions ws
				JOIN workers w ON w.id = ws.worker_id AND w.organization_id = ws.organization_id
				JOIN worker_deployments wd ON wd.session_id = ws.id AND wd.organization_id = ws.organization_id
				JOIN deployments d ON d.bundle_digest = wd.bundle_digest AND d.organization_id = ws.organization_id
				WHERE ws.organization_id = $1::uuid
				  AND d.id = $2::uuid
				  AND ws.environment_id = $3::uuid
				  AND w.environment_id = $3::uuid
				  AND w.status = 'ACTIVE'
				  AND ws.revoked_at IS NULL
				  AND ws.expires_at > clock_timestamp()
			`, orgID, run.DeploymentID, run.EnvironmentID).Scan(&activeWorkers)
			if err != nil {
				return fmt.Errorf("query active compatible workers: %w", err)
			}
		}

		var waitingReason *string
		isTerminal := run.Status == contracts.RunStatusSUCCEEDED ||
			run.Status == contracts.RunStatusFAILED ||
			run.Status == contracts.RunStatusCANCELLED

		if !isTerminal && activeWorkers == 0 {
			hasRunningAttempt := false
			for _, st := range steps {
				for _, att := range st.Attempts {
					if att.Status == "RUNNING" {
						hasRunningAttempt = true
						break
					}
				}
				if hasRunningAttempt {
					break
				}
			}
			if !hasRunningAttempt {
				reason := "NO_COMPATIBLE_WORKERS"
				waitingReason = &reason
			}
		}

		reconciliationCases := make([]ReconciliationCaseDTO, 0)
		if len(steps) > 0 {
			stepIDs := make([]string, len(steps))
			for i, st := range steps {
				stepIDs[i] = st.ID
			}
			caseRows, err := tx.Query(ctx, `
				SELECT id::text, step_id::text, attempt_id::text, reason, evidence,
					status, resolution, actor_id::text, revision, resolved_at, created_at
				FROM reconciliation_cases
				WHERE organization_id = $1::uuid AND step_id = ANY($2::uuid[])
				ORDER BY created_at ASC, id ASC
			`, orgID, stepIDs)
			if err != nil {
				return fmt.Errorf("query reconciliation cases: %w", err)
			}
			for caseRows.Next() {
				var c ReconciliationCaseDTO
				var rawEvidence []byte
				var resolvedAt *time.Time
				var createdAt time.Time
				if err := caseRows.Scan(&c.ID, &c.StepID, &c.AttemptID, &c.Reason, &rawEvidence,
					&c.Status, &c.Resolution, &c.ActorID, &c.Revision, &resolvedAt, &createdAt); err != nil {
					caseRows.Close()
					return err
				}
				if len(rawEvidence) > 0 && string(rawEvidence) != "null" {
					_ = json.Unmarshal(rawEvidence, &c.Evidence)
				}
				if resolvedAt != nil {
					s := resolvedAt.UTC().Format(time.RFC3339Nano)
					c.ResolvedAt = &s
				}
				c.CreatedAt = createdAt.UTC().Format(time.RFC3339Nano)
				reconciliationCases = append(reconciliationCases, c)
			}
			caseRows.Close()
			if err := caseRows.Err(); err != nil {
				return err
			}
		}

		if run.Status == contracts.RunStatusFAILED && runError == nil {
			if run.ReasonCode != nil {
				runError = map[string]any{
					"code":    *run.ReasonCode,
					"message": *run.ReasonCode,
				}
			} else {
				for _, st := range steps {
					for _, att := range st.Attempts {
						if att.Error != nil {
							runError = att.Error
							break
						}
					}
					if runError != nil {
						break
					}
				}
			}
		}

		snapshot = &RunSnapshotDTO{
			RunDTO:                  *run,
			LastEventSequence:       lastEventSeq,
			Steps:                   steps,
			ReconciliationCases:     reconciliationCases,
			Output:                  output,
			Error:                   runError,
			WaitingReason:           waitingReason,
			ActiveCompatibleWorkers: activeWorkers,
		}
		return nil
	})

	if err != nil {
		return nil, err
	}
	return snapshot, nil
}

func (s *Service) ListRuns(ctx context.Context, orgID, envParam string, cursor *string, limit int) (*RunListResponseDTO, error) {
	if limit <= 0 || limit > 100 {
		limit = 25
	}
	env, err := s.resolveEnvironment(ctx, orgID, envParam)
	if err != nil {
		if errors.Is(err, tenant.ErrEnvironmentAmbiguous) {
			return nil, err
		}
		return nil, ErrEnvironmentNotFound
	}

	var items []RunDTO
	var nextCursor *string

	err = s.pool.WithTenantReadOnlyRepeatableReadTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		var rows pgx.Rows
		var qErr error
		if cursor != nil && *cursor != "" {
			cursorTime, cursorID, err := decodeCursor(*cursor)
			if err != nil {
				return ErrInvalidCursor
			}
			query := `
				SELECT r.id::text, r.organization_id::text, e.project_id::text, r.environment_id::text,
					r.workflow_name, r.deployment_id::text, r.status, r.reason_code, r.revision,
					r.created_at, r.deadline_at
				FROM runs r
				JOIN environments e ON e.id = r.environment_id AND e.organization_id = r.organization_id
				WHERE r.organization_id = $1::uuid AND r.environment_id = $2::uuid
					AND (r.created_at < $3 OR (r.created_at = $3 AND r.id < $4::uuid))
				ORDER BY r.created_at DESC, r.id DESC
				LIMIT $5
			`
			rows, qErr = tx.Query(ctx, query, orgID, env.ID, cursorTime, cursorID, limit+1)
		} else {
			query := `
				SELECT r.id::text, r.organization_id::text, e.project_id::text, r.environment_id::text,
					r.workflow_name, r.deployment_id::text, r.status, r.reason_code, r.revision,
					r.created_at, r.deadline_at
				FROM runs r
				JOIN environments e ON e.id = r.environment_id AND e.organization_id = r.organization_id
				WHERE r.organization_id = $1::uuid AND r.environment_id = $2::uuid
				ORDER BY r.created_at DESC, r.id DESC
				LIMIT $3
			`
			rows, qErr = tx.Query(ctx, query, orgID, env.ID, limit+1)
		}
		if qErr != nil {
			return fmt.Errorf("query runs: %w", qErr)
		}
		defer rows.Close()

		type rawRun struct {
			dto       RunDTO
			createdAt time.Time
		}
		var rawList []rawRun
		for rows.Next() {
			var r RunDTO
			var statusStr string
			var createdAt time.Time
			var deadlineAt *time.Time
			if err := rows.Scan(
				&r.ID, &r.OrganizationID, &r.ProjectID, &r.EnvironmentID,
				&r.WorkflowName, &r.DeploymentID, &statusStr, &r.ReasonCode, &r.Revision,
				&createdAt, &deadlineAt,
			); err != nil {
				return err
			}
			r.Status = contracts.RunStatus(statusStr)
			r.CreatedAt = createdAt.UTC().Format(time.RFC3339Nano)
			if deadlineAt != nil {
				d := deadlineAt.UTC().Format(time.RFC3339Nano)
				r.DeadlineAt = &d
			}
			rawList = append(rawList, rawRun{dto: r, createdAt: createdAt})
		}
		if err := rows.Err(); err != nil {
			return err
		}

		if len(rawList) > limit {
			last := rawList[limit-1]
			nc := encodeCursor(last.createdAt, last.dto.ID)
			nextCursor = &nc
			rawList = rawList[:limit]
		}

		items = make([]RunDTO, len(rawList))
		for i := range rawList {
			items[i] = rawList[i].dto
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if items == nil {
		items = []RunDTO{}
	}

	return &RunListResponseDTO{
		Items:      items,
		NextCursor: nextCursor,
	}, nil
}

func (s *Service) ListWorkers(ctx context.Context, orgID, envParam string, cursor *string, limit int) (*WorkerListResponseDTO, error) {
	if limit <= 0 || limit > 100 {
		limit = 25
	}
	env, err := s.resolveEnvironment(ctx, orgID, envParam)
	if err != nil {
		if errors.Is(err, tenant.ErrEnvironmentAmbiguous) {
			return nil, err
		}
		return nil, ErrEnvironmentNotFound
	}

	var items []WorkerDTO
	var nextCursor *string

	err = s.pool.WithTenantReadOnlyRepeatableReadTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		var rows pgx.Rows
		var qErr error
		if cursor != nil && *cursor != "" {
			cursorTime, cursorID, err := decodeCursor(*cursor)
			if err != nil {
				return ErrInvalidCursor
			}
			query := `
				SELECT w.id::text, w.environment_id::text, w.pool_name, w.status, w.created_at
				FROM workers w
				WHERE w.organization_id = $1::uuid AND w.environment_id = $2::uuid
					AND (w.created_at < $3 OR (w.created_at = $3 AND w.id < $4::uuid))
				ORDER BY w.created_at DESC, w.id DESC
				LIMIT $5
			`
			rows, qErr = tx.Query(ctx, query, orgID, env.ID, cursorTime, cursorID, limit+1)
		} else {
			query := `
				SELECT w.id::text, w.environment_id::text, w.pool_name, w.status, w.created_at
				FROM workers w
				WHERE w.organization_id = $1::uuid AND w.environment_id = $2::uuid
				ORDER BY w.created_at DESC, w.id DESC
				LIMIT $3
			`
			rows, qErr = tx.Query(ctx, query, orgID, env.ID, limit+1)
		}
		if qErr != nil {
			return fmt.Errorf("query workers: %w", qErr)
		}
		defer rows.Close()

		type rawWorker struct {
			dto       WorkerDTO
			createdAt time.Time
		}
		var rawList []rawWorker
		for rows.Next() {
			var w WorkerDTO
			var createdAt time.Time
			if err := rows.Scan(&w.ID, &w.EnvironmentID, &w.Pool, &w.Status, &createdAt); err != nil {
				return err
			}
			w.DeploymentDigests = []string{}
			rawList = append(rawList, rawWorker{dto: w, createdAt: createdAt})
		}
		if err := rows.Err(); err != nil {
			return err
		}

		if len(rawList) > limit {
			last := rawList[limit-1]
			nc := encodeCursor(last.createdAt, last.dto.ID)
			nextCursor = &nc
			rawList = rawList[:limit]
		}

		if len(rawList) > 0 {
			workerIDs := make([]string, len(rawList))
			for i, rw := range rawList {
				workerIDs[i] = rw.dto.ID
			}
			dRows, dErr := tx.Query(ctx, `
				SELECT ws.worker_id::text, wd.bundle_digest
				FROM worker_deployments wd
				JOIN worker_sessions ws ON ws.id = wd.session_id AND ws.organization_id = wd.organization_id
				WHERE ws.organization_id = $1::uuid AND ws.worker_id = ANY($2::uuid[])
					AND ws.revoked_at IS NULL AND ws.expires_at > clock_timestamp()
				GROUP BY ws.worker_id, wd.bundle_digest
				ORDER BY wd.bundle_digest ASC
			`, orgID, workerIDs)
			if dErr != nil {
				return fmt.Errorf("query worker digests: %w", dErr)
			}
			defer dRows.Close()

			digestsByWorker := make(map[string][]string)
			for dRows.Next() {
				var wid, digest string
				if err := dRows.Scan(&wid, &digest); err != nil {
					return err
				}
				digestsByWorker[wid] = append(digestsByWorker[wid], digest)
			}
			if err := dRows.Err(); err != nil {
				return err
			}
			for i := range rawList {
				if d, ok := digestsByWorker[rawList[i].dto.ID]; ok {
					rawList[i].dto.DeploymentDigests = d
				}
			}
		}

		items = make([]WorkerDTO, len(rawList))
		for i := range rawList {
			items[i] = rawList[i].dto
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if items == nil {
		items = []WorkerDTO{}
	}

	return &WorkerListResponseDTO{
		Items:      items,
		NextCursor: nextCursor,
	}, nil
}

func (s *Service) GetRunEvents(ctx context.Context, orgID, runID string, cursor int64, limit int, hasPayloadRead bool) (*RunEventsResponseDTO, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	var resp *RunEventsResponseDTO

	err := s.pool.WithTenantReadOnlyRepeatableReadTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		var exists bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM runs WHERE id = $1::uuid AND organization_id = $2::uuid)`, runID, orgID).Scan(&exists); err != nil {
			return err
		}
		if !exists {
			return ErrRunNotFound
		}

		rows, err := tx.Query(ctx, `
			SELECT id::text, run_id::text, sequence, version, event_type, payload, committed_at
			FROM run_events
			WHERE organization_id = $1::uuid AND run_id = $2::uuid AND sequence > $3
			ORDER BY sequence ASC
			LIMIT $4
		`, orgID, runID, cursor, limit+1)
		if err != nil {
			return fmt.Errorf("query run events: %w", err)
		}
		defer rows.Close()

		events := make([]RunEventDTO, 0)
		for rows.Next() {
			var ev RunEventDTO
			var rawPayload []byte
			var committedAt time.Time
			if err := rows.Scan(&ev.ID, &ev.RunID, &ev.Sequence, &ev.SchemaVersion, &ev.Type, &rawPayload, &committedAt); err != nil {
				return err
			}
			ev.Type = CanonicalEventType(ev.Type)
			ev.CommittedAt = committedAt.UTC().Format(time.RFC3339Nano)
			if len(rawPayload) > 0 {
				var parsedPayload any
				if err := json.Unmarshal(rawPayload, &parsedPayload); err == nil {
					if !hasPayloadRead {
						parsedPayload = SanitizeEventPayload(parsedPayload)
					}
					ev.Payload = parsedPayload
				} else {
					ev.Payload = map[string]any{}
				}
			} else {
				ev.Payload = map[string]any{}
			}
			events = append(events, ev)
		}
		if err := rows.Err(); err != nil {
			return err
		}

		var hasMore bool
		var nextCursor *int64
		if len(events) > limit {
			hasMore = true
			events = events[:limit]
			lastSeq := events[limit-1].Sequence
			nextCursor = &lastSeq
		}

		resp = &RunEventsResponseDTO{
			Events:     events,
			HasMore:    hasMore,
			NextCursor: nextCursor,
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (s *Service) GetRunLogs(ctx context.Context, orgID, runID string, stepID, attemptID *string, cursor *string, limit int) (*RunLogsResponseDTO, error) {
	if limit <= 0 || limit > 100 {
		limit = 50
	}
	var resp *RunLogsResponseDTO

	err := s.pool.WithTenantReadOnlyRepeatableReadTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		var runExists bool
		err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM runs WHERE id = $1::uuid AND organization_id = $2::uuid)`, runID, orgID).Scan(&runExists)
		if err != nil {
			return err
		}
		if !runExists {
			return ErrRunNotFound
		}

		var cursorTime *time.Time
		var cursorID *string
		if cursor != nil && *cursor != "" {
			t, id, err := decodeCursor(*cursor)
			if err != nil {
				return fmt.Errorf("invalid cursor: %w", err)
			}
			cursorTime = &t
			cursorID = &id
		}

		query := `
			SELECT id::text, run_id::text, step_id::text, attempt_id::text, sequence, timestamp, level, message, created_at
			FROM task_logs
			WHERE organization_id = $1::uuid AND run_id = $2::uuid
				AND ($3::uuid IS NULL OR step_id = $3::uuid)
				AND ($4::uuid IS NULL OR attempt_id = $4::uuid)
				AND ($5::timestamptz IS NULL OR (created_at, id) > ($5::timestamptz, $6::uuid))
			ORDER BY created_at ASC, id ASC
			LIMIT $7
		`
		var stepUUID, attemptUUID any = nil, nil
		if stepID != nil && *stepID != "" {
			stepUUID = *stepID
		}
		if attemptID != nil && *attemptID != "" {
			attemptUUID = *attemptID
		}

		rows, err := tx.Query(ctx, query, orgID, runID, stepUUID, attemptUUID, cursorTime, cursorID, limit+1)
		if err != nil {
			return fmt.Errorf("query task logs: %w", err)
		}
		defer rows.Close()

		type logItemWithCreatedAt struct {
			rec       TaskLogRecordDTO
			createdAt time.Time
		}

		itemsWithMeta := make([]logItemWithCreatedAt, 0)
		for rows.Next() {
			var rec TaskLogRecordDTO
			var ts, createdAt time.Time
			if err := rows.Scan(&rec.ID, &rec.RunID, &rec.StepID, &rec.AttemptID, &rec.Sequence, &ts, &rec.Level, &rec.Message, &createdAt); err != nil {
				return err
			}
			rec.Timestamp = ts.UTC().Format(time.RFC3339Nano)
			itemsWithMeta = append(itemsWithMeta, logItemWithCreatedAt{rec: rec, createdAt: createdAt})
		}
		if err := rows.Err(); err != nil {
			return err
		}

		var nextCursor *string
		if len(itemsWithMeta) > limit {
			last := itemsWithMeta[limit-1]
			nc := encodeCursor(last.createdAt, last.rec.ID)
			nextCursor = &nc
			itemsWithMeta = itemsWithMeta[:limit]
		}

		items := make([]TaskLogRecordDTO, len(itemsWithMeta))
		for i, it := range itemsWithMeta {
			items[i] = it.rec
		}

		var isExpired bool
		var message *string
		var droppedCount int
		var budgetExhausted bool
		if err := tx.QueryRow(ctx, `
			SELECT COALESCE(SUM(ta.dropped_log_count), 0)::integer,
			       COALESCE(BOOL_OR(ta.log_budget_exhausted), FALSE)
			FROM task_attempts ta
			JOIN run_steps rs ON rs.id = ta.step_id AND rs.organization_id = ta.organization_id
			WHERE rs.run_id = $1::uuid
			  AND ta.organization_id = $2::uuid
			  AND ($3::uuid IS NULL OR rs.id = $3::uuid)
			  AND ($4::uuid IS NULL OR ta.id = $4::uuid)
		`, runID, orgID, stepUUID, attemptUUID).Scan(&droppedCount, &budgetExhausted); err != nil {
			return fmt.Errorf("get task log drop state: %w", err)
		}

		// Only evaluate expiration if 0 items returned on initial query (no cursor)
		if len(items) == 0 && (cursor == nil || *cursor == "") {
			var hadLogsRecorded bool
			err := tx.QueryRow(ctx, `
				SELECT EXISTS (
					SELECT 1
					FROM task_attempts ta
					JOIN run_steps rs ON rs.id = ta.step_id AND rs.organization_id = ta.organization_id
					WHERE rs.run_id = $1::uuid
					  AND ta.organization_id = $2::uuid
					  AND ($3::uuid IS NULL OR rs.id = $3::uuid)
					  AND ($4::uuid IS NULL OR ta.id = $4::uuid)
					  AND ta.logs_recorded = TRUE
				)
			`, runID, orgID, stepUUID, attemptUUID).Scan(&hadLogsRecorded)
			if err != nil {
				return fmt.Errorf("check recorded logs: %w", err)
			}

			if hadLogsRecorded {
				isExpired = true
				msg := "Logs have expired due to the 7-day retention policy"
				message = &msg
			}
		}

		resp = &RunLogsResponseDTO{
			Items:           items,
			NextCursor:      nextCursor,
			Expired:         isExpired,
			Message:         message,
			DroppedCount:    droppedCount,
			BudgetExhausted: budgetExhausted,
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// PruneExpiredTaskLogs deletes one bounded batch of task logs older than 7 days
// using database time. Each call has its own tenant transaction so scheduler
// sweeps commit between batches rather than holding a tenant transaction open.
func (s *Service) PruneExpiredTaskLogs(ctx context.Context, orgID string, batchSize int) (int64, error) {
	if batchSize <= 0 {
		batchSize = 1000
	}
	var pruned int64
	err := s.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		tag, err := tx.Exec(ctx, `
			DELETE FROM task_logs
			WHERE id IN (
				SELECT id FROM task_logs
				WHERE organization_id = $1::uuid AND created_at < clock_timestamp() - INTERVAL '7 days'
				ORDER BY created_at ASC, id ASC
				LIMIT $2
			)
		`, orgID, batchSize)
		if err != nil {
			return fmt.Errorf("prune expired task logs: %w", err)
		}
		pruned = tag.RowsAffected()
		return nil
	})
	if err != nil {
		return 0, err
	}
	return pruned, nil
}

func CanonicalEventType(dbType string) string {
	switch strings.ToUpper(dbType) {
	case "RUN_CREATED":
		return "run.created"
	case "TASK_CLAIMED":
		return "attempt.claimed"
	case "TASK_STARTED":
		return "attempt.started"
	case "TASK_COMPLETED":
		return "attempt.completed"
	case "TASK_LOST":
		return "attempt.lost"
	case "STEP_READY":
		return "step.ready"
	case "STEP_SUCCEEDED":
		return "step.succeeded"
	case "STEP_FAILED":
		return "step.failed"
	case "RUN_COMPLETED":
		return "run.completed"
	case "RUN_SUCCEEDED":
		return "run.succeeded"
	case "RUN_FAILED":
		return "run.failed"
	case "RUN_CANCELLED":
		return "run.cancelled"
	case "RUN_PAUSED":
		return "run.paused"
	case "RUN_RESUMED":
		return "run.resumed"
	default:
		if strings.Contains(dbType, ".") {
			return strings.ToLower(dbType)
		}
		parts := strings.SplitN(strings.ToLower(dbType), "_", 2)
		if len(parts) == 2 {
			return parts[0] + "." + parts[1]
		}
		return "run." + strings.ToLower(dbType)
	}
}

func SanitizeEventPayload(p any) any {
	m, ok := p.(map[string]any)
	if !ok {
		return map[string]any{}
	}
	sanitized := make(map[string]any)
	allowedKeys := map[string]bool{
		"runId":        true,
		"workflowName": true,
		"deploymentId": true,
		"stepId":       true,
		"attemptId":    true,
		"epoch":        true,
		"reason":       true,
		"status":       true,
		"sequence":     true,
		"recovery":     true,
		"nodeId":       true,
		"timeoutMs":    true,
	}
	for k, v := range m {
		if allowedKeys[k] {
			sanitized[k] = v
		}
	}
	return sanitized
}

func encodeCursor(t time.Time, id string) string {
	raw := fmt.Sprintf("%s|%s", t.UTC().Format(time.RFC3339Nano), id)
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

func decodeCursor(cursor string) (time.Time, string, error) {
	b, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return time.Time{}, "", err
	}
	parts := strings.SplitN(string(b), "|", 2)
	if len(parts) != 2 {
		return time.Time{}, "", errors.New("invalid cursor format")
	}
	t, err := time.Parse(time.RFC3339Nano, parts[0])
	if err != nil {
		return time.Time{}, "", err
	}
	return t, parts[1], nil
}

func queryRunDTO(ctx context.Context, tx storage.Tx, orgID, runID string) (*RunDTO, error) {
	var run RunDTO
	var statusStr string
	var createdAt time.Time
	var deadlineAt *time.Time
	query := `SELECT r.id::text, r.organization_id::text, e.project_id::text, r.environment_id::text,
		r.workflow_name, r.deployment_id::text, r.status, r.reason_code, r.revision,
		r.created_at, r.deadline_at, r.termination_confirmed
		FROM runs r
		JOIN environments e ON e.id = r.environment_id AND e.organization_id = r.organization_id
		WHERE r.id = $1::uuid AND r.organization_id = $2::uuid`
	err := tx.QueryRow(ctx, query, runID, orgID).Scan(
		&run.ID, &run.OrganizationID, &run.ProjectID, &run.EnvironmentID,
		&run.WorkflowName, &run.DeploymentID, &statusStr, &run.ReasonCode, &run.Revision,
		&createdAt, &deadlineAt, &run.TerminationConfirmed,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrRunNotFound
		}
		return nil, fmt.Errorf("query run: %w", err)
	}
	run.Status = contracts.RunStatus(statusStr)
	run.CreatedAt = createdAt.UTC().Format(time.RFC3339Nano)
	if deadlineAt != nil {
		d := deadlineAt.UTC().Format(time.RFC3339Nano)
		run.DeadlineAt = &d
	}
	return &run, nil
}

func (s *Service) GetRunEventRetentionBounds(ctx context.Context, orgID, runID string) (minSeq int64, maxSeq int64, err error) {
	err = s.pool.WithTenantReadOnlyRepeatableReadTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		var minS, maxS *int64
		qErr := tx.QueryRow(ctx, `
			SELECT MIN(sequence), MAX(sequence)
			FROM run_events
			WHERE organization_id = $1::uuid AND run_id = $2::uuid
		`, orgID, runID).Scan(&minS, &maxS)
		if qErr != nil {
			return qErr
		}
		if minS != nil {
			minSeq = *minS
		}
		if maxS != nil {
			maxSeq = *maxS
		}
		return nil
	})
	return minSeq, maxSeq, err
}
