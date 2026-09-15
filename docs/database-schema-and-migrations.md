# Database Schema, RLS Boundaries, and Migration Contract

## 1. Architectural Scope and Blueprint Traceability

- **Blueprint sections**: §11 (Persistence, transactions, and checkpoints), §18 (Data model and data lifecycle), §24 (Authentication, authorization, and tenant isolation), §26 (Deployment, delivery, and migrations).
- **Invariants enforced**:
  - `INV-01`: Tenant, project, and environment isolation via `FORCE ROW LEVEL SECURITY` and composite foreign keys.
  - `INV-03`: At most one current ownership lease exists per step (`task_leases` primary key on `step_id`).
  - `INV-06`: State transitions, execution events, and outbox intents are committed atomically in the same transaction. _(Note: Issue #3 establishes the complete schema foundation—`runs`, `run_steps`, `run_events`, `outbox_events`, `task_attempts`, `task_leases`, and `timers`. Full runtime orchestration and transaction boundary enforcement will be delivered in the M1 execution engine milestone)._
  - `INV-08`: Explicit lock hierarchy (`environment_admissions` $\rightarrow$ `runs` $\rightarrow$ `run_steps` sorted $\rightarrow$ `attempts/leases`).
- **Failure Mode coverage**:
  - `F-27`: Tenant context reused in connection pool. Prevented by transaction-local `SET LOCAL app.current_organization_id` (via `set_config('app.current_organization_id', $1, true)`), which automatically resets upon transaction commit or rollback, ensuring clean connection reuse.

---

## 2. PostgreSQL Role and Privilege Model

In accordance with Blueprint §24.3 and §26.3, runtime operations, background scheduling, and database migrations use strictly separated database roles configured via `scripts/bootstrap-db-roles.sql`:

| Role                | DDL Privileges     | DML Privileges                                      | RLS Policy Status                            | Purpose                                                       |
| :------------------ | :----------------- | :-------------------------------------------------- | :------------------------------------------- | :------------------------------------------------------------ |
| `deadbolt_migrator` | Yes (schema owner) | Yes                                                 | Bypassed during DDL migration execution      | Executes ordered migrations with pinned session advisory lock |
| `deadbolt_runtime`  | **No** (revoked)   | `SELECT, INSERT, UPDATE, DELETE` on `public` tables | **FORCE ROW LEVEL SECURITY** (`NOBYPASSRLS`) | Application runtime used by Go control plane                  |
| `deadbolt_system`   | **No**             | **None** on tenant tables (`public.*` revoked)      | Restricted security definer                  | Used by scheduler for tenant ID enumeration only              |

### Security Invariant: Missing Context Fails Closed

The tenant context function:

```sql
CREATE OR REPLACE FUNCTION app.current_organization_id() RETURNS uuid AS $$
BEGIN
    RETURN NULLIF(current_setting('app.current_organization_id', true), '')::uuid;
EXCEPTION WHEN OTHERS THEN
    RETURN NULL;
END;
$$ LANGUAGE plpgsql STABLE SECURITY DEFINER;
```

If `app.current_organization_id` has not been set via `SET LOCAL`, the function evaluates to `NULL`. Since `organization_id = NULL` evaluates to `FALSE` in SQL boolean logic, all tenant-isolated queries fail closed, returning 0 rows.

### Restricted Discovery Functions (Blueprint §24.3)

To prevent broad administrative access while supporting tenant discovery and system scheduling, two narrowly scoped functions are defined with `SECURITY DEFINER` and locked-down `search_path`:

1. `app.discover_user_memberships(p_user_id UUID)`:
   - Executed by `deadbolt_runtime` before tenant context is set.
   - Fixed `search_path = app, public, pg_temp`.
   - Returns exclusively the active memberships (`organization_id`, `organization_name`, `role`, `status`) belonging to the authenticated user identity.
2. `app.enumerate_scheduler_tenants()`:
   - Executed exclusively by `deadbolt_system`.
   - Returns tenant (`organization_id`) IDs that require background scheduling.
   - `deadbolt_system` has NO permission to query tenant tables (`runs`, `run_steps`, etc.) directly. The scheduler enumerates tenant IDs through this function, then processes each tenant within an isolated, per-tenant transaction under RLS.

---

## 3. Ordered Migrations

Migrations are stored in `migrations/` and executed sequentially using `goose` under a pinned session advisory lock:

1. `00001_roles_and_tenants.sql`:
   - `organizations`, `organization_members`
   - `projects`, `environments` (composite keys `(organization_id, project_id)`)
   - `environment_admissions` (concurrency quota lock rows)
   - `api_keys`
   - Discovery functions: `app.discover_user_memberships`, `app.enumerate_scheduler_tenants`
2. `00002_workflows_and_deployments.sql`:
   - `deployments` (immutable manifests, bundle digest, runtime versions)
   - `workflow_definitions`, `task_definitions`
   - `workflow_channels` (active deployment pointers with revisions)
3. `00003_execution_and_claims.sql`:
   - `workers`, `worker_sessions`, `worker_deployments`
   - `runs`, `run_steps`, `task_attempts`, `task_leases`
   - `run_events` (append-only history)
   - `timers`, `idempotency_records`, `outbox_events` (migration `00014` adds dispatch diagnostics and due-scan indexing)
   - Composite foreign keys:
     - `task_attempts(organization_id, session_id)` $\rightarrow$ `worker_sessions(organization_id, id)`
     - `task_leases(organization_id, session_id)` $\rightarrow$ `worker_sessions(organization_id, id)`
4. `00004_v1_entities.sql`:
   - `approvals`, `reconciliation_cases`, `stop_commands`
   - `schedules`, `schedule_occurrences`
   - `webhook_endpoints`, `webhook_deliveries`
   - `artifacts`, `audit_events`, `usage_records`
   - Composite foreign keys:
     - `artifacts(organization_id, run_id)` $\rightarrow$ `runs(organization_id, id)`
     - `artifacts(organization_id, step_id)` $\rightarrow$ `run_steps(organization_id, id)`

### Single-Migrator Advisory Lock Serialization

All migrations acquire `SELECT pg_advisory_lock(7142893)` on a dedicated physical session connection in `migrator.Runner` via Goose Provider's `lock.NewPostgresSessionLocker`. Lock acquisition, migration DDL execution, and lock release are strictly serialized through the exact same physical database session. If another migrator process attempts to migrate concurrently, it blocks until the lock holder releases the advisory lock upon completion or session termination.

---

## 4. Prescribed Lock Order and Claim Model

To eliminate lock inversions and deadlocks across concurrent claimers, task completers, and background reconcilers (Blueprint §11.2):

### 1. Claim Transaction Flow

1. **Candidate Discovery (Non-locking scan):**
   ```sql
   SELECT id, run_id FROM run_steps
   WHERE environment_id = $1 AND state = 'READY'
   ORDER BY eligible_at ASC, id ASC
   LIMIT 1;
   ```
2. **Authoritative Lock Hierarchy:**
   - Lock `environment_admissions` row `FOR UPDATE` (concurrency quota check).
   - Lock `runs` row `FOR UPDATE`.
   - Lock `run_steps` row `FOR UPDATE`.
   - **Revalidate state:** If step `state != 'READY'` or run `status != 'RUNNING'`, roll back and retry candidate selection (stale candidate retry).
   - Update `run_steps` to `RUNNING` with epoch increment.
   - Insert `task_attempts` (`CLAIMED`).
   - Insert `task_leases` with lease expiration.
   - Commit transaction.

### 2. Task Completion Flow

1. Lock `runs` row `FOR UPDATE`.
2. Lock `run_steps` row `FOR UPDATE`.
3. Delete lease from `task_leases`.
4. Update `run_steps` to `SUCCEEDED`.
5. Update `task_attempts` to `SUCCEEDED`.
6. Commit transaction.

### 3. Reconciler Flow

1. Lock `runs` row `FOR UPDATE`.
2. Lock child `run_steps` rows `FOR UPDATE` strictly sorted in ascending ID order.
3. Validate state consistency and commit.

Because all paths acquire locks in the exact order: `environment_admissions` $\rightarrow$ `runs` $\rightarrow$ `run_steps` (ascending ID order), cross-transaction deadlocks (`SQLSTATE 40P01`) are mathematically eliminated.
