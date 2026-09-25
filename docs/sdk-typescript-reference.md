# Deadbolt TypeScript SDK & Contract Reference

> **Document Version:** 1.0.0  
> **Traceability:** REQ-EXEC-01, REQ-DX-01, REQ-VERSION-01, INV-02, INV-07, F-28  
> **Blueprint References:** §4 (Developer journey), §6 (Execution model: declarative DAG), §12 (Worker protocol), §14 (Idempotency & reconciliation), §20 (Public API & SDK), §22 (CLI & local development)  
> **ADR Baseline:** [ADR-01](decisions/ADR-01.md), [ADR-02](decisions/ADR-02.md), [ADR-08](decisions/ADR-08.md), [ADR-10](decisions/ADR-10.md), [ADR-14](decisions/ADR-14.md)

---

## 1. Overview & Architectural Principles

The Deadbolt TypeScript SDK (`@runtime/sdk`) provides the developer programming model for defining durable tasks, declaring linear DAG workflows, compiling them into immutable local bundles, and controlling workflow runs through the Control Plane API.

### Core Guarantees

1. **Declarative DAG, Not Arbitrary Replay ([ADR-01](decisions/ADR-01.md)):** Workflows are stored as declarative JSON graphs. The SDK builder emits static graph manifests that the Go control plane scheduler executes without executing customer JavaScript on the platform.
2. **Mandatory Explicit Recovery Policy ([ADR-08](decisions/ADR-08.md)):** Every task must explicitly declare its recovery mode: `safe`, `idempotent`, or `reconcile`. Blind retries of non-idempotent side effects are forbidden.
3. **Immutable Deployments & Pinned Bundles ([ADR-10](decisions/ADR-10.md)):** Deployments are immutable manifests cryptographically pinned to SHA-256 hashes of the code bundle and dependency lockfile (`nodeRuntimeMajor: 24`, `targetOS: "linux"`).
4. **Strict Secret Isolation ([ADR-11](decisions/ADR-11.md)):** Secret _values_ never enter the manifest or logs. Only allowlisted environment variable names (`secretNames`) are declared; task secrets remain local to customer worker hosts.
5. **Idempotent Client Invocations ([ADR-14](decisions/ADR-14.md)):** Starting a workflow run strictly requires an `Idempotency-Key` header. HTTP `202 Accepted` indicates persisted acceptance in the control plane database, not completion. Results are retrieved via polling.

---

## 2. Task Definition (`defineTask`)

Tasks encapsulate individual units of executable business logic.

```ts
import { defineTask } from "@runtime/sdk";

export const searchTask = defineTask({
  name: "search-web",
  inputSchema: {
    type: "object",
    properties: { query: { type: "string" } },
    required: ["query"],
    additionalProperties: false,
  },
  outputSchema: {
    type: "object",
    properties: {
      results: { type: "array", items: { type: "string" } },
    },
    required: ["results"],
    additionalProperties: false,
  },
  recovery: "safe",
  retry: { maxAttempts: 3, initialDelayMs: 1000, maxDelayMs: 30000 },
  timeoutMs: 60000,
  handler: async (input, ctx) => {
    ctx.logger.info("Executing search", { query: input.query });
    return { results: ["page1", "page2"] };
  },
});
```

### Configuration Parameters

| Field                 | Type                                  | Required                 | Default             | Description                                                       |
| --------------------- | ------------------------------------- | ------------------------ | ------------------- | ----------------------------------------------------------------- |
| `name`                | `string`                              | **Yes**                  | —                   | Unique task name (`^[a-zA-Z0-9_-]{1,64}$`).                       |
| `inputSchema`         | `object`                              | **Yes**                  | —                   | JSON Schema 2020-12 subset for payload validation.                |
| `outputSchema`        | `object`                              | **Yes**                  | —                   | JSON Schema 2020-12 subset for return value validation.           |
| `recovery`            | `safe` \| `idempotent` \| `reconcile` | **Yes**                  | —                   | Explicit recovery policy declaration.                             |
| `idempotencyWindowMs` | `number`                              | Required if `idempotent` | —                   | Guaranteed provider deduplication window.                         |
| `timeoutMs`           | `number`                              | No                       | `300000` (5 min)    | Maximum execution duration before timing out (1 to 3,600,000 ms). |
| `retry`               | `object`                              | No                       | See below           | Retry policy with exponential backoff.                            |
| `entrypoint`          | `string`                              | No                       | `./tasks/<name>.js` | Path to entrypoint script within bundle.                          |
| `handler`             | `Function`                            | No                       | —                   | `async (input, ctx) => Promise<output>`.                          |

### Recovery Policies

- **`safe`:** The task is safely repeatable (e.g. read-only queries, pure idempotent transformations). If interrupted, the scheduler may automatically retry up to `maxAttempts`.
- **`idempotent`:** The task performs an external side effect with an upstream deduplication guarantee (e.g. Stripe API with idempotency keys).
  - **Invariance Rule:** `idempotencyWindowMs >= 5000 + timeoutMs`. The 5,000 ms covers the claim-to-start dispatch budget.
  - The task handler receives `ctx.operationId` which remains stable across retries of the same step.
- **`reconcile`:** The task interacts with external systems without guaranteed idempotency. On lease loss, timeout, or ambiguous errors, the step enters `WAITING` with `reason_code=RECONCILIATION`, requiring human review before continuing.

---

## 3. Execution Context Contract (`TaskContext`)

Handlers receive an execution context conforming to Blueprint §12.3:

```ts
export interface TaskContext {
  readonly stepId: string;
  readonly attemptId: string;
  readonly operationId: string;
  readonly signal: AbortSignal;
  readonly logger: TaskLogger;
  readonly log: TaskLogger; // Alias for logger
  readonly env: Readonly<Record<string, string>>;
}
```

- `ctx.stepId`: The workflow graph node identifier.
- `ctx.attemptId`: The unique execution attempt ID.
- `ctx.operationId`: Deterministic, stable operation key for deduplication across retries (`env_id:run_id:node_id` hash).
- `ctx.signal`: Standard `AbortSignal` triggered upon step timeout or run cancellation.
- `ctx.logger`: Redaction-aware logger (`info`, `warn`, `error`, `debug`). Automatically sanitizes sensitive keys (`password`, `token`, `secret`, `api_key`, `authorization`) and bearer/JWT tokens.
- `ctx.env`: Read-only map of allowlisted worker environment variables.

---

## 4. Workflow Definition (`defineWorkflow`)

Workflows declare dependencies between tasks using a linear directed acyclic graph.

```ts
import { defineWorkflow, input, output } from "@runtime/sdk";
import { searchTask, analyzeTask, reportTask } from "./tasks.js";

export const researchWorkflow = defineWorkflow({
  name: "research-report",
  inputSchema: {
    type: "object",
    properties: { query: { type: "string" } },
    required: ["query"],
    additionalProperties: false,
  },
  outputSchema: {
    type: "object",
    properties: { reportUrl: { type: "string" } },
    required: ["reportUrl"],
    additionalProperties: false,
  },
  nodes: [
    {
      id: "search",
      type: "task",
      task: searchTask,
      input: { query: input("/query") },
    },
    {
      id: "analyze",
      type: "task",
      task: analyzeTask,
      after: ["search"],
      input: { pages: output("search", "/results") },
    },
    {
      id: "report",
      type: "task",
      task: reportTask,
      after: ["analyze"],
      input: { analysis: output("analyze", "/summary") },
    },
  ],
  output: {
    reportUrl: output("report", "/url"),
  },
});
```

### Reference Descriptors & Mapping Helpers

- `input(pointer: string, defaultValue?: JSONValue)`: References a field from the workflow input using RFC 6901 JSON Pointer syntax (e.g. `input("/query")`).
- `output(stepId: string, pointer: string, defaultValue?: JSONValue)`: References a field from an ancestor step's output (e.g. `output("search", "/results")`).
- `literal(value: JSONValue)`: Tags a literal JSON object to prevent collision with reference descriptors.

### Graph Rules & Local Validation

At definition time (`validateOnInit: true`), `defineWorkflow` validates the complete graph:

1. **Linear Graph Enforcement:** In MVP/M1, graphs must be strictly linear (one root, at most one predecessor and one successor per node).
2. **Capability Gates:** Unsupported node types (`choice`, `merge`, `approval`, `delay`) fail locally with `UNSUPPORTED_CAPABILITY`.
3. **Task Resolution:** Every referenced task must be resolved; unresolved task names fail with `MISSING_TASK_REF`.
4. **No Cycles:** Cycle detection ensures directed acyclic structure (`CYCLE_DETECTED`).
5. **No Orphan Leaves:** Every leaf node must either connect to workflow `output` or declare `sideEffect: true` (`ORPHAN_LEAF`).

---

## 5. Local Bundle & Manifest Packager (`buildDeploymentBundle`)

`buildDeploymentBundle` compiles tasks and workflows into an immutable deployment package matching `contracts/manifest/deployment.schema.json`.

```ts
import { buildDeploymentBundle } from "@runtime/sdk";
import { researchWorkflow } from "./workflow.js";

const bundle = buildDeploymentBundle({
  workflows: [researchWorkflow],
  targetOS: "linux",
  targetArchitecture: "amd64",
  secretNames: ["SEARCH_API_KEY", "STORAGE_KEY"],
  dependencyLockContent: fs.readFileSync("pnpm-lock.yaml", "utf8"),
  bundleFiles: {
    "tasks.js": fs.readFileSync("./dist/tasks.js"),
    "workflow.js": fs.readFileSync("./dist/workflow.js"),
  },
});

console.log("Bundle SHA-256 Digest:", bundle.bundleDigest);
console.log("Dependency Lock Digest:", bundle.dependencyLockDigest);
```

### Pinned Manifest Constraints

```json
{
  "manifestVersion": 1,
  "sdkVersion": "0.1.0",
  "protocolMajor": 1,
  "nodeRuntimeMajor": 24,
  "targetOS": "linux",
  "targetArchitecture": "amd64",
  "dependencyLockDigest": "2dc6315dda00b4c3efc1f7a1ed0a0f771c2f0341b0db5ee8141cd5552f613f5c",
  "bundleDigest": "6f8aaaf00a0991e2c9a9746f3e84c8511979a0560151d028f96444f3c1ec499d",
  "secretNames": ["SEARCH_API_KEY", "STORAGE_KEY"],
  "tasks": [...],
  "workflows": [...]
}
```

- **Runtime Pinning:** `nodeRuntimeMajor` is pinned to `24`, `targetOS` to `linux`.
- **Secret Isolation:** `secretNames` must match `^[A-Za-z_][A-Za-z0-9_]*$`. Secret values are rejected at build time.

---

## 6. Deadbolt Client (`DeadboltClient`)

The client interacts with the Control Plane REST API (`contracts/openapi/control-plane.yaml`).

```ts
import { DeadboltClient } from "@runtime/sdk";

const client = new DeadboltClient({
  baseUrl: "https://api.deadbolt.cloud",
  apiKey: "ak_live_...",
  environment: "staging",
});
```

### Creating Runs (`client.runs.create`)

```ts
const run = await client.runs.create({
  workflow: "research-report",
  input: { query: "durable workflows" },
  idempotencyKey: "req_20260912_001",
});

console.log(run.id); // "run_01j7..."
console.log(run.status); // "QUEUED"
console.log(run.isAccepted); // true
```

> [!IMPORTANT]
> **HTTP 202 Semantics:** `client.runs.create()` requires an `Idempotency-Key` header and returns HTTP `202 Accepted`. This denotes **persisted acceptance** in the PostgreSQL database; it does not indicate workflow completion.

### Polling Run Outcomes (`client.runs.pollResult`)

```ts
try {
  const result = await client.runs.pollResult(run.id, {
    intervalMs: 1000,
    timeoutMs: 60000,
  });
  console.log("Result:", result);
} catch (err) {
  if (err instanceof DeadboltExecutionError) {
    console.error("Run failed with code:", err.code, err.message);
  } else if (err instanceof DeadboltTimeoutError) {
    console.error("Run timed out while polling");
  }
}
```

### Retrieving Status & Snapshots

- `client.runs.get(runId)`: Returns full `RunSnapshot` including `steps`, `status`, `revision`, and `output`.
- `client.runs.getStatus(runId)`: Returns lightweight `{ id, status, revision, reasonCode }`.
- `client.deployments.register(manifest)`: Registers a deployment manifest in the control plane.

---

## 7. Runnable Linear Pipeline Example

A complete $A \rightarrow B \rightarrow C$ pipeline example is provided in `sdk/typescript/examples/linear-pipeline/`:

- `tasks.ts`: `validate-signup` $\rightarrow$ `provision-account` $\rightarrow$ `send-welcome-email`.
- `workflow.ts`: Connects input to validation, validation to provisioning, and provisioning to notification.
- `build.ts`: Builds the immutable local bundle and validates manifest compliance.
- `run.ts`: Submits an idempotent create request and polls for the completed outcome.

To build and verify:

```bash
node sdk/typescript/examples/linear-pipeline/build.js
```

## 8. Acceptance Evidence Boundary

The SDK, local bundle, client, runner, and Go worker protocol evidence for issue #9 is recorded in
[M1 SDK Bundles Acceptance Evidence](reports/M1-sdk-bundles-acceptance.md).

That report is the source of truth for separating:

- implemented behavior;
- automated tests passed;
- hosted CI passed;
- deployed to staging;
- acceptance verified.

Local manifest generation and clean-clone CI smoke tests are not treated as proof that the real
staging control plane accepted and executed the exact immutable artifact. The staging A -> B -> C
run, provenance capture, recovery exercise, and end-to-end error/security observations must be
recorded separately before production promotion.

### Rollout and Compatibility Notes

The M1 runner protocol requires `stepId` as a distinct identity from `operationId`. New workers must
send `stepId`, and new runners reject payloads that omit it before customer code starts. Rollback
should restore a known-good matching worker/runner artifact or image digest; binary rollback does
not roll back database state or make synthetic digest manifests equivalent to real artifact
provenance.
