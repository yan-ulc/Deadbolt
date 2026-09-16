# Quickstart: Deadbolt Developer CLI Journey

This guide walks you through the end-to-end Deadbolt developer journey using exclusively official, public command-line and API interfaces:

```text
clean project ↓ runtime init ↓ runtime dev ↓ runtime build ↓ runtime doctor ↓ runtime login
      ↓ runtime deploy ↓ runtime worker enroll/start ↓ runtime deployments activate
      ↓ runtime runs create ↓ runtime runs inspect & logs
```

Every step adheres to **Blueprint §4, §6, §12, §14, §20, §22, §24** and **ADR-01, ADR-02, ADR-08, ADR-10**.

> [!IMPORTANT]
> **No Database Backdoors:** Product operations never use direct PostgreSQL modifications or internal shortcuts. All orchestration is managed through official authenticated HTTP APIs and Ed25519 worker protocols.
> **Zero-Leak Secret Protection:** Customer secret values are never printed in diagnostic reports, logs, or command output.

---

## Prerequisites

- **Go 1.24+**
- **Node.js 24.x** (`v24.21.0` pinned in runtime contracts)
- **Docker & Docker Compose** (for running local control-plane and broker services)

Install the Deadbolt CLI:

```bash
go build -o /usr/local/bin/runtime ./cmd/runtime/main.go
```

Verify the installation:

```bash
runtime --help
```

---

## 1. Project Initialization (`runtime init`)

Scaffold a clean workflow project containing valid DAG definitions, task handlers, and schema declarations:

```bash
runtime init --name order-service --dir ./order-service
cd ./order-service
```

This creates:

- `deadbolt.config.json`: Non-secret project configuration (name, default workflow, required secret names).
- `workflow.json`: Declarative DAG workflow specification (`validate` → `provision` → `notify`) with explicit JSON Pointer input/output mappings.
- `tasks.json`: Task contract definitions with JSON schemas and explicit recovery policies (`safe` or `idempotent`).
- `tasks/validate.js`, `tasks/provision.js`, `tasks/notify.js`: Pure JavaScript task implementations.
- `.env.example`: Secret name placeholders (never commit actual secrets).
- `.gitignore`: Ensures secrets and compiled bundles are never committed.

---

## 2. Local Development Stack (`runtime dev`)

Start the local orchestration stack (Control Plane, PostgreSQL, Redis) and a local worker agent in one command:

```bash
runtime dev
```

Key local mode characteristics:

- **Account-free & loopback-restricted:** Runs strictly on `127.0.0.1:8080`.
- **Zero cloud telemetry:** Leaks no customer data or telemetry outside the local host.
- **Auto-terminating:** Press `Ctrl+C` to gracefully drain running tasks and stop all containers.

---

## 3. Deterministic Bundle Assembly (`runtime build`)

Compile your workflow code into an immutable `.tar` bundle archive and a validated deployment manifest:

```bash
runtime build --dir . --arch arm64 --os linux
```

Output:

- `dist/manifest.json`: Strictly validated against `contracts/manifest/deployment.schema.json`.
- `bundles/<bundleDigest>.tar`: Deterministic archive with sorted tar headers and clamped timestamps.

Key outputs:

- **Bundle Digest (SHA-256):** `sha256(bundle.tar)` pinned identity.
- **Dependency Lock Digest (SHA-256):** Deterministic lock hash.

---

## 4. Environment & Health Diagnostics (`runtime doctor`)

Run actionable diagnostic checks before deploying or activating workloads:

```bash
runtime doctor --manifest ./dist/manifest.json --bundle-dir ./bundles
```

The doctor command verifies:

- Docker daemon availability.
- Node.js runtime toolchain conformance (Node 24.x).
- Control plane `/livez` and `/readyz` health endpoints.
- Deployment manifest schema validation.
- Bundle archive integrity and digest matching.
- Host architecture compatibility (`darwin/arm64`, `linux/amd64`, etc.).
- Required task secret presence in worker environment (**masked as `Present (value masked)`**).

---

## 5. Hosted Authentication (`runtime login`)

Authenticate the CLI against your Deadbolt control plane:

### Interactive Browser Login (PKCE OAuth)

```bash
runtime login --url https://api.deadbolt.cloud
```

### Headless / API Key Authentication

```bash
runtime login --url https://api.deadbolt.cloud --api-key <YOUR_ADMIN_KEY> --org <ORG_ID> --env staging
```

Credentials are automatically stored in:

1. **OS Keychain:** macOS Keychain (`security`) or Linux Secret Service (`secret-tool`).
2. **Encrypted Local Fallback:** `~/.deadbolt/credentials.json` (encrypted with host-derived machine key, file mode `0600`, directory mode `0700`).

---

## 6. Manifest Registration (`runtime deploy`)

Register your immutable deployment manifest on the control plane:

```bash
runtime deploy --env staging --manifest ./dist/manifest.json
```

> [!NOTE]
> **Manifest-Only Registration:** Deadbolt's control plane never accepts or stores customer source code. Only the metadata manifest and cryptographic digests are registered. Worker nodes load bundles directly from local storage or private artifact repositories.

---

## 7. Self-Hosted Worker Enrollment & Startup (`runtime worker`)

Deadbolt uses Ed25519 cryptographic challenge-response nonces for mutual authentication.

### Step 7a: Enroll Worker 1 & Worker 2

```bash
# Enroll Worker 1
runtime worker enroll --key-path ~/.deadbolt/worker1.key --env staging --create-token

# Enroll Worker 2
runtime worker enroll --key-path ~/.deadbolt/worker2.key --env staging --create-token
```

This generates an Ed25519 keypair, signs the single-use challenge nonce from `/worker/v1/challenge`, binds the public key to the environment pool, and writes key/identity files with `0600` permissions.

### Step 7b: Start Worker 1 & Worker 2 Agents

```bash
# Terminal 1: Worker 1
runtime worker start --key-path ~/.deadbolt/worker1.key --bundle-dir ./bundles --slots 2

# Terminal 2: Worker 2
runtime worker start --key-path ~/.deadbolt/worker2.key --bundle-dir ./bundles --slots 2
```

### Step 7c: Verify Active Workers

```bash
runtime worker list --env staging
```

Expected output:

```text
WORKER ID                             POOL     STATUS  DEPLOYMENTS
baa5c105-b3fb-41d6-a0a0-e6ab3f67f9f3  default  ACTIVE  b08aa9dba0b518aafaeafcb8e47bf4a32014a005c072e823d83593e284473879
5c77f72f-465e-4585-a393-fb547e05ea8c  default  ACTIVE  b08aa9dba0b518aafaeafcb8e47bf4a32014a005c072e823d83593e284473879
```

---

## 8. Deployment Activation (`runtime deployments activate`)

Activate the deployment on the workflow channel:

```bash
runtime deployments activate <DEPLOYMENT_ID> --workflow customer-onboarding --env staging
```

### Activation Preflight Enforcement

- **Strict High-Availability Rule:** Activation requires at least **2 compatible online workers** advertising the deployment's bundle digest.
- **Development Exception:** In non-production environments with only 1 worker online, pass `--allow-single-worker` (a failover recovery warning will be displayed).
- If 0 compatible workers are online, activation fails closed with `409 WORKER_PREFLIGHT_FAILED`.

---

## 9. Workflow Run Creation (`runtime runs create`)

Submit a workflow execution run via the public control plane interface:

```bash
runtime runs create \
  --workflow customer-onboarding \
  --env staging \
  --input '{"email":"alice@example.com","name":"Alice User"}' \
  --idempotency-key "order-run-$(date +%s)"
```

The control plane persists the run, initializes DAG step states (`validate: READY`, `provision: BLOCKED`, `notify: BLOCKED`), and returns `HTTP 202 Accepted`.

---

## 10. Run Listing (`runtime runs list`)

List recent workflow runs in the environment:

```bash
runtime runs list --env staging --limit 10
```

Output:

```text
RUN ID                                WORKFLOW             STATUS     REASON  CREATED AT
4b51cf6b-9de6-4cce-ad27-c5a3d8c6bb2a  customer-onboarding  SUCCEEDED  -       2026-09-16T14:26:46Z
```

---

## 11. Run Inspection (`runtime runs inspect`)

Inspect the detailed snapshot of a run, including DAG step statuses, attempt histories, monotonic epochs, failure reasons, and committed outputs:

```bash
runtime runs inspect <RUN_ID>
```

Output:

```text
Run ID:              4b51cf6b-9de6-4cce-ad27-c5a3d8c6bb2a
Workflow:            customer-onboarding
Status:              SUCCEEDED
Revision:            1
Last Event Sequence: 13
Created At:          2026-09-16T14:26:46Z

Steps & Attempts:
  STEP ID                               NODE       STATUS     EPOCH  ATTEMPTS
  4e6f7e39-f8a1-4d15-a03a-64b94dbe597b  validate   SUCCEEDED  1      1 attempt(s)
    └── Attempt #1:                     [SUCCEEDED] Started: 2026-09-16T14:26:47Z  ID: 19627c3a-3c93-46c2-816b-04194b3844eb
  ce2d0906-d400-4bc7-8c41-56297cacb13d  provision  SUCCEEDED  1      1 attempt(s)
    └── Attempt #1:                     [SUCCEEDED] Started: 2026-09-16T14:26:47Z  ID: bfef7751-b7e6-46bb-a9b9-22b442a10c56
  bfb4aa7e-2c54-451b-90e9-d5ab6fa4cc77  notify     SUCCEEDED  1      1 attempt(s)
    └── Attempt #1:                     [SUCCEEDED] Started: 2026-09-16T14:26:47Z  ID: 75e87fd4-c389-4189-8d42-346d74fadd77

Output:
{
  "accountId": "acc_usr_4e6f7e39-f8a1-4d15-a03a-64b94dbe597b",
  "deliveryId": "del_op_6358025066a4d4614a38509156bef54136d3c10bf2588077456c3d3764c5065a"
}
```

---

## 12. Execution Logs Streaming (`runtime logs`)

Stream stdout/stderr logs from task executions across workers:

```bash
runtime logs <RUN_ID>
```

Filter logs by specific step or attempt:

```bash
runtime logs <RUN_ID> --step <STEP_ID>
runtime logs <RUN_ID> --attempt <ATTEMPT_ID>
```

Example output:

```text
[14:26:47.000] [INFO ] #1 {"timestamp":"...","level":"INFO","attemptId":"...","message":"Starting task execution","meta":[{"taskName":"tasks/validate.js"}]}
[14:26:47.000] [INFO ] #2 {"timestamp":"...","level":"INFO","attemptId":"...","message":"Validating signup request","meta":[{"email":"alice@example.com"}]}
[14:26:47.000] [INFO ] #3 {"timestamp":"...","level":"INFO","attemptId":"...","message":"Task completed successfully","meta":[{"durationMs":15}]}
[14:26:47.000] [INFO ] #1 {"timestamp":"...","level":"INFO","attemptId":"...","message":"Provisioning customer account","meta":[{"userId":"usr_4e6f7e39..."}]}
[14:26:47.000] [INFO ] #2 {"timestamp":"...","level":"INFO","attemptId":"...","message":"Dispatching welcome email with stable operationId","meta":[{"operationId":"op_[REDACTED_HASH]","email":"alice@example.com"}]}
```

---

## 13. Automated Two-Worker Linear Pipeline Script

To run this entire sequence automatically with a single script:

```bash
./sdk/typescript/examples/linear-pipeline/two-workers-run.sh
```

The script automatically:

1. Validates prerequisites with `runtime doctor`.
2. Assembles bundle and manifest with `runtime build`.
3. Registers manifest with `runtime deploy`.
4. Enrolls two workers with `runtime worker enroll`.
5. Starts Worker 1 & Worker 2 background agents.
6. Verifies HA preflight and activates with `runtime deployments activate`.
7. Submits workflow run with `runtime runs create`.
8. Polls until completion and inspects final snapshot with `runtime runs inspect`.
9. Displays task logs with `runtime logs`.
10. Traps script exit and cleanly terminates both worker processes.
