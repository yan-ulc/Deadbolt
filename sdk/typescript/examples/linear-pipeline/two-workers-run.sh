#!/usr/bin/env bash
# ==============================================================================
# Two-Worker Linear Pipeline Execution Demonstration ($A -> B -> C)
# 
# Demonstrates a complete developer journey executing a durable linear pipeline
# across two independent self-hosted workers without any direct database access.
#
# Adheres strictly to Blueprint §4, §6, §12, §14, §20, §22 and ADR-01, ADR-02, ADR-10.
# ==============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"

# Resolve runtime CLI executable
if command -v runtime &> /dev/null; then
    RUNTIME="runtime"
elif [ -f "$REPO_ROOT/bin/runtime" ]; then
    RUNTIME="$REPO_ROOT/bin/runtime"
else
    RUNTIME="go run $REPO_ROOT/cmd/runtime/main.go"
fi

echo "======================================================================"
echo " Deadbolt Two-Worker Linear Pipeline Demonstration ($A -> B -> C)"
echo "======================================================================"
echo "Runtime command: $RUNTIME"

# Target environment & API configuration
export DEADBOLT_API_URL="${DEADBOLT_API_URL:-http://localhost:8080}"
export DEADBOLT_ENV="${DEADBOLT_ENV:-staging}"
export NOTIFICATION_API_KEY="${NOTIFICATION_API_KEY:-demo-notification-secret-key}"

# Temporary scratch directory for worker keys and credentials
DEMO_DIR="$(mktemp -d /tmp/deadbolt-two-workers-XXXXXX)"
export DEADBOLT_CREDENTIALS_DIR="$DEMO_DIR/credentials"
mkdir -p "$DEADBOLT_CREDENTIALS_DIR"

WORKER1_PID=""
WORKER2_PID=""

cleanup() {
    echo ""
    echo "==> Cleaning up background worker processes..."
    if [ -n "$WORKER1_PID" ] && kill -0 "$WORKER1_PID" 2>/dev/null; then
        echo "Stopping Worker 1 (PID $WORKER1_PID)..."
        kill -TERM "$WORKER1_PID" 2>/dev/null || true
    fi
    if [ -n "$WORKER2_PID" ] && kill -0 "$WORKER2_PID" 2>/dev/null; then
        echo "Stopping Worker 2 (PID $WORKER2_PID)..."
        kill -TERM "$WORKER2_PID" 2>/dev/null || true
    fi
    wait "$WORKER1_PID" 2>/dev/null || true
    wait "$WORKER2_PID" 2>/dev/null || true
    rm -rf "$DEMO_DIR"
    echo "✓ Cleanup complete."
}
trap cleanup EXIT

cd "$SCRIPT_DIR"

# Step 1: System and environment diagnostics
echo ""
echo "==> Step 1: Running system preflight checks"
$RUNTIME doctor

# Step 2: Build immutable deployment bundle and manifest
echo ""
echo "==> Step 2: Building immutable deployment bundle and manifest"
BUILD_OUTPUT=$($RUNTIME build --dir . --json)
BUNDLE_DIGEST=$(echo "$BUILD_OUTPUT" | grep -o '"bundleDigest": "[^"]*' | cut -d'"' -f4)
MANIFEST_PATH=$(echo "$BUILD_OUTPUT" | grep -o '"manifestPath": "[^"]*' | cut -d'"' -f4)

echo "Bundle Digest: $BUNDLE_DIGEST"
echo "Manifest:      $MANIFEST_PATH"

# Step 3: Register deployment manifest on control plane
echo ""
echo "==> Step 3: Registering deployment manifest on control plane"
DEPLOY_OUTPUT=$($RUNTIME deploy --env "$DEADBOLT_ENV" --manifest "$MANIFEST_PATH" --json)
DEPLOYMENT_ID=$(echo "$DEPLOY_OUTPUT" | grep -o '"id": "[^"]*' | cut -d'"' -f4)
echo "Registered Deployment ID: $DEPLOYMENT_ID"

# Step 4: Verify activation preflight rejection when 0 workers online
echo ""
echo "==> Step 4: Testing activation preflight (must fail with 0 workers)"
if $RUNTIME deployments activate "$DEPLOYMENT_ID" --workflow customer-onboarding --env "$DEADBOLT_ENV" 2>/dev/null; then
    echo "ERROR: Expected activation to fail when 0 compatible workers are online!"
    exit 1
else
    echo "✓ Activation correctly rejected by control-plane preflight (requires 2 online workers)."
fi

# Step 5: Enroll Worker 1 and Worker 2
echo ""
echo "==> Step 5: Enrolling two independent worker identities"
WORKER1_KEY="$DEMO_DIR/worker1.key"
WORKER2_KEY="$DEMO_DIR/worker2.key"

echo "Enrolling Worker 1..."
$RUNTIME worker enroll --key-path "$WORKER1_KEY" --env "$DEADBOLT_ENV" --create-token
echo "Enrolling Worker 2..."
$RUNTIME worker enroll --key-path "$WORKER2_KEY" --env "$DEADBOLT_ENV" --create-token

# Step 6: Start Worker 1 and Worker 2 in background
echo ""
echo "==> Step 6: Starting Worker 1 and Worker 2 agents"
RUNNER_PATH="$REPO_ROOT/runner/node/dist/index.js"

$RUNTIME worker start \
    --key-path "$WORKER1_KEY" \
    --bundle-dir "$SCRIPT_DIR/bundles" \
    --runner-path "$RUNNER_PATH" \
    --slots 2 &
WORKER1_PID=$!
echo "Worker 1 launched (PID: $WORKER1_PID)"

$RUNTIME worker start \
    --key-path "$WORKER2_KEY" \
    --bundle-dir "$SCRIPT_DIR/bundles" \
    --runner-path "$RUNNER_PATH" \
    --slots 2 &
WORKER2_PID=$!
echo "Worker 2 launched (PID: $WORKER2_PID)"

echo "Waiting for workers to establish sessions and advertise bundle digests..."
sleep 3

# Step 7: Inspect active workers
echo ""
echo "==> Step 7: Listing active workers in $DEADBOLT_ENV environment"
$RUNTIME worker list --env "$DEADBOLT_ENV"

# Step 8: Activate deployment on the workflow channel
echo ""
echo "==> Step 8: Activating deployment (now valid with 2 online workers)"
$RUNTIME deployments activate "$DEPLOYMENT_ID" --workflow customer-onboarding --env "$DEADBOLT_ENV"

# Step 9: Create workflow execution run
echo ""
echo "==> Step 9: Creating workflow run via public CLI interface"
RUN_OUTPUT=$($RUNTIME runs create \
    --workflow customer-onboarding \
    --env "$DEADBOLT_ENV" \
    --input '{"email":"alice@example.com","name":"Alice User"}' \
    --idempotency-key "demo-run-$(date +%s)" \
    --json)
RUN_ID=$(echo "$RUN_OUTPUT" | grep -o '"id": "[^"]*' | cut -d'"' -f4)
echo "Created Run ID: $RUN_ID"

# Step 10: List runs
echo ""
echo "==> Step 10: Listing runs in environment"
$RUNTIME runs list --env "$DEADBOLT_ENV"

# Step 11: Poll run snapshot until terminal outcome
echo ""
echo "==> Step 11: Monitoring run execution across workers ($A -> B -> C)..."
MAX_ATTEMPTS=30
ATTEMPT=0
FINAL_STATUS="UNKNOWN"

while [ $ATTEMPT -lt $MAX_ATTEMPTS ]; do
    ATTEMPT=$((ATTEMPT + 1))
    INSPECT_JSON=$($RUNTIME runs inspect "$RUN_ID" --json 2>/dev/null || echo "{}")
    STATUS=$(echo "$INSPECT_JSON" | grep -o '"status": "[^"]*' | head -n 1 | cut -d'"' -f4)
    echo "  [Poll $ATTEMPT/$MAX_ATTEMPTS] Run status: $STATUS"
    if [ "$STATUS" = "SUCCEEDED" ] || [ "$STATUS" = "FAILED" ]; then
        FINAL_STATUS="$STATUS"
        break
    fi
    sleep 1
done

# Step 12: Detailed inspect of terminal state
echo ""
echo "==> Step 12: Inspecting final run snapshot"
$RUNTIME runs inspect "$RUN_ID"

# Step 13: View execution logs
echo ""
echo "==> Step 13: Streaming execution logs from run"
$RUNTIME logs "$RUN_ID"

if [ "$FINAL_STATUS" = "SUCCEEDED" ]; then
    echo ""
    echo "======================================================================"
    echo " ✓ Two-Worker Linear Pipeline Execution Completed Successfully!"
    echo "   All operations performed exclusively through official public APIs."
    echo "======================================================================"
    exit 0
else
    echo ""
    echo "ERROR: Run did not reach terminal SUCCEEDED status (status: $FINAL_STATUS)"
    exit 1
fi
