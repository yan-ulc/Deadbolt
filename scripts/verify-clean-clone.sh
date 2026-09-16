#!/usr/bin/env bash
set -euo pipefail

# Automated Gate M0 verification for a fresh clone. This is a fail-closed local
# wrapper around the Foundation contracts workflow's checks; it is not hosted CI
# or staging-deployment evidence.

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

require_command() {
    if ! command -v "$1" >/dev/null 2>&1; then
        echo "[FAIL] Required command not found: $1" >&2
        exit 1
    fi
}

compose() {
    docker compose -p deadbolt-clean-clone -f deploy/compose/docker-compose.yml --profile core "$@"
}

compose_logs_and_fail() {
    compose logs || true
    exit 1
}

cleanup_compose() {
    if [ "${COMPOSE_STARTED:-false}" = "true" ]; then
        compose down -v || true
    fi
}

for command in node pnpm go docker curl; do
    require_command "$command"
done

echo "=================================================================="
echo " [DEADBOLT] Verifying Clean-Clone Foundation & Gate M0 Invariants"
echo "=================================================================="

echo "--> [1/8] Installing locked dependencies and checking configuration..."
pnpm install --frozen-lockfile --ignore-scripts
pnpm check:config

# 2. Executable Contracts & Parser Parity
echo "--> [2/8] Validating OpenAPI, canonical schemas, and Go/TS contract parity..."
pnpm check:contracts
pnpm check:parity
node scripts/check-candidates.mjs

# 3. Formatting & Code Style
echo "--> [3/8] Enforcing formatting across Prettier and Go..."
pnpm lint
test -z "$(gofmt -l contracts internal tests cmd scripts)"

# 4. TypeScript Workspace Build, Typecheck, and Tests
echo "--> [4/8] Building and testing TypeScript packages..."
pnpm typecheck
pnpm test

# 5. Go Static Analysis and Race-Detector Tests
echo "--> [5/8] Running Go static analysis and race-detector suites..."
go test -race ./...
go vet ./...
go mod verify
pnpm audit

# 6. SP-03 Worker Process Lifecycle Tests
echo "--> [6/8] Running the complete SP-03 lifecycle suite..."
go test -race ./tests/spikes/sp03

# 7. Deployment Configuration and Script Syntax Checks
echo "--> [7/8] Validating deploy configuration and local Compose migration smoke..."
node scripts/install-tools.mjs sqlc goose gitleaks govulncheck
bin/sqlc version
bin/goose -version
bin/govulncheck ./...
docker build -f deploy/Dockerfile.control-plane --build-arg COMMIT_SHA="$(git rev-parse HEAD)" -t deadbolt-control-plane:clean-clone .
docker compose -f deploy/compose/docker-compose.yml --profile core config --quiet
docker compose -f deploy/compose/docker-compose.yml --profile telemetry config --quiet
docker compose -f deploy/compose/docker-compose.yml --profile fault config --quiet
DEADBOLT_DB_ADMIN_PASSWORD=mock_admin DEADBOLT_MIGRATOR_PASSWORD=mock_migrator DEADBOLT_RUNTIME_PASSWORD=mock_runtime DEADBOLT_SYSTEM_PASSWORD=mock_system DATABASE_URL=postgres://deadbolt_runtime:mock_runtime@localhost:5432/mock MIGRATOR_DATABASE_URL=postgres://deadbolt_migrator:mock_migrator@localhost:5432/mock SYSTEM_DATABASE_URL=postgres://deadbolt_system:mock_system@localhost:5432/mock DEADBOLT_IMAGE=ghcr.io/ryanakml/deadbolt/control-plane@sha256:1111222233334444555566667777888899990000aaaaabbbbbcccccdddddeeeee DEADBOLT_POSTGRES_IMAGE=ghcr.io/ryanakml/deadbolt/postgres@sha256:1111222233334444555566667777888899990000aaaaabbbbbcccccdddddeeeee DEADBOLT_OIDC_ISSUER=https://i DEADBOLT_OIDC_CLIENT_ID=id DEADBOLT_OIDC_CLIENT_SECRET=s DEADBOLT_STAGING_DOMAIN=staging.deadbolt.cloud docker compose -f deploy/compose/docker-compose.staging.yml --profile slot-blue --profile slot-green config --quiet
DEADBOLT_STAGING_DOMAIN=staging.deadbolt.cloud DEADBOLT_SNIPPET_ONLY=true DRY_RUN=true ./scripts/reload-caddy.sh
DRY_RUN=true ./scripts/check-backup-readiness.sh
DRY_RUN=true ./scripts/bootstrap-staging-cluster.sh
DRY_RUN=true ./scripts/deploy-staging.sh
DRY_RUN=true ./scripts/rollback-staging.sh
DRY_RUN=true ./scripts/retention.sh
DRY_RUN=true ./scripts/take-base-backup.sh
DRY_RUN=true ./scripts/restore-staging-db.sh
DRY_RUN=true ./scripts/setup-backup-cron.sh
docker build -f deploy/Dockerfile.postgres .

COMPOSE_STARTED=true
trap cleanup_compose EXIT
compose up -d || compose_logs_and_fail
for i in $(seq 1 30); do
    if curl -fs http://127.0.0.1:8080/livez >/dev/null 2>&1; then
        break
    fi
    sleep 2
done
curl -fs http://127.0.0.1:8080/livez >/dev/null || compose_logs_and_fail
# Prove the final image serves the bundled Inspector assets. This intentionally
# runs after Compose starts, rather than reading files from the checkout.
curl -fs http://127.0.0.1:8080/dashboard/ >/dev/null || compose_logs_and_fail
curl -fs http://127.0.0.1:8080/dashboard/index.js >/dev/null || compose_logs_and_fail
curl -fs http://127.0.0.1:8080/dashboard/styles.css >/dev/null || compose_logs_and_fail
node scripts/verify-dashboard-esm.mjs http://127.0.0.1:8080/dashboard || compose_logs_and_fail
compose exec -T control-plane /usr/local/bin/control-plane --migrate
READYZ_OK=false
for i in $(seq 1 15); do
    if curl -fs http://127.0.0.1:8080/readyz >/dev/null 2>&1; then
        READYZ_OK=true
        break
    fi
    sleep 1
done
if [ "$READYZ_OK" != "true" ]; then
    compose_logs_and_fail
fi
compose stop
compose start
curl -fs http://127.0.0.1:8080/livez >/dev/null || compose_logs_and_fail
compose down -v
COMPOSE_STARTED=false
trap - EXIT

# 8. Secret Scan (Redacted)
echo "--> [8/8] Running Gitleaks secret scan..."
bin/gitleaks git --redact --no-banner --log-opts="--all"

echo "=================================================================="
echo " [DEADBOLT] Gate M0 Clean-Clone Verification: ALL GATES PASSED"
echo "=================================================================="
