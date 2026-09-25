export * from "./types.js";
export * from "./stream.js";
export * from "./inspector.js";
export * from "./api.js";
import { DashboardApiClient, isUnauthorized, isConflict, newIdempotencyKey, } from "./api.js";
import { createEnvironmentSelection, formatEnvironmentLabel, getSelectedEnvironmentId, selectEnvironment, setSelectionCatalog, setSelectionOrganization, } from "./api.js";
import { resolveOrgState } from "./auth.js";
import { RunInspector } from "./inspector.js";
import { shouldShowWorkerWait, terminalStepEmptyText, openCaseForStep, reconciliationHoldText, terminationBannerText, RESOLVE_ACTIONS, } from "./inspector.js";
import { clearStreamErrorOnLive, createStreamErrorBanner, markGlobalError, markStreamError, } from "./stream.js";
// DOM Bootstrap for browser runtime
if (typeof document !== "undefined") {
    document.addEventListener("DOMContentLoaded", () => {
        initDashboard();
    });
}
function initDashboard() {
    const api = new DashboardApiClient();
    // Canonical environment selection for the active organization. The
    // selector value is always the environment UUID, never a bare name.
    const envSelection = createEnvironmentSelection();
    let activeInspector = null;
    // Scoped banner state: only a transient stream error may be cleared on
    // SSE reconnect. Bootstrap/API errors stay visible.
    const streamBanner = createStreamErrorBanner();
    const envSelect = document.getElementById("env-select");
    const themeToggle = document.getElementById("theme-toggle");
    const runsNavBtn = document.getElementById("nav-runs");
    const workersNavBtn = document.getElementById("nav-workers");
    if (envSelect) {
        envSelect.addEventListener("change", () => {
            selectEnvironment(envSelection, envSelect.value || null);
            loadRunsList(api, getSelectedEnvironmentId(envSelection));
        });
    }
    if (themeToggle) {
        themeToggle.addEventListener("click", () => {
            const isDark = document.documentElement.classList.toggle("dark");
            localStorage.setItem("theme", isDark ? "dark" : "light");
            themeToggle.setAttribute("aria-pressed", isDark ? "true" : "false");
        });
    }
    if (runsNavBtn) {
        runsNavBtn.addEventListener("click", () => {
            showView("runs-view");
            loadRunsList(api, getSelectedEnvironmentId(envSelection));
        });
    }
    if (workersNavBtn) {
        workersNavBtn.addEventListener("click", () => {
            showView("workers-view");
            loadWorkersList(api, getSelectedEnvironmentId(envSelection));
        });
    }
    // Check URL params for deep linking to /runs/:id
    const urlParams = new URLSearchParams(window.location.search);
    const runIdParam = urlParams.get("runId");
    // Auth bootstrap gate: Issue #14 data loading starts only after the BFF
    // session (and a valid organization context) is established. While
    // unauthenticated, no protected API is called.
    void bootstrap();
    async function bootstrap() {
        let session;
        try {
            session = await api.getSession();
        }
        catch (err) {
            renderError(err instanceof Error ? err : new Error(String(err)));
            return;
        }
        await enterWithSession(session);
    }
    async function enterWithSession(session) {
        if (!session) {
            clearEnvironmentState();
            showAuthRequired("Sign in to view workflow runs.");
            return;
        }
        const state = resolveOrgState(session);
        if (state.kind === "ready") {
            await enterApp(session);
            return;
        }
        if (state.kind === "empty") {
            clearEnvironmentState();
            showAuthRequired("This identity has no organization yet. Create one with `runtime bootstrap`, then reload.");
            return;
        }
        if (state.kind === "select") {
            clearEnvironmentState();
            showOrgSelect(state.orgs);
            return;
        }
        // Exactly one membership: establish it deterministically, then enter.
        try {
            clearEnvironmentState();
            await api.switchOrganization(state.orgId);
            await enterWithSession(await api.getSession());
        }
        catch (err) {
            renderError(err instanceof Error ? err : new Error(String(err)));
        }
    }
    async function enterApp(session) {
        hideAuthView();
        const label = document.getElementById("session-label");
        if (label) {
            label.textContent = session.user?.email ?? "";
            label.classList.remove("hidden");
        }
        // Bind the catalog to the newly established organization. Any selection
        // from a previous org is cleared first so it can never be reused.
        const orgId = session.active_organization_id ?? null;
        await loadEnvironmentCatalog(orgId);
        if (runIdParam) {
            inspectRun(runIdParam);
        }
        else {
            showView("runs-view");
            loadRunsList(api, getSelectedEnvironmentId(envSelection));
        }
    }
    function clearEnvironmentState() {
        setSelectionOrganization(envSelection, null);
        const sel = document.getElementById("env-select");
        if (sel) {
            sel.innerHTML = "";
            const opt = document.createElement("option");
            opt.value = "";
            opt.textContent = "Loading environments…";
            sel.appendChild(opt);
            sel.disabled = true;
        }
    }
    function populateEnvironmentSelector(catalog) {
        const sel = document.getElementById("env-select");
        if (!sel)
            return;
        sel.innerHTML = "";
        if (catalog.length === 0) {
            const opt = document.createElement("option");
            opt.value = "";
            opt.textContent = "No environments yet";
            sel.appendChild(opt);
            sel.disabled = true;
            return;
        }
        for (const env of catalog) {
            const opt = document.createElement("option");
            // Canonical environment UUID is the only value runs/workers use.
            opt.value = env.environmentId;
            opt.textContent = formatEnvironmentLabel(env);
            sel.appendChild(opt);
        }
        sel.disabled = false;
        const selected = getSelectedEnvironmentId(envSelection);
        if (selected)
            sel.value = selected;
    }
    function renderEnvironmentEmptyState() {
        const runsBody = document.getElementById("runs-table-body");
        if (runsBody) {
            runsBody.innerHTML =
                '<tr><td colspan="5" class="empty-state">No environments yet. Create a project environment with `runtime bootstrap`, then reload.</td></tr>';
        }
        const workersBody = document.getElementById("workers-table-body");
        if (workersBody) {
            workersBody.innerHTML =
                '<tr><td colspan="4" class="empty-state">No environments yet. Create a project environment with `runtime bootstrap`, then reload.</td></tr>';
        }
    }
    async function loadEnvironmentCatalog(orgId) {
        // Switching organizations clears stale catalog/selection first.
        setSelectionOrganization(envSelection, orgId);
        const loading = document.getElementById("env-select");
        if (loading) {
            loading.innerHTML = "";
            const opt = document.createElement("option");
            opt.value = "";
            opt.textContent = "Loading environments…";
            loading.appendChild(opt);
            loading.disabled = true;
        }
        let catalog;
        try {
            catalog = await api.loadEnvironmentCatalog();
        }
        catch (err) {
            const sel = document.getElementById("env-select");
            if (sel) {
                sel.innerHTML = "";
                const opt = document.createElement("option");
                opt.value = "";
                opt.textContent = "Failed to load environments";
                sel.appendChild(opt);
                sel.disabled = true;
            }
            renderError(err instanceof Error ? err : new Error(String(err)));
            return;
        }
        setSelectionCatalog(envSelection, catalog);
        populateEnvironmentSelector(catalog);
        if (catalog.length === 0) {
            renderEnvironmentEmptyState();
        }
    }
    function showAuthRequired(message) {
        teardownAppViews();
        const msg = document.getElementById("auth-message");
        if (msg)
            msg.textContent = message;
        const login = document.getElementById("login-link");
        if (login)
            login.classList.remove("hidden");
        const orgWrap = document.getElementById("org-select-wrap");
        if (orgWrap)
            orgWrap.classList.add("hidden");
        showView("auth-view");
    }
    function hideAuthView() {
        const login = document.getElementById("login-link");
        if (login)
            login.classList.add("hidden");
        const orgWrap = document.getElementById("org-select-wrap");
        if (orgWrap)
            orgWrap.classList.add("hidden");
    }
    function showOrgSelect(orgs) {
        teardownAppViews();
        const msg = document.getElementById("auth-message");
        if (msg)
            msg.textContent = "Select an organization to continue.";
        const login = document.getElementById("login-link");
        if (login)
            login.classList.add("hidden");
        const orgWrap = document.getElementById("org-select-wrap");
        const select = document.getElementById("org-select");
        const cont = document.getElementById("org-continue-btn");
        if (orgWrap && select && cont) {
            select.innerHTML = "";
            for (const o of orgs) {
                const opt = document.createElement("option");
                opt.value = o.OrganizationID;
                opt.textContent = `${o.OrganizationName} (${o.Role})`;
                select.appendChild(opt);
            }
            cont.onclick = () => {
                if (!select.value)
                    return;
                cont.textContent = "Switching...";
                // Drop previous-org environment state before establishing the new
                // org so its IDs can never be reused.
                clearEnvironmentState();
                teardownAppViews();
                api
                    .switchOrganization(select.value)
                    .then(() => api.getSession())
                    .then((s) => enterWithSession(s))
                    .catch((err) => renderError(err instanceof Error ? err : new Error(String(err))))
                    .finally(() => {
                    cont.textContent = "Continue";
                });
            };
            orgWrap.classList.remove("hidden");
        }
        showView("auth-view");
    }
    // teardownAppViews stops live streams and clears protected data so an
    // expired session neither leaks data nor invents workflow failure.
    function teardownAppViews() {
        if (activeInspector) {
            activeInspector.destroy();
            activeInspector = null;
        }
        clearEnvironmentState();
        for (const id of ["runs-table-body", "workers-table-body"]) {
            const el = document.getElementById(id);
            if (el)
                el.innerHTML = "";
        }
        const insp = document.getElementById("inspector-content");
        if (insp)
            insp.innerHTML = "";
        const label = document.getElementById("session-label");
        if (label) {
            label.textContent = "";
            label.classList.add("hidden");
        }
    }
    function handleUnauthorized() {
        teardownAppViews();
        showAuthRequired("Session expired. Sign in again to continue.");
    }
    function showView(viewId) {
        if (activeInspector) {
            activeInspector.destroy();
            activeInspector = null;
        }
        const views = document.querySelectorAll(".view-panel");
        views.forEach((v) => v.classList.add("hidden"));
        const target = document.getElementById(viewId);
        if (target) {
            target.classList.remove("hidden");
        }
    }
    async function loadRunsList(client, envId) {
        const listContainer = document.getElementById("runs-table-body");
        if (!listContainer)
            return;
        // Zero-environment (or not-yet-loaded) state is honest and issues no
        // protected request with an ambiguous/empty environment value.
        if (!envId) {
            renderEnvironmentEmptyState();
            return;
        }
        listContainer.innerHTML =
            '<tr><td colspan="5" class="loading">Loading workflow runs...</td></tr>';
        try {
            const resp = await client.listRuns(envId);
            if (resp.items.length === 0) {
                listContainer.innerHTML =
                    '<tr><td colspan="5" class="empty-state">No workflow runs found in this environment.</td></tr>';
                return;
            }
            listContainer.innerHTML = "";
            for (const run of resp.items) {
                const row = document.createElement("tr");
                row.innerHTML = `
          <td><button class="link-btn inspect-link" data-run-id="${run.id}">${run.id}</button></td>
          <td>${escapeHtml(run.workflowName)}</td>
          <td><span class="badge status-${run.status.toLowerCase()}">${run.status}</span></td>
          <td>${run.reasonCode ? escapeHtml(run.reasonCode) : "-"}</td>
          <td>${new Date(run.createdAt).toLocaleString()}</td>
        `;
                listContainer.appendChild(row);
            }
            listContainer.querySelectorAll(".inspect-link").forEach((btn) => {
                btn.addEventListener("click", (e) => {
                    const id = e.currentTarget.getAttribute("data-run-id");
                    if (id)
                        inspectRun(id);
                });
            });
        }
        catch (err) {
            if (isUnauthorized(err)) {
                handleUnauthorized();
                return;
            }
            listContainer.innerHTML = `<tr><td colspan="5" class="error-state">Failed to load runs: ${escapeHtml(err instanceof Error ? err.message : String(err))}</td></tr>`;
        }
    }
    async function loadWorkersList(client, envId) {
        const listContainer = document.getElementById("workers-table-body");
        if (!listContainer)
            return;
        if (!envId) {
            renderEnvironmentEmptyState();
            return;
        }
        listContainer.innerHTML =
            '<tr><td colspan="4" class="loading">Loading workers...</td></tr>';
        try {
            const resp = await client.listWorkers(envId);
            if (resp.items.length === 0) {
                listContainer.innerHTML =
                    '<tr><td colspan="4" class="empty-state">No workers registered for this environment.</td></tr>';
                return;
            }
            listContainer.innerHTML = "";
            for (const worker of resp.items) {
                const row = document.createElement("tr");
                const digests = worker.deploymentDigests.length > 0
                    ? worker.deploymentDigests
                        .map((d) => `<code class="digest-tag">${escapeHtml(d.slice(0, 16))}...</code>`)
                        .join(" ")
                    : '<span class="text-muted">(none)</span>';
                row.innerHTML = `
          <td><code>${worker.id}</code></td>
          <td>${escapeHtml(worker.pool)}</td>
          <td><span class="badge status-${worker.status.toLowerCase()}">${worker.status}</span></td>
          <td>${digests}</td>
        `;
                listContainer.appendChild(row);
            }
        }
        catch (err) {
            if (isUnauthorized(err)) {
                handleUnauthorized();
                return;
            }
            listContainer.innerHTML = `<tr><td colspan="4" class="error-state">Failed to load workers: ${escapeHtml(err instanceof Error ? err.message : String(err))}</td></tr>`;
        }
    }
    function inspectRun(runId) {
        showView("inspector-view");
        const container = document.getElementById("inspector-content");
        if (!container)
            return;
        container.innerHTML = '<div class="loading">Loading Run Inspector...</div>';
        if (activeInspector) {
            activeInspector.destroy();
        }
        let currentStreamFreshness = "DISCONNECTED";
        activeInspector = new RunInspector(runId);
        activeInspector.subscribe({
            onSnapshotUpdated: (snapshot) => renderSnapshot(snapshot),
            onFreshnessChanged: (freshness) => {
                currentStreamFreshness = freshness;
                renderFreshness(freshness);
                if (freshness === "LIVE" && clearStreamErrorOnLive(streamBanner)) {
                    clearBanner();
                }
            },
            onEventsUpdated: (events, hasMore, nextCursor) => renderEvents(events, hasMore, nextCursor),
            onLogsUpdated: (logs, err) => renderLogs(logs, err),
            onError: (err) => {
                if (isUnauthorized(err)) {
                    handleUnauthorized();
                    return;
                }
                renderStreamError(err);
            },
        });
        activeInspector.load();
        function renderSnapshot(snap) {
            const container = document.getElementById("inspector-content");
            if (!container)
                return;
            const stepsHtml = snap.steps
                .map((st) => {
                const attemptsHtml = st.attempts
                    .map((att) => {
                    const started = att.startedAt
                        ? new Date(att.startedAt).toLocaleTimeString()
                        : "-";
                    return `
            <div class="attempt-card status-${att.status.toLowerCase()}">
              <div class="attempt-header">
                <span class="attempt-title">Attempt #${att.attemptNumber}</span>
                <span class="badge status-${att.status.toLowerCase()}">${att.status}</span>
              </div>
              <div class="attempt-details">
                <span>Session: <code>${att.workerSessionId ? att.workerSessionId.slice(0, 8) + "..." : "-"}</code></span>
                <span>Started: ${started}</span>
                <span>Epoch: ${att.ownershipEpoch ?? "-"}</span>
              </div>
            </div>
          `;
                })
                    .join("");
                let noAttemptsHtml = '<div class="no-attempts text-muted">No attempts claimed yet</div>';
                if (shouldShowWorkerWait(st.status, snap.waitingReason, snap.activeCompatibleWorkers)) {
                    noAttemptsHtml = `
              <div class="no-attempts waiting-warning">
                <strong>No compatible workers available.</strong>
                <div class="recovery-hint">
                  Waiting for active worker advertising deployment <code>${escapeHtml(snap.deploymentId.slice(0, 8))}...</code>. Ensure an enrolled worker is running.
                </div>
              </div>
            `;
                }
                else if (st.status === "CANCELLED" ||
                    st.status === "FAILED" ||
                    st.status === "SUCCEEDED") {
                    noAttemptsHtml = `<div class="no-attempts text-muted">${escapeHtml(terminalStepEmptyText(st.status))}</div>`;
                }
                // Unknown-outcome hold: the provider may already have received the
                // operation. Only audited resolutions are offered, never blind retry.
                let holdHtml = "";
                const openCase = openCaseForStep(snap, st.id);
                if (st.status === "WAITING" && openCase) {
                    const evidenceRef = evidenceReference(openCase);
                    holdHtml = `
              <div class="hold-banner" role="status">
                <strong>Waiting for reconciliation.</strong>
                <div class="recovery-hint">${escapeHtml(reconciliationHoldText(openCase.reason))}</div>
                ${evidenceRef ? `<div class="hold-evidence">Reference: <code>${escapeHtml(evidenceRef)}</code></div>` : ""}
                <div class="hold-meta">Case <code>${escapeHtml(openCase.id.slice(0, 8))}…</code> · revision ${openCase.revision}</div>
                <button class="resolve-link" data-case-id="${escapeHtml(openCase.id)}" data-revision="${openCase.revision}" data-step-id="${escapeHtml(st.id)}">Resolve</button>
              </div>
            `;
                }
                return `
          <div class="step-card" data-step-id="${st.id}">
            <div class="step-header">
              <h4>${escapeHtml(st.nodeId)}</h4>
              <span class="badge status-${st.status.toLowerCase()}">${st.status}</span>
              ${st.completionSource ? `<span class="badge source-${st.completionSource.toLowerCase()}">${escapeHtml(st.completionSource)}</span>` : ""}
            </div>
            ${holdHtml}
            <div class="attempts-container">
              ${attemptsHtml || noAttemptsHtml}
            </div>
          </div>
        `;
            })
                .join("");
            container.innerHTML = `
        <div class="inspector-header">
          <div class="run-title-group">
            <h2>${escapeHtml(snap.workflowName)}</h2>
            <span class="run-id-label">ID: <code>${snap.id}</code></span>
          </div>
          <div class="run-badges">
            <span class="badge status-${snap.status.toLowerCase()}">${snap.status}</span>
            <span id="stream-freshness-badge" class="badge freshness-badge freshness-${currentStreamFreshness.toLowerCase()}">${currentStreamFreshness}</span>
            ${snap.status === "QUEUED" || snap.status === "RUNNING" || snap.status === "WAITING" ? `<button id="cancel-run-btn" class="danger-btn">Cancel run</button>` : ""}
          </div>
        </div>

        ${(() => {
                const banner = terminationBannerText(snap.status, snap.terminationConfirmed);
                return banner
                    ? `<div class="termination-banner" role="status">${escapeHtml(banner)}</div>`
                    : "";
            })()}

        <div class="meta-grid">
          <div class="meta-item"><label>Revision</label><div>${snap.revision}</div></div>
          <div class="meta-item"><label>Last Event Seq</label><div>${snap.lastEventSequence}</div></div>
          <div class="meta-item"><label>Deployment ID</label><div><code>${snap.deploymentId.slice(0, 8)}...</code></div></div>
          <div class="meta-item"><label>Deadline</label><div>${snap.deadlineAt ? new Date(snap.deadlineAt).toLocaleString() : "None"}</div></div>
          <div class="meta-item"><label>Created At</label><div>${new Date(snap.createdAt).toLocaleString()}</div></div>
          ${snap.reasonCode ? `<div class="meta-item"><label>Reason</label><div>${escapeHtml(snap.reasonCode)}</div></div>` : ""}
        </div>

        <section class="steps-section">
          <h3>Execution Graph & Attempts</h3>
          <div class="steps-grid">${stepsHtml}</div>
        </section>

        <section class="events-section">
          <h3>Execution Event History</h3>
          <div id="events-container" class="events-timeline">
            <div class="loading">Loading event history...</div>
          </div>
        </section>

        <section class="logs-section">
          <h3>Task Diagnostic Logs</h3>
          <div id="logs-container" class="logs-container">
            <div class="loading">Loading logs...</div>
          </div>
        </section>

        ${snap.output
                ? `
          <section class="output-section">
            <h3>Workflow Output</h3>
            <pre class="code-block">${escapeHtml(JSON.stringify(snap.output, null, 2))}</pre>
          </section>
        `
                : ""}

        ${snap.error
                ? `
          <section class="error-section">
            <h3>Workflow Error</h3>
            <pre class="code-block error-text">${escapeHtml(JSON.stringify(snap.error, null, 2))}</pre>
          </section>
        `
                : ""}
      `;
            // Re-apply current transport freshness
            renderFreshness(currentStreamFreshness);
            // Wire durable cancellation. The backend stays authoritative:
            // expectedRevision is captured at open time and 409s refresh in-dialog.
            const cancelBtn = container.querySelector("#cancel-run-btn");
            if (cancelBtn) {
                cancelBtn.addEventListener("click", (e) => {
                    openCancelDialog(api, snap, e.currentTarget);
                });
            }
            // Wire audited resolution dialogs. The backend stays authoritative:
            // expectedRevision is captured at open time and 409s refresh in-dialog.
            container.querySelectorAll(".resolve-link").forEach((btn) => {
                btn.addEventListener("click", (e) => {
                    const el = e.currentTarget;
                    const caseId = el.getAttribute("data-case-id");
                    const stepId = el.getAttribute("data-step-id");
                    const revision = Number(el.getAttribute("data-revision"));
                    if (caseId && stepId && Number.isFinite(revision)) {
                        openResolveDialog(api, snap, stepId, caseId, revision, el);
                    }
                });
            });
            // Re-render cached events and logs if activeInspector already has them
            if (activeInspector) {
                const evs = activeInspector.getEvents();
                if (evs.length > 0) {
                    renderEvents(evs, false, null);
                }
            }
        }
    }
    function renderFreshness(f) {
        const badge = document.getElementById("stream-freshness-badge");
        if (!badge)
            return;
        badge.className = `badge freshness-badge freshness-${f.toLowerCase()}`;
        badge.textContent = f;
        badge.setAttribute("aria-label", `Stream status: ${f}`);
    }
    function renderEvents(events, hasMore, nextCursor) {
        const container = document.getElementById("events-container");
        if (!container)
            return;
        if (events.length === 0) {
            container.innerHTML =
                '<div class="text-muted">No events recorded yet.</div>';
            return;
        }
        const cardsHtml = events
            .map((ev) => {
            const time = new Date(ev.committedAt).toLocaleTimeString();
            const payloadStr = JSON.stringify(ev.payload, null, 2);
            return `
          <div class="event-card" data-sequence="${ev.sequence}">
            <div class="event-header">
              <span class="event-type">${escapeHtml(ev.type)}</span>
              <span class="event-seq">#${ev.sequence}</span>
            </div>
            <div class="event-time">${time}</div>
            <pre class="event-payload">${escapeHtml(payloadStr)}</pre>
          </div>
        `;
        })
            .join("");
        let loadMoreHtml = "";
        if (hasMore && nextCursor !== null) {
            loadMoreHtml = `<button id="load-more-events-btn" class="load-more-btn">Load Earlier Events</button>`;
        }
        container.innerHTML = cardsHtml + loadMoreHtml;
        if (hasMore && nextCursor !== null) {
            const btn = document.getElementById("load-more-events-btn");
            if (btn) {
                btn.addEventListener("click", () => {
                    btn.textContent = "Loading...";
                    btn.setAttribute("disabled", "true");
                    activeInspector?.fetchEvents(nextCursor, true);
                });
            }
        }
    }
    function renderLogs(logs, error) {
        const container = document.getElementById("logs-container");
        if (!container)
            return;
        if (error) {
            container.innerHTML = `<div class="logs-notice logs-restricted">${escapeHtml(error)}</div>`;
            return;
        }
        if (!logs) {
            container.innerHTML = '<div class="text-muted">No logs available.</div>';
            return;
        }
        if (logs.expired) {
            container.innerHTML = `<div class="logs-notice logs-expired">${escapeHtml(logs.message ?? "Logs expired due to retention policy.")}</div>`;
            return;
        }
        if (logs.items.length === 0) {
            container.innerHTML =
                '<div class="text-muted">No logs recorded for this execution.</div>';
            return;
        }
        let warningNotice = "";
        if (logs.budgetExhausted) {
            warningNotice = `<div class="logs-notice logs-restricted">Log budget exhausted (1 MiB per attempt limit reached). ${logs.droppedCount ? logs.droppedCount + " records dropped." : ""}</div>`;
        }
        else if (logs.droppedCount) {
            warningNotice = `<div class="logs-notice logs-restricted">${logs.droppedCount} log record(s) dropped (exceeded 16 KiB per-line limit).</div>`;
        }
        const logLines = logs.items
            .map((item) => {
            const time = new Date(item.timestamp).toLocaleTimeString();
            return `<div class="log-line log-${item.level}"><span class="log-time">${time}</span> <span class="log-level">[${item.level.toUpperCase()}]</span> <span class="log-msg">${escapeHtml(item.message)}</span></div>`;
        })
            .join("");
        let loadMoreHtml = "";
        if (logs.nextCursor) {
            loadMoreHtml = `<button id="load-more-logs-btn" class="load-more-btn">Load More Logs</button>`;
        }
        container.innerHTML = `${warningNotice}<div class="log-terminal">${logLines}</div>${loadMoreHtml}`;
        if (logs.nextCursor) {
            const btn = document.getElementById("load-more-logs-btn");
            if (btn) {
                btn.addEventListener("click", () => {
                    btn.textContent = "Loading...";
                    btn.setAttribute("disabled", "true");
                    activeInspector?.fetchLogs(undefined, undefined, logs.nextCursor, true);
                });
            }
        }
    }
    function renderError(err) {
        // Bootstrap/API errors are unrelated to transient stream recovery and
        // must never be cleared implicitly on reconnect.
        markGlobalError(streamBanner, err.message);
        const banner = document.getElementById("global-error-banner");
        if (banner) {
            banner.textContent = err.message;
            banner.classList.remove("hidden");
            banner.dataset.errorKind = "global";
        }
    }
    function renderStreamError(err) {
        markStreamError(streamBanner, err.message);
        const banner = document.getElementById("global-error-banner");
        if (banner) {
            banner.textContent = err.message;
            banner.classList.remove("hidden");
            banner.dataset.errorKind = "stream";
        }
    }
    function clearBanner() {
        const banner = document.getElementById("global-error-banner");
        if (banner) {
            banner.textContent = "";
            banner.classList.add("hidden");
            delete banner.dataset.errorKind;
        }
    }
    function escapeHtml(str) {
        const div = document.createElement("div");
        div.textContent = str;
        return div.innerHTML;
    }
    // evidenceReference surfaces the hold's recorded reference (operation ID
    // or provider reference) without ever implying the outcome is known.
    function evidenceReference(c) {
        const ev = c.evidence;
        if (ev && typeof ev === "object") {
            for (const key of ["reference", "operationId", "externalRef"]) {
                if (typeof ev[key] === "string" && ev[key].length > 0) {
                    return ev[key];
                }
            }
        }
        return "";
    }
    // openResolveDialog offers only the three audited resolutions for an
    // unknown outcome. There is deliberately no blind "retry anyway": retries
    // go through confirm_not_executed_retry within budget, and every decision
    // binds the revision read at open time.
    function openResolveDialog(api, snap, stepId, caseId, revision, invoker) {
        closeResolveDialog();
        const step = snap.steps.find((s) => s.id === stepId);
        const overlay = document.createElement("div");
        overlay.className = "dialog-overlay";
        overlay.id = "resolve-dialog-overlay";
        const optionsHtml = RESOLVE_ACTIONS.map((opt, i) => `
        <label class="resolve-option">
          <input type="radio" name="resolve-action" value="${opt.action}" ${i === 0 ? "checked" : ""} />
          <span><strong>${escapeHtml(opt.label)}</strong><br />
          <span class="text-muted">${escapeHtml(opt.hint)}</span></span>
        </label>
      `).join("");
        overlay.innerHTML = `
      <div class="dialog" role="dialog" aria-modal="true" aria-labelledby="resolve-dialog-title">
        <h3 id="resolve-dialog-title">Resolve unknown outcome — ${escapeHtml(step?.nodeId ?? stepId)}</h3>
        <p class="text-muted">The provider may already have received this operation. Your decision is audited with your identity.</p>
        <fieldset>
          <legend>Decision</legend>
          ${optionsHtml}
        </fieldset>
        <label>Evidence reference (required)
          <input id="resolve-evidence" type="text" placeholder="e.g. provider payment ID, message ID" autocomplete="off" />
        </label>
        <label>Decision reason (required, max 280 characters)
          <input id="resolve-reason" type="text" placeholder="Why is this decision correct?" maxlength="280" autocomplete="off" />
        </label>
        <label id="resolve-result-label" style="display:none">Result JSON (required for confirm succeeded)
          <textarea id="resolve-result" rows="4" placeholder='{"key": "value"}'></textarea>
        </label>
        <div id="resolve-error" class="dialog-error" role="alert" style="display:none"></div>
        <div class="dialog-actions">
          <button id="resolve-cancel">Cancel</button>
          <button id="resolve-submit">Submit decision</button>
        </div>
      </div>
    `;
        document.body.appendChild(overlay);
        // One command identity for this decision: retries of the same ambiguous
        // submit reuse it, so the server can dedupe instead of double-deciding.
        const idempotencyKey = newIdempotencyKey();
        const errorBox = overlay.querySelector("#resolve-error");
        const evidenceInput = overlay.querySelector("#resolve-evidence");
        const reasonInput = overlay.querySelector("#resolve-reason");
        const resultLabel = overlay.querySelector("#resolve-result-label");
        const resultInput = overlay.querySelector("#resolve-result");
        const submitBtn = overlay.querySelector("#resolve-submit");
        const showError = (msg) => {
            errorBox.textContent = msg;
            errorBox.style.display = "block";
        };
        const syncResultVisibility = () => {
            const checked = overlay.querySelector('input[name="resolve-action"]:checked');
            resultLabel.style.display =
                checked?.value === "confirm_succeeded" ? "block" : "none";
        };
        overlay
            .querySelectorAll('input[name="resolve-action"]')
            .forEach((r) => r.addEventListener("change", syncResultVisibility));
        syncResultVisibility();
        const close = () => {
            closeResolveDialog();
            invoker?.focus();
        };
        overlay.querySelector("#resolve-cancel").addEventListener("click", close);
        overlay.addEventListener("keydown", (e) => {
            if (e.key === "Escape")
                close();
        });
        overlay.addEventListener("mousedown", (e) => {
            if (e.target === overlay)
                close();
        });
        evidenceInput.focus();
        submitBtn.addEventListener("click", () => {
            const checked = overlay.querySelector('input[name="resolve-action"]:checked');
            const action = (checked?.value ?? "confirm_succeeded");
            const evidence = evidenceInput.value.trim();
            if (!evidence) {
                showError("Evidence reference is required.");
                return;
            }
            const reason = reasonInput.value.trim();
            if (!reason) {
                showError("Decision reason is required.");
                return;
            }
            if (reason.length > 280) {
                showError("Decision reason must be at most 280 characters.");
                return;
            }
            let result;
            if (action === "confirm_succeeded") {
                if (!resultInput.value.trim()) {
                    showError("Result JSON is required to confirm success.");
                    return;
                }
                try {
                    result = JSON.parse(resultInput.value);
                }
                catch {
                    showError("Result must be valid JSON.");
                    return;
                }
            }
            submitBtn.setAttribute("disabled", "true");
            const body = { action, evidence, reason, expectedRevision: revision };
            if (action === "confirm_succeeded")
                body.result = result;
            api
                .resolveReconciliationCase(caseId, body, idempotencyKey)
                .then(() => {
                close();
                void activeInspector
                    ?.fetchSnapshot()
                    .catch((err) => renderError(err instanceof Error ? err : new Error(String(err))));
            })
                .catch((err) => {
                submitBtn.removeAttribute("disabled");
                if (isConflict(err)) {
                    showError("This case changed since you opened it (409). The latest state was reloaded — review it before acting.");
                    void activeInspector?.fetchSnapshot().catch(() => undefined);
                    return;
                }
                showError(err instanceof Error ? err.message : String(err));
            });
        });
    }
    function closeResolveDialog() {
        document.getElementById("resolve-dialog-overlay")?.remove();
    }
    // openCancelDialog confirms durable cancellation. Cancelling stops
    // platform execution and revokes worker ownership; it never rolls back
    // external side effects already performed.
    function openCancelDialog(api, snap, invoker) {
        closeCancelDialog();
        const overlay = document.createElement("div");
        overlay.className = "dialog-overlay";
        overlay.id = "cancel-dialog-overlay";
        overlay.innerHTML = `
      <div class="dialog" role="dialog" aria-modal="true" aria-labelledby="cancel-dialog-title">
        <h3 id="cancel-dialog-title">Cancel run — ${escapeHtml(snap.workflowName)}</h3>
        <p class="text-muted">This revokes worker ownership and stops nonterminal work immediately.
        Already-succeeded steps are preserved. External effects already performed are
        <strong>not</strong> rolled back — verify provider state afterwards.</p>
        <div class="hold-meta">Run revision ${snap.revision}</div>
        <div id="cancel-error" class="dialog-error" role="alert" style="display:none"></div>
        <div class="dialog-actions">
          <button id="cancel-dismiss">Keep running</button>
          <button id="cancel-confirm">Confirm cancel</button>
        </div>
      </div>
    `;
        document.body.appendChild(overlay);
        // One command identity for this decision, reused if the submit is
        // ambiguously delivered.
        const idempotencyKey = newIdempotencyKey();
        const errorBox = overlay.querySelector("#cancel-error");
        const confirmBtn = overlay.querySelector("#cancel-confirm");
        const close = () => {
            closeCancelDialog();
            invoker?.focus();
        };
        overlay.querySelector("#cancel-dismiss").addEventListener("click", close);
        overlay.addEventListener("keydown", (e) => {
            if (e.key === "Escape")
                close();
        });
        overlay.addEventListener("mousedown", (e) => {
            if (e.target === overlay)
                close();
        });
        confirmBtn.focus();
        confirmBtn.addEventListener("click", () => {
            confirmBtn.setAttribute("disabled", "true");
            api
                .cancelRun(snap.id, snap.revision, idempotencyKey)
                .then(() => {
                close();
                void activeInspector
                    ?.fetchSnapshot()
                    .catch((err) => renderError(err instanceof Error ? err : new Error(String(err))));
            })
                .catch((err) => {
                confirmBtn.removeAttribute("disabled");
                if (isConflict(err)) {
                    errorBox.textContent =
                        "This run changed since you opened it (409). The latest state was reloaded — review it before acting.";
                    errorBox.style.display = "block";
                    void activeInspector?.fetchSnapshot().catch(() => undefined);
                    return;
                }
                errorBox.textContent =
                    err instanceof Error ? err.message : String(err);
                errorBox.style.display = "block";
            });
        });
    }
    function closeCancelDialog() {
        document.getElementById("cancel-dialog-overlay")?.remove();
    }
}
//# sourceMappingURL=index.js.map