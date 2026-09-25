import {
  Run,
  Worker,
  RunSnapshot,
  RunEventsResponse,
  TaskLogsResponse,
  ResolveReconciliationRequest,
  ResolveReconciliationResponse,
} from "./types.js";

export interface ListRunsResponse {
  items: Run[];
  nextCursor?: string | null;
}

export interface ListWorkersResponse {
  items: Worker[];
  nextCursor?: string | null;
}

export interface AuthMembership {
  OrganizationID: string;
  OrganizationName: string;
  Role: string;
  Status: string;
}

export interface AuthSession {
  user: { id: string; email: string; name: string };
  active_organization_id: string | null;
  memberships: AuthMembership[];
}

export interface SwitchOrgResponse {
  session_id: string;
  active_organization_id: string;
  csrf_token: string;
}

export interface ProjectSummary {
  id: string;
  name: string;
}

export interface EnvironmentSummary {
  id: string;
  name: string;
}

// CatalogEnvironment is one selectable environment with enough project
// context to disambiguate identical environment names across projects.
// The option value is always the canonical environment UUID.
export interface CatalogEnvironment {
  projectId: string;
  projectName: string;
  environmentId: string;
  environmentName: string;
}

export function formatEnvironmentLabel(env: CatalogEnvironment): string {
  return `${env.projectName} / ${env.environmentName}`;
}

// EnvironmentSelection tracks the dashboard's canonical environment choice
// for the active organization. Switching organizations clears stale
// catalog/selection so an environment ID from the previous org can never
// be reused.
export interface EnvironmentSelection {
  orgId: string | null;
  catalog: CatalogEnvironment[];
  selectedEnvironmentId: string | null;
}

export function createEnvironmentSelection(): EnvironmentSelection {
  return { orgId: null, catalog: [], selectedEnvironmentId: null };
}

export function setSelectionOrganization(
  sel: EnvironmentSelection,
  orgId: string | null,
): void {
  if (sel.orgId !== orgId) {
    sel.orgId = orgId;
    sel.catalog = [];
    sel.selectedEnvironmentId = null;
  }
}

export function setSelectionCatalog(
  sel: EnvironmentSelection,
  catalog: CatalogEnvironment[],
): string | null {
  sel.catalog = [...catalog];
  if (
    sel.selectedEnvironmentId &&
    catalog.some((e) => e.environmentId === sel.selectedEnvironmentId)
  ) {
    return sel.selectedEnvironmentId;
  }
  sel.selectedEnvironmentId =
    catalog.length > 0 ? catalog[0].environmentId : null;
  return sel.selectedEnvironmentId;
}

export function selectEnvironment(
  sel: EnvironmentSelection,
  envId: string | null,
): string | null {
  if (!envId) {
    sel.selectedEnvironmentId = null;
    return null;
  }
  if (sel.catalog.some((e) => e.environmentId === envId)) {
    sel.selectedEnvironmentId = envId;
    return envId;
  }
  return sel.selectedEnvironmentId;
}

export function getSelectedEnvironmentId(
  sel: EnvironmentSelection,
): string | null {
  return sel.selectedEnvironmentId;
}

// apiFetch keeps every dashboard request on same-origin cookies explicitly.
// The dashboard never handles bearer tokens; authentication is the HttpOnly
// BFF session cookie.
export async function apiFetch(
  input: string,
  init: RequestInit = {},
): Promise<Response> {
  return fetch(input, { credentials: "same-origin", ...init });
}

export function isUnauthorized(err: unknown): boolean {
  return err instanceof Error && err.message.includes("HTTP 401");
}

export function isConflict(err: unknown): boolean {
  return err instanceof Error && err.message.includes("HTTP 409");
}

// newIdempotencyKey mints a unique command key for one user decision. The
// dialog keeps the key for the lifetime of its submission so a retry of the
// same ambiguous submit reuses the identity instead of forking commands.
export function newIdempotencyKey(): string {
  const cryptoObj = typeof crypto !== "undefined" ? crypto : undefined;
  if (cryptoObj && "randomUUID" in cryptoObj) {
    return (cryptoObj as Crypto).randomUUID();
  }
  return `resolve-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 14)}`;
}

// readCsrfToken reads the SPA-readable CSRF bootstrap cookie without ever
// touching the HttpOnly session value. Both hosted (__Host-) and local
// (non-secure) cookie names are accepted.
export function readCsrfToken(): string {
  if (typeof document === "undefined") return "";
  for (const part of document.cookie.split(";")) {
    const [rawName, ...rest] = part.trim().split("=");
    const name = rawName.trim();
    if (name === "__Host-csrf_token" || name === "deadbolt_local_csrf") {
      return decodeURIComponent(rest.join("=").trim());
    }
  }
  return "";
}

export class DashboardApiClient {
  private baseUrl: string;

  constructor(baseUrl = "") {
    this.baseUrl = baseUrl.replace(/\/$/, "");
  }

  // getSession returns the current BFF session, or null when the browser is
  // unauthenticated. Other failures are thrown for the bootstrap to render.
  public async getSession(): Promise<AuthSession | null> {
    const res = await apiFetch(`${this.baseUrl}/api/auth/session`);
    if (res.status === 401) return null;
    if (!res.ok) {
      throw new Error(
        `Failed to get session (HTTP ${res.status}): ${res.statusText}`,
      );
    }
    return res.json() as Promise<AuthSession>;
  }

  // switchOrganization establishes the session's active organization. The
  // server rotates session cookies; CSRF rules still apply.
  public async switchOrganization(orgId: string): Promise<SwitchOrgResponse> {
    const res = await apiFetch(`${this.baseUrl}/api/auth/switch-org`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-CSRF-Token": readCsrfToken(),
      },
      body: JSON.stringify({ organization_id: orgId }),
    });
    if (!res.ok) {
      throw new Error(
        `Failed to switch organization (HTTP ${res.status}): ${res.statusText}`,
      );
    }
    return res.json() as Promise<SwitchOrgResponse>;
  }

  public async listRuns(
    environment: string,
    cursor?: string,
    limit = 25,
  ): Promise<ListRunsResponse> {
    // environment MUST be the canonical environment UUID. Bare names that
    // exist in several projects are ambiguous and rejected server-side.
    const params = new URLSearchParams({ environment });
    if (cursor) params.set("cursor", cursor);
    if (limit) params.set("limit", String(limit));

    const res = await apiFetch(`${this.baseUrl}/v1/runs?${params.toString()}`);
    if (!res.ok) {
      throw new Error(
        `Failed to list runs (HTTP ${res.status}): ${res.statusText}`,
      );
    }
    return res.json() as Promise<ListRunsResponse>;
  }

  public async getRun(runId: string): Promise<RunSnapshot> {
    const res = await apiFetch(
      `${this.baseUrl}/v1/runs/${encodeURIComponent(runId)}`,
    );
    if (!res.ok) {
      throw new Error(
        `Failed to get run (HTTP ${res.status}): ${res.statusText}`,
      );
    }
    return res.json() as Promise<RunSnapshot>;
  }

  public async listWorkers(
    environment: string,
    cursor?: string,
    limit = 25,
  ): Promise<ListWorkersResponse> {
    // environment MUST be the canonical environment UUID (see listRuns).
    const params = new URLSearchParams({ environment });
    if (cursor) params.set("cursor", cursor);
    if (limit) params.set("limit", String(limit));

    const res = await apiFetch(
      `${this.baseUrl}/v1/workers?${params.toString()}`,
    );
    if (!res.ok) {
      throw new Error(
        `Failed to list workers (HTTP ${res.status}): ${res.statusText}`,
      );
    }
    return res.json() as Promise<ListWorkersResponse>;
  }

  public async getRunEvents(
    runId: string,
    cursor?: number,
    limit = 50,
  ): Promise<RunEventsResponse> {
    const params = new URLSearchParams();
    if (cursor !== undefined && cursor !== null) {
      params.set("cursor", String(cursor));
    }
    if (limit) {
      params.set("limit", String(limit));
    }
    const q = params.toString() ? `?${params.toString()}` : "";
    const res = await apiFetch(
      `${this.baseUrl}/v1/runs/${encodeURIComponent(runId)}/events${q}`,
    );
    if (!res.ok) {
      throw new Error(
        `Failed to get run events (HTTP ${res.status}): ${res.statusText}`,
      );
    }
    return res.json() as Promise<RunEventsResponse>;
  }

  public async getRunLogs(
    runId: string,
    stepId?: string,
    attemptId?: string,
    cursor?: string,
    limit = 50,
  ): Promise<TaskLogsResponse> {
    const params = new URLSearchParams();
    if (stepId) params.set("stepId", stepId);
    if (attemptId) params.set("attemptId", attemptId);
    if (cursor) params.set("cursor", cursor);
    if (limit) params.set("limit", String(limit));

    const q = params.toString() ? `?${params.toString()}` : "";
    const res = await apiFetch(
      `${this.baseUrl}/v1/runs/${encodeURIComponent(runId)}/logs${q}`,
    );
    if (!res.ok) {
      throw new Error(
        `Failed to get run logs (HTTP ${res.status}): ${res.statusText}`,
      );
    }
    return res.json() as Promise<TaskLogsResponse>;
  }

  // resolveReconciliationCase submits one audited human decision for an
  // unknown-outcome hold. The caller binds the expectedRevision read from
  // the snapshot; a 409 means the case changed and the dialog must refresh
  // instead of retrying blindly. Every decision carries an Idempotency-Key:
  // pass the dialog's key so an ambiguous resubmit reuses the same command
  // identity instead of creating a second decision.
  public async resolveReconciliationCase(
    caseId: string,
    body: ResolveReconciliationRequest,
    idempotencyKey?: string,
  ): Promise<ResolveReconciliationResponse> {
    const res = await apiFetch(
      `${this.baseUrl}/v1/reconciliation-cases/${encodeURIComponent(caseId)}/resolve`,
      {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-CSRF-Token": readCsrfToken(),
          "Idempotency-Key": idempotencyKey ?? newIdempotencyKey(),
        },
        body: JSON.stringify(body),
      },
    );
    if (!res.ok) {
      throw new Error(
        `Failed to resolve case (HTTP ${res.status}): ${res.statusText}`,
      );
    }
    return res.json() as Promise<ResolveReconciliationResponse>;
  }

  // cancelRun requests durable cancellation. The caller binds the revision
  // read from the snapshot; a 409 means the run changed and the dialog must
  // refresh instead of retrying blindly. Cancelling never rolls back
  // external side effects already performed.
  public async cancelRun(
    runId: string,
    expectedRevision: number,
    idempotencyKey?: string,
  ): Promise<Run> {
    const res = await apiFetch(
      `${this.baseUrl}/v1/runs/${encodeURIComponent(runId)}/cancel`,
      {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-CSRF-Token": readCsrfToken(),
          "Idempotency-Key": idempotencyKey ?? newIdempotencyKey(),
        },
        body: JSON.stringify({ expectedRevision }),
      },
    );
    if (!res.ok) {
      throw new Error(
        `Failed to cancel run (HTTP ${res.status}): ${res.statusText}`,
      );
    }
    return res.json() as Promise<Run>;
  }

  // listProjects discovers the active organization's projects through the
  // existing public tenant API using the BFF session cookie.
  public async listProjects(): Promise<ProjectSummary[]> {
    const res = await apiFetch(`${this.baseUrl}/v1/projects`);
    if (!res.ok) {
      throw new Error(
        `Failed to list projects (HTTP ${res.status}): ${res.statusText}`,
      );
    }
    const data = (await res.json()) as { projects?: ProjectSummary[] };
    return data.projects ?? [];
  }

  // listProjectEnvironments lists one project's environments through the
  // existing public tenant API using the BFF session cookie.
  public async listProjectEnvironments(
    projectId: string,
  ): Promise<EnvironmentSummary[]> {
    const res = await apiFetch(
      `${this.baseUrl}/v1/projects/${encodeURIComponent(projectId)}/environments`,
    );
    if (!res.ok) {
      throw new Error(
        `Failed to list environments (HTTP ${res.status}): ${res.statusText}`,
      );
    }
    const data = (await res.json()) as {
      environments?: EnvironmentSummary[];
    };
    return data.environments ?? [];
  }

  // loadEnvironmentCatalog discovers every environment in the active
  // organization with project context. Callers MUST use environmentId as
  // the selector value and for runs/workers requests.
  public async loadEnvironmentCatalog(): Promise<CatalogEnvironment[]> {
    const projects = await this.listProjects();
    const catalog: CatalogEnvironment[] = [];
    for (const p of projects) {
      if (!p || !p.id) continue;
      const envs = await this.listProjectEnvironments(p.id);
      for (const e of envs) {
        if (!e || !e.id) continue;
        catalog.push({
          projectId: p.id,
          projectName: p.name ?? p.id,
          environmentId: e.id,
          environmentName: e.name ?? e.id,
        });
      }
    }
    return catalog;
  }
}
