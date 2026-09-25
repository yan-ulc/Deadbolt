import { Run, Worker, RunSnapshot, RunEventsResponse, TaskLogsResponse, ResolveReconciliationRequest, ResolveReconciliationResponse } from "./types.js";
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
    user: {
        id: string;
        email: string;
        name: string;
    };
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
export interface CatalogEnvironment {
    projectId: string;
    projectName: string;
    environmentId: string;
    environmentName: string;
}
export declare function formatEnvironmentLabel(env: CatalogEnvironment): string;
export interface EnvironmentSelection {
    orgId: string | null;
    catalog: CatalogEnvironment[];
    selectedEnvironmentId: string | null;
}
export declare function createEnvironmentSelection(): EnvironmentSelection;
export declare function setSelectionOrganization(sel: EnvironmentSelection, orgId: string | null): void;
export declare function setSelectionCatalog(sel: EnvironmentSelection, catalog: CatalogEnvironment[]): string | null;
export declare function selectEnvironment(sel: EnvironmentSelection, envId: string | null): string | null;
export declare function getSelectedEnvironmentId(sel: EnvironmentSelection): string | null;
export declare function apiFetch(input: string, init?: RequestInit): Promise<Response>;
export declare function isUnauthorized(err: unknown): boolean;
export declare function isConflict(err: unknown): boolean;
export declare function newIdempotencyKey(): string;
export declare function readCsrfToken(): string;
export declare class DashboardApiClient {
    private baseUrl;
    constructor(baseUrl?: string);
    getSession(): Promise<AuthSession | null>;
    switchOrganization(orgId: string): Promise<SwitchOrgResponse>;
    listRuns(environment: string, cursor?: string, limit?: number): Promise<ListRunsResponse>;
    getRun(runId: string): Promise<RunSnapshot>;
    listWorkers(environment: string, cursor?: string, limit?: number): Promise<ListWorkersResponse>;
    getRunEvents(runId: string, cursor?: number, limit?: number): Promise<RunEventsResponse>;
    getRunLogs(runId: string, stepId?: string, attemptId?: string, cursor?: string, limit?: number): Promise<TaskLogsResponse>;
    resolveReconciliationCase(caseId: string, body: ResolveReconciliationRequest, idempotencyKey?: string): Promise<ResolveReconciliationResponse>;
    cancelRun(runId: string, expectedRevision: number, idempotencyKey?: string): Promise<Run>;
    listProjects(): Promise<ProjectSummary[]>;
    listProjectEnvironments(projectId: string): Promise<EnvironmentSummary[]>;
    loadEnvironmentCatalog(): Promise<CatalogEnvironment[]>;
}
//# sourceMappingURL=api.d.ts.map