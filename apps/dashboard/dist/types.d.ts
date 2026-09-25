export type RunStatus = "QUEUED" | "RUNNING" | "WAITING" | "PAUSING" | "PAUSED" | "CANCELLING" | "SUCCEEDED" | "FAILED" | "CANCELLED";
export type StepStatus = "BLOCKED" | "READY" | "RUNNING" | "WAITING" | "SUCCEEDED" | "FAILED" | "CANCELLED";
export type AttemptStatus = "CLAIMED" | "RUNNING" | "SUCCEEDED" | "FAILED" | "TIMED_OUT" | "LOST" | "CANCELLED";
export interface TaskAttempt {
    id: string;
    attemptNumber: number;
    status: AttemptStatus;
    workerSessionId?: string;
    ownershipEpoch?: number;
    startedAt?: string;
    completedAt?: string;
    error?: unknown;
}
export interface RunStep {
    id: string;
    nodeId: string;
    status: StepStatus;
    currentEpoch: number;
    completionSource?: string | null;
    attempts: TaskAttempt[];
}
export interface Run {
    id: string;
    organizationId?: string;
    projectId?: string;
    environmentId?: string;
    workflowName: string;
    deploymentId: string;
    status: RunStatus;
    reasonCode?: string;
    revision: number;
    createdAt: string;
    deadlineAt?: string;
    terminationConfirmed?: boolean | null;
}
export interface RunSnapshot extends Run {
    lastEventSequence: number;
    steps: RunStep[];
    reconciliationCases?: ReconciliationCase[];
    output?: unknown;
    error?: unknown;
    waitingReason?: string | null;
    activeCompatibleWorkers?: number;
}
export interface ReconciliationCase {
    id: string;
    stepId: string;
    attemptId?: string | null;
    reason: string;
    evidence?: unknown;
    status: "OPEN" | "RESOLVED";
    resolution?: string | null;
    actorId?: string | null;
    revision: number;
    resolvedAt?: string | null;
    createdAt: string;
}
export type ResolveAction = "confirm_succeeded" | "confirm_not_executed_retry" | "fail_run";
export interface ResolveReconciliationRequest {
    action: ResolveAction;
    evidence: string;
    reason: string;
    result?: unknown;
    expectedRevision: number;
}
export interface ResolveReconciliationResponse {
    caseId: string;
    resolved: boolean;
    revision: number;
}
export interface Worker {
    id: string;
    environmentId: string;
    pool: string;
    status: "ACTIVE" | "DRAINING" | "REVOKED";
    deploymentDigests: string[];
}
export interface RunEvent {
    id: string;
    runId: string;
    sequence: number;
    schemaVersion: number;
    type: string;
    requestId?: string;
    payload: Record<string, unknown>;
    committedAt: string;
}
export interface RunEventsResponse {
    events: RunEvent[];
    hasMore: boolean;
    nextCursor?: number | null;
}
export interface TaskLogRecord {
    id: string;
    runId: string;
    stepId: string;
    attemptId: string;
    sequence: number;
    timestamp: string;
    level: "debug" | "info" | "warn" | "error";
    message: string;
}
export interface TaskLogsResponse {
    items: TaskLogRecord[];
    nextCursor?: string;
    expired: boolean;
    message?: string;
    droppedCount?: number;
    budgetExhausted?: boolean;
}
export type StreamFreshness = "LIVE" | "RECONNECTING" | "STALE" | "DISCONNECTED";
export interface ResyncControlEvent {
    reason: "RETENTION_GAP" | string;
    code?: "RESYNC_REQUIRED" | string;
    lastAvailableSequence: number;
}
//# sourceMappingURL=types.d.ts.map