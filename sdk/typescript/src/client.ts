import { ContractError, fail, type JSONValue } from "./json.js";
import type { RunStatus, StepStatus } from "./enums.js";
import { type ObjectValue } from "./schema.js";

export interface ErrorEnvelope {
  code: string;
  message: string;
  requestId?: string;
  details?: JSONValue;
  retryable?: boolean;
}

export class DeadboltApiError extends Error {
  readonly status: number;
  readonly code: string;
  readonly requestId?: string;
  readonly details?: JSONValue;
  readonly retryable?: boolean;

  constructor(status: number, envelope: ErrorEnvelope) {
    super(`[${status}] ${envelope.code}: ${envelope.message}`);
    this.name = "DeadboltApiError";
    this.status = status;
    this.code = envelope.code;
    this.requestId = envelope.requestId;
    this.details = envelope.details;
    this.retryable = envelope.retryable;
  }
}

export class DeadboltExecutionError extends Error {
  readonly code: string;
  readonly snapshot: RunSnapshot;

  constructor(code: string, message: string, snapshot: RunSnapshot) {
    super(message);
    this.name = "DeadboltExecutionError";
    this.code = code;
    this.snapshot = snapshot;
  }
}

export class DeadboltTimeoutError extends Error {
  constructor(message = "Polling timed out waiting for run to complete") {
    super(message);
    this.name = "DeadboltTimeoutError";
  }
}

export interface Run {
  readonly id: string;
  readonly organizationId?: string;
  readonly projectId?: string;
  readonly environmentId?: string;
  readonly workflowName: string;
  readonly deploymentId: string;
  readonly status: RunStatus;
  readonly reasonCode?: string;
  readonly revision: number;
  readonly createdAt: string;
  readonly deadlineAt?: string;
  /** HTTP 202 Accepted denotes persisted acceptance in the control plane, not completion. */
  readonly isAccepted: boolean;
}

export interface RunStep {
  readonly id: string;
  readonly nodeId: string;
  readonly status: StepStatus;
  readonly currentEpoch?: number;
  readonly attempts?: readonly unknown[];
}

export interface RunSnapshot extends Run {
  readonly lastEventSequence: number;
  readonly steps: readonly RunStep[];
  readonly output?: JSONValue;
  readonly error?: ErrorEnvelope;
}

export interface DeadboltClientOptions {
  baseUrl?: string;
  apiKey?: string;
  environment?: string;
  fetch?: typeof fetch;
  pollIntervalMs?: number;
  pollTimeoutMs?: number;
}

export interface CreateRunOptions {
  workflow: string;
  input: JSONValue;
  /** Mandatory idempotency key to prevent duplicate runs per Blueprint §14.2 & §20.1 */
  idempotencyKey: string;
  environment?: string;
  deploymentId?: string;
}

export interface PollOptions {
  intervalMs?: number;
  timeoutMs?: number;
  signal?: AbortSignal;
}

export class DeadboltClient {
  readonly baseUrl: string;
  readonly apiKey?: string;
  readonly defaultEnvironment: string;
  private readonly customFetch: typeof fetch;
  readonly defaultPollIntervalMs: number;
  readonly defaultPollTimeoutMs: number;

  constructor(options: DeadboltClientOptions = {}) {
    const envUrl =
      typeof process !== "undefined" && process.env?.DEADBOLT_API_URL
        ? process.env.DEADBOLT_API_URL
        : undefined;
    this.baseUrl = (
      options.baseUrl ??
      envUrl ??
      "http://localhost:8080"
    ).replace(/\/+$/, "");
    this.apiKey = options.apiKey;
    this.defaultEnvironment = options.environment ?? "development";
    this.customFetch = options.fetch ?? globalThis.fetch.bind(globalThis);
    this.defaultPollIntervalMs = options.pollIntervalMs ?? 500;
    this.defaultPollTimeoutMs = options.pollTimeoutMs ?? 60000;
  }

  private async request<T>(
    method: string,
    path: string,
    headers: Record<string, string> = {},
    body?: unknown,
  ): Promise<{ status: number; data: T }> {
    const url = `${this.baseUrl}${path.startsWith("/") ? path : `/${path}`}`;
    const reqHeaders: Record<string, string> = {
      Accept: "application/json",
      ...headers,
    };

    if (this.apiKey && !reqHeaders.Authorization) {
      reqHeaders.Authorization = `Bearer ${this.apiKey}`;
    }

    let reqBody: string | undefined;
    if (body !== undefined) {
      reqHeaders["Content-Type"] = "application/json";
      reqBody = JSON.stringify(body);
    }

    const response = await this.customFetch(url, {
      method,
      headers: reqHeaders,
      body: reqBody,
    });

    let data: any;
    const text = await response.text();
    if (text) {
      try {
        data = JSON.parse(text);
      } catch {
        data = { code: "INVALID_JSON_RESPONSE", message: text };
      }
    }

    if (!response.ok) {
      const envelope: ErrorEnvelope = {
        code: data?.code ?? `HTTP_${response.status}`,
        message: data?.message ?? response.statusText,
        requestId: data?.requestId,
        details: data?.details,
        retryable: data?.retryable,
      };
      throw new DeadboltApiError(response.status, envelope);
    }

    return { status: response.status, data: data as T };
  }

  readonly runs = {
    /**
     * Creates and enqueues a new workflow run.
     * Enforces Idempotency-Key header.
     * HTTP 202 response represents persisted acceptance in the control plane, not completion.
     */
    create: async (options: CreateRunOptions): Promise<Run> => {
      if (
        !options.idempotencyKey ||
        typeof options.idempotencyKey !== "string" ||
        options.idempotencyKey.trim() === ""
      ) {
        fail("MISSING_IDEMPOTENCY_KEY");
      }

      const workflowName = options.workflow;
      const environment = options.environment ?? this.defaultEnvironment;

      const body: Record<string, unknown> = {
        environment,
        input: options.input,
      };
      if (options.deploymentId) {
        body.deploymentId = options.deploymentId;
      }

      const res = await this.request<Run>(
        "POST",
        `/v1/workflows/${encodeURIComponent(workflowName)}/runs`,
        { "Idempotency-Key": options.idempotencyKey },
        body,
      );

      // HTTP 202 is persisted acceptance per Blueprint §4.3 & §20.1
      return {
        ...res.data,
        isAccepted: res.status === 202,
      };
    },

    /**
     * Retrieves the current snapshot of a run.
     */
    get: async (runId: string): Promise<RunSnapshot> => {
      if (!runId || typeof runId !== "string") {
        fail("INVALID_RUN_ID");
      }
      const res = await this.request<RunSnapshot>(
        "GET",
        `/v1/runs/${encodeURIComponent(runId)}`,
      );
      return {
        ...res.data,
        isAccepted: true,
      };
    },

    /**
     * Gets the current status of a run.
     */
    getStatus: async (
      runId: string,
    ): Promise<{
      id: string;
      status: RunStatus;
      revision: number;
      reasonCode?: string;
    }> => {
      const snapshot = await this.runs.get(runId);
      return {
        id: snapshot.id,
        status: snapshot.status,
        revision: snapshot.revision,
        reasonCode: snapshot.reasonCode,
      };
    },

    /**
     * Polls the run status until completion (SUCCEEDED, FAILED, CANCELLED).
     * Returns the output on SUCCEEDED, or throws DeadboltExecutionError on failure.
     */
    pollResult: async (
      runId: string,
      options: PollOptions = {},
    ): Promise<JSONValue | undefined> => {
      const intervalMs = options.intervalMs ?? this.defaultPollIntervalMs;
      const timeoutMs = options.timeoutMs ?? this.defaultPollTimeoutMs;
      const startTime = Date.now();

      while (true) {
        if (options.signal?.aborted) {
          throw new ContractError("ABORTED", "Polling aborted by signal");
        }

        const snapshot = await this.runs.get(runId);

        if (snapshot.status === "SUCCEEDED") {
          return snapshot.output;
        }

        if (snapshot.status === "FAILED") {
          throw new DeadboltExecutionError(
            "RUN_FAILED",
            snapshot.reasonCode || snapshot.error?.message || "Run failed",
            snapshot,
          );
        }

        if (snapshot.status === "CANCELLED") {
          throw new DeadboltExecutionError(
            "RUN_CANCELLED",
            snapshot.reasonCode || "Run cancelled",
            snapshot,
          );
        }

        if (Date.now() - startTime >= timeoutMs) {
          throw new DeadboltTimeoutError(
            `Timed out after ${timeoutMs}ms waiting for run ${runId} to complete (current status: ${snapshot.status})`,
          );
        }

        await new Promise((resolve) => setTimeout(resolve, intervalMs));
      }
    },
  };

  readonly deployments = {
    /**
     * Registers an immutable deployment manifest in the control plane.
     */
    register: async (
      manifest: ObjectValue,
    ): Promise<{ id: string; status: string }> => {
      const res = await this.request<{ id: string; status: string }>(
        "POST",
        "/v1/deployments",
        {},
        manifest,
      );
      return res.data;
    },
  };

  readonly artifacts = {
    /**
     * Reserves an upload for one attempt: enforces caps/quota, binds live
     * ownership, and returns the artifact id plus a single-object PUT URL.
     */
    create: async (options: {
      runId: string;
      attemptId: string;
      ownershipEpoch: number;
      sizeBytes: number;
      sha256: string;
      idempotencyKey: string;
    }): Promise<{ id: string; uploadUrl: string; expiresAt: string }> => {
      const res = await this.request<{
        id: string;
        uploadUrl: string;
        expiresAt: string;
      }>(
        "POST",
        "/v1/artifacts",
        { "Idempotency-Key": options.idempotencyKey },
        {
          runId: options.runId,
          attemptId: options.attemptId,
          ownershipEpoch: options.ownershipEpoch,
          sizeBytes: options.sizeBytes,
          sha256: options.sha256,
        },
      );
      return res.data;
    },
    /**
     * Finalizes an upload after verifying size and SHA-256 server-side.
     */
    finalize: async (options: {
      id: string;
      attemptId: string;
      ownershipEpoch: number;
      sha256: string;
      idempotencyKey: string;
    }): Promise<{ id: string; status: string }> => {
      const res = await this.request<{ id: string; status: string }>(
        "POST",
        `/v1/artifacts/${encodeURIComponent(options.id)}/finalize`,
        { "Idempotency-Key": options.idempotencyKey },
        {
          attemptId: options.attemptId,
          ownershipEpoch: options.ownershipEpoch,
          sha256: options.sha256,
        },
      );
      return res.data;
    },
    /**
     * Mints a short-lived attachment download URL for a READY artifact.
     */
    downloadUrl: async (
      id: string,
    ): Promise<{ id: string; downloadUrl: string; expiresAt: string }> => {
      const res = await this.request<{
        id: string;
        downloadUrl: string;
        expiresAt: string;
      }>("GET", `/v1/artifacts/${encodeURIComponent(id)}`);
      return res.data;
    },
  };
}
