import { assertJSON, ContractError, fail, type JSONValue } from "./json.js";
import { schemas } from "./schema-data.js";
import {
  object,
  validateAgainst,
  validateSchema,
  type ObjectValue,
} from "./schema.js";
import { pointerParts, reference } from "./mapping.js";
export interface ValidationError {
  code: string;
  message: string;
  path?: string;
}
export interface ValidationResult {
  valid: boolean;
  errors: ValidationError[];
}
const check = (fn: () => void): ValidationResult => {
  try {
    fn();
    return { valid: true, errors: [] };
  } catch (e) {
    if (e instanceof ContractError)
      return { valid: false, errors: [{ code: e.code, message: e.message }] };
    throw e;
  }
};
export function validateTask(task: JSONValue): void {
  assertJSON(task);
  if (!validateAgainst(task, schemas["task.schema.json"])) fail("INVALID_TASK");
  const t = object(task);
  validateSchema(t.inputSchema, schemas["payload-schema.schema.json"]);
  validateSchema(t.outputSchema, schemas["payload-schema.schema.json"]);
  const retry = object(t.retry ?? {});
  if (Number(retry.initialDelayMs ?? 1000) > Number(retry.maxDelayMs ?? 30000))
    fail("INVALID_TASK");
  if (
    t.recovery === "idempotent" &&
    Number(t.idempotencyWindowMs) < 5000 + Number(t.timeoutMs ?? 300000)
  )
    fail("INVALID_TASK");
}
export function normalizeTask(task: JSONValue): ObjectValue {
  assertJSON(task);
  validateTask(task);
  const t = object(task);
  return {
    ...t,
    timeoutMs: t.timeoutMs ?? 300000,
    retry: {
      maxAttempts: 3,
      initialDelayMs: 1000,
      maxDelayMs: 30000,
      ...object(t.retry ?? {}),
    },
  };
}
function guaranteed(schema: JSONValue, parts: string[]): boolean {
  if (!parts.length) return true;
  const s = object(schema);
  if (s.oneOf)
    return (s.oneOf as JSONValue[]).every((x) => guaranteed(x, parts));
  const [key, ...rest] = parts;
  if (s.type === "object") {
    const p = object(s.properties ?? {});
    return (
      Array.isArray(s.required) &&
      s.required.includes(key) &&
      Object.hasOwn(p, key) &&
      guaranteed(p[key], rest)
    );
  }
  if (s.type === "array")
    return (
      /^(0|[1-9][0-9]*)$/.test(key) &&
      key === String(Number(key)) &&
      Number(key) < Number(s.minItems ?? 0) &&
      !!s.items &&
      guaranteed(s.items, rest)
    );
  return false;
}
function workflow(manifest: JSONValue, definitions: JSONValue[]): void {
  const m = object(manifest);
  if (m.manifestVersion !== 1) fail("UNSUPPORTED_MANIFEST_VERSION");
  if (!Array.isArray(m.nodes) || m.nodes.length === 0) fail("EMPTY_NODES");
  if (m.nodes.length > 50) fail("NODE_COUNT_EXCEEDED");
  if (!validateAgainst(m, schemas["workflow.schema.json"]))
    fail("INVALID_MANIFEST");
  validateSchema(m.inputSchema, schemas["payload-schema.schema.json"]);
  validateSchema(m.outputSchema, schemas["payload-schema.schema.json"]);
  const tasks = new Map<string, ObjectValue>();
  for (const t of definitions) {
    validateTask(t);
    const task = object(t);
    if (tasks.has(String(task.name))) fail("INVALID_TASK");
    tasks.set(String(task.name), task);
  }
  const nodes = m.nodes as ObjectValue[],
    byId = new Map<string, ObjectValue>();
  for (const n of nodes) {
    const id = String(n.id);
    if (byId.has(id)) fail("DUPLICATE_NODE_ID");
    byId.set(id, n);
    if (n.type !== "task") fail("UNSUPPORTED_CAPABILITY");
    if (!tasks.has(String(n.task))) fail("MISSING_TASK_REF");
  }
  for (const n of nodes)
    for (const d of (n.after ?? []) as string[]) {
      if (!byId.has(d)) fail("MISSING_DEPENDENCY");
    }
  const ancestors = new Map<string, Set<string>>(),
    visiting = new Set<string>();
  const visit = (id: string): Set<string> => {
    if (visiting.has(id)) fail("CYCLE_DETECTED");
    if (ancestors.has(id)) return ancestors.get(id)!;
    visiting.add(id);
    const set = new Set<string>();
    for (const d of (byId.get(id)!.after ?? []) as string[]) {
      set.add(d);
      for (const x of visit(d)) set.add(x);
    }
    visiting.delete(id);
    ancestors.set(id, set);
    return set;
  };
  nodes.forEach((n) => visit(String(n.id)));
  const used = new Set<string>();
  const mapping = (
    v: JSONValue,
    allowed: Set<string>,
    isOutput = false,
  ): void => {
    if (v === null || typeof v !== "object") return;
    if (Array.isArray(v)) {
      v.forEach((x) => mapping(x, allowed, isOutput));
      return;
    }
    if (Object.hasOwn(v, "literal")) {
      if (Object.keys(v).length !== 1) fail("INPUT_MAPPING_ERROR");
      return;
    }
    if (Object.hasOwn(v, "$ref")) {
      reference(v);
      let source = m.inputSchema;
      if (v.$ref === "step.output") {
        const id = String(v.stepId);
        if (!allowed.has(id)) fail("INPUT_MAPPING_ERROR");
        source = tasks.get(String(byId.get(id)!.task))!.outputSchema;
        if (isOutput) used.add(id);
      }
      if (
        !guaranteed(source, pointerParts(v.pointer)) &&
        !Object.hasOwn(v, "default")
      )
        fail("INPUT_MAPPING_ERROR");
      return;
    }
    Object.values(v).forEach((x) => mapping(x, allowed, isOutput));
  };
  nodes.forEach((n) => mapping(n.input ?? {}, ancestors.get(String(n.id))!));
  mapping(m.output, new Set(byId.keys()), true);
  const referencedAsDependency = new Set(
    nodes.flatMap((n) => (n.after ?? []) as string[]),
  );
  for (const n of nodes)
    if (
      !referencedAsDependency.has(String(n.id)) &&
      !used.has(String(n.id)) &&
      n.sideEffect !== true
    )
      fail("ORPHAN_LEAF");
}
export function validateWorkflowManifest(
  manifest: unknown,
  tasks: unknown = [],
): ValidationResult {
  return check(() => {
    assertJSON(manifest);
    assertJSON(tasks);
    if (!Array.isArray(tasks)) fail("INVALID_TASK");
    workflow(manifest, tasks);
  });
}
export function validateDeployment(value: unknown): ValidationResult {
  return check(() => {
    assertJSON(value);
    if (!validateAgainst(value, schemas["deployment.schema.json"]))
      fail("INVALID_MANIFEST");
    const m = object(value),
      names = new Set<string>();
    for (const w of m.workflows as JSONValue[]) {
      const name = String(object(w).name);
      if (names.has(name)) fail("INVALID_MANIFEST");
      names.add(name);
      workflow(w, m.tasks as JSONValue[]);
    }
  });
}
