package contracts

import "strconv"

func ValidateTask(task any) error {
	if err := checkJSON(task, 0); err != nil {
		return err
	}
	if !ValidateAgainst(task, "manifest/task.schema.json") {
		return failure("INVALID_TASK")
	}
	t := obj(task)
	for _, key := range []string{"inputSchema", "outputSchema"} {
		if err := ValidateSchema(t[key]); err != nil {
			return err
		}
	}
	retry := obj(t["retry"])
	if number(retry["initialDelayMs"], 1000) > number(retry["maxDelayMs"], 30000) {
		return failure("INVALID_TASK")
	}
	if t["recovery"] == "idempotent" && number(t["idempotencyWindowMs"], 0) < 5000+number(t["timeoutMs"], 300000) {
		return failure("INVALID_TASK")
	}
	return nil
}
func NormalizeTask(task any) (any, error) {
	if err := checkJSON(task, 0); err != nil {
		return nil, err
	}
	if err := ValidateTask(task); err != nil {
		return nil, err
	}
	out := map[string]any{}
	for k, v := range obj(task) {
		out[k] = v
	}
	out["timeoutMs"] = number(out["timeoutMs"], 300000)
	retry := map[string]any{"maxAttempts": float64(3), "initialDelayMs": float64(1000), "maxDelayMs": float64(30000)}
	for k, v := range obj(out["retry"]) {
		retry[k] = v
	}
	out["retry"] = retry
	return out, nil
}
func guaranteed(schema any, parts []string) bool {
	if len(parts) == 0 {
		return true
	}
	s := obj(schema)
	if variants := arr(s["oneOf"]); variants != nil {
		for _, v := range variants {
			if !guaranteed(v, parts) {
				return false
			}
		}
		return true
	}
	key := parts[0]
	if s["type"] == "object" {
		p := obj(s["properties"])
		value, ok := p[key]
		return ok && contains(arr(s["required"]), key) && guaranteed(value, parts[1:])
	}
	if s["type"] == "array" {
		i, err := strconv.Atoi(key)
		return err == nil && indexPattern.MatchString(key) && float64(i) < number(s["minItems"], 0) && s["items"] != nil && guaranteed(s["items"], parts[1:])
	}
	return false
}
func ValidateWorkflow(manifest any, definitions []any) error {
	if err := checkJSON(manifest, 0); err != nil {
		return err
	}
	m := obj(manifest)
	if m == nil {
		return failure("INVALID_MANIFEST")
	}
	if m["manifestVersion"] != float64(1) {
		return failure("UNSUPPORTED_MANIFEST_VERSION")
	}
	nodes := arr(m["nodes"])
	if len(nodes) == 0 {
		return failure("EMPTY_NODES")
	}
	if len(nodes) > 50 {
		return failure("NODE_COUNT_EXCEEDED")
	}
	if !ValidateAgainst(m, "manifest/workflow.schema.json") {
		return failure("INVALID_MANIFEST")
	}
	for _, key := range []string{"inputSchema", "outputSchema"} {
		if err := ValidateSchema(m[key]); err != nil {
			return err
		}
	}
	tasks := map[string]map[string]any{}
	for _, t := range definitions {
		if err := ValidateTask(t); err != nil {
			return err
		}
		task := obj(t)
		name := str(task["name"])
		if tasks[name] != nil {
			return failure("INVALID_TASK")
		}
		tasks[name] = task
	}
	byID := map[string]map[string]any{}
	for _, v := range nodes {
		n := obj(v)
		id := str(n["id"])
		if byID[id] != nil {
			return failure("DUPLICATE_NODE_ID")
		}
		byID[id] = n
		if n["type"] != "task" {
			return failure("UNSUPPORTED_CAPABILITY")
		}
		if tasks[str(n["task"])] == nil {
			return failure("MISSING_TASK_REF")
		}
	}
	for _, v := range nodes {
		for _, dep := range arr(obj(v)["after"]) {
			d := str(dep)
			if byID[d] == nil {
				return failure("MISSING_DEPENDENCY")
			}
		}
	}
	ancestors := map[string]map[string]bool{}
	visiting := map[string]bool{}
	var visit func(string) (map[string]bool, error)
	visit = func(id string) (map[string]bool, error) {
		if visiting[id] {
			return nil, failure("CYCLE_DETECTED")
		}
		if a := ancestors[id]; a != nil {
			return a, nil
		}
		visiting[id] = true
		set := map[string]bool{}
		for _, dep := range arr(byID[id]["after"]) {
			d := str(dep)
			set[d] = true
			other, err := visit(d)
			if err != nil {
				return nil, err
			}
			for x := range other {
				set[x] = true
			}
		}
		delete(visiting, id)
		ancestors[id] = set
		return set, nil
	}
	for _, v := range nodes {
		if _, err := visit(str(obj(v)["id"])); err != nil {
			return err
		}
	}
	used := map[string]bool{}
	var mapping func(any, map[string]bool, bool) error
	mapping = func(value any, allowed map[string]bool, isOutput bool) error {
		switch v := value.(type) {
		case []any:
			for _, x := range v {
				if err := mapping(x, allowed, isOutput); err != nil {
					return err
				}
			}
		case map[string]any:
			if _, ok := v["literal"]; ok {
				if len(v) != 1 {
					return failure("INPUT_MAPPING_ERROR")
				}
				return nil
			}
			if _, ok := v["$ref"]; ok {
				if err := Reference(v); err != nil {
					return err
				}
				source := m["inputSchema"]
				if v["$ref"] == "step.output" {
					id := str(v["stepId"])
					if !allowed[id] {
						return failure("INPUT_MAPPING_ERROR")
					}
					source = tasks[str(byID[id]["task"])]["outputSchema"]
					if isOutput {
						used[id] = true
					}
				}
				parts, _ := PointerParts(v["pointer"])
				_, hasDefault := v["default"]
				if !guaranteed(source, parts) && !hasDefault {
					return failure("INPUT_MAPPING_ERROR")
				}
				return nil
			}
			for _, x := range v {
				if err := mapping(x, allowed, isOutput); err != nil {
					return err
				}
			}
		}
		return nil
	}
	all := map[string]bool{}
	for _, v := range nodes {
		n := obj(v)
		id := str(n["id"])
		all[id] = true
		if err := mapping(n["input"], ancestors[id], false); err != nil {
			return err
		}
	}
	if err := mapping(m["output"], all, true); err != nil {
		return err
	}
	referencedAsDependency := map[string]bool{}
	for _, v := range nodes {
		for _, dep := range arr(obj(v)["after"]) {
			referencedAsDependency[str(dep)] = true
		}
	}
	for _, v := range nodes {
		n := obj(v)
		id := str(n["id"])
		if !referencedAsDependency[id] && !used[id] && n["sideEffect"] != true {
			return failure("ORPHAN_LEAF")
		}
	}
	return nil
}
func ValidateDeployment(v any) error {
	if err := checkJSON(v, 0); err != nil {
		return err
	}
	if !ValidateAgainst(v, "manifest/deployment.schema.json") {
		return failure("INVALID_MANIFEST")
	}
	m := obj(v)
	names := map[string]bool{}
	for _, w := range arr(m["workflows"]) {
		name := str(obj(w)["name"])
		if names[name] {
			return failure("INVALID_MANIFEST")
		}
		names[name] = true
		if err := ValidateWorkflow(w, arr(m["tasks"])); err != nil {
			return err
		}
	}
	return nil
}
