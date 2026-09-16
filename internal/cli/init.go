package cli

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
)

// HandleInit scaffolds a clean project ready for local or hosted execution
func HandleInit(args []string) error {
	fs := flag.NewFlagSet("runtime init", flag.ContinueOnError)
	dirFlag := fs.String("dir", "", "Target directory (default .)")
	forceFlag := fs.Bool("force", false, "Overwrite existing files")
	nameFlag := fs.String("name", "onboarding-pipeline", "Project name")
	if err := fs.Parse(args); err != nil {
		return err
	}

	targetDir := "."
	if *dirFlag != "" {
		targetDir = *dirFlag
	} else if fs.NArg() > 0 {
		targetDir = fs.Arg(0)
	}

	return InitProject(targetDir, *nameFlag, *forceFlag)
}

// InitProject generates the standard project scaffold
func InitProject(targetDir, projectName string, force bool) error {
	if err := os.MkdirAll(targetDir, 0o755); err != nil {
		return fmt.Errorf("create directory: %w", err)
	}

	tasksDir := filepath.Join(targetDir, "tasks")
	bundlesDir := filepath.Join(targetDir, "bundles")
	distDir := filepath.Join(targetDir, "dist")
	for _, d := range []string{tasksDir, bundlesDir, distDir} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			return fmt.Errorf("create directory %s: %w", d, err)
		}
	}

	files := map[string]string{
		"deadbolt.config.json": mustFormatJSON(map[string]any{
			"project":   projectName,
			"workflow":  "customer-onboarding",
			"bundleDir": "./bundles",
			"secrets":   []string{"NOTIFICATION_API_KEY"},
		}),
		"package.json": mustFormatJSON(map[string]any{
			"name":        projectName,
			"version":     "0.1.0",
			"private":     true,
			"type":        "module",
			"description": "Deadbolt durable workflow pipeline",
		}),
		"workflow.json": mustFormatJSON(map[string]any{
			"manifestVersion": 1,
			"name":            "customer-onboarding",
			"inputSchema": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"email": map[string]any{"type": "string"},
					"name":  map[string]any{"type": "string"},
				},
				"required":             []string{"email", "name"},
				"additionalProperties": false,
			},
			"outputSchema": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"accountId":  map[string]any{"type": "string"},
					"deliveryId": map[string]any{"type": "string"},
				},
				"required":             []string{"accountId", "deliveryId"},
				"additionalProperties": false,
			},
			"nodes": []map[string]any{
				{
					"id":    "validate",
					"type":  "task",
					"task":  "validate-signup",
					"after": []string{},
					"input": map[string]any{
						"email": map[string]any{"$ref": "run.input", "pointer": "/email"},
						"name":  map[string]any{"$ref": "run.input", "pointer": "/name"},
					},
				},
				{
					"id":    "provision",
					"type":  "task",
					"task":  "provision-account",
					"after": []string{"validate"},
					"input": map[string]any{
						"userId": map[string]any{"$ref": "step.output", "stepId": "validate", "pointer": "/userId"},
						"email":  map[string]any{"$ref": "step.output", "stepId": "validate", "pointer": "/email"},
					},
				},
				{
					"id":    "notify",
					"type":  "task",
					"task":  "send-welcome-email",
					"after": []string{"provision"},
					"input": map[string]any{
						"accountId": map[string]any{"$ref": "step.output", "stepId": "provision", "pointer": "/accountId"},
						"email":     map[string]any{"$ref": "step.output", "stepId": "validate", "pointer": "/email"},
					},
				},
			},
			"output": map[string]any{
				"accountId":  map[string]any{"$ref": "step.output", "stepId": "provision", "pointer": "/accountId"},
				"deliveryId": map[string]any{"$ref": "step.output", "stepId": "notify", "pointer": "/deliveryId"},
			},
		}),
		"tasks.json": mustFormatJSON([]map[string]any{
			{
				"name":       "validate-signup",
				"entrypoint": "tasks/validate.js",
				"recovery":   "safe",
				"timeoutMs":  30000,
				"inputSchema": map[string]any{
					"type": "object",
					"properties": map[string]any{
						"email": map[string]any{"type": "string"},
						"name":  map[string]any{"type": "string"},
					},
					"required":             []string{"email", "name"},
					"additionalProperties": true,
				},
				"outputSchema": map[string]any{
					"type": "object",
					"properties": map[string]any{
						"userId": map[string]any{"type": "string"},
						"email":  map[string]any{"type": "string"},
						"name":   map[string]any{"type": "string"},
					},
					"required":             []string{"userId", "email"},
					"additionalProperties": true,
				},
			},
			{
				"name":       "provision-account",
				"entrypoint": "tasks/provision.js",
				"recovery":   "safe",
				"timeoutMs":  30000,
				"inputSchema": map[string]any{
					"type": "object",
					"properties": map[string]any{
						"userId": map[string]any{"type": "string"},
						"email":  map[string]any{"type": "string"},
					},
					"required":             []string{"userId", "email"},
					"additionalProperties": true,
				},
				"outputSchema": map[string]any{
					"type": "object",
					"properties": map[string]any{
						"accountId": map[string]any{"type": "string"},
						"status":    map[string]any{"type": "string"},
					},
					"required":             []string{"accountId"},
					"additionalProperties": true,
				},
			},
			{
				"name":                "send-welcome-email",
				"entrypoint":          "tasks/notify.js",
				"recovery":            "idempotent",
				"idempotencyWindowMs": 60000,
				"timeoutMs":           30000,
				"inputSchema": map[string]any{
					"type": "object",
					"properties": map[string]any{
						"accountId": map[string]any{"type": "string"},
						"email":     map[string]any{"type": "string"},
					},
					"required":             []string{"accountId", "email"},
					"additionalProperties": true,
				},
				"outputSchema": map[string]any{
					"type": "object",
					"properties": map[string]any{
						"deliveryId": map[string]any{"type": "string"},
						"delivered":  map[string]any{"type": "boolean"},
					},
					"required":             []string{"deliveryId"},
					"additionalProperties": true,
				},
			},
		}),
		filepath.Join("tasks", "validate.js"): `export default async function task(input, ctx) {
  if (ctx && ctx.logger) {
    ctx.logger.info("Validating signup request", { email: input.email });
  }
  return {
    userId: "usr_" + (ctx ? ctx.stepId : "test"),
    email: (input.email || "").toLowerCase().trim(),
    name: (input.name || "").trim()
  };
}
`,
		filepath.Join("tasks", "provision.js"): `export default async function task(input, ctx) {
  if (ctx && ctx.logger) {
    ctx.logger.info("Provisioning customer account", { userId: input.userId });
  }
  return {
    accountId: "acc_" + input.userId,
    status: "ACTIVE"
  };
}
`,
		filepath.Join("tasks", "notify.js"): `export default async function task(input, ctx) {
  const opId = ctx ? ctx.operationId : "op_sample";
  if (ctx && ctx.logger) {
    ctx.logger.info("Dispatching welcome email with stable operationId", {
      operationId: opId,
      email: input.email
    });
  }
  return {
    deliveryId: "del_" + opId,
    delivered: true
  };
}
`,
		".env.example": `# Non-secret placeholder names (never commit actual secret values)
NOTIFICATION_API_KEY=
DATABASE_URL=
`,
		".gitignore": `node_modules/
dist/
bundles/
.env
*.key
*.identity
.deadbolt/
`,
	}

	for relPath, content := range files {
		fullPath := filepath.Join(targetDir, relPath)
		if !force {
			if _, err := os.Stat(fullPath); err == nil {
				return fmt.Errorf("file %s already exists (use --force to overwrite)", fullPath)
			}
		}
		if err := os.WriteFile(fullPath, []byte(content), 0o644); err != nil {
			return fmt.Errorf("write %s: %w", fullPath, err)
		}
	}

	fmt.Printf("Scaffolded new Deadbolt project in %s\n", targetDir)
	fmt.Println("Created files:")
	fmt.Println("  - deadbolt.config.json (non-secret project configuration)")
	fmt.Println("  - workflow.json        (declarative DAG workflow: validate -> provision -> notify)")
	fmt.Println("  - tasks/               (task handlers with explicit recovery policies)")
	fmt.Println("  - .env.example         (placeholder secret names)")
	fmt.Println("  - .gitignore           (protects secrets and artifacts)")
	fmt.Println("\nNext steps:")
	fmt.Println("  1. Run `runtime doctor` to verify system prerequisites.")
	fmt.Println("  2. Run `runtime dev` to start the local stack and execute locally.")
	fmt.Println("  3. Run `runtime build` to compile the immutable bundle and manifest.")

	return nil
}

func mustFormatJSON(v any) string {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		panic(err)
	}
	return string(b) + "\n"
}

func fileExists(p string) bool {
	_, err := os.Stat(p)
	return !errors.Is(err, os.ErrNotExist)
}
