package cli

import (
	"archive/tar"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/Ryanakml/Deadbolt/internal/contracts"
)

type BuildOptions struct {
	ProjectDir   string
	WorkflowPath string
	BundleDir    string
	OutputDir    string
	TargetArch   string
	TargetOS     string
}

type BuildResult struct {
	BundleDigest         string   `json:"bundleDigest"`
	DependencyLockDigest string   `json:"dependencyLockDigest"`
	TargetArchitecture   string   `json:"targetArchitecture"`
	TargetOS             string   `json:"targetOS"`
	BundlePath           string   `json:"bundlePath"`
	ManifestPath         string   `json:"manifestPath"`
	SecretNames          []string `json:"secretNames"`
	Tasks                []string `json:"tasks"`
	Workflows            []string `json:"workflows"`
}

// HandleBuild compiles local tasks, creates deterministic .tar bundle, and produces validated deployment manifest
func HandleBuild(args []string) error {
	fs := flag.NewFlagSet("runtime build", flag.ContinueOnError)
	dirFlag := fs.String("dir", ".", "Project root directory")
	workflowFlag := fs.String("workflow", "", "Path to workflow definition JSON")
	bundleDirFlag := fs.String("bundle-dir", "", "Directory to store compiled bundle .tar files")
	outDirFlag := fs.String("out-dir", "", "Directory to write output manifest.json")
	archFlag := fs.String("arch", "amd64", "Target architecture (amd64 or arm64)")
	osFlag := fs.String("os", "linux", "Target operating system (linux)")
	jsonFlag := fs.Bool("json", false, "Output build result as JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}

	opts := BuildOptions{
		ProjectDir:   *dirFlag,
		WorkflowPath: *workflowFlag,
		BundleDir:    *bundleDirFlag,
		OutputDir:    *outDirFlag,
		TargetArch:   *archFlag,
		TargetOS:     *osFlag,
	}

	result, err := BuildDeployment(opts)
	if err != nil {
		return fmt.Errorf("build failed: %w", err)
	}

	if *jsonFlag {
		b, _ := json.MarshalIndent(result, "", "  ")
		fmt.Println(string(b))
	} else {
		fmt.Println("Immutable Deployment Bundle Built Successfully!")
		fmt.Printf("Bundle Digest (SHA-256):         %s\n", result.BundleDigest)
		fmt.Printf("Dependency Lock Digest (SHA-256): %s\n", result.DependencyLockDigest)
		fmt.Printf("Target Architecture:             %s\n", result.TargetArchitecture)
		fmt.Printf("Target OS:                       %s\n", result.TargetOS)
		fmt.Printf("Bundle Artifact:                 %s\n", result.BundlePath)
		fmt.Printf("Deployment Manifest:             %s\n", result.ManifestPath)
		fmt.Printf("Registered Workflows:            %s\n", strings.Join(result.Workflows, ", "))
		fmt.Printf("Registered Tasks:                %s\n", strings.Join(result.Tasks, ", "))
		fmt.Println("\nTo register this deployment on the control plane, run:")
		fmt.Printf("  runtime deploy --env staging --manifest %s\n", result.ManifestPath)
	}

	return nil
}

// BuildDeployment assembles the deterministic bundle and validated deployment manifest
func BuildDeployment(opts BuildOptions) (*BuildResult, error) {
	if opts.ProjectDir == "" {
		opts.ProjectDir = "."
	}
	if opts.TargetArch == "" {
		opts.TargetArch = "amd64"
	}
	if opts.TargetOS == "" {
		opts.TargetOS = "linux"
	}
	if opts.BundleDir == "" {
		opts.BundleDir = filepath.Join(opts.ProjectDir, "bundles")
	}
	if opts.OutputDir == "" {
		opts.OutputDir = filepath.Join(opts.ProjectDir, "dist")
	}

	if err := os.MkdirAll(opts.BundleDir, 0o755); err != nil {
		return nil, fmt.Errorf("create bundle dir: %w", err)
	}
	if err := os.MkdirAll(opts.OutputDir, 0o755); err != nil {
		return nil, fmt.Errorf("create output dir: %w", err)
	}

	// 1. Read deadbolt.config.json if present
	var secretNames []string
	if prj, err := LoadProjectConfig(opts.ProjectDir); err == nil && prj != nil {
		if opts.WorkflowPath == "" && prj.Workflow != "" {
			opts.WorkflowPath = filepath.Join(opts.ProjectDir, "workflow.json")
		}
		if prj.TargetArch != "" {
			opts.TargetArch = prj.TargetArch
		}
		secretNames = append(secretNames, prj.Secrets...)
	}

	// 2. Discover task files in tasks/
	tasksDir := filepath.Join(opts.ProjectDir, "tasks")
	taskFiles := make(map[string][]byte)

	if info, err := os.Stat(tasksDir); err == nil && info.IsDir() {
		entries, err := os.ReadDir(tasksDir)
		if err != nil {
			return nil, fmt.Errorf("read tasks dir: %w", err)
		}
		for _, e := range entries {
			if !e.IsDir() && (strings.HasSuffix(e.Name(), ".js") || strings.HasSuffix(e.Name(), ".mjs") || strings.HasSuffix(e.Name(), ".ts")) {
				p := filepath.Join(tasksDir, e.Name())
				data, err := os.ReadFile(p)
				if err != nil {
					return nil, fmt.Errorf("read task %s: %w", e.Name(), err)
				}
				taskFiles["tasks/"+e.Name()] = data
			}
		}
	}

	// Also check if src/tasks exists
	srcTasksDir := filepath.Join(opts.ProjectDir, "src", "tasks")
	if info, err := os.Stat(srcTasksDir); err == nil && info.IsDir() {
		entries, _ := os.ReadDir(srcTasksDir)
		for _, e := range entries {
			if !e.IsDir() && (strings.HasSuffix(e.Name(), ".js") || strings.HasSuffix(e.Name(), ".mjs")) {
				data, _ := os.ReadFile(filepath.Join(srcTasksDir, e.Name()))
				taskFiles["tasks/"+e.Name()] = data
			}
		}
	}

	// If no task files found, check root for sample task
	if len(taskFiles) == 0 {
		sampleCode := []byte("export default async function task(input, ctx) { return { status: 'completed' }; }\n")
		taskFiles["tasks/task-simple.js"] = sampleCode
	}

	// 3. Build deterministic tarball: sorted file keys, fixed modtime
	var tarBuf bytes.Buffer
	tw := tar.NewWriter(&tarBuf)

	sortedPaths := make([]string, 0, len(taskFiles))
	for p := range taskFiles {
		sortedPaths = append(sortedPaths, p)
	}
	sort.Strings(sortedPaths)

	// Fixed epoch timestamp for deterministic reproducible builds
	deterministicTime := time.Unix(1700000000, 0).UTC()

	for _, relPath := range sortedPaths {
		content := taskFiles[relPath]
		hdr := &tar.Header{
			Name:     relPath,
			Mode:     0o644,
			Size:     int64(len(content)),
			ModTime:  deterministicTime,
			Typeflag: tar.TypeReg,
		}
		if err := tw.WriteHeader(hdr); err != nil {
			return nil, fmt.Errorf("write tar header: %w", err)
		}
		if _, err := tw.Write(content); err != nil {
			return nil, fmt.Errorf("write tar content: %w", err)
		}
	}
	if err := tw.Close(); err != nil {
		return nil, fmt.Errorf("close tar archive: %w", err)
	}

	tarBytes := tarBuf.Bytes()
	bundleSHA := sha256.Sum256(tarBytes)
	bundleDigest := hex.EncodeToString(bundleSHA[:])

	// Write bundle to <bundleDir>/<bundleDigest>.tar
	bundlePath := filepath.Join(opts.BundleDir, bundleDigest+".tar")
	if err := os.WriteFile(bundlePath, tarBytes, 0o644); err != nil {
		return nil, fmt.Errorf("write bundle archive: %w", err)
	}

	// 4. Calculate dependencyLockDigest
	lockDigest := calculateLockDigest(opts.ProjectDir)

	// 5. Load or generate workflow definition
	var workflows []any
	var tasks []any

	if opts.WorkflowPath == "" {
		candidates := []string{
			filepath.Join(opts.ProjectDir, "workflow.json"),
			filepath.Join(opts.ProjectDir, "dist", "workflow.json"),
		}
		for _, c := range candidates {
			if fileExists(c) {
				opts.WorkflowPath = c
				break
			}
		}
	}

	if opts.WorkflowPath != "" && fileExists(opts.WorkflowPath) {
		wfData, err := os.ReadFile(opts.WorkflowPath)
		if err != nil {
			return nil, fmt.Errorf("read workflow %s: %w", opts.WorkflowPath, err)
		}
		var parsedWF map[string]any
		if err := json.Unmarshal(wfData, &parsedWF); err != nil {
			return nil, fmt.Errorf("parse workflow: %w", err)
		}
		workflows = append(workflows, parsedWF)
	}

	// Fallback to default workflow if not provided
	if len(workflows) == 0 {
		workflows = append(workflows, map[string]any{
			"manifestVersion": 1,
			"name":            "default-workflow",
			"inputSchema": map[string]any{
				"type":                 "object",
				"properties":           map[string]any{"val": map[string]any{"type": "string"}},
				"required":             []string{"val"},
				"additionalProperties": false,
			},
			"outputSchema": map[string]any{
				"type":                 "object",
				"properties":           map[string]any{"val": map[string]any{"type": "string"}},
				"required":             []string{"val"},
				"additionalProperties": false,
			},
			"nodes": []map[string]any{
				{
					"id":    "step-1",
					"type":  "task",
					"task":  "task-simple",
					"after": []string{},
					"input": map[string]any{
						"val": map[string]any{"$ref": "run.input", "pointer": "/val"},
					},
				},
			},
			"output": map[string]any{
				"val": map[string]any{"$ref": "step.output", "stepId": "step-1", "pointer": "/val"},
			},
		})
	}

	// Check if tasks.json exists in project dir
	tasksPath := filepath.Join(opts.ProjectDir, "tasks.json")
	if fileExists(tasksPath) {
		tData, err := os.ReadFile(tasksPath)
		if err == nil {
			var parsedTasks []any
			if err := json.Unmarshal(tData, &parsedTasks); err == nil {
				tasks = append(tasks, parsedTasks...)
			}
		}
	}

	// 6. If tasks.json was not present, build task definitions corresponding to workflow nodes
	if len(tasks) == 0 {
		taskNamesMap := make(map[string]bool)
		stepToTask := make(map[string]string)
		taskOutputs := make(map[string]map[string]bool)
		taskInputs := make(map[string]map[string]bool)

		for _, wfItem := range workflows {
			if wfMap, ok := wfItem.(map[string]any); ok {
				if nodes, ok := wfMap["nodes"].([]any); ok {
					for _, n := range nodes {
						if nMap, ok := n.(map[string]any); ok {
							sID, _ := nMap["id"].(string)
							tName, _ := nMap["task"].(string)
							if tName != "" {
								taskNamesMap[tName] = true
								if sID != "" {
									stepToTask[sID] = tName
								}
								if _, ok := taskOutputs[tName]; !ok {
									taskOutputs[tName] = make(map[string]bool)
								}
								if _, ok := taskInputs[tName]; !ok {
									taskInputs[tName] = make(map[string]bool)
								}
							}
						}
					}

					// Collect input references across nodes
					for _, n := range nodes {
						if nMap, ok := n.(map[string]any); ok {
							tName, _ := nMap["task"].(string)
							if inMap, ok := nMap["input"].(map[string]any); ok {
								for inKey, inVal := range inMap {
									if tName != "" {
										taskInputs[tName][inKey] = true
									}
									if refMap, ok := inVal.(map[string]any); ok {
										if refMap["$ref"] == "step.output" {
											srcStep, _ := refMap["stepId"].(string)
											ptr, _ := refMap["pointer"].(string)
											prop := strings.TrimPrefix(ptr, "/")
											if srcTask := stepToTask[srcStep]; srcTask != "" && prop != "" {
												taskOutputs[srcTask][prop] = true
											}
										}
									}
								}
							}
						}
					}
				}

				// Collect references from workflow output
				if outMap, ok := wfMap["output"].(map[string]any); ok {
					for _, outVal := range outMap {
						if refMap, ok := outVal.(map[string]any); ok {
							if refMap["$ref"] == "step.output" {
								srcStep, _ := refMap["stepId"].(string)
								ptr, _ := refMap["pointer"].(string)
								prop := strings.TrimPrefix(ptr, "/")
								if srcTask := stepToTask[srcStep]; srcTask != "" && prop != "" {
									taskOutputs[srcTask][prop] = true
								}
							}
						}
					}
				}
			}
		}

		for tName := range taskNamesMap {
			entrypoint := fmt.Sprintf("tasks/%s.js", tName)
			for k := range taskFiles {
				if strings.TrimPrefix(k, "tasks/") == tName+".js" || strings.TrimPrefix(k, "tasks/") == tName+".mjs" {
					entrypoint = k
					break
				}
			}

			recovery := "safe"
			var window any
			if strings.Contains(tName, "email") || strings.Contains(tName, "notify") || strings.Contains(tName, "idempotent") {
				recovery = "idempotent"
				window = float64(60000)
			}

			// Build properties & required for outputSchema
			outProps := make(map[string]any)
			outReq := make([]string, 0)
			for prop := range taskOutputs[tName] {
				outProps[prop] = map[string]any{"type": "string"}
				outReq = append(outReq, prop)
			}

			// Build properties & required for inputSchema
			inProps := make(map[string]any)
			inReq := make([]string, 0)
			for prop := range taskInputs[tName] {
				inProps[prop] = map[string]any{"type": "string"}
				inReq = append(inReq, prop)
			}

			taskObj := map[string]any{
				"name":       tName,
				"entrypoint": entrypoint,
				"recovery":   recovery,
				"timeoutMs":  float64(30000),
				"inputSchema": map[string]any{
					"type":                 "object",
					"properties":           inProps,
					"required":             inReq,
					"additionalProperties": true,
				},
				"outputSchema": map[string]any{
					"type":                 "object",
					"properties":           outProps,
					"required":             outReq,
					"additionalProperties": true,
				},
			}
			if recovery == "idempotent" {
				taskObj["idempotencyWindowMs"] = window
			}
			tasks = append(tasks, taskObj)
		}
	}

	// Deduplicate secret names
	uniqueSecrets := make([]string, 0)
	secMap := make(map[string]bool)
	for _, s := range secretNames {
		s = strings.TrimSpace(s)
		if s != "" && !secMap[s] {
			secMap[s] = true
			uniqueSecrets = append(uniqueSecrets, s)
		}
	}

	// 7. Assemble deployment manifest
	manifest := map[string]any{
		"manifestVersion":      1,
		"sdkVersion":           "0.1.0",
		"protocolMajor":        1,
		"nodeRuntimeMajor":     24,
		"targetOS":             opts.TargetOS,
		"targetArchitecture":   opts.TargetArch,
		"dependencyLockDigest": lockDigest,
		"bundleDigest":         bundleDigest,
		"secretNames":          uniqueSecrets,
		"tasks":                tasks,
		"workflows":            workflows,
	}

	// 8. Marshal and validate against canonical deployment.schema.json
	manifestJSON, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("marshal manifest: %w", err)
	}

	canonicalParsed, err := contracts.ParseJSON(manifestJSON)
	if err != nil {
		return nil, fmt.Errorf("canonical parse error: %w", err)
	}

	if err := contracts.ValidateDeployment(canonicalParsed); err != nil {
		return nil, fmt.Errorf("contract validation error: %w", err)
	}

	// 9. Write output manifests
	distManifestPath := filepath.Join(opts.OutputDir, "manifest.json")
	if err := os.WriteFile(distManifestPath, manifestJSON, 0o644); err != nil {
		return nil, fmt.Errorf("write dist manifest: %w", err)
	}

	bundleManifestPath := filepath.Join(opts.BundleDir, bundleDigest+"-manifest.json")
	if err := os.WriteFile(bundleManifestPath, manifestJSON, 0o644); err != nil {
		return nil, fmt.Errorf("write bundle manifest: %w", err)
	}

	var taskNames []string
	for _, t := range tasks {
		if tm, ok := t.(map[string]any); ok {
			taskNames = append(taskNames, tm["name"].(string))
		}
	}
	var wfNames []string
	for _, w := range workflows {
		if wm, ok := w.(map[string]any); ok {
			wfNames = append(wfNames, wm["name"].(string))
		}
	}

	return &BuildResult{
		BundleDigest:         bundleDigest,
		DependencyLockDigest: lockDigest,
		TargetArchitecture:   opts.TargetArch,
		TargetOS:             opts.TargetOS,
		BundlePath:           bundlePath,
		ManifestPath:         distManifestPath,
		SecretNames:          uniqueSecrets,
		Tasks:                taskNames,
		Workflows:            wfNames,
	}, nil
}

func calculateLockDigest(projectDir string) string {
	candidates := []string{
		filepath.Join(projectDir, "pnpm-lock.yaml"),
		filepath.Join(projectDir, "package-lock.json"),
		filepath.Join(projectDir, "package.json"),
	}

	for _, c := range candidates {
		if data, err := os.ReadFile(c); err == nil && len(data) > 0 {
			h := sha256.Sum256(data)
			return hex.EncodeToString(h[:])
		}
	}

	// Deterministic fallback hash if no lockfile is present
	fallback := sha256.Sum256([]byte("deadbolt-default-lock-v1\n"))
	return hex.EncodeToString(fallback[:])
}
