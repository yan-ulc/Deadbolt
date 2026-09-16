package cli

import (
	"fmt"
)

const CLIVersion = "0.1.0"

// Run dispatches CLI subcommands according to the developer journey architecture
func Run(args []string) error {
	if len(args) < 1 {
		PrintUsage()
		return fmt.Errorf("command required")
	}

	cmd := args[0]
	cmdArgs := args[1:]

	switch cmd {
	case "init":
		return HandleInit(cmdArgs)
	case "dev":
		return RunDev(cmdArgs)
	case "doctor":
		return HandleDoctor(cmdArgs)
	case "build":
		return HandleBuild(cmdArgs)
	case "login":
		return RunLogin(cmdArgs)
	case "deploy":
		return HandleDeploy(cmdArgs)
	case "deployments":
		return HandleDeployments(cmdArgs)
	case "worker", "workers":
		return HandleWorker(cmdArgs)
	case "runs":
		return RunRuns(cmdArgs)
	case "logs":
		return RunLogs(cmdArgs)
	case "version", "-v", "--version":
		fmt.Printf("Deadbolt Runtime CLI v%s\n", CLIVersion)
		return nil
	case "help", "-h", "--help":
		PrintUsage()
		return nil
	default:
		PrintUsage()
		return fmt.Errorf("unknown command %q", cmd)
	}
}

// PrintUsage prints the main help screen for the Deadbolt Runtime CLI
func PrintUsage() {
	fmt.Println(`Deadbolt Runtime CLI

Usage:
  runtime <command> [subcommand] [flags]

The Developer Journey Commands:
  init          Scaffold a clean Deadbolt project and workflow
  dev           Start local development environment (Docker Compose + worker)
  doctor        Inspect environment prerequisites, connectivity, digests, and secrets
  build         Package deterministic task bundle and generate immutable manifest
  login         Authenticate with hosted Deadbolt or local dev control plane
  deploy        Register immutable manifest with control plane
  worker        Manage worker agents (enroll, start, drain, list)
  deployments   Manage deployment activations (activate, list)
  runs          Execute and inspect workflow runs (create, list, inspect)
  logs          Stream task execution logs for a workflow run

Diagnostic & Information:
  version       Display CLI version
  help          Display this help message

Configuration:
  DEADBOLT_API_URL    Control plane base URL (default: http://localhost:8080)
  DEADBOLT_API_KEY    Control plane API Key (or loaded from OS keychain)
  DEADBOLT_ORG_ID     Target organization ID (or loaded from OS keychain)
  DEADBOLT_ENV        Target environment: development, staging, production
  DEADBOLT_PROJECT    Target project ID or name (from deadbolt.config.json)

For detailed help on any command:
  runtime <command> --help`)
}
