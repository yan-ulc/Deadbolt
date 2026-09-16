package cli

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// RunLogs handles "runtime logs <run_id> [--step <step_id>] [--attempt <attempt_id>] [--cursor <cursor>] [--limit <limit>] [--json]"
func RunLogs(args []string) error {
	fs := flag.NewFlagSet("logs", flag.ContinueOnError)
	stepFlag := fs.String("step", "", "Filter by Step ID")
	attemptFlag := fs.String("attempt", "", "Filter by Attempt ID")
	cursorFlag := fs.String("cursor", "", "Keyset cursor for pagination")
	limitFlag := fs.Int("limit", 50, "Limit log records")
	cpURLFlag := fs.String("control-plane-url", "", "Control plane base URL")
	jsonFlag := fs.Bool("json", false, "Output as JSON")

	var flagArgs []string
	var runID string
	for i := 0; i < len(args); i++ {
		if strings.HasPrefix(args[i], "-") {
			flagArgs = append(flagArgs, args[i])
			if !strings.Contains(args[i], "=") && i+1 < len(args) && !strings.HasPrefix(args[i+1], "-") {
				if args[i] != "-json" && args[i] != "--json" {
					i++
					flagArgs = append(flagArgs, args[i])
				}
			}
		} else if runID == "" {
			runID = args[i]
		}
	}

	if err := fs.Parse(flagArgs); err != nil {
		return err
	}

	if runID == "" && fs.NArg() > 0 {
		runID = fs.Arg(0)
	}
	if runID == "" {
		fmt.Println("Usage: runtime logs <run_id> [--step <step_id>] [--attempt <attempt_id>] [--cursor <cursor>] [--limit <limit>] [--json]")
		return fmt.Errorf("run ID is required")
	}

	cfg := LoadConfig()
	if *cpURLFlag != "" {
		cfg.APIURL = strings.TrimRight(*cpURLFlag, "/")
	}

	q := url.Values{}
	if *stepFlag != "" {
		q.Set("stepId", *stepFlag)
	}
	if *attemptFlag != "" {
		q.Set("attemptId", *attemptFlag)
	}
	if *cursorFlag != "" {
		q.Set("cursor", *cursorFlag)
	}
	if *limitFlag > 0 {
		q.Set("limit", fmt.Sprintf("%d", *limitFlag))
	}

	apiPath := fmt.Sprintf("/v1/runs/%s/logs?%s", url.PathEscape(runID), q.Encode())
	req, err := cfg.NewRequest("GET", apiPath, nil)
	if err != nil {
		return fmt.Errorf("failed to construct request: %w", err)
	}

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to get logs: %s", FormatAPIError(resp.StatusCode, body))
	}

	if *jsonFlag {
		fmt.Println(string(body))
		return nil
	}

	var result struct {
		Items []struct {
			ID        string `json:"id"`
			Sequence  int64  `json:"sequence"`
			Timestamp string `json:"timestamp"`
			Level     string `json:"level"`
			Message   string `json:"message"`
		} `json:"items"`
		NextCursor *string `json:"nextCursor"`
		Expired    bool    `json:"expired"`
		Message    *string `json:"message"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return fmt.Errorf("failed to parse log response: %w", err)
	}

	if result.Expired {
		msg := "Logs expired due to 7-day retention policy"
		if result.Message != nil {
			msg = *result.Message
		}
		fmt.Printf("Notice: %s\n", msg)
		return nil
	}

	if len(result.Items) == 0 {
		fmt.Println("No logs recorded for this run.")
		return nil
	}

	for _, item := range result.Items {
		ts, _ := time.Parse(time.RFC3339Nano, item.Timestamp)
		fmt.Printf("[%s] [%-5s] #%d %s\n", ts.Format("15:04:05.000"), strings.ToUpper(item.Level), item.Sequence, item.Message)
	}

	if result.NextCursor != nil && *result.NextCursor != "" {
		fmt.Printf("\nNext cursor: %s (use --cursor %s to view next page)\n", *result.NextCursor, *result.NextCursor)
	}

	return nil
}
