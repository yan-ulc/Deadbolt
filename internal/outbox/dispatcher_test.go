package outbox

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestConfigDefaultsAreBounded(t *testing.T) {
	c := (Config{}).withDefaults()
	if c.BatchSize != defaultBatchSize || c.DispatchTick != defaultDispatchTick {
		t.Fatalf("unexpected dispatcher defaults: %+v", c)
	}
	if c.InitialRetry != defaultInitialRetry || c.MaxRetry != defaultMaxRetry {
		t.Fatalf("unexpected retry defaults: %+v", c)
	}
	if c.Subject != DefaultWakeupSubject {
		t.Fatalf("unexpected wake-up subject: %q", c.Subject)
	}

	c = (Config{BatchSize: -1, InitialRetry: 5 * time.Second, MaxRetry: time.Second}).withDefaults()
	if c.MaxRetry != c.InitialRetry {
		t.Fatalf("max retry must not be below initial retry: %+v", c)
	}
}

func TestRetryDelayUsesExponentialBackoffAndCap(t *testing.T) {
	d := NewDispatcher(nil, nil, nil, Config{
		InitialRetry: 100 * time.Millisecond,
		MaxRetry:     500 * time.Millisecond,
	}, nil)
	checks := []struct {
		attempt int
		want    time.Duration
	}{
		{1, 100 * time.Millisecond},
		{2, 200 * time.Millisecond},
		{3, 400 * time.Millisecond},
		{4, 500 * time.Millisecond},
	}
	for _, check := range checks {
		if got := d.retryDelay(check.attempt); got != check.want {
			t.Fatalf("attempt %d: got %s, want %s", check.attempt, got, check.want)
		}
	}
}

func TestWakeupHintDoesNotContainOriginalPayload(t *testing.T) {
	hint, err := EncodeHint(Hint{
		EventID:        "event-1",
		OrganizationID: "org-1",
		Subject:        "execution.state_changed",
		PayloadVersion: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(hint), "secret") || strings.Contains(string(hint), "output") {
		t.Fatalf("wake-up hint contains forbidden payload content: %s", hint)
	}
	var decoded map[string]any
	if err := json.Unmarshal(hint, &decoded); err != nil {
		t.Fatal(err)
	}
	if _, ok := decoded["payload"]; ok {
		t.Fatal("wake-up hint must not contain the original payload")
	}
}
