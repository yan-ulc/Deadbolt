package gateway

import "testing"

func TestTCPNATSCheckerNormalizesURL(t *testing.T) {
	checker := NewTCPNATSChecker("nats://broker.internal:4222")
	if checker.addr != "broker.internal:4222" {
		t.Fatalf("unexpected normalized NATS address: %q", checker.addr)
	}

	checker = NewTCPNATSChecker("127.0.0.1:4222")
	if checker.addr != "127.0.0.1:4222" {
		t.Fatalf("raw host:port must remain unchanged: %q", checker.addr)
	}
}
