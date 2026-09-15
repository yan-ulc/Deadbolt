package integration_test

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/Ryanakml/Deadbolt/internal/outbox"
	"github.com/Ryanakml/Deadbolt/internal/storage"
	"github.com/Ryanakml/Deadbolt/internal/storage/testdb"
	"github.com/Ryanakml/Deadbolt/internal/tenant"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go"
)

// TestOutboxDispatcherRealPostgresAndJetStream verifies the actual boundary:
// a tenant-scoped outbox row is reserved through PostgreSQL, published with a
// stable JetStream message ID, acknowledged, and then marked published.
// It intentionally skips only when the external dependencies are unavailable;
// CI/staging acceptance must run it with both dependencies provisioned.
func TestOutboxDispatcherRealPostgresAndJetStream(t *testing.T) {
	tc := setupTenantContext(t)
	defer tc.cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	ownerID, err := tenant.NewUUID()
	if err != nil {
		t.Fatal(err)
	}
	org, err := tc.service.CreateOrganization(ctx, ownerID, "outbox integration")
	if err != nil {
		t.Fatal(err)
	}

	systemURL := testdb.GetRoleDatabaseURL("deadbolt_system", "deadbolt_integration_test")
	systemPool, err := pgxpool.New(ctx, systemURL)
	if err != nil {
		t.Fatal(err)
	}
	defer systemPool.Close()

	natsURL := os.Getenv("DEADBOLT_NATS_URL")
	if natsURL == "" {
		natsURL = os.Getenv("NATS_URL")
	}
	if natsURL == "" {
		natsURL = "nats://127.0.0.1:4222"
	}
	nc, err := nats.Connect(natsURL)
	if err != nil {
		t.Skipf("NATS integration dependency unavailable: %v", err)
	}
	defer nc.Close()
	js, err := nc.JetStream()
	if err != nil {
		t.Fatal(err)
	}
	if err := outbox.EnsureWakeupStream(js); err != nil {
		t.Fatal(err)
	}

	sub, err := js.SubscribeSync(outbox.DefaultWakeupSubject,
		nats.BindStream(outbox.WakeupStreamName), nats.DeliverNew(), nats.ManualAck())
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()

	eventID, err := tenant.NewUUID()
	if err != nil {
		t.Fatal(err)
	}
	if err := tc.pool.WithTenantTx(ctx, org.ID, func(ctx context.Context, tx storage.Tx) error {
		_, err := tx.Exec(ctx, `INSERT INTO outbox_events (organization_id, event_id, subject, payload, payload_version)
			VALUES ($1::uuid, $2::uuid, 'execution.state_changed', '{}'::jsonb, 1)`, org.ID, eventID)
		return err
	}); err != nil {
		t.Fatal(err)
	}

	dispatcher := outbox.NewDispatcher(
		tc.pool,
		outbox.NewSQLTenantSource(systemPool),
		outbox.NewJetStreamPublisher(js),
		outbox.Config{BatchSize: 8, InitialRetry: 10 * time.Millisecond, MaxRetry: time.Second},
		nil,
	)
	if err := dispatcher.DispatchOnce(ctx); err != nil {
		t.Fatal(err)
	}

	msg, err := sub.NextMsg(5 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := msg.Ack(); err != nil {
		t.Fatal(err)
	}
	var hint outbox.Hint
	if err := json.Unmarshal(msg.Data, &hint); err != nil {
		t.Fatal(err)
	}
	if hint.EventID != eventID || hint.OrganizationID != org.ID || hint.Subject != "execution.state_changed" || hint.PayloadVersion != 1 {
		t.Fatalf("unexpected wake-up hint: %+v", hint)
	}
	if string(msg.Data) == "{}" || len(msg.Data) == 0 {
		t.Fatalf("expected non-empty wake-up hint")
	}
	// Re-publishing the same event ID is the crash-after-publish-before-mark
	// window. JetStream must deduplicate the broker record; database idempotency
	// remains the authority even if a redelivery occurs after consumer failure.
	if err := outbox.NewJetStreamPublisher(js).Publish(ctx, outbox.DefaultWakeupSubject, eventID, msg.Data); err != nil {
		t.Fatal(err)
	}
	if _, err := sub.NextMsg(500 * time.Millisecond); err != nats.ErrTimeout {
		t.Fatalf("duplicate event ID produced an unexpected delivery: %v", err)
	}

	var published bool
	if err := tc.pool.WithTenantTx(ctx, org.ID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `SELECT published_at IS NOT NULL FROM outbox_events WHERE event_id=$1::uuid`, eventID).Scan(&published)
	}); err != nil {
		t.Fatal(err)
	}
	if !published {
		t.Fatal("outbox event was delivered but not marked published")
	}
}
