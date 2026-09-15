package outbox

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
)

const (
	WakeupStreamName   = "DEADBOLT_RUNTIME"
	WakeupConsumerName = "deadbolt-control-plane"
	WakeupRetentionAge = 24 * time.Hour
	WakeupDuplicateAge = 24 * time.Hour
)

// JetStreamPublisher waits for the JetStream publish acknowledgement. A nil
// acknowledgement is never treated as success by the dispatcher because the
// underlying Publish call returns only after the server confirms persistence.
type JetStreamPublisher struct {
	js nats.JetStreamContext
}

func NewJetStreamPublisher(js nats.JetStreamContext) *JetStreamPublisher {
	return &JetStreamPublisher{js: js}
}

func (p *JetStreamPublisher) Publish(ctx context.Context, subject, eventID string, payload []byte) error {
	if p == nil || p.js == nil {
		return errors.New("nats publisher is not configured")
	}
	if _, err := p.js.Publish(subject, payload, nats.Context(ctx), nats.MsgId(eventID)); err != nil {
		return fmt.Errorf("publish JetStream wake-up hint: %w", err)
	}
	return nil
}

// EnsureWakeupStream creates or updates the internal stream used by the
// dispatcher. It deliberately accepts only the versioned wake-up subject.
func EnsureWakeupStream(js nats.JetStreamContext) error {
	if js == nil {
		return errors.New("cannot configure JetStream on a nil context")
	}
	config := &nats.StreamConfig{
		Name:       WakeupStreamName,
		Subjects:   []string{DefaultWakeupSubject},
		Storage:    nats.FileStorage,
		Retention:  nats.LimitsPolicy,
		MaxAge:     WakeupRetentionAge,
		Duplicates: WakeupDuplicateAge,
		Discard:    nats.DiscardOld,
	}
	info, err := js.StreamInfo(WakeupStreamName)
	if errors.Is(err, nats.ErrStreamNotFound) {
		if _, err := js.AddStream(config); err != nil {
			return fmt.Errorf("create JetStream wake-up stream: %w", err)
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect JetStream wake-up stream: %w", err)
	}
	if info.Config.Storage != config.Storage || info.Config.Retention != config.Retention || info.Config.MaxAge != config.MaxAge || info.Config.Duplicates != config.Duplicates || !contains(info.Config.Subjects, DefaultWakeupSubject) {
		updated := info.Config
		updated.Subjects = appendUnique(updated.Subjects, DefaultWakeupSubject)
		updated.Storage = config.Storage
		updated.Retention = config.Retention
		updated.MaxAge = config.MaxAge
		updated.Duplicates = config.Duplicates
		if _, err := js.UpdateStream(&updated); err != nil {
			return fmt.Errorf("update JetStream wake-up stream: %w", err)
		}
	}
	return nil
}

// WakeupSubscriber consumes hints inside the control plane. It never claims
// task ownership; the callback only triggers an authoritative DB scan.
type WakeupSubscriber struct {
	js       nats.JetStreamContext
	onWakeup func()
	sub      *nats.Subscription
}

func NewWakeupSubscriber(js nats.JetStreamContext, onWakeup func()) *WakeupSubscriber {
	return &WakeupSubscriber{js: js, onWakeup: onWakeup}
}

func (s *WakeupSubscriber) Start() error {
	if s == nil || s.js == nil || s.onWakeup == nil {
		return errors.New("wakeup subscriber is not configured")
	}
	if s.sub != nil {
		return errors.New("wakeup subscriber already started")
	}
	sub, err := s.js.QueueSubscribe(DefaultWakeupSubject, WakeupConsumerName, func(msg *nats.Msg) {
		s.onWakeup()
		if err := msg.Ack(); err != nil {
			// The message may be redelivered. The callback is intentionally
			// idempotent because it only schedules a DB scan.
			return
		}
	}, nats.Durable(WakeupConsumerName), nats.ManualAck(), nats.DeliverAll(), nats.MaxAckPending(1))
	if err != nil {
		return fmt.Errorf("subscribe JetStream wake-up hints: %w", err)
	}
	s.sub = sub
	return nil
}

func (s *WakeupSubscriber) Drain(ctx context.Context) error {
	if s == nil || s.sub == nil {
		return nil
	}
	if err := s.sub.Drain(); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}

func EncodeHint(h Hint) ([]byte, error) {
	return json.Marshal(h)
}

func contains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func appendUnique(values []string, want string) []string {
	if contains(values, want) {
		return values
	}
	return append(values, want)
}
