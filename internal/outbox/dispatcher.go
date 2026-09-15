// Package outbox delivers transactional outbox intents as non-authoritative
// wake-up hints. PostgreSQL remains the source of truth for ownership/state.
package outbox

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Ryanakml/Deadbolt/internal/storage"
	"github.com/jackc/pgx/v5/pgxpool"
)

const DefaultWakeupSubject = "runtime.v1.wakeup"

const (
	defaultBatchSize    = 32
	defaultDispatchTick = time.Second
	defaultInitialRetry = time.Second
	defaultMaxRetry     = time.Minute
	maxErrorLength      = 2048
)

// Publisher is deliberately smaller than a broker client. Publish must return
// only after the broker has acknowledged the message.
type Publisher interface {
	Publish(ctx context.Context, subject, eventID string, payload []byte) error
}

// TenantSource lists organization IDs without exposing tenant tables to the
// system role. The dispatcher re-enters the runtime pool with each tenant
// context before reading or updating outbox rows.
type TenantSource interface {
	ListTenantIDs(ctx context.Context) ([]string, error)
}

type SQLTenantSource struct {
	pool *pgxpool.Pool
}

func NewSQLTenantSource(pool *pgxpool.Pool) *SQLTenantSource {
	return &SQLTenantSource{pool: pool}
}

func (s *SQLTenantSource) ListTenantIDs(ctx context.Context) ([]string, error) {
	if s == nil || s.pool == nil {
		return nil, errors.New("outbox tenant source: nil system pool")
	}
	rows, err := s.pool.Query(ctx, `SELECT organization_id::text FROM app.enumerate_scheduler_tenants()`)
	if err != nil {
		return nil, fmt.Errorf("list scheduler tenants: %w", err)
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan scheduler tenant: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate scheduler tenants: %w", err)
	}
	return ids, nil
}

type Config struct {
	BatchSize    int
	DispatchTick time.Duration
	InitialRetry time.Duration
	MaxRetry     time.Duration
	Subject      string
}

func (c Config) withDefaults() Config {
	if c.BatchSize <= 0 {
		c.BatchSize = defaultBatchSize
	}
	if c.DispatchTick <= 0 {
		c.DispatchTick = defaultDispatchTick
	}
	if c.InitialRetry <= 0 {
		c.InitialRetry = defaultInitialRetry
	}
	if c.MaxRetry <= 0 {
		c.MaxRetry = defaultMaxRetry
	}
	if c.MaxRetry < c.InitialRetry {
		c.MaxRetry = c.InitialRetry
	}
	if strings.TrimSpace(c.Subject) == "" {
		c.Subject = DefaultWakeupSubject
	}
	return c
}

type Metrics struct {
	Batches          atomic.Int64
	EventsReserved   atomic.Int64
	EventsPublished  atomic.Int64
	PublishFailures  atomic.Int64
	ReservationError atomic.Int64
}

type Event struct {
	ID             string
	EventID        string
	OrganizationID string
	SourceSubject  string
	PayloadVersion int
	Attempts       int
}

// Hint is intentionally small. The original outbox payload is not forwarded
// to NATS; a wake-up hint must not carry secrets or task outputs.
type Hint struct {
	EventID        string `json:"eventId"`
	OrganizationID string `json:"organizationId"`
	Subject        string `json:"subject"`
	PayloadVersion int    `json:"payloadVersion"`
}

type Dispatcher struct {
	runtimePool  *storage.Pool
	tenantSource TenantSource
	publisher    Publisher
	config       Config
	logger       *log.Logger
	metrics      *Metrics
}

func NewDispatcher(runtimePool *storage.Pool, tenantSource TenantSource, publisher Publisher, config Config, logger *log.Logger) *Dispatcher {
	if logger == nil {
		logger = log.Default()
	}
	config = config.withDefaults()
	return &Dispatcher{
		runtimePool:  runtimePool,
		tenantSource: tenantSource,
		publisher:    publisher,
		config:       config,
		logger:       logger,
		metrics:      &Metrics{},
	}
}

func (d *Dispatcher) Metrics() *Metrics { return d.metrics }

func (d *Dispatcher) Run(ctx context.Context) error {
	if d.runtimePool == nil || d.tenantSource == nil || d.publisher == nil {
		return errors.New("outbox dispatcher: runtime pool, tenant source, and publisher are required")
	}

	if err := d.DispatchOnce(ctx); err != nil {
		d.logger.Printf("[OUTBOX] initial dispatch failed: %v", err)
	}
	ticker := time.NewTicker(d.config.DispatchTick)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := d.DispatchOnce(ctx); err != nil {
				d.logger.Printf("[OUTBOX] dispatch failed: %v", err)
			}
		}
	}
}

func (d *Dispatcher) DispatchOnce(ctx context.Context) error {
	if d.runtimePool == nil || d.tenantSource == nil || d.publisher == nil {
		return errors.New("outbox dispatcher is not configured")
	}
	tenantIDs, err := d.tenantSource.ListTenantIDs(ctx)
	if err != nil {
		return err
	}
	var firstErr error
	for _, organizationID := range tenantIDs {
		if err := d.dispatchTenant(ctx, organizationID); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	d.metrics.Batches.Add(1)
	return firstErr
}

func (d *Dispatcher) dispatchTenant(ctx context.Context, organizationID string) error {
	events, err := d.reserveBatch(ctx, organizationID)
	if err != nil {
		d.metrics.ReservationError.Add(1)
		return err
	}
	d.metrics.EventsReserved.Add(int64(len(events)))
	var firstErr error
	for _, event := range events {
		hint, err := json.Marshal(Hint{
			EventID:        event.EventID,
			OrganizationID: event.OrganizationID,
			Subject:        event.SourceSubject,
			PayloadVersion: event.PayloadVersion,
		})
		if err == nil {
			err = d.publisher.Publish(ctx, d.config.Subject, event.EventID, hint)
		}
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			d.metrics.PublishFailures.Add(1)
			if updateErr := d.recordFailure(ctx, organizationID, event.ID, err); updateErr != nil {
				d.logger.Printf("[OUTBOX] failed to record publish error for %s: %v", event.EventID, updateErr)
			}
			continue
		}
		if err := d.markPublished(ctx, organizationID, event.ID); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			d.logger.Printf("[OUTBOX] publish ACK received but mark failed for %s: %v", event.EventID, err)
			if updateErr := d.recordFailure(ctx, organizationID, event.ID, err); updateErr != nil {
				d.logger.Printf("[OUTBOX] failed to record mark error for %s: %v", event.EventID, updateErr)
			}
			continue
		}
		d.metrics.EventsPublished.Add(1)
	}
	return firstErr
}

func (d *Dispatcher) reserveBatch(ctx context.Context, organizationID string) ([]Event, error) {
	var events []Event
	err := d.runtimePool.WithTenantTx(ctx, organizationID, func(ctx context.Context, tx storage.Tx) error {
		rows, err := tx.Query(ctx, `
			SELECT id::text, event_id::text, organization_id::text, subject, payload_version, attempts
			FROM outbox_events
			WHERE published_at IS NULL AND next_at <= clock_timestamp()
			ORDER BY next_at, created_at, id
			FOR UPDATE SKIP LOCKED
			LIMIT $1`, d.config.BatchSize)
		if err != nil {
			return fmt.Errorf("reserve outbox rows: %w", err)
		}
		defer rows.Close()

		for rows.Next() {
			var event Event
			if err := rows.Scan(&event.ID, &event.EventID, &event.OrganizationID, &event.SourceSubject, &event.PayloadVersion, &event.Attempts); err != nil {
				return fmt.Errorf("scan outbox row: %w", err)
			}
			events = append(events, event)
		}
		if err := rows.Err(); err != nil {
			return fmt.Errorf("iterate outbox rows: %w", err)
		}

		for i := range events {
			delay := d.retryDelay(events[i].Attempts + 1)
			if _, err := tx.Exec(ctx, `
				UPDATE outbox_events
				SET attempts = attempts + 1,
				    next_at = clock_timestamp() + $2::interval,
				    last_error = NULL
				WHERE id = $1::uuid AND published_at IS NULL`, events[i].ID, durationInterval(delay)); err != nil {
				return fmt.Errorf("reserve outbox row %s: %w", events[i].EventID, err)
			}
		}
		return nil
	})
	return events, err
}

func (d *Dispatcher) markPublished(ctx context.Context, organizationID, id string) error {
	return d.runtimePool.WithTenantTx(ctx, organizationID, func(ctx context.Context, tx storage.Tx) error {
		var updated int
		if err := tx.QueryRow(ctx, `
			UPDATE outbox_events
			SET published_at = clock_timestamp(), last_error = NULL
			WHERE id = $1::uuid AND published_at IS NULL
			RETURNING 1`, id).Scan(&updated); err != nil {
			return fmt.Errorf("mark outbox event published: %w", err)
		}
		if updated != 1 {
			return errors.New("mark outbox event published: row was already published")
		}
		return nil
	})
}

func (d *Dispatcher) recordFailure(ctx context.Context, organizationID, id string, publishErr error) error {
	message := publishErr.Error()
	if len(message) > maxErrorLength {
		message = message[:maxErrorLength]
	}
	return d.runtimePool.WithTenantTx(ctx, organizationID, func(ctx context.Context, tx storage.Tx) error {
		_, err := tx.Exec(ctx, `
			UPDATE outbox_events
			SET last_error = $2
			WHERE id = $1::uuid AND published_at IS NULL`, id, message)
		return err
	})
}

func (d *Dispatcher) retryDelay(attempt int) time.Duration {
	if attempt <= 1 {
		return d.config.InitialRetry
	}
	power := math.Pow(2, float64(attempt-1))
	delay := time.Duration(float64(d.config.InitialRetry) * power)
	if delay < 0 || delay > d.config.MaxRetry {
		return d.config.MaxRetry
	}
	return delay
}

func durationInterval(d time.Duration) string {
	return fmt.Sprintf("%d microseconds", d.Microseconds())
}
