-- +goose Up

-- Dispatcher diagnostics. The event remains retryable while published_at is NULL.
ALTER TABLE outbox_events
    ADD COLUMN IF NOT EXISTS last_error TEXT;

-- Keep pending dispatch scans bounded by due time and stable creation order.
CREATE INDEX IF NOT EXISTS idx_outbox_events_pending_due
    ON outbox_events (next_at, created_at, id)
    WHERE published_at IS NULL;

-- +goose Down
DROP INDEX IF EXISTS idx_outbox_events_pending_due;
ALTER TABLE outbox_events DROP COLUMN IF EXISTS last_error;
