package migrator

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"sync"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/lock"
)

// MigrationAdvisoryLockID is a constant 64-bit integer used for single-migrator serialization.
// See Blueprint §26.3: "A single migrator uses an advisory lock; app runtime has no DDL permission."
const MigrationAdvisoryLockID int64 = 7142893

// LatestSchemaVersion defines the expected schema version for readiness and health checks.
const LatestSchemaVersion int64 = 14

type Runner struct {
	db            *sql.DB
	migrationsDir string
	lockConn      *sql.Conn
	mu            sync.Mutex
}

func NewRunner(db *sql.DB, migrationsDir string) *Runner {
	return &Runner{
		db:            db,
		migrationsDir: migrationsDir,
	}
}

func (r *Runner) newProvider() (*goose.Provider, error) {
	if _, err := os.Stat(r.migrationsDir); err != nil {
		return nil, fmt.Errorf("migrations directory not found at %s: %w", r.migrationsDir, err)
	}

	sessionLocker, err := lock.NewPostgresSessionLocker(
		lock.WithLockID(MigrationAdvisoryLockID),
		lock.WithLockTimeout(1, 60), // retry every 1 second, up to 60 seconds
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create postgres session locker: %w", err)
	}

	provider, err := goose.NewProvider(
		goose.DialectPostgres,
		r.db,
		os.DirFS(r.migrationsDir),
		goose.WithSessionLocker(sessionLocker),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize goose provider: %w", err)
	}
	return provider, nil
}

// AcquireAdvisoryLock acquires an exclusive session-level advisory lock on PostgreSQL
// pinned to a dedicated physical connection.
func (r *Runner) AcquireAdvisoryLock(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.lockConn != nil {
		return fmt.Errorf("migration advisory lock already held by this runner")
	}

	conn, err := r.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("failed to obtain dedicated connection for advisory lock: %w", err)
	}

	if _, err := conn.ExecContext(ctx, "SELECT pg_advisory_lock($1)", MigrationAdvisoryLockID); err != nil {
		_ = conn.Close()
		return fmt.Errorf("failed to acquire migration advisory lock %d: %w", MigrationAdvisoryLockID, err)
	}

	r.lockConn = conn
	return nil
}

// ReleaseAdvisoryLock releases the exclusive session-level advisory lock on the pinned connection.
func (r *Runner) ReleaseAdvisoryLock(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.lockConn == nil {
		return nil
	}

	defer func() {
		_ = r.lockConn.Close()
		r.lockConn = nil
	}()

	if _, err := r.lockConn.ExecContext(ctx, "SELECT pg_advisory_unlock($1)", MigrationAdvisoryLockID); err != nil {
		return fmt.Errorf("failed to release migration advisory lock %d: %w", MigrationAdvisoryLockID, err)
	}

	return nil
}

// Up runs all pending migrations under the session advisory lock using Goose Provider.
// The session locker guarantees that lock acquisition, migration execution, and release
// are serialized on the exact same physical database session connection.
func (r *Runner) Up(ctx context.Context) error {
	provider, err := r.newProvider()
	if err != nil {
		return err
	}

	if _, err := provider.Up(ctx); err != nil {
		return fmt.Errorf("goose up failed: %w", err)
	}

	return nil
}

// UpTo runs migrations up to a specific version under the session advisory lock.
func (r *Runner) UpTo(ctx context.Context, version int64) error {
	provider, err := r.newProvider()
	if err != nil {
		return err
	}

	if _, err := provider.UpTo(ctx, version); err != nil {
		return fmt.Errorf("goose up-to %d failed: %w", version, err)
	}

	return nil
}

// DownTo rolls back migrations down to a specific version under the session advisory lock.
func (r *Runner) DownTo(ctx context.Context, version int64) error {
	provider, err := r.newProvider()
	if err != nil {
		return err
	}

	if _, err := provider.DownTo(ctx, version); err != nil {
		return fmt.Errorf("goose down-to %d failed: %w", version, err)
	}

	return nil
}

// Version returns the current database migration version.
// Reading the current migration version does not acquire the DDL advisory lock.
func (r *Runner) Version(ctx context.Context) (int64, error) {
	if err := goose.SetDialect("postgres"); err != nil {
		return 0, fmt.Errorf("failed to set goose dialect: %w", err)
	}
	return goose.GetDBVersionContext(ctx, r.db)
}
