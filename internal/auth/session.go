package auth

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrSessionNotFound    = errors.New("session not found")
	ErrSessionRevoked     = errors.New("session has been revoked")
	ErrSessionExpired     = errors.New("session absolute expiration reached")
	ErrSessionIdleTimeout = errors.New("session idle timeout exceeded")
	ErrInvalidCSRFToken   = errors.New("invalid CSRF token")
)

// User represents a registered platform user
type User struct {
	ID        string    `json:"id"`
	Email     string    `json:"email"`
	Name      string    `json:"name"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// Session represents an authenticated human user session
type Session struct {
	ID                   string     `json:"id"`
	SessionTokenHash     string     `json:"-"`
	UserID               string     `json:"user_id"`
	ActiveOrganizationID *string    `json:"active_organization_id,omitempty"`
	CSRFTokenHash        string     `json:"-"`
	IdleExpiresAt        time.Time  `json:"idle_expires_at"`
	AbsoluteExpiresAt    time.Time  `json:"absolute_expires_at"`
	RevokedAt            *time.Time `json:"revoked_at,omitempty"`
	RevocationReason     *string    `json:"revocation_reason,omitempty"`
	IPAddress            string     `json:"ip_address,omitempty"`
	UserAgent            string     `json:"user_agent,omitempty"`
	LastSeenAt           time.Time  `json:"last_seen_at"`
	CreatedAt            time.Time  `json:"created_at"`
}

// HashToken computes hex-encoded SHA-256 hash of a raw token
func HashToken(rawToken string) string {
	h := sha256.Sum256([]byte(rawToken))
	return hex.EncodeToString(h[:])
}

// GenerateRandomToken generates a 256-bit cryptographically secure random token (hex-encoded)
func GenerateRandomToken() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("failed to generate crypto token: %w", err)
	}
	return hex.EncodeToString(b), nil
}

// GenerateCSRFToken generates a 256-bit cryptographically secure random CSRF token
func GenerateCSRFToken() (string, error) {
	return GenerateRandomToken()
}

// SessionStore handles database persistence for authentication and sessions
type SessionStore struct {
	pool *pgxpool.Pool
}

// NewSessionStore creates a new SessionStore
func NewSessionStore(pool *pgxpool.Pool) *SessionStore {
	return &SessionStore{pool: pool}
}

// GetOrCreateUserFromOIDC upserts user and oidc_identities from verified OIDC identity
func (s *SessionStore) GetOrCreateUserFromOIDC(ctx context.Context, identity *Identity) (*User, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	claimsJSON, err := json.Marshal(identity.RawClaims)
	if err != nil {
		claimsJSON = []byte("{}")
	}

	// 1. Check if oidc_identity exists
	var userID string
	queryErr := tx.QueryRow(ctx, `
		SELECT user_id FROM oidc_identities
		WHERE issuer = $1 AND subject = $2
	`, identity.Issuer, identity.Subject).Scan(&userID)

	if queryErr == nil {
		// Existing identity: update email & raw claims
		_, err = tx.Exec(ctx, `
			UPDATE oidc_identities
			SET email = $1, raw_claims = $2, updated_at = clock_timestamp()
			WHERE issuer = $3 AND subject = $4
		`, identity.Email, claimsJSON, identity.Issuer, identity.Subject)
		if err != nil {
			return nil, fmt.Errorf("failed to update oidc identity: %w", err)
		}

		// Update user name and email if available
		_, err = tx.Exec(ctx, `
			UPDATE users
			SET email = COALESCE(NULLIF($1, ''), email),
			    name = COALESCE(NULLIF($2, ''), name),
			    updated_at = clock_timestamp()
			WHERE id = $3
		`, identity.Email, identity.Name, userID)
		if err != nil {
			return nil, fmt.Errorf("failed to update user: %w", err)
		}
	} else if errors.Is(queryErr, pgx.ErrNoRows) {
		// New user: insert into users
		err = tx.QueryRow(ctx, `
			INSERT INTO users (email, name)
			VALUES ($1, $2)
			RETURNING id
		`, identity.Email, identity.Name).Scan(&userID)
		if err != nil {
			return nil, fmt.Errorf("failed to insert new user: %w", err)
		}

		// Insert into oidc_identities
		_, err = tx.Exec(ctx, `
			INSERT INTO oidc_identities (user_id, issuer, subject, email, raw_claims)
			VALUES ($1, $2, $3, $4, $5)
		`, userID, identity.Issuer, identity.Subject, identity.Email, claimsJSON)
		if err != nil {
			return nil, fmt.Errorf("failed to insert oidc identity: %w", err)
		}
	} else {
		return nil, fmt.Errorf("failed to query oidc identity: %w", queryErr)
	}

	// Fetch full user record
	var u User
	err = tx.QueryRow(ctx, `
		SELECT id, COALESCE(email, ''), COALESCE(name, ''), created_at, updated_at
		FROM users
		WHERE id = $1
	`, userID).Scan(&u.ID, &u.Email, &u.Name, &u.CreatedAt, &u.UpdatedAt)
	if err != nil {
		return nil, fmt.Errorf("failed to load user record: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("failed to commit user upsert: %w", err)
	}

	return &u, nil
}

// CreateSession generates a new session and CSRF token, saving hashes in auth_sessions
func (s *SessionStore) CreateSession(
	ctx context.Context,
	userID string,
	activeOrgID *string,
	ipAddress, userAgent string,
	idleTimeout, absoluteTimeout time.Duration,
) (*Session, string, string, error) {
	rawSessionToken, err := GenerateRandomToken()
	if err != nil {
		return nil, "", "", err
	}
	sessionTokenHash := HashToken(rawSessionToken)

	rawCSRFToken, err := GenerateRandomToken()
	if err != nil {
		return nil, "", "", err
	}
	csrfTokenHash := HashToken(rawCSRFToken)

	now := time.Now()
	idleExpiresAt := now.Add(idleTimeout)
	absoluteExpiresAt := now.Add(absoluteTimeout)

	var sess Session
	err = s.pool.QueryRow(ctx, `
		INSERT INTO auth_sessions (
			session_token_hash, user_id, active_organization_id, csrf_token_hash,
			idle_expires_at, absolute_expires_at, ip_address, user_agent,
			last_seen_at, created_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $9)
		RETURNING id, session_token_hash, user_id, active_organization_id, csrf_token_hash,
		          idle_expires_at, absolute_expires_at, revoked_at, revocation_reason,
		          ip_address, user_agent, last_seen_at, created_at
	`, sessionTokenHash, userID, activeOrgID, csrfTokenHash,
		idleExpiresAt, absoluteExpiresAt, ipAddress, userAgent, now,
	).Scan(
		&sess.ID, &sess.SessionTokenHash, &sess.UserID, &sess.ActiveOrganizationID, &sess.CSRFTokenHash,
		&sess.IdleExpiresAt, &sess.AbsoluteExpiresAt, &sess.RevokedAt, &sess.RevocationReason,
		&sess.IPAddress, &sess.UserAgent, &sess.LastSeenAt, &sess.CreatedAt,
	)
	if err != nil {
		return nil, "", "", fmt.Errorf("failed to create session: %w", err)
	}

	return &sess, rawSessionToken, rawCSRFToken, nil
}

// CreateCLISession creates a distinct non-cookie human credential. Only the
// unprefixed random token hash is persisted; the dbcli_ namespace prevents a
// dashboard cookie session from ever being accepted as a Bearer credential.
func (s *SessionStore) CreateCLISession(ctx context.Context, userID string, activeOrgID *string, ipAddress, userAgent string, idleTimeout, absoluteTimeout time.Duration) (*Session, string, error) {
	sess, raw, _, err := s.CreateSession(ctx, userID, activeOrgID, ipAddress, userAgent, idleTimeout, absoluteTimeout)
	if err != nil {
		return nil, "", err
	}
	return sess, "dbcli_" + raw, nil
}

func (s *SessionStore) ValidateCLISession(ctx context.Context, token string, idleTimeout time.Duration) (*Session, error) {
	if !strings.HasPrefix(token, "dbcli_") {
		return nil, ErrSessionNotFound
	}
	return s.ValidateSession(ctx, strings.TrimPrefix(token, "dbcli_"), idleTimeout)
}

// ValidateSession validates the raw session token against DB revocation and idle/absolute expiry
func (s *SessionStore) ValidateSession(ctx context.Context, rawSessionToken string, idleTimeout time.Duration) (*Session, error) {
	if rawSessionToken == "" {
		return nil, ErrSessionNotFound
	}
	sessionTokenHash := HashToken(rawSessionToken)

	var sess Session
	err := s.pool.QueryRow(ctx, `
		SELECT id, session_token_hash, user_id, active_organization_id, csrf_token_hash,
		       idle_expires_at, absolute_expires_at, revoked_at, revocation_reason,
		       ip_address, user_agent, last_seen_at, created_at
		FROM auth_sessions
		WHERE session_token_hash = $1
	`, sessionTokenHash).Scan(
		&sess.ID, &sess.SessionTokenHash, &sess.UserID, &sess.ActiveOrganizationID, &sess.CSRFTokenHash,
		&sess.IdleExpiresAt, &sess.AbsoluteExpiresAt, &sess.RevokedAt, &sess.RevocationReason,
		&sess.IPAddress, &sess.UserAgent, &sess.LastSeenAt, &sess.CreatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrSessionNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("failed to query session: %w", err)
	}

	// 1. Check DB revocation
	if sess.RevokedAt != nil {
		return nil, ErrSessionRevoked
	}

	now := time.Now()

	// 2. Check absolute expiry (7 days)
	if now.After(sess.AbsoluteExpiresAt) {
		return nil, ErrSessionExpired
	}

	// 3. Check idle expiry (12 hours)
	if now.After(sess.IdleExpiresAt) {
		return nil, ErrSessionIdleTimeout
	}

	// 4. Update last_seen_at and extend idle_expires_at
	newIdle := now.Add(idleTimeout)
	_, _ = s.pool.Exec(ctx, `
		UPDATE auth_sessions
		SET last_seen_at = $1, idle_expires_at = $2
		WHERE id = $3
	`, now, newIdle, sess.ID)
	sess.LastSeenAt = now
	sess.IdleExpiresAt = newIdle

	return &sess, nil
}

// RotateSession atomically rotates session ID and tokens, revoking the previous session
func (s *SessionStore) RotateSession(
	ctx context.Context,
	oldSessionID string,
	userID string,
	activeOrgID *string,
	ipAddress, userAgent string,
	idleTimeout, absoluteTimeout time.Duration,
) (*Session, string, string, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, "", "", fmt.Errorf("failed to begin rotation tx: %w", err)
	}
	defer tx.Rollback(ctx)

	// Revoke old session
	if oldSessionID != "" {
		_, err = tx.Exec(ctx, `
			UPDATE auth_sessions
			SET revoked_at = clock_timestamp(), revocation_reason = 'ROTATED'
			WHERE id = $1 AND revoked_at IS NULL
		`, oldSessionID)
		if err != nil {
			return nil, "", "", fmt.Errorf("failed to revoke rotated session: %w", err)
		}
	}

	rawSessionToken, err := GenerateRandomToken()
	if err != nil {
		return nil, "", "", err
	}
	sessionTokenHash := HashToken(rawSessionToken)

	rawCSRFToken, err := GenerateRandomToken()
	if err != nil {
		return nil, "", "", err
	}
	csrfTokenHash := HashToken(rawCSRFToken)

	now := time.Now()
	idleExpiresAt := now.Add(idleTimeout)
	absoluteExpiresAt := now.Add(absoluteTimeout)

	var sess Session
	err = tx.QueryRow(ctx, `
		INSERT INTO auth_sessions (
			session_token_hash, user_id, active_organization_id, csrf_token_hash,
			idle_expires_at, absolute_expires_at, ip_address, user_agent,
			last_seen_at, created_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $9)
		RETURNING id, session_token_hash, user_id, active_organization_id, csrf_token_hash,
		          idle_expires_at, absolute_expires_at, revoked_at, revocation_reason,
		          ip_address, user_agent, last_seen_at, created_at
	`, sessionTokenHash, userID, activeOrgID, csrfTokenHash,
		idleExpiresAt, absoluteExpiresAt, ipAddress, userAgent, now,
	).Scan(
		&sess.ID, &sess.SessionTokenHash, &sess.UserID, &sess.ActiveOrganizationID, &sess.CSRFTokenHash,
		&sess.IdleExpiresAt, &sess.AbsoluteExpiresAt, &sess.RevokedAt, &sess.RevocationReason,
		&sess.IPAddress, &sess.UserAgent, &sess.LastSeenAt, &sess.CreatedAt,
	)
	if err != nil {
		return nil, "", "", fmt.Errorf("failed to create rotated session: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, "", "", fmt.Errorf("failed to commit session rotation: %w", err)
	}

	return &sess, rawSessionToken, rawCSRFToken, nil
}

// RevokeSession revokes a session by ID in the database
func (s *SessionStore) RevokeSession(ctx context.Context, sessionID, reason string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE auth_sessions
		SET revoked_at = clock_timestamp(), revocation_reason = $1
		WHERE id = $2 AND revoked_at IS NULL
	`, reason, sessionID)
	if err != nil {
		return fmt.Errorf("failed to revoke session %s: %w", sessionID, err)
	}
	return nil
}

// ValidateCSRFToken checks whether the provided CSRF token matches the session hash
func ValidateCSRFToken(session *Session, rawCSRFToken string) bool {
	if session == nil || rawCSRFToken == "" {
		return false
	}
	computedHash := HashToken(rawCSRFToken)
	return subtle.ConstantTimeCompare([]byte(computedHash), []byte(session.CSRFTokenHash)) == 1
}
