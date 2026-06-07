package dbmanager

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"net"
	"strings"
	"time"
)

// DBTx defines the interface for database transaction operations.
type DBTx interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	Commit() error
	Rollback() error
}

// DBConnection defines the interface for database connection operations.
type DBConnection interface {
	BeginTx(ctx context.Context, opts *sql.TxOptions) (DBTx, error)
}

// sqlDBWrapper wraps *sql.DB to implement DBConnection.
type sqlDBWrapper struct {
	db *sql.DB
}

func (w *sqlDBWrapper) BeginTx(ctx context.Context, opts *sql.TxOptions) (DBTx, error) {
	return w.db.BeginTx(ctx, opts)
}

// DbManager manages database connections and operations.
type DbManager struct {
	Db DBConnection
}

// NewDbManager creates a new DbManager with a standard *sql.DB.
func NewDbManager(db *sql.DB) *DbManager {
	return &DbManager{Db: &sqlDBWrapper{db: db}}
}

// NewDbManagerWithConn creates a new DbManager with a custom DBConnection (useful for testing).
func NewDbManagerWithConn(db DBConnection) *DbManager {
	return &DbManager{Db: db}
}

// RevokeUser executes revocation statements for a user.
// It ensures transaction safety, proper connection reclamation, and error propagation.
func (m *DbManager) RevokeUser(ctx context.Context, statements []string, username string) error {
	if m.Db == nil {
		return errors.New("database connection is nil")
	}

	var lastErr error
	backoff := 100 * time.Millisecond
	maxBackoff := 2 * time.Second
	maxAttempts := 3

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		err := m.revokeUserAttempt(ctx, statements, username)
		if err == nil {
			return nil
		}

		lastErr = err
		if !isTransient(err) {
			log.Printf("[ERROR] permanent revocation failure for user %s: %v", username, err)
			return err
		}

		log.Printf("[WARN] transient revocation failure for user %s (attempt %d/%d): %v", username, attempt, maxAttempts, err)

		if attempt < maxAttempts {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
				backoff *= 2
				if backoff > maxBackoff {
					backoff = maxBackoff
				}
			}
		}
	}

	return fmt.Errorf("revocation failed after %d attempts: %w", maxAttempts, lastErr)
}

func (m *DbManager) revokeUserAttempt(ctx context.Context, statements []string, username string) error {
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
	}

	tx, err := m.Db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to start transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	for _, stmt := range statements {
		_, err := tx.ExecContext(ctx, stmt, username)
		if err != nil {
			return fmt.Errorf("failed to execute statement %q: %w", stmt, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

func isTransient(err error) bool {
	if err == nil {
		return false
	}
	if _, ok := err.(net.Error); ok {
		return true
	}
	errStr := strings.ToLower(err.Error())
	transientKeywords := []string{
		"timeout",
		"connection refused",
		"connection reset",
		"deadline exceeded",
		"eof",
		"broken pipe",
	}
	for _, kw := range transientKeywords {
		if strings.Contains(errStr, kw) {
			return true
		}
	}
	return false
}
