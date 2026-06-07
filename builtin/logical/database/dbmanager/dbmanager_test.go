package dbmanager

import (
	"context"
	"database/sql"
	"errors"
	"testing"
)

type MockDBConnection struct {
	BeginTxFunc func(ctx context.Context, opts *sql.TxOptions) (DBTx, error)
}

func (m *MockDBConnection) BeginTx(ctx context.Context, opts *sql.TxOptions) (DBTx, error) {
	if m.BeginTxFunc != nil {
		return m.BeginTxFunc(ctx, opts)
	}
	return nil, errors.New("not implemented")
}

type MockDBTx struct {
	ExecContextFunc func(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	CommitFunc      func() error
	RollbackFunc    func() error
	RollbackCalled  bool
}

func (m *MockDBTx) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	if m.ExecContextFunc != nil {
		return m.ExecContextFunc(ctx, query, args...)
	}
	return nil, nil
}

func (m *MockDBTx) Commit() error {
	if m.CommitFunc != nil {
		return m.CommitFunc()
	}
	return nil
}

func (m *MockDBTx) Rollback() error {
	m.RollbackCalled = true
	if m.RollbackFunc != nil {
		return m.RollbackFunc()
	}
	return nil
}

func TestRevokeUser_ConnectionFailure(t *testing.T) {
	mockConn := &MockDBConnection{
		BeginTxFunc: func(ctx context.Context, opts *sql.TxOptions) (DBTx, error) {
			return nil, errors.New("connection timeout")
		},
	}

	mgr := NewDbManagerWithConn(mockConn)

	ctx := context.Background()
	err := mgr.RevokeUser(ctx, []string{"DROP USER ?"}, "testuser")
	if err == nil {
		t.Error("expected error during revocation, got nil")
	}

	if err.Error() != "revocation failed after 3 attempts: failed to start transaction: connection timeout" {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestRevokeUser_StatementFailure(t *testing.T) {
	mockTx := &MockDBTx{
		ExecContextFunc: func(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
			return nil, errors.New("syntax error")
		},
	}

	mockConn := &MockDBConnection{
		BeginTxFunc: func(ctx context.Context, opts *sql.TxOptions) (DBTx, error) {
			return mockTx, nil
		},
	}

	mgr := NewDbManagerWithConn(mockConn)

	ctx := context.Background()
	err := mgr.RevokeUser(ctx, []string{"DROP USER ?"}, "testuser")
	if err == nil {
		t.Error("expected error during revocation, got nil")
	}

	if !mockTx.RollbackCalled {
		t.Error("expected Rollback to be called on statement failure")
	}
}

func TestRevokeUser_Success(t *testing.T) {
	mockTx := &MockDBTx{}
	mockConn := &MockDBConnection{
		BeginTxFunc: func(ctx context.Context, opts *sql.TxOptions) (DBTx, error) {
			return mockTx, nil
		},
	}

	mgr := NewDbManagerWithConn(mockConn)

	ctx := context.Background()
	err := mgr.RevokeUser(ctx, []string{"DROP USER ?"}, "testuser")
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
}

func TestRevokeUser_RetryOnTransientError(t *testing.T) {
	attempts := 0
	mockConn := &MockDBConnection{
		BeginTxFunc: func(ctx context.Context, opts *sql.TxOptions) (DBTx, error) {
			attempts++
			if attempts < 3 {
				return nil, errors.New("connection timeout")
			}
			return &MockDBTx{}, nil
		},
	}

	mgr := NewDbManagerWithConn(mockConn)

	ctx := context.Background()
	err := mgr.RevokeUser(ctx, []string{"DROP USER ?"}, "testuser")
	if err != nil {
		t.Fatalf("expected success after retries, got error: %v", err)
	}

	if attempts != 3 {
		t.Errorf("expected 3 attempts, got %d", attempts)
	}
}
