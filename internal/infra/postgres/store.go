// Package postgres implements the persistence ports with pgx.
package postgres

import (
	"context"
	"errors"
	"math/rand"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/pedro/jungle-wallet/internal/app"
)

// querier is satisfied by pgx.Tx and *pgxpool.Pool.
type querier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Store implements app.UnitOfWork, app.Reader and app.OutboxStore.
type Store struct {
	pool        *pgxpool.Pool
	metrics     app.Metrics
	maxAttempts int
}

func NewStore(pool *pgxpool.Pool, metrics app.Metrics) *Store {
	if metrics == nil {
		metrics = app.NopMetrics{}
	}
	return &Store{pool: pool, metrics: metrics, maxAttempts: 5}
}

// Ping is used by the readiness probe.
func (s *Store) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }

// Do runs fn in a READ COMMITTED transaction. Correctness comes from explicit
// per-wallet row locks, unique constraints and CHECK constraints, not from the
// isolation level. Deadlocks, serialization failures and lost version guards
// are retried a few times; everything else is returned classified.
func (s *Store) Do(ctx context.Context, fn func(ctx context.Context, tx app.Tx) error) error {
	var last error
	for attempt := 0; attempt < s.maxAttempts; attempt++ {
		err := s.doOnce(ctx, fn)
		if err == nil {
			return nil
		}
		if !retryable(err) {
			return err
		}
		last = err
		s.metrics.ConcurrencyConflict()
		select {
		case <-ctx.Done():
			return app.Transient(ctx.Err())
		case <-time.After(time.Duration(attempt+1)*10*time.Millisecond + time.Duration(rand.Intn(20))*time.Millisecond):
		}
	}
	return app.Transient(last)
}

func (s *Store) doOnce(ctx context.Context, fn func(ctx context.Context, tx app.Tx) error) error {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return mapErr(err)
	}
	// Rollback after a successful Commit is a harmless no-op (pgx.ErrTxClosed).
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if err := fn(ctx, &pgTx{q: tx}); err != nil {
		return err
	}
	// If Commit fails with an unknown outcome the error is transient: the caller
	// retries with the same idempotency key / messageId, which is safe.
	return mapErr(tx.Commit(ctx))
}

func retryable(err error) bool {
	if errors.Is(err, app.ErrConcurrentUpdate) {
		return true
	}
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && (pgErr.Code == "40001" || pgErr.Code == "40P01")
}

// mapErr classifies a database error for the application layer.
func mapErr(err error) error {
	switch {
	case err == nil:
		return nil
	case app.IsClassified(err):
		return err // already classified by an inner layer
	case errors.Is(err, pgx.ErrNoRows):
		return app.ErrNotFound
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return app.Transient(err)
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && len(pgErr.Code) >= 2 {
		switch pgErr.Code[:2] {
		case "22", "23", "42", "0A": // data, constraint, syntax, unsupported: retrying cannot help
			return app.Permanent(err)
		}
	}
	return app.Transient(err) // connection loss, shutdown, timeouts, deadlocks, ...
}

func isUnique(err error, constraint string) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505" && (constraint == "" || pgErr.ConstraintName == constraint)
}

type pgTx struct{ q querier }

func (t *pgTx) Wallets() app.WalletRepo           { return walletRepo{t.q} }
func (t *pgTx) Transactions() app.TransactionRepo { return txRepo{t.q} }
func (t *pgTx) Ledger() app.LedgerRepo            { return ledgerRepo{t.q} }
func (t *pgTx) Outbox() app.OutboxRepo            { return outboxRepo{t.q} }
func (t *pgTx) Inbox() app.InboxRepo              { return inboxRepo{t.q} }

// Connect creates the pool.
func Connect(ctx context.Context, dsn string, maxConns int32) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	if maxConns > 0 {
		cfg.MaxConns = maxConns
	}
	cfg.MaxConnLifetime = 30 * time.Minute
	cfg.HealthCheckPeriod = 30 * time.Second
	return pgxpool.NewWithConfig(ctx, cfg)
}
