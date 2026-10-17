package postgres

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/pedro/jungle-wallet/internal/app"
	"github.com/pedro/jungle-wallet/internal/domain"
)

func (s *Store) GetWallet(ctx context.Context, id string) (*domain.Wallet, error) {
	w, err := scanWallet(s.pool.QueryRow(ctx, `SELECT `+walletCols+` FROM wallets WHERE id = $1::uuid`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, app.ErrWalletNotFound
	}
	return w, mapErr(err)
}

func (s *Store) GetTransaction(ctx context.Context, id string) (*domain.WagerTransaction, error) {
	return txRepo{s.pool}.one(ctx, `id = $1::uuid`, "", id)
}

func (s *Store) GetProviderTransaction(ctx context.Context, providerID, externalID string) (*domain.WagerTransaction, error) {
	return txRepo{s.pool}.FindByExternalID(ctx, providerID, externalID)
}

func (s *Store) ListDuePending(ctx context.Context, limit int) ([]string, error) {
	rows, err := s.pool.Query(ctx, `SELECT id::text FROM wager_transactions
		WHERE status = 'PENDING_REFERENCE' AND next_attempt_at <= now()
		ORDER BY next_attempt_at LIMIT $1`, limit)
	if err != nil {
		return nil, mapErr(err)
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, mapErr(err)
		}
		ids = append(ids, id)
	}
	return ids, mapErr(rows.Err())
}

// LedgerPage paginates by the monotonic seq column (assigned while the wallet
// lock is held, hence ordered per wallet). The cursor is opaque to clients.
func (s *Store) LedgerPage(ctx context.Context, walletID, cursor string, limit int) (app.LedgerPage, error) {
	after, err := decodeCursor(cursor)
	if err != nil {
		return app.LedgerPage{}, app.NewValidationError(err)
	}
	rows, err := s.pool.Query(ctx, `SELECT l.seq, l.id::text, l.wallet_id::text, l.transaction_id::text, l.direction,
			l.amount_minor, l.balance_before_minor, l.balance_after_minor, l.created_at, w.currency
		FROM wallet_ledger_entries l JOIN wallets w ON w.id = l.wallet_id
		WHERE l.wallet_id = $1::uuid AND l.seq > $2 ORDER BY l.seq LIMIT $3`, walletID, after, limit+1)
	if err != nil {
		return app.LedgerPage{}, mapErr(err)
	}
	defer rows.Close()
	var page app.LedgerPage
	var lastSeq int64
	for rows.Next() {
		var seq, amount, before, afterBal int64
		var it app.LedgerItem
		var dir, cur string
		if err := rows.Scan(&seq, &it.ID, &it.WalletID, &it.TransactionID, &dir, &amount, &before, &afterBal, &it.CreatedAt, &cur); err != nil {
			return app.LedgerPage{}, mapErr(err)
		}
		if len(page.Items) == limit { // the extra row only proves there is a next page
			page.NextCursor = encodeCursor(lastSeq)
			break
		}
		var e1, e2, e3, e4 error
		it.Direction = domain.Direction(dir)
		it.Money, e1 = domain.NewMoney(amount, cur)
		it.BalanceBefore, e2 = domain.NewMoney(before, cur)
		it.BalanceAfter, e3 = domain.NewMoney(afterBal, cur)
		if err := errors.Join(e1, e2, e3, e4); err != nil {
			return app.LedgerPage{}, app.Permanent(err)
		}
		page.Items = append(page.Items, it)
		lastSeq = seq
	}
	return page, mapErr(rows.Err())
}

func encodeCursor(seq int64) string {
	return base64.RawURLEncoding.EncodeToString([]byte("v1:" + strconv.FormatInt(seq, 10)))
}

func decodeCursor(c string) (int64, error) {
	if c == "" {
		return 0, nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(c)
	if err != nil || !strings.HasPrefix(string(raw), "v1:") {
		return 0, fmt.Errorf("invalid cursor")
	}
	n, err := strconv.ParseInt(strings.TrimPrefix(string(raw), "v1:"), 10, 64)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("invalid cursor")
	}
	return n, nil
}

// Reconcile reads the stored balance and the ledger sum inside one
// REPEATABLE READ, READ ONLY transaction, so both come from the same snapshot
// even while writers keep committing. It never changes data.
func (s *Store) Reconcile(ctx context.Context, walletID string) (app.Reconciliation, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return app.Reconciliation{}, mapErr(err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	var cur string
	var stored, calc, n int64
	err = tx.QueryRow(ctx, `SELECT currency, balance_minor FROM wallets WHERE id = $1::uuid`, walletID).Scan(&cur, &stored)
	if errors.Is(err, pgx.ErrNoRows) {
		return app.Reconciliation{}, app.ErrWalletNotFound
	} else if err != nil {
		return app.Reconciliation{}, mapErr(err)
	}
	err = tx.QueryRow(ctx, `SELECT COALESCE(SUM(CASE direction WHEN 'CREDIT' THEN amount_minor ELSE -amount_minor END), 0)::bigint,
			COUNT(*) FROM wallet_ledger_entries WHERE wallet_id = $1::uuid`, walletID).Scan(&calc, &n)
	if err != nil {
		return app.Reconciliation{}, mapErr(err)
	}
	st, e1 := domain.NewMoney(stored, cur)
	cm, e2 := domain.NewMoney(calc, cur)
	if err := errors.Join(e1, e2); err != nil {
		return app.Reconciliation{}, app.Permanent(err)
	}
	diff, err := st.Sub(cm)
	if err != nil {
		return app.Reconciliation{}, app.Permanent(err)
	}
	return app.Reconciliation{WalletID: walletID, Stored: st, Calculated: cm, Difference: diff,
		Consistent: diff.IsZero(), CheckedEntries: n}, nil
}

// ----------------------------------------------------------- outbox (leases)

// Claim leases due records. FOR UPDATE SKIP LOCKED makes concurrent publishers
// pick disjoint rows; locked_until lets another instance take over records of
// a publisher that crashed before publishing or acknowledging them.
func (s *Store) Claim(ctx context.Context, owner string, limit int, lease time.Duration) ([]app.OutboxRecord, error) {
	rows, err := s.pool.Query(ctx, `
		WITH due AS (
			SELECT event_id FROM outbox_events
			WHERE published_at IS NULL AND next_attempt_at <= now()
			  AND (locked_until IS NULL OR locked_until < now())
			ORDER BY seq LIMIT $1 FOR UPDATE SKIP LOCKED)
		UPDATE outbox_events o
		   SET locked_by = $2, locked_until = now() + make_interval(secs => $3::double precision), attempts = o.attempts + 1
		  FROM due WHERE o.event_id = due.event_id
		RETURNING o.event_id::text, o.aggregate_id::text, o.event_type, o.payload::text, o.occurred_at, o.attempts`,
		limit, owner, lease.Seconds())
	if err != nil {
		return nil, mapErr(err)
	}
	defer rows.Close()
	var out []app.OutboxRecord
	for rows.Next() {
		var r app.OutboxRecord
		var payload string
		if err := rows.Scan(&r.EventID, &r.AggregateID, &r.EventType, &payload, &r.OccurredAt, &r.Attempts); err != nil {
			return nil, mapErr(err)
		}
		r.Payload = []byte(payload)
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, mapErr(err)
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].OccurredAt.Before(out[j].OccurredAt) })
	return out, nil
}

func (s *Store) MarkPublished(ctx context.Context, eventID string) error {
	_, err := s.pool.Exec(ctx, `UPDATE outbox_events SET published_at = now(), locked_by = NULL, locked_until = NULL
		WHERE event_id = $1::uuid AND published_at IS NULL`, eventID)
	return mapErr(err)
}

func (s *Store) Release(ctx context.Context, eventID string, retryIn time.Duration) error {
	_, err := s.pool.Exec(ctx, `UPDATE outbox_events SET locked_by = NULL, locked_until = NULL,
		next_attempt_at = now() + make_interval(secs => $2::double precision)
		WHERE event_id = $1::uuid AND published_at IS NULL`, eventID, retryIn.Seconds())
	return mapErr(err)
}

func (s *Store) Lag(ctx context.Context) (time.Duration, error) {
	var secs float64 // time only; money never goes through floating point
	err := s.pool.QueryRow(ctx, `SELECT COALESCE(EXTRACT(EPOCH FROM (now() - MIN(occurred_at))), 0)::float8
		FROM outbox_events WHERE published_at IS NULL`).Scan(&secs)
	if err != nil {
		return 0, mapErr(err)
	}
	return time.Duration(secs * float64(time.Second)), nil
}
