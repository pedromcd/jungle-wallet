package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/pedro/jungle-wallet/internal/app"
	"github.com/pedro/jungle-wallet/internal/domain"
)

func nullIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}

// ------------------------------------------------------------------ wallets

type walletRepo struct{ q querier }

const walletCols = `id::text, player_id::text, currency, balance_minor, version, created_at, updated_at`

func scanWallet(row pgx.Row) (*domain.Wallet, error) {
	var id, player, cur string
	var bal, ver int64
	var created, updated time.Time
	if err := row.Scan(&id, &player, &cur, &bal, &ver, &created, &updated); err != nil {
		return nil, err
	}
	m, err := domain.NewMoney(bal, cur)
	if err != nil {
		return nil, app.Permanent(err)
	}
	w, err := domain.RehydrateWallet(id, player, m, ver, created, updated)
	if err != nil {
		return nil, app.Permanent(err)
	}
	return w, nil
}

func (r walletRepo) Insert(ctx context.Context, w *domain.Wallet) error {
	_, err := r.q.Exec(ctx, `INSERT INTO wallets (id, player_id, currency, balance_minor, version, created_at, updated_at)
		VALUES ($1::uuid, $2::uuid, $3, $4, $5, $6, $6)`,
		w.ID(), w.PlayerID(), w.Currency(), w.Balance().Minor(), w.Version(), w.CreatedAt())
	if isUnique(err, "wallets_player_currency_key") {
		return app.ErrWalletExists
	}
	return mapErr(err)
}

// LockByID takes FOR NO KEY UPDATE: it serialises every writer of this wallet
// but does not conflict with the FOR KEY SHARE locks that foreign keys of other
// tables take, which avoids lock-order deadlocks.
func (r walletRepo) LockByID(ctx context.Context, id string) (*domain.Wallet, error) {
	w, err := scanWallet(r.q.QueryRow(ctx, `SELECT `+walletCols+` FROM wallets WHERE id = $1::uuid FOR NO KEY UPDATE`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, app.ErrWalletNotFound
	}
	return w, mapErr(err)
}

func (r walletRepo) Save(ctx context.Context, w *domain.Wallet, expected int64) error {
	tag, err := r.q.Exec(ctx, `UPDATE wallets SET balance_minor = $1, version = $2, updated_at = $3
		WHERE id = $4::uuid AND version = $5`, w.Balance().Minor(), w.Version(), w.UpdatedAt(), w.ID(), expected)
	if err != nil {
		return mapErr(err)
	}
	if tag.RowsAffected() != 1 {
		return app.ErrConcurrentUpdate
	}
	return nil
}

// -------------------------------------------------------------- transactions

type txRepo struct{ q querier }

const txCols = `id::text, origin, COALESCE(provider_id,''), COALESCE(external_transaction_id,''),
	COALESCE(idempotency_key,''), COALESCE(payload_hash,''), wallet_id::text, player_id::text,
	COALESCE(round_id,''), COALESCE(game_id,''), kind, amount_minor, currency,
	COALESCE(reference_external_transaction_id,''), COALESCE(reference_transaction_id::text,''),
	status, COALESCE(failure_code,''), result_balance_minor, attempts, next_attempt_at, created_at, updated_at`

func scanTx(row pgx.Row) (*domain.WagerTransaction, error) {
	var s domain.TransactionSnapshot
	var origin, kind, status, failure, cur string
	var amount int64
	var result *int64
	if err := row.Scan(&s.ID, &origin, &s.ProviderID, &s.ExternalID, &s.IdempotencyKey, &s.PayloadHash,
		&s.WalletID, &s.PlayerID, &s.RoundID, &s.GameID, &kind, &amount, &cur,
		&s.ReferenceExternalID, &s.ReferenceTxID, &status, &failure, &result, &s.Attempts,
		&s.NextAttemptAt, &s.CreatedAt, &s.UpdatedAt); err != nil {
		return nil, err
	}
	m, err := domain.NewMoney(amount, cur)
	if err != nil {
		return nil, app.Permanent(err)
	}
	s.Origin, s.Kind, s.Status, s.FailureCode, s.Money = domain.Origin(origin), domain.Kind(kind), domain.Status(status), domain.FailureCode(failure), m
	if result != nil {
		rb, err := domain.NewMoney(*result, cur)
		if err != nil {
			return nil, app.Permanent(err)
		}
		s.ResultBalance = &rb
	}
	t, err := domain.RehydrateTransaction(s)
	if err != nil {
		return nil, app.Permanent(err)
	}
	return t, nil
}

func (r txRepo) one(ctx context.Context, where, lock string, args ...any) (*domain.WagerTransaction, error) {
	t, err := scanTx(r.q.QueryRow(ctx, `SELECT `+txCols+` FROM wager_transactions WHERE `+where+lock, args...))
	if err != nil {
		return nil, mapErr(err) // pgx.ErrNoRows -> app.ErrNotFound
	}
	return t, nil
}

func (r txRepo) FindByIdempotencyKey(ctx context.Context, providerID, key string) (*domain.WagerTransaction, error) {
	return r.one(ctx, `provider_id = $1 AND idempotency_key = $2`, "", providerID, key)
}

func (r txRepo) FindByExternalID(ctx context.Context, providerID, externalID string) (*domain.WagerTransaction, error) {
	return r.one(ctx, `provider_id = $1 AND external_transaction_id = $2`, "", providerID, externalID)
}

func (r txRepo) Insert(ctx context.Context, t *domain.WagerTransaction) error {
	s := t.Snapshot()
	_, err := r.q.Exec(ctx, `INSERT INTO wager_transactions
		(id, origin, wallet_id, player_id, kind, amount_minor, currency, status, result_balance_minor, attempts, created_at, updated_at)
		VALUES ($1::uuid, $2, $3::uuid, $4::uuid, $5, $6, $7, $8, $9, 0, $10, $11)`,
		s.ID, string(s.Origin), s.WalletID, s.PlayerID, string(s.Kind), s.Money.Minor(), s.Money.Currency(),
		string(s.Status), resultMinor(s.ResultBalance), s.CreatedAt, s.UpdatedAt)
	return mapErr(err)
}

func (r txRepo) InsertIfAbsent(ctx context.Context, t *domain.WagerTransaction) (bool, error) {
	s := t.Snapshot()
	// ON CONFLICT DO NOTHING without a target covers both unique constraints
	// (idempotency key and external id); a concurrent in-flight duplicate makes
	// this statement wait for the other transaction to finish.
	tag, err := r.q.Exec(ctx, `INSERT INTO wager_transactions
		(id, origin, provider_id, external_transaction_id, idempotency_key, payload_hash, wallet_id, player_id,
		 round_id, game_id, kind, amount_minor, currency, reference_external_transaction_id, status, attempts, created_at, updated_at)
		VALUES ($1::uuid, 'EXTERNAL', $2, $3, $4, $5, $6::uuid, $7::uuid, $8, $9, $10, $11, $12, $13, 'PENDING', 0, $14, $14)
		ON CONFLICT DO NOTHING`,
		s.ID, s.ProviderID, s.ExternalID, s.IdempotencyKey, s.PayloadHash, s.WalletID, s.PlayerID,
		s.RoundID, s.GameID, string(s.Kind), s.Money.Minor(), s.Money.Currency(),
		nullIfEmpty(s.ReferenceExternalID), s.CreatedAt)
	if err != nil {
		return false, mapErr(err)
	}
	return tag.RowsAffected() == 1, nil
}

func resultMinor(m *domain.Money) any {
	if m == nil {
		return nil
	}
	return m.Minor()
}

func (r txRepo) Update(ctx context.Context, t *domain.WagerTransaction) error {
	s := t.Snapshot()
	tag, err := r.q.Exec(ctx, `UPDATE wager_transactions
		SET status = $2, failure_code = $3, result_balance_minor = $4, reference_transaction_id = $5::uuid,
		    attempts = $6, next_attempt_at = $7, updated_at = $8
		WHERE id = $1::uuid`,
		s.ID, string(s.Status), nullIfEmpty(string(s.FailureCode)), resultMinor(s.ResultBalance),
		nullIfEmpty(s.ReferenceTxID), s.Attempts, s.NextAttemptAt, s.UpdatedAt)
	if isUnique(err, "wtx_one_reversal_uq") {
		// Two reversals raced for the same transaction: retry, the pre-check
		// will then reject the loser with REFERENCE_ALREADY_REVERSED.
		return app.ErrConcurrentUpdate
	}
	if err != nil {
		return mapErr(err)
	}
	if tag.RowsAffected() != 1 {
		return app.ErrNotFound
	}
	return nil
}

func (r txRepo) IsReversed(ctx context.Context, id string) (bool, error) {
	var ok bool
	err := r.q.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM wager_transactions
		WHERE reference_transaction_id = $1::uuid AND kind IN ('REFUND','ROLLBACK') AND status = 'PROCESSED')`, id).Scan(&ok)
	return ok, mapErr(err)
}

func (r txRepo) WalletIDOf(ctx context.Context, id string) (string, error) {
	var w string
	err := r.q.QueryRow(ctx, `SELECT wallet_id::text FROM wager_transactions WHERE id = $1::uuid`, id).Scan(&w)
	return w, mapErr(err)
}

func (r txRepo) LockDueForRetry(ctx context.Context, id string) (*domain.WagerTransaction, error) {
	return r.one(ctx, `id = $1::uuid AND status = 'PENDING_REFERENCE' AND next_attempt_at <= now()`, " FOR UPDATE", id)
}

func (r txRepo) LockForUpdate(ctx context.Context, id string) (*domain.WagerTransaction, error) {
	return r.one(ctx, `id = $1::uuid`, " FOR UPDATE", id)
}

// ------------------------------------------------------------------- ledger

type ledgerRepo struct{ q querier }

func (r ledgerRepo) Append(ctx context.Context, e *domain.LedgerEntry) error {
	_, err := r.q.Exec(ctx, `INSERT INTO wallet_ledger_entries
		(id, wallet_id, transaction_id, direction, amount_minor, balance_before_minor, balance_after_minor, created_at)
		VALUES ($1::uuid, $2::uuid, $3::uuid, $4, $5, $6, $7, $8)`,
		e.ID(), e.WalletID(), e.TransactionID(), string(e.Direction()), e.Money().Minor(),
		e.BalanceBefore().Minor(), e.BalanceAfter().Minor(), e.CreatedAt())
	return mapErr(err)
}

// ------------------------------------------------------------------- outbox

type outboxRepo struct{ q querier }

func (r outboxRepo) Add(ctx context.Context, ev app.OutboxEvent) error {
	_, err := r.q.Exec(ctx, `INSERT INTO outbox_events (event_id, aggregate_id, event_type, payload, occurred_at)
		VALUES ($1::uuid, $2::uuid, $3, $4::jsonb, $5)`,
		ev.EventID, ev.AggregateID, ev.EventType, string(ev.Payload), ev.OccurredAt)
	return mapErr(err)
}

// -------------------------------------------------------------------- inbox

type inboxRepo struct{ q querier }

func (r inboxRepo) Begin(ctx context.Context, consumer, messageID, hash string) (app.InboxState, error) {
	tag, err := r.q.Exec(ctx, `INSERT INTO inbox_messages (consumer_name, message_id, payload_hash)
		VALUES ($1, $2, $3) ON CONFLICT DO NOTHING`, consumer, messageID, hash)
	if err != nil {
		return app.InboxNew, mapErr(err)
	}
	if tag.RowsAffected() == 1 {
		return app.InboxNew, nil
	}
	// A concurrent consumer of the same message made the insert wait until it
	// committed, so the row we see here is final.
	var stored string
	var completed *time.Time
	if err := r.q.QueryRow(ctx, `SELECT payload_hash, completed_at FROM inbox_messages
		WHERE consumer_name = $1 AND message_id = $2`, consumer, messageID).Scan(&stored, &completed); err != nil {
		return app.InboxNew, mapErr(err)
	}
	switch {
	case stored != hash:
		return app.InboxNew, app.ErrInboxPayloadMismatch
	case completed != nil:
		return app.InboxDuplicate, nil
	}
	return app.InboxNew, nil
}

func (r inboxRepo) Complete(ctx context.Context, consumer, messageID string) error {
	_, err := r.q.Exec(ctx, `UPDATE inbox_messages SET completed_at = now()
		WHERE consumer_name = $1 AND message_id = $2`, consumer, messageID)
	return mapErr(err)
}
