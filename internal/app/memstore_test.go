package app

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/pedro/jungle-wallet/internal/domain"
)

// memStore is a deliberately simple fake: one global mutex serialises units of
// work and a commit swaps a cloned state in. It lets unit tests verify the
// business rules (idempotency, reversals, pending references, inbox/outbox
// ordering) without infrastructure. Real row-level concurrency, constraints and
// crash recovery are covered by the integration suite against PostgreSQL.

type walletRow struct {
	id, playerID string
	balance      domain.Money
	version      int64
	created      time.Time
	updated      time.Time
}

type ledgerRow struct {
	id, walletID, txID string
	dir                domain.Direction
	money, before, aft domain.Money
}

type inboxRow struct {
	hash string
	done bool
}

type memDB struct {
	wallets map[string]walletRow
	txs     map[string]domain.TransactionSnapshot
	ledger  []ledgerRow
	outbox  []OutboxEvent
	inbox   map[string]inboxRow
	now     func() time.Time
}

func (d *memDB) clone() *memDB {
	c := &memDB{now: d.now, wallets: map[string]walletRow{}, txs: map[string]domain.TransactionSnapshot{},
		inbox: map[string]inboxRow{}, ledger: append([]ledgerRow(nil), d.ledger...), outbox: append([]OutboxEvent(nil), d.outbox...)}
	for k, v := range d.wallets {
		c.wallets[k] = v
	}
	for k, v := range d.txs {
		c.txs[k] = v
	}
	for k, v := range d.inbox {
		c.inbox[k] = v
	}
	return c
}

type memStore struct {
	mu       sync.Mutex
	db       *memDB
	failNext int // number of upcoming Do calls that fail with a transient error
	clock    time.Time
}

func newMemStore() *memStore {
	s := &memStore{clock: time.Date(2026, 10, 16, 12, 0, 0, 0, time.UTC)}
	s.db = &memDB{wallets: map[string]walletRow{}, txs: map[string]domain.TransactionSnapshot{},
		inbox: map[string]inboxRow{}, now: s.now}
	return s
}

func (s *memStore) now() time.Time          { return s.clock }
func (s *memStore) advance(d time.Duration) { s.mu.Lock(); s.clock = s.clock.Add(d); s.mu.Unlock() }

func (s *memStore) Do(ctx context.Context, fn func(ctx context.Context, tx Tx) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failNext > 0 {
		s.failNext--
		return Transient(errors.New("database unavailable"))
	}
	work := s.db.clone()
	if err := fn(ctx, &memTx{db: work}); err != nil {
		return err // rollback: the clone is dropped
	}
	s.db = work
	return nil
}

type memTx struct{ db *memDB }

func (t *memTx) Wallets() WalletRepo           { return memWallets{t.db} }
func (t *memTx) Transactions() TransactionRepo { return memTxs{t.db} }
func (t *memTx) Ledger() LedgerRepo            { return memLedger{t.db} }
func (t *memTx) Outbox() OutboxRepo            { return memOutboxRepo{t.db} }
func (t *memTx) Inbox() InboxRepo              { return memInbox{t.db} }

type memWallets struct{ db *memDB }

func (r memWallets) Insert(_ context.Context, w *domain.Wallet) error {
	for _, x := range r.db.wallets {
		if x.playerID == w.PlayerID() && x.balance.Currency() == w.Currency() {
			return ErrWalletExists
		}
	}
	r.db.wallets[w.ID()] = walletRow{w.ID(), w.PlayerID(), w.Balance(), w.Version(), w.CreatedAt(), w.UpdatedAt()}
	return nil
}

func (r memWallets) LockByID(_ context.Context, id string) (*domain.Wallet, error) {
	x, ok := r.db.wallets[id]
	if !ok {
		return nil, ErrWalletNotFound
	}
	return domain.RehydrateWallet(x.id, x.playerID, x.balance, x.version, x.created, x.updated)
}

func (r memWallets) Save(_ context.Context, w *domain.Wallet, expected int64) error {
	x, ok := r.db.wallets[w.ID()]
	if !ok || x.version != expected {
		return ErrConcurrentUpdate
	}
	x.balance, x.version, x.updated = w.Balance(), w.Version(), w.UpdatedAt()
	r.db.wallets[w.ID()] = x
	return nil
}

type memTxs struct{ db *memDB }

func (r memTxs) find(pred func(domain.TransactionSnapshot) bool) (*domain.WagerTransaction, error) {
	for _, s := range r.db.txs {
		if pred(s) {
			return domain.RehydrateTransaction(s)
		}
	}
	return nil, ErrNotFound
}

func (r memTxs) FindByIdempotencyKey(_ context.Context, p, k string) (*domain.WagerTransaction, error) {
	return r.find(func(s domain.TransactionSnapshot) bool { return s.ProviderID == p && s.IdempotencyKey == k })
}

func (r memTxs) FindByExternalID(_ context.Context, p, e string) (*domain.WagerTransaction, error) {
	return r.find(func(s domain.TransactionSnapshot) bool { return s.ProviderID == p && s.ExternalID == e })
}

func (r memTxs) Insert(_ context.Context, t *domain.WagerTransaction) error {
	r.db.txs[t.ID()] = t.Snapshot()
	return nil
}

func (r memTxs) InsertIfAbsent(_ context.Context, t *domain.WagerTransaction) (bool, error) {
	for _, s := range r.db.txs {
		if s.ProviderID == t.ProviderID() && (s.IdempotencyKey == t.IdempotencyKey() || s.ExternalID == t.ExternalID()) {
			return false, nil
		}
	}
	r.db.txs[t.ID()] = t.Snapshot()
	return true, nil
}

func (r memTxs) Update(_ context.Context, t *domain.WagerTransaction) error {
	if _, ok := r.db.txs[t.ID()]; !ok {
		return ErrNotFound
	}
	r.db.txs[t.ID()] = t.Snapshot()
	return nil
}

func (r memTxs) IsReversed(_ context.Context, id string) (bool, error) {
	for _, s := range r.db.txs {
		if s.ReferenceTxID == id && s.Status == domain.StatusProcessed &&
			(s.Kind == domain.KindRefund || s.Kind == domain.KindRollback) {
			return true, nil
		}
	}
	return false, nil
}

func (r memTxs) WalletIDOf(_ context.Context, id string) (string, error) {
	s, ok := r.db.txs[id]
	if !ok {
		return "", ErrNotFound
	}
	return s.WalletID, nil
}

func (r memTxs) LockDueForRetry(_ context.Context, id string) (*domain.WagerTransaction, error) {
	s, ok := r.db.txs[id]
	if !ok || s.Status != domain.StatusPendingReference || s.NextAttemptAt == nil || s.NextAttemptAt.After(r.db.now()) {
		return nil, ErrNotFound
	}
	return domain.RehydrateTransaction(s)
}

func (r memTxs) LockForUpdate(_ context.Context, id string) (*domain.WagerTransaction, error) {
	s, ok := r.db.txs[id]
	if !ok {
		return nil, ErrNotFound
	}
	return domain.RehydrateTransaction(s)
}

type memLedger struct{ db *memDB }

func (r memLedger) Append(_ context.Context, e *domain.LedgerEntry) error {
	for _, x := range r.db.ledger {
		if x.walletID == e.WalletID() && x.txID == e.TransactionID() {
			return errors.New("ledger uniqueness violated")
		}
	}
	r.db.ledger = append(r.db.ledger, ledgerRow{e.ID(), e.WalletID(), e.TransactionID(), e.Direction(), e.Money(), e.BalanceBefore(), e.BalanceAfter()})
	return nil
}

type memOutboxRepo struct{ db *memDB }

func (r memOutboxRepo) Add(_ context.Context, ev OutboxEvent) error {
	r.db.outbox = append(r.db.outbox, ev)
	return nil
}

type memInbox struct{ db *memDB }

func (r memInbox) Begin(_ context.Context, consumer, id, hash string) (InboxState, error) {
	k := consumer + "/" + id
	if x, ok := r.db.inbox[k]; ok {
		if x.hash != hash {
			return InboxNew, ErrInboxPayloadMismatch
		}
		if x.done {
			return InboxDuplicate, nil
		}
	}
	r.db.inbox[k] = inboxRow{hash: hash}
	return InboxNew, nil
}

func (r memInbox) Complete(_ context.Context, consumer, id string) error {
	k := consumer + "/" + id
	x := r.db.inbox[k]
	x.done = true
	r.db.inbox[k] = x
	return nil
}

// ---- Reader ----------------------------------------------------------------

func (s *memStore) GetWallet(_ context.Context, id string) (*domain.Wallet, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	x, ok := s.db.wallets[id]
	if !ok {
		return nil, ErrWalletNotFound
	}
	return domain.RehydrateWallet(x.id, x.playerID, x.balance, x.version, x.created, x.updated)
}

func (s *memStore) LedgerPage(context.Context, string, string, int) (LedgerPage, error) {
	return LedgerPage{}, nil
}

func (s *memStore) GetTransaction(_ context.Context, id string) (*domain.WagerTransaction, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	x, ok := s.db.txs[id]
	if !ok {
		return nil, ErrNotFound
	}
	return domain.RehydrateTransaction(x)
}

func (s *memStore) GetProviderTransaction(_ context.Context, p, e string) (*domain.WagerTransaction, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return memTxs{s.db}.FindByExternalID(context.Background(), p, e)
}

func (s *memStore) Reconcile(_ context.Context, id string) (Reconciliation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	w, ok := s.db.wallets[id]
	if !ok {
		return Reconciliation{}, ErrWalletNotFound
	}
	calc, _ := domain.Zero(w.balance.Currency())
	var n int64
	for _, e := range s.db.ledger {
		if e.walletID != id {
			continue
		}
		n++
		if e.dir == domain.Credit {
			calc, _ = calc.Add(e.money)
		} else {
			calc, _ = calc.Sub(e.money)
		}
	}
	diff, _ := w.balance.Sub(calc)
	return Reconciliation{WalletID: id, Stored: w.balance, Calculated: calc, Difference: diff, Consistent: diff.IsZero(), CheckedEntries: n}, nil
}

func (s *memStore) ListDuePending(_ context.Context, limit int) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var ids []string
	for id, t := range s.db.txs {
		if t.Status == domain.StatusPendingReference && t.NextAttemptAt != nil && !t.NextAttemptAt.After(s.clock) {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	if len(ids) > limit {
		ids = ids[:limit]
	}
	return ids, nil
}

func (s *memStore) outboxTypes() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []string
	for _, e := range s.db.outbox {
		out = append(out, e.EventType)
	}
	return out
}

func (s *memStore) ledgerCount(walletID string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, e := range s.db.ledger {
		if e.walletID == walletID {
			n++
		}
	}
	return n
}
