package app

import (
	"context"
	"time"

	"github.com/pedro/jungle-wallet/internal/domain"
)

// UnitOfWork runs fn inside one SQL transaction. Adapters retry fn on
// deadlock/serialization failures, so fn must not keep state outside the Tx.
type UnitOfWork interface {
	Do(ctx context.Context, fn func(ctx context.Context, tx Tx) error) error
}

// Tx exposes the repositories bound to one SQL transaction.
type Tx interface {
	Wallets() WalletRepo
	Transactions() TransactionRepo
	Ledger() LedgerRepo
	Outbox() OutboxRepo
	Inbox() InboxRepo
}

type WalletRepo interface {
	// Insert creates the wallet; ErrWalletExists on (player, currency) conflict.
	Insert(ctx context.Context, w *domain.Wallet) error
	// LockByID takes the per-wallet row lock (SELECT ... FOR UPDATE).
	LockByID(ctx context.Context, id string) (*domain.Wallet, error)
	// Save persists balance and version, guarded by expectedVersion
	// (ErrConcurrentUpdate when the guard does not match).
	Save(ctx context.Context, w *domain.Wallet, expectedVersion int64) error
}

type TransactionRepo interface {
	FindByIdempotencyKey(ctx context.Context, providerID, key string) (*domain.WagerTransaction, error)
	FindByExternalID(ctx context.Context, providerID, externalID string) (*domain.WagerTransaction, error)
	// Insert stores an internal (OPENING) transaction.
	Insert(ctx context.Context, t *domain.WagerTransaction) error
	// InsertIfAbsent stores an external transaction; false when any uniqueness
	// constraint (idempotency key or external id) already holds a row.
	InsertIfAbsent(ctx context.Context, t *domain.WagerTransaction) (bool, error)
	Update(ctx context.Context, t *domain.WagerTransaction) error
	// IsReversed: does id already have a PROCESSED REFUND/ROLLBACK?
	IsReversed(ctx context.Context, id string) (bool, error)
	WalletIDOf(ctx context.Context, id string) (string, error)
	// LockDueForRetry locks a PENDING_REFERENCE row whose next attempt is due.
	LockDueForRetry(ctx context.Context, id string) (*domain.WagerTransaction, error)
	// LockForUpdate locks a transaction row regardless of its status.
	LockForUpdate(ctx context.Context, id string) (*domain.WagerTransaction, error)
}

type LedgerRepo interface {
	Append(ctx context.Context, e *domain.LedgerEntry) error
}

// OutboxEvent is staged in the same SQL transaction as the state change.
type OutboxEvent struct {
	EventID     string
	AggregateID string
	EventType   string
	Payload     []byte // immutable JSON snapshot of the whole envelope
	OccurredAt  time.Time
}

type OutboxRepo interface {
	Add(ctx context.Context, ev OutboxEvent) error
}

type InboxState int

const (
	InboxNew InboxState = iota
	InboxDuplicate
)

type InboxRepo interface {
	// Begin registers (consumer, messageID). InboxDuplicate when already
	// completed; ErrInboxPayloadMismatch when the stored hash differs.
	Begin(ctx context.Context, consumer, messageID, hash string) (InboxState, error)
	Complete(ctx context.Context, consumer, messageID string) error
}

type LedgerItem struct {
	ID            string
	WalletID      string
	TransactionID string
	Direction     domain.Direction
	Money         domain.Money
	BalanceBefore domain.Money
	BalanceAfter  domain.Money
	CreatedAt     time.Time
}

type LedgerPage struct {
	Items      []LedgerItem
	NextCursor string // opaque; empty on the last page
}

type Reconciliation struct {
	WalletID       string
	Stored         domain.Money
	Calculated     domain.Money
	Difference     domain.Money // stored - calculated
	Consistent     bool
	CheckedEntries int64
}

// Reader serves queries outside the write path.
type Reader interface {
	GetWallet(ctx context.Context, id string) (*domain.Wallet, error)
	LedgerPage(ctx context.Context, walletID, cursor string, limit int) (LedgerPage, error)
	GetTransaction(ctx context.Context, id string) (*domain.WagerTransaction, error)
	GetProviderTransaction(ctx context.Context, providerID, externalID string) (*domain.WagerTransaction, error)
	// Reconcile rebuilds the balance from the ledger in a single consistent
	// snapshot. It never modifies anything.
	Reconcile(ctx context.Context, walletID string) (Reconciliation, error)
	ListDuePending(ctx context.Context, limit int) ([]string, error)
}

type OutboxRecord struct {
	EventID     string
	AggregateID string
	EventType   string
	Payload     []byte
	OccurredAt  time.Time
	Attempts    int
}

// OutboxStore is the publisher-side view of the outbox (outside business Tx).
type OutboxStore interface {
	// Claim leases up to limit unpublished, due records to owner. Leases that
	// expired (crashed publisher) are claimable again.
	Claim(ctx context.Context, owner string, limit int, lease time.Duration) ([]OutboxRecord, error)
	MarkPublished(ctx context.Context, eventID string) error
	// Release gives a record back with a retry delay.
	Release(ctx context.Context, eventID string, retryIn time.Duration) error
	// Lag is the age of the oldest unpublished record (0 when none).
	Lag(ctx context.Context) (time.Duration, error)
}

// EventSink publishes one outbox record to the integration destination.
type EventSink interface {
	Publish(ctx context.Context, rec OutboxRecord) error
}

// Metrics is the observability port; adapters map it to Prometheus.
type Metrics interface {
	Transaction(kind, status string)
	Duplicate(source string)
	Retry(component string)
	DeadLetter()
	ConcurrencyConflict()
	ReconciliationDivergence()
	ProcessingSeconds(source string, d time.Duration)
	OutboxLagSeconds(s float64)
}

// NopMetrics discards everything (tests).
type NopMetrics struct{}

func (NopMetrics) Transaction(string, string)              {}
func (NopMetrics) Duplicate(string)                        {}
func (NopMetrics) Retry(string)                            {}
func (NopMetrics) DeadLetter()                             {}
func (NopMetrics) ConcurrencyConflict()                    {}
func (NopMetrics) ReconciliationDivergence()               {}
func (NopMetrics) ProcessingSeconds(string, time.Duration) {}
func (NopMetrics) OutboxLagSeconds(float64)                {}
