package domain

import (
	"fmt"
	"regexp"
	"time"
)

type Kind string

const (
	KindOpening  Kind = "OPENING" // internal only
	KindBet      Kind = "BET"
	KindWin      Kind = "WIN"
	KindLoss     Kind = "LOSS"
	KindRefund   Kind = "REFUND"
	KindRollback Kind = "ROLLBACK"
)

// Status of a transaction. PROCESSED, REJECTED and FAILED are terminal.
//
//	PENDING           -> PENDING_REFERENCE | PROCESSED | REJECTED | FAILED
//	PENDING_REFERENCE -> PENDING_REFERENCE (retry) | PROCESSED | REJECTED | FAILED
type Status string

const (
	StatusPending          Status = "PENDING"
	StatusPendingReference Status = "PENDING_REFERENCE"
	StatusProcessed        Status = "PROCESSED"
	StatusRejected         Status = "REJECTED"
	StatusFailed           Status = "FAILED"
)

// Origin separates provider operations from the internal OPENING credit.
type Origin string

const (
	OriginExternal Origin = "EXTERNAL"
	OriginInternal Origin = "INTERNAL"
)

var (
	identRe = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,128}$`)
	keyRe   = regexp.MustCompile(`^[\x21-\x7e]{1,255}$`)
	hashRe  = regexp.MustCompile(`^[0-9a-f]{64}$`)
)

// ValidIdentifier reports whether s is acceptable as a provider, external
// transaction, round, game or reference identifier.
func ValidIdentifier(s string) bool { return identRe.MatchString(s) }

// ValidIdempotencyKey reports whether s is an acceptable idempotency key.
func ValidIdempotencyKey(s string) bool { return keyRe.MatchString(s) }

// WagerTransaction is the audited record of one operation on a wallet.
type WagerTransaction struct {
	id                  string
	origin              Origin
	providerID          string
	externalID          string
	idempotencyKey      string
	payloadHash         string
	walletID            string
	playerID            string
	roundID             string
	gameID              string
	kind                Kind
	money               Money
	referenceExternalID string
	referenceTxID       string
	status              Status
	failureCode         FailureCode
	resultBalance       *Money
	attempts            int
	nextAttemptAt       *time.Time
	createdAt           time.Time
	updatedAt           time.Time
}

// ExternalParams carries the data of an operation sent by a provider.
type ExternalParams struct {
	ID                  string
	ProviderID          string
	ExternalID          string
	IdempotencyKey      string
	PayloadHash         string
	WalletID            string
	PlayerID            string
	RoundID             string
	GameID              string
	Kind                Kind
	Money               Money
	ReferenceExternalID string
}

// NewExternalTransaction validates and creates a PENDING provider operation.
// Rules: BET/WIN/REFUND/ROLLBACK need a positive amount, LOSS exactly zero;
// REFUND/ROLLBACK need a reference, BET/LOSS must not carry one.
func NewExternalTransaction(p ExternalParams, now time.Time) (*WagerTransaction, error) {
	switch p.Kind {
	case KindOpening:
		return nil, ErrOpeningNotAllowed
	case KindBet, KindWin, KindLoss, KindRefund, KindRollback:
	default:
		return nil, fmt.Errorf("%w: unknown kind %q", ErrInvalidEntity, p.Kind)
	}
	if p.ID == "" || !ValidIdentifier(p.ProviderID) || !ValidIdentifier(p.ExternalID) ||
		!ValidIdentifier(p.RoundID) || !ValidIdentifier(p.GameID) {
		return nil, fmt.Errorf("%w: identifiers", ErrInvalidEntity)
	}
	if !ValidIdempotencyKey(p.IdempotencyKey) || !hashRe.MatchString(p.PayloadHash) {
		return nil, fmt.Errorf("%w: idempotency key or payload hash", ErrInvalidEntity)
	}
	if _, err := ParseID(p.WalletID); err != nil {
		return nil, fmt.Errorf("%w: wallet id", ErrInvalidEntity)
	}
	if _, err := ParseID(p.PlayerID); err != nil {
		return nil, fmt.Errorf("%w: player id", ErrInvalidEntity)
	}
	if !p.Money.Initialized() {
		return nil, ErrUninitializedMoney
	}
	if p.Kind == KindLoss {
		if !p.Money.IsZero() {
			return nil, fmt.Errorf("%w: LOSS requires amount 0.00", ErrInvalidAmount)
		}
	} else if !p.Money.IsPositive() {
		return nil, fmt.Errorf("%w: %s requires a positive amount", ErrInvalidAmount, p.Kind)
	}
	switch p.Kind {
	case KindRefund, KindRollback:
		if !ValidIdentifier(p.ReferenceExternalID) {
			return nil, fmt.Errorf("%w: %s requires referenceExternalTransactionId", ErrInvalidEntity, p.Kind)
		}
	case KindBet, KindLoss:
		if p.ReferenceExternalID != "" {
			return nil, fmt.Errorf("%w: %s must not carry a reference", ErrInvalidEntity, p.Kind)
		}
	case KindWin:
		if p.ReferenceExternalID != "" && !ValidIdentifier(p.ReferenceExternalID) {
			return nil, fmt.Errorf("%w: invalid reference", ErrInvalidEntity)
		}
	}
	return &WagerTransaction{
		id: p.ID, origin: OriginExternal, providerID: p.ProviderID, externalID: p.ExternalID,
		idempotencyKey: p.IdempotencyKey, payloadHash: p.PayloadHash, walletID: p.WalletID,
		playerID: p.PlayerID, roundID: p.RoundID, gameID: p.GameID, kind: p.Kind, money: p.Money,
		referenceExternalID: p.ReferenceExternalID, status: StatusPending, createdAt: now, updatedAt: now,
	}, nil
}

// NewOpeningTransaction creates the internal OPENING credit of a new wallet.
// Provider, external id, key, hash, round, game and reference do not apply.
func NewOpeningTransaction(id, walletID, playerID string, initial Money, now time.Time) (*WagerTransaction, error) {
	if id == "" || !initial.IsPositive() {
		return nil, fmt.Errorf("%w: OPENING needs an id and a positive amount", ErrInvalidEntity)
	}
	if _, err := ParseID(walletID); err != nil {
		return nil, fmt.Errorf("%w: wallet id", ErrInvalidEntity)
	}
	if _, err := ParseID(playerID); err != nil {
		return nil, fmt.Errorf("%w: player id", ErrInvalidEntity)
	}
	return &WagerTransaction{id: id, origin: OriginInternal, walletID: walletID, playerID: playerID,
		kind: KindOpening, money: initial, status: StatusPending, createdAt: now, updatedAt: now}, nil
}

// TransactionSnapshot is the plain-data form used by persistence adapters.
type TransactionSnapshot struct {
	ID, ProviderID, ExternalID, IdempotencyKey, PayloadHash string
	WalletID, PlayerID, RoundID, GameID                     string
	Origin                                                  Origin
	Kind                                                    Kind
	Money                                                   Money
	ReferenceExternalID, ReferenceTxID                      string
	Status                                                  Status
	FailureCode                                             FailureCode
	ResultBalance                                           *Money
	Attempts                                                int
	NextAttemptAt                                           *time.Time
	CreatedAt, UpdatedAt                                    time.Time
}

// RehydrateTransaction rebuilds a persisted transaction without re-validating
// business rules or triggering transitions.
func RehydrateTransaction(s TransactionSnapshot) (*WagerTransaction, error) {
	if s.ID == "" || !s.Money.Initialized() {
		return nil, fmt.Errorf("%w: persisted transaction", ErrInvalidEntity)
	}
	switch s.Status {
	case StatusPending, StatusPendingReference, StatusProcessed, StatusRejected, StatusFailed:
	default:
		return nil, fmt.Errorf("%w: persisted status %q", ErrInvalidEntity, s.Status)
	}
	return &WagerTransaction{
		id: s.ID, origin: s.Origin, providerID: s.ProviderID, externalID: s.ExternalID,
		idempotencyKey: s.IdempotencyKey, payloadHash: s.PayloadHash, walletID: s.WalletID,
		playerID: s.PlayerID, roundID: s.RoundID, gameID: s.GameID, kind: s.Kind, money: s.Money,
		referenceExternalID: s.ReferenceExternalID, referenceTxID: s.ReferenceTxID, status: s.Status,
		failureCode: s.FailureCode, resultBalance: s.ResultBalance, attempts: s.Attempts,
		nextAttemptAt: s.NextAttemptAt, createdAt: s.CreatedAt, updatedAt: s.UpdatedAt,
	}, nil
}

// Snapshot exports the current state for persistence.
func (t *WagerTransaction) Snapshot() TransactionSnapshot {
	return TransactionSnapshot{
		ID: t.id, ProviderID: t.providerID, ExternalID: t.externalID, IdempotencyKey: t.idempotencyKey,
		PayloadHash: t.payloadHash, WalletID: t.walletID, PlayerID: t.playerID, RoundID: t.roundID,
		GameID: t.gameID, Origin: t.origin, Kind: t.kind, Money: t.money,
		ReferenceExternalID: t.referenceExternalID, ReferenceTxID: t.referenceTxID, Status: t.status,
		FailureCode: t.failureCode, ResultBalance: t.resultBalance, Attempts: t.attempts,
		NextAttemptAt: t.nextAttemptAt, CreatedAt: t.createdAt, UpdatedAt: t.updatedAt,
	}
}

func (t *WagerTransaction) ID() string                  { return t.id }
func (t *WagerTransaction) Origin() Origin              { return t.origin }
func (t *WagerTransaction) ProviderID() string          { return t.providerID }
func (t *WagerTransaction) ExternalID() string          { return t.externalID }
func (t *WagerTransaction) IdempotencyKey() string      { return t.idempotencyKey }
func (t *WagerTransaction) PayloadHash() string         { return t.payloadHash }
func (t *WagerTransaction) WalletID() string            { return t.walletID }
func (t *WagerTransaction) PlayerID() string            { return t.playerID }
func (t *WagerTransaction) RoundID() string             { return t.roundID }
func (t *WagerTransaction) GameID() string              { return t.gameID }
func (t *WagerTransaction) Kind() Kind                  { return t.kind }
func (t *WagerTransaction) Money() Money                { return t.money }
func (t *WagerTransaction) ReferenceExternalID() string { return t.referenceExternalID }
func (t *WagerTransaction) ReferenceTxID() string       { return t.referenceTxID }
func (t *WagerTransaction) Status() Status              { return t.status }
func (t *WagerTransaction) FailureCode() FailureCode    { return t.failureCode }
func (t *WagerTransaction) Attempts() int               { return t.attempts }
func (t *WagerTransaction) NextAttemptAt() *time.Time   { return t.nextAttemptAt }
func (t *WagerTransaction) CreatedAt() time.Time        { return t.createdAt }
func (t *WagerTransaction) UpdatedAt() time.Time        { return t.updatedAt }

// ResultBalance is the wallet balance observed when the outcome was decided.
// Replays return this value, not the wallet's current balance.
func (t *WagerTransaction) ResultBalance() (Money, bool) {
	if t.resultBalance == nil {
		return Money{}, false
	}
	return *t.resultBalance, true
}

// IsTerminal is true for PROCESSED, REJECTED and FAILED.
func (t *WagerTransaction) IsTerminal() bool {
	return t.status == StatusProcessed || t.status == StatusRejected || t.status == StatusFailed
}

// NeedsReference is true when the operation depends on another transaction.
func (t *WagerTransaction) NeedsReference() bool { return t.referenceExternalID != "" }

func (t *WagerTransaction) guard(to Status) error {
	if t.IsTerminal() {
		return fmt.Errorf("%w: %s is terminal, cannot move to %s", ErrInvalidTransition, t.status, to)
	}
	return nil
}

// MarkProcessed completes the operation. referenceTxID is the internal id of
// the resolved reference ("" when none).
func (t *WagerTransaction) MarkProcessed(balance Money, referenceTxID string, now time.Time) error {
	if err := t.guard(StatusProcessed); err != nil {
		return err
	}
	if !balance.Initialized() {
		return ErrUninitializedMoney
	}
	t.status, t.referenceTxID, t.resultBalance, t.nextAttemptAt, t.updatedAt = StatusProcessed, referenceTxID, &balance, nil, now
	return nil
}

// Reject records a definitive business rejection.
func (t *WagerTransaction) Reject(code FailureCode, balance Money, now time.Time) error {
	if err := t.guard(StatusRejected); err != nil {
		return err
	}
	if code == "" || !balance.Initialized() {
		return fmt.Errorf("%w: rejection needs a failure code and balance", ErrInvalidEntity)
	}
	t.status, t.failureCode, t.resultBalance, t.nextAttemptAt, t.updatedAt = StatusRejected, code, &balance, nil, now
	return nil
}

// WaitForReference parks (or re-schedules) the operation until its reference
// exists. attempts counts retries already performed.
func (t *WagerTransaction) WaitForReference(attempts int, next, now time.Time) error {
	if err := t.guard(StatusPendingReference); err != nil {
		return err
	}
	if !t.NeedsReference() {
		return fmt.Errorf("%w: %s has no reference to wait for", ErrInvalidTransition, t.kind)
	}
	t.status, t.attempts, t.nextAttemptAt, t.updatedAt = StatusPendingReference, attempts, &next, now
	return nil
}

// Fail records a permanent internal failure for audit purposes.
func (t *WagerTransaction) Fail(code FailureCode, now time.Time) error {
	if err := t.guard(StatusFailed); err != nil {
		return err
	}
	t.status, t.failureCode, t.nextAttemptAt, t.updatedAt = StatusFailed, code, nil, now
	return nil
}
