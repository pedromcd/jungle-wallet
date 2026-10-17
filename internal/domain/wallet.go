package domain

import (
	"fmt"
	"time"
)

// Direction of a balance movement.
type Direction string

const (
	Debit  Direction = "DEBIT"
	Credit Direction = "CREDIT"
)

func (d Direction) valid() bool { return d == Debit || d == Credit }

// Wallet is the root of the financial aggregate. State is encapsulated: the
// balance only changes through Apply, which preserves balance >= 0 and bumps
// the version exactly once per effective balance change.
type Wallet struct {
	id        string
	playerID  string
	balance   Money
	version   int64
	createdAt time.Time
	updatedAt time.Time
}

// OpenWallet creates a new wallet (version 1). The initial balance may be zero
// but never negative; the matching OPENING ledger entry is the caller's job.
func OpenWallet(id, playerID string, initial Money, now time.Time) (*Wallet, error) {
	return newWallet(id, playerID, initial, 1, now, now)
}

// RehydrateWallet rebuilds a persisted wallet. It never re-applies movements.
func RehydrateWallet(id, playerID string, balance Money, version int64, createdAt, updatedAt time.Time) (*Wallet, error) {
	return newWallet(id, playerID, balance, version, createdAt, updatedAt)
}

func newWallet(id, playerID string, balance Money, version int64, createdAt, updatedAt time.Time) (*Wallet, error) {
	if _, err := ParseID(id); err != nil {
		return nil, fmt.Errorf("%w: wallet id", ErrInvalidEntity)
	}
	if _, err := ParseID(playerID); err != nil {
		return nil, fmt.Errorf("%w: player id", ErrInvalidEntity)
	}
	if !balance.Initialized() {
		return nil, ErrUninitializedMoney
	}
	if balance.IsNegative() {
		return nil, fmt.Errorf("%w: negative balance", ErrInvalidEntity)
	}
	if version < 1 {
		return nil, fmt.Errorf("%w: version must be >= 1", ErrInvalidEntity)
	}
	return &Wallet{id: id, playerID: playerID, balance: balance, version: version, createdAt: createdAt, updatedAt: updatedAt}, nil
}

func (w *Wallet) ID() string           { return w.id }
func (w *Wallet) PlayerID() string     { return w.playerID }
func (w *Wallet) Balance() Money       { return w.balance }
func (w *Wallet) Currency() string     { return w.balance.Currency() }
func (w *Wallet) Version() int64       { return w.version }
func (w *Wallet) CreatedAt() time.Time { return w.createdAt }
func (w *Wallet) UpdatedAt() time.Time { return w.updatedAt }

// Movement is the result of an applied balance change.
type Movement struct {
	Direction Direction
	Money     Money
	Before    Money
	After     Money
}

// Apply debits or credits the wallet. It either succeeds completely or leaves
// the wallet untouched. Debits that would produce a negative balance fail with
// ErrInsufficientFunds.
func (w *Wallet) Apply(dir Direction, m Money, now time.Time) (Movement, error) {
	if !dir.valid() {
		return Movement{}, fmt.Errorf("%w: direction %q", ErrInvalidEntity, dir)
	}
	if !m.Initialized() {
		return Movement{}, ErrUninitializedMoney
	}
	if m.Currency() != w.balance.Currency() {
		return Movement{}, fmt.Errorf("%w: wallet %s, movement %s", ErrCurrencyMismatch, w.balance.Currency(), m.Currency())
	}
	if !m.IsPositive() {
		return Movement{}, fmt.Errorf("%w: movement must be positive", ErrInvalidAmount)
	}
	var after Money
	var err error
	if dir == Debit {
		after, err = w.balance.Sub(m)
		if err == nil && after.IsNegative() {
			return Movement{}, ErrInsufficientFunds
		}
	} else {
		after, err = w.balance.Add(m)
	}
	if err != nil {
		return Movement{}, err
	}
	mv := Movement{Direction: dir, Money: m, Before: w.balance, After: after}
	w.balance = after
	w.version++
	w.updatedAt = now
	return mv, nil
}

// LedgerEntry is an immutable, append-only record of a balance movement.
type LedgerEntry struct {
	id            string
	walletID      string
	transactionID string
	direction     Direction
	money         Money
	balanceBefore Money
	balanceAfter  Money
	createdAt     time.Time
}

// NewLedgerEntry validates balanceAfter = balanceBefore ± money.
func NewLedgerEntry(id, walletID, transactionID string, dir Direction, m, before, after Money, now time.Time) (*LedgerEntry, error) {
	if id == "" || walletID == "" || transactionID == "" || !dir.valid() {
		return nil, fmt.Errorf("%w: ledger entry identity", ErrInvalidEntity)
	}
	if !m.IsPositive() {
		return nil, fmt.Errorf("%w: ledger amount must be positive", ErrInvalidAmount)
	}
	var want Money
	var err error
	if dir == Debit {
		want, err = before.Sub(m)
	} else {
		want, err = before.Add(m)
	}
	if err != nil {
		return nil, err
	}
	if !want.Equal(after) || after.IsNegative() {
		return nil, fmt.Errorf("%w: balanceAfter inconsistent with balanceBefore and movement", ErrInvalidEntity)
	}
	return &LedgerEntry{id: id, walletID: walletID, transactionID: transactionID, direction: dir,
		money: m, balanceBefore: before, balanceAfter: after, createdAt: now}, nil
}

func (e *LedgerEntry) ID() string            { return e.id }
func (e *LedgerEntry) WalletID() string      { return e.walletID }
func (e *LedgerEntry) TransactionID() string { return e.transactionID }
func (e *LedgerEntry) Direction() Direction  { return e.direction }
func (e *LedgerEntry) Money() Money          { return e.money }
func (e *LedgerEntry) BalanceBefore() Money  { return e.balanceBefore }
func (e *LedgerEntry) BalanceAfter() Money   { return e.balanceAfter }
func (e *LedgerEntry) CreatedAt() time.Time  { return e.createdAt }
