package app

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/pedro/jungle-wallet/internal/domain"
)

const (
	DefaultLedgerLimit = 50
	MaxLedgerLimit     = 200
)

// WalletService covers wallet opening and read-side use cases.
type WalletService struct {
	uow     UnitOfWork
	reader  Reader
	metrics Metrics
	log     *slog.Logger
	now     func() time.Time
}

func NewWalletService(uow UnitOfWork, reader Reader, metrics Metrics, log *slog.Logger) *WalletService {
	if metrics == nil {
		metrics = NopMetrics{}
	}
	if log == nil {
		log = slog.Default()
	}
	return &WalletService{uow: uow, reader: reader, metrics: metrics, log: log, now: time.Now}
}

// Open creates a wallet. A positive initial balance also creates, in the same
// commit, the OPENING transaction (PROCESSED), its CREDIT ledger entry and the
// WagerTransactionProcessed + WalletBalanceChanged outbox events. A zero
// initial balance creates none of those. The wallet starts at version 1.
func (s *WalletService) Open(ctx context.Context, playerID string, initial domain.Money, correlationID string) (*domain.Wallet, error) {
	player, err := domain.ParseID(playerID)
	if err != nil {
		return nil, invalid(fmt.Errorf("playerId: %w", err))
	}
	if !initial.Initialized() || initial.IsNegative() {
		return nil, invalid(fmt.Errorf("%w: initialBalance must be a non-negative amount", domain.ErrInvalidAmount))
	}
	var wallet *domain.Wallet
	err = s.uow.Do(ctx, func(ctx context.Context, tx Tx) error {
		now := s.now()
		w, err := domain.OpenWallet(domain.NewID(), player, initial, now)
		if err != nil {
			return invalid(err)
		}
		if err := tx.Wallets().Insert(ctx, w); err != nil {
			return err
		}
		wallet = w
		if !initial.IsPositive() {
			return nil
		}
		corr := correlationID
		if corr == "" {
			corr = w.ID()
		}
		ot, err := domain.NewOpeningTransaction(domain.NewID(), w.ID(), player, initial, now)
		if err != nil {
			return invalid(err)
		}
		if err := ot.MarkProcessed(initial, "", now); err != nil {
			return err
		}
		if err := tx.Transactions().Insert(ctx, ot); err != nil {
			return err
		}
		zero, err := domain.Zero(initial.Currency())
		if err != nil {
			return err
		}
		entry, err := domain.NewLedgerEntry(domain.NewID(), w.ID(), ot.ID(), domain.Credit, initial, zero, initial, now)
		if err != nil {
			return err
		}
		if err := tx.Ledger().Append(ctx, entry); err != nil {
			return err
		}
		evP, err := processedEvent(ot, initial, corr, now)
		if err != nil {
			return err
		}
		evB, err := balanceChangedEvent(w, ot, domain.Movement{Direction: domain.Credit, Money: initial, Before: zero, After: initial}, corr, now)
		if err != nil {
			return err
		}
		if err := tx.Outbox().Add(ctx, evP); err != nil {
			return err
		}
		return tx.Outbox().Add(ctx, evB)
	})
	if err != nil {
		return nil, err
	}
	return wallet, nil
}

func (s *WalletService) Get(ctx context.Context, walletID string) (*domain.Wallet, error) {
	id, err := domain.ParseID(walletID)
	if err != nil {
		return nil, invalid(err)
	}
	return s.reader.GetWallet(ctx, id)
}

// Ledger returns one page ordered by insertion; cursor is opaque.
func (s *WalletService) Ledger(ctx context.Context, walletID, cursor string, limit int) (LedgerPage, error) {
	id, err := domain.ParseID(walletID)
	if err != nil {
		return LedgerPage{}, invalid(err)
	}
	switch {
	case limit == 0:
		limit = DefaultLedgerLimit
	case limit < 0 || limit > MaxLedgerLimit:
		return LedgerPage{}, invalid(fmt.Errorf("limit must be between 1 and %d", MaxLedgerLimit))
	}
	if _, err := s.reader.GetWallet(ctx, id); err != nil {
		return LedgerPage{}, err
	}
	return s.reader.LedgerPage(ctx, id, cursor, limit)
}

func (s *WalletService) Transaction(ctx context.Context, id string) (*domain.WagerTransaction, error) {
	tid, err := domain.ParseID(id)
	if err != nil {
		return nil, invalid(err)
	}
	return s.reader.GetTransaction(ctx, tid)
}

func (s *WalletService) ProviderTransaction(ctx context.Context, providerID, externalID string) (*domain.WagerTransaction, error) {
	if !domain.ValidIdentifier(providerID) || !domain.ValidIdentifier(externalID) {
		return nil, invalid(fmt.Errorf("%w: provider or external transaction id", domain.ErrInvalidEntity))
	}
	return s.reader.GetProviderTransaction(ctx, providerID, externalID)
}

// Reconcile compares the stored balance with the ledger-derived balance.
// It reports divergences (response, log, metric) and never changes data.
func (s *WalletService) Reconcile(ctx context.Context, walletID string) (Reconciliation, error) {
	id, err := domain.ParseID(walletID)
	if err != nil {
		return Reconciliation{}, invalid(err)
	}
	r, err := s.reader.Reconcile(ctx, id)
	if err != nil {
		return Reconciliation{}, err
	}
	if !r.Consistent {
		s.metrics.ReconciliationDivergence()
		s.log.Error("reconciliation divergence", "walletId", id, "stored", r.Stored.String(),
			"calculated", r.Calculated.String(), "difference", r.Difference.String())
	}
	return r, nil
}
