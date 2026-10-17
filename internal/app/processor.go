package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/pedro/jungle-wallet/internal/domain"
)

// Options tunes the pending-reference policy.
type Options struct {
	MaxPendingAttempts int           // retries before REFERENCE_NOT_FOUND (default 8)
	PendingTTL         time.Duration // hard age limit for a pending reference (default 15m)
	BackoffBase        time.Duration // first retry delay (default 2s)
	BackoffMax         time.Duration // cap of the exponential backoff (default 5m)
}

func (o Options) withDefaults() Options {
	if o.MaxPendingAttempts <= 0 {
		o.MaxPendingAttempts = 8
	}
	if o.PendingTTL <= 0 {
		o.PendingTTL = 15 * time.Minute
	}
	if o.BackoffBase <= 0 {
		o.BackoffBase = 2 * time.Second
	}
	if o.BackoffMax <= 0 {
		o.BackoffMax = 5 * time.Minute
	}
	return o
}

// Backoff returns min(base*2^attempt, max).
func Backoff(attempt int, base, max time.Duration) time.Duration {
	if attempt < 0 {
		attempt = 0
	}
	if attempt > 30 {
		attempt = 30
	}
	d := base << uint(attempt)
	if d <= 0 || d > max {
		return max
	}
	return d
}

// Outcome is what callers (HTTP, SQS) learn about an operation.
type Outcome struct {
	TransactionID string
	Status        domain.Status
	FailureCode   domain.FailureCode
	Balance       *domain.Money // balance observed when the outcome was decided
	NextAttemptAt *time.Time
	Replay        bool
}

func outcomeOf(t *domain.WagerTransaction, replay bool) Outcome {
	o := Outcome{TransactionID: t.ID(), Status: t.Status(), FailureCode: t.FailureCode(),
		NextAttemptAt: t.NextAttemptAt(), Replay: replay}
	if b, ok := t.ResultBalance(); ok {
		o.Balance = &b
	}
	return o
}

// Processor applies provider operations. HTTP and SQS share it, so they share
// the same idempotency and financial guarantees.
type Processor struct {
	uow     UnitOfWork
	reader  Reader
	opts    Options
	metrics Metrics
	log     *slog.Logger
	now     func() time.Time
}

func NewProcessor(uow UnitOfWork, reader Reader, opts Options, metrics Metrics, log *slog.Logger) *Processor {
	if metrics == nil {
		metrics = NopMetrics{}
	}
	if log == nil {
		log = slog.Default()
	}
	return &Processor{uow: uow, reader: reader, opts: opts.withDefaults(), metrics: metrics, log: log, now: time.Now}
}

// Process registers and applies the operation in ONE SQL transaction:
// idempotency record, wallet movement, ledger entry, final state and outbox
// events commit together (or not at all).
func (p *Processor) Process(ctx context.Context, cmd Command) (Outcome, error) {
	start := time.Now()
	var out Outcome
	err := p.uow.Do(ctx, func(ctx context.Context, tx Tx) error {
		var err error
		out, err = p.processInTx(ctx, tx, cmd)
		return err
	})
	if err != nil {
		return Outcome{}, err
	}
	p.record(cmd, out, "http", "")
	p.metrics.ProcessingSeconds("http", time.Since(start))
	return out, nil
}

// record updates metrics and emits one traceable log line per operation. Only
// identifiers and outcomes are logged: never amounts, tokens or payloads.
func (p *Processor) record(cmd Command, out Outcome, source, messageID string) {
	if out.Replay {
		p.metrics.Duplicate(source)
	}
	p.metrics.Transaction(string(cmd.Params.Kind), string(out.Status))
	attrs := []any{"source", source, "correlationId", cmd.CorrelationID, "transactionId", out.TransactionID,
		"walletId", cmd.Params.WalletID, "providerId", cmd.Params.ProviderID, "kind", string(cmd.Params.Kind),
		"status", string(out.Status), "replay", out.Replay}
	if messageID != "" {
		attrs = append(attrs, "messageId", messageID)
	}
	if out.FailureCode != "" {
		attrs = append(attrs, "failureCode", string(out.FailureCode))
	}
	p.log.Info("operation handled", attrs...)
}

// processInTx is shared with the SQS handler, which wraps it together with the
// inbox registration in a single SQL transaction.
func (p *Processor) processInTx(ctx context.Context, tx Tx, cmd Command) (Outcome, error) {
	txs := tx.Transactions()
	// Fast path: replays never take the wallet lock.
	if out, found, err := p.existing(ctx, txs, cmd); err != nil || found {
		return out, err
	}
	// Lock order is always wallet first, then rows of that wallet's operations.
	w, err := tx.Wallets().LockByID(ctx, cmd.Params.WalletID)
	if err != nil {
		return Outcome{}, err
	}
	now := p.now()
	t, err := domain.NewExternalTransaction(cmd.Params, now)
	if err != nil {
		return Outcome{}, invalid(err)
	}
	inserted, err := txs.InsertIfAbsent(ctx, t)
	if err != nil {
		return Outcome{}, err
	}
	if !inserted { // lost a race with a concurrent identical request
		out, found, err := p.existing(ctx, txs, cmd)
		if err != nil || found {
			return out, err
		}
		return Outcome{}, Transient(errors.New("idempotency row disappeared after conflict"))
	}
	return p.evaluate(ctx, tx, w, t, cmd.CorrelationID, false)
}

func (p *Processor) existing(ctx context.Context, txs TransactionRepo, cmd Command) (Outcome, bool, error) {
	prm := cmd.Params
	byKey := func() (Outcome, bool, error) {
		ex, err := txs.FindByIdempotencyKey(ctx, prm.ProviderID, prm.IdempotencyKey)
		switch {
		case err == nil:
			if ex.PayloadHash() != prm.PayloadHash {
				return Outcome{}, true, ErrIdempotencyKeyConflict
			}
			return outcomeOf(ex, true), true, nil
		case errors.Is(err, ErrNotFound):
			return Outcome{}, false, nil
		}
		return Outcome{}, false, err
	}
	if out, found, err := byKey(); err != nil || found {
		return out, found, err
	}
	_, err := txs.FindByExternalID(ctx, prm.ProviderID, prm.ExternalID)
	switch {
	case errors.Is(err, ErrNotFound):
		return Outcome{}, false, nil
	case err != nil:
		return Outcome{}, false, err
	}
	// Under READ COMMITTED every statement has its own snapshot: an identical
	// request may have committed between the two lookups, so the row we just
	// found by external id can be that very request. Look the key up again
	// before declaring a conflict (found by key => replay, otherwise it really
	// was registered under another key).
	if out, found, err := byKey(); err != nil || found {
		return out, found, err
	}
	return Outcome{}, true, ErrDuplicateExternalTransaction
}

// evaluate decides the fate of a PENDING or PENDING_REFERENCE transaction while
// the wallet row lock is held. retry is true when called by the worker.
func (p *Processor) evaluate(ctx context.Context, tx Tx, w *domain.Wallet, t *domain.WagerTransaction, corr string, retry bool) (Outcome, error) {
	now := p.now()
	if corr == "" {
		corr = t.ID()
	}
	if t.PlayerID() != w.PlayerID() {
		return p.reject(ctx, tx, w, t, domain.FailPlayerMismatch, corr, now)
	}
	if t.Money().Currency() != w.Currency() {
		return p.reject(ctx, tx, w, t, domain.FailCurrencyMismatch, corr, now)
	}

	var ref *domain.WagerTransaction
	if t.NeedsReference() {
		r, err := tx.Transactions().FindByExternalID(ctx, t.ProviderID(), t.ReferenceExternalID())
		switch {
		case err == nil:
			ref = r
		case !errors.Is(err, ErrNotFound):
			return Outcome{}, err
		}
		reversed := false
		if ref != nil && ref.Status() == domain.StatusProcessed &&
			(t.Kind() == domain.KindRefund || t.Kind() == domain.KindRollback) {
			if reversed, err = tx.Transactions().IsReversed(ctx, ref.ID()); err != nil {
				return Outcome{}, err
			}
		}
		switch d := domain.EvaluateReference(t, ref, reversed); d.Verdict {
		case domain.Wait:
			return p.wait(ctx, tx, w, t, ref, corr, retry, now)
		case domain.RejectVerdict:
			return p.reject(ctx, tx, w, t, d.Code, corr, now)
		}
	}

	if t.Kind() == domain.KindLoss { // no movement, no ledger, version untouched
		if err := t.MarkProcessed(w.Balance(), "", now); err != nil {
			return Outcome{}, err
		}
		if err := tx.Transactions().Update(ctx, t); err != nil {
			return Outcome{}, err
		}
		ev, err := processedEvent(t, w.Balance(), corr, now)
		if err != nil {
			return Outcome{}, err
		}
		return outcomeOf(t, false), tx.Outbox().Add(ctx, ev)
	}

	dir := domain.MovementDirection(t, ref)
	mv, err := w.Apply(dir, t.Money(), now)
	switch {
	case errors.Is(err, domain.ErrInsufficientFunds):
		code := domain.FailInsufficientFunds
		if t.Kind() != domain.KindBet {
			code = domain.FailReversalInsufficientFunds // distinct code, as required
		}
		return p.reject(ctx, tx, w, t, code, corr, now)
	case err != nil:
		return Outcome{}, Permanent(fmt.Errorf("apply movement: %w", err))
	}
	entry, err := domain.NewLedgerEntry(domain.NewID(), w.ID(), t.ID(), mv.Direction, mv.Money, mv.Before, mv.After, now)
	if err != nil {
		return Outcome{}, Permanent(err)
	}
	if err := tx.Wallets().Save(ctx, w, w.Version()-1); err != nil {
		return Outcome{}, err
	}
	if err := tx.Ledger().Append(ctx, entry); err != nil {
		return Outcome{}, err
	}
	refID := ""
	if ref != nil {
		refID = ref.ID()
	}
	if err := t.MarkProcessed(w.Balance(), refID, now); err != nil {
		return Outcome{}, err
	}
	if err := tx.Transactions().Update(ctx, t); err != nil {
		return Outcome{}, err
	}
	evP, err := processedEvent(t, w.Balance(), corr, now)
	if err != nil {
		return Outcome{}, err
	}
	evB, err := balanceChangedEvent(w, t, mv, corr, now)
	if err != nil {
		return Outcome{}, err
	}
	if err := tx.Outbox().Add(ctx, evP); err != nil {
		return Outcome{}, err
	}
	return outcomeOf(t, false), tx.Outbox().Add(ctx, evB)
}

func (p *Processor) reject(ctx context.Context, tx Tx, w *domain.Wallet, t *domain.WagerTransaction, code domain.FailureCode, corr string, now time.Time) (Outcome, error) {
	if err := t.Reject(code, w.Balance(), now); err != nil {
		return Outcome{}, err
	}
	if err := tx.Transactions().Update(ctx, t); err != nil {
		return Outcome{}, err
	}
	ev, err := rejectedEvent(t, code, w.Balance(), corr, now)
	if err != nil {
		return Outcome{}, err
	}
	return outcomeOf(t, false), tx.Outbox().Add(ctx, ev)
}

func (p *Processor) wait(ctx context.Context, tx Tx, w *domain.Wallet, t *domain.WagerTransaction, ref *domain.WagerTransaction, corr string, retry bool, now time.Time) (Outcome, error) {
	if !retry { // first time: park the operation and announce it
		next := now.Add(Backoff(0, p.opts.BackoffBase, p.opts.BackoffMax))
		if err := t.WaitForReference(0, next, now); err != nil {
			return Outcome{}, err
		}
		if err := tx.Transactions().Update(ctx, t); err != nil {
			return Outcome{}, err
		}
		ev, err := pendingReferenceEvent(t, next, corr, now)
		if err != nil {
			return Outcome{}, err
		}
		return outcomeOf(t, false), tx.Outbox().Add(ctx, ev)
	}
	attempts := t.Attempts() + 1
	if attempts > p.opts.MaxPendingAttempts || now.Sub(t.CreatedAt()) > p.opts.PendingTTL {
		code := domain.FailReferenceNotFound
		if ref != nil { // exists, but still PENDING / PENDING_REFERENCE
			code = domain.FailReferencePendingTimeout
		}
		return p.reject(ctx, tx, w, t, code, corr, now)
	}
	next := now.Add(Backoff(attempts, p.opts.BackoffBase, p.opts.BackoffMax))
	if err := t.WaitForReference(attempts, next, now); err != nil {
		return Outcome{}, err
	}
	p.metrics.Retry("pending_reference")
	return outcomeOf(t, false), tx.Transactions().Update(ctx, t)
}

// RetryPending re-evaluates due PENDING_REFERENCE operations. Any instance may
// run it: the wallet lock plus the "due" predicate make concurrent workers
// harmless, and the state lives in PostgreSQL, so restarts lose nothing.
func (p *Processor) RetryPending(ctx context.Context, batch int) (int, error) {
	ids, err := p.reader.ListDuePending(ctx, batch)
	if err != nil {
		return 0, err
	}
	done := 0
	for _, id := range ids {
		if ctx.Err() != nil {
			return done, ctx.Err()
		}
		out, ok, err := p.retryOne(ctx, id)
		switch {
		case err == nil && ok:
			done++
			if out.Status == domain.StatusProcessed || out.Status == domain.StatusRejected {
				p.metrics.Transaction("pending_reference", string(out.Status))
			}
			p.log.Info("pending reference re-evaluated", "transactionId", out.TransactionID, "correlationId", out.TransactionID,
				"status", string(out.Status), "failureCode", string(out.FailureCode))
		case err != nil && IsTransient(err):
			p.log.Warn("pending reference retry postponed", "transactionId", id, "error", err)
		case err != nil:
			p.log.Error("pending reference failed permanently", "transactionId", id, "error", err)
			if ferr := p.markFailed(ctx, id); ferr != nil {
				p.log.Error("could not record FAILED state", "transactionId", id, "error", ferr)
			}
		}
	}
	return done, nil
}

func (p *Processor) retryOne(ctx context.Context, id string) (Outcome, bool, error) {
	var out Outcome
	var ok bool
	err := p.uow.Do(ctx, func(ctx context.Context, tx Tx) error {
		out, ok = Outcome{}, false
		walletID, err := tx.Transactions().WalletIDOf(ctx, id)
		if errors.Is(err, ErrNotFound) {
			return nil
		} else if err != nil {
			return err
		}
		w, err := tx.Wallets().LockByID(ctx, walletID)
		if err != nil {
			return err
		}
		t, err := tx.Transactions().LockDueForRetry(ctx, id)
		if errors.Is(err, ErrNotFound) {
			return nil // someone else handled it, or it is not due anymore
		} else if err != nil {
			return err
		}
		if out, err = p.evaluate(ctx, tx, w, t, "", true); err != nil {
			return err
		}
		ok = true
		return nil
	})
	return out, ok, err
}

func (p *Processor) markFailed(ctx context.Context, id string) error {
	return p.uow.Do(ctx, func(ctx context.Context, tx Tx) error {
		walletID, err := tx.Transactions().WalletIDOf(ctx, id)
		if err != nil {
			return err
		}
		if _, err := tx.Wallets().LockByID(ctx, walletID); err != nil {
			return err
		}
		t, err := tx.Transactions().LockForUpdate(ctx, id)
		if err != nil {
			return err
		}
		if t.IsTerminal() {
			return nil
		}
		if err := t.Fail(domain.FailInternalError, p.now()); err != nil {
			return err
		}
		return tx.Transactions().Update(ctx, t)
	})
}
