package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pedro/jungle-wallet/internal/domain"
)

var ctx = context.Background()

const player = "0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1"

type env struct {
	t    *testing.T
	st   *memStore
	proc *Processor
	ws   *WalletService
	h    *MessageHandler
	w    string // wallet id
}

func money(t testing.TB, amount string) domain.Money {
	t.Helper()
	m, err := domain.ParseMoney(amount, "BRL")
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func setup(t *testing.T, initial string) *env {
	t.Helper()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	st := newMemStore()
	proc := NewProcessor(st, st, Options{MaxPendingAttempts: 3, PendingTTL: time.Hour, BackoffBase: time.Second, BackoffMax: time.Minute}, nil, log)
	proc.now = st.now
	ws := NewWalletService(st, st, nil, log)
	ws.now = st.now
	w, err := ws.Open(ctx, player, money(t, initial), "corr-open")
	if err != nil {
		t.Fatalf("open wallet: %v", err)
	}
	return &env{t: t, st: st, proc: proc, ws: ws, h: NewMessageHandler(st, proc, nil, log), w: w.ID()}
}

func (e *env) cmd(kind, ext, amount, ref string) Command {
	e.t.Helper()
	c, err := NewCommand(CommandInput{ProviderID: "provider-a", ExternalTransactionID: ext,
		IdempotencyKey: "provider-a:" + ext, PlayerID: player, WalletID: e.w, RoundID: "round-1", GameID: "game-1",
		Kind: kind, Money: money(e.t, amount), ReferenceExternalTransactionID: ref, CorrelationID: "corr-" + ext})
	if err != nil {
		e.t.Fatalf("NewCommand(%s %s): %v", kind, ext, err)
	}
	return c
}

func (e *env) do(kind, ext, amount, ref string) Outcome {
	e.t.Helper()
	out, err := e.proc.Process(ctx, e.cmd(kind, ext, amount, ref))
	if err != nil {
		e.t.Fatalf("Process(%s %s): %v", kind, ext, err)
	}
	return out
}

func (e *env) balance() string {
	e.t.Helper()
	w, err := e.ws.Get(ctx, e.w)
	if err != nil {
		e.t.Fatal(err)
	}
	return w.Balance().Amount()
}

func (e *env) reconcile() {
	e.t.Helper()
	r, err := e.ws.Reconcile(ctx, e.w)
	if err != nil || !r.Consistent {
		e.t.Fatalf("ledger and wallet diverged: %+v err=%v", r, err)
	}
}

func TestOpenWallet(t *testing.T) {
	e := setup(t, "1000.00")
	w, _ := e.ws.Get(ctx, e.w)
	if w.Version() != 1 || w.Balance().Amount() != "1000.00" {
		t.Fatalf("wallet: v=%d bal=%v", w.Version(), w.Balance())
	}
	if e.st.ledgerCount(e.w) != 1 {
		t.Fatal("OPENING must create exactly one ledger entry")
	}
	types := e.st.outboxTypes()
	if len(types) != 2 || types[0] != EventTransactionProcessed || types[1] != EventWalletBalanceChanged {
		t.Fatalf("opening events: %v", types)
	}
	// payload contract of WalletBalanceChanged
	var env Envelope[BalanceChangedData]
	if err := json.Unmarshal(e.st.db.outbox[1].Payload, &env); err != nil {
		t.Fatal(err)
	}
	d := env.Data
	if env.Version != 1 || env.EventType != EventWalletBalanceChanged || env.EventID == "" || env.CorrelationID != "corr-open" ||
		d.Direction != domain.Credit || d.BalanceBefore.Amount() != "0.00" || d.BalanceAfter.Amount() != "1000.00" || d.WalletVersion != 1 {
		t.Fatalf("balance-changed payload: %+v", env)
	}
	// same player+currency => conflict
	if _, err := e.ws.Open(ctx, player, money(t, "5.00"), ""); !errors.Is(err, ErrWalletExists) {
		t.Fatalf("duplicate wallet: %v", err)
	}
	// zero initial balance: no OPENING, no ledger, no financial events
	other := "0192f28f-5dc0-7d58-bdb2-814ad6a0f4a9"
	z, err := e.ws.Open(ctx, other, money(t, "0.00"), "")
	if err != nil || z.Version() != 1 || e.st.ledgerCount(z.ID()) != 0 || len(e.st.outboxTypes()) != 2 {
		t.Fatalf("zero wallet: err=%v ledger=%d events=%v", err, e.st.ledgerCount(z.ID()), e.st.outboxTypes())
	}
}

func TestBetIsIdempotentAndReplaysOriginalBalance(t *testing.T) {
	e := setup(t, "100.00")
	first := e.do("BET", "bet-1", "25.00", "")
	if first.Status != domain.StatusProcessed || first.Replay || first.Balance.Amount() != "75.00" {
		t.Fatalf("first: %+v", first)
	}
	// unrelated movement changes the wallet balance afterwards
	e.do("BET", "bet-2", "5.00", "")
	replay := e.do("BET", "bet-1", "25.00", "")
	if !replay.Replay || replay.TransactionID != first.TransactionID || replay.Balance.Amount() != "75.00" {
		t.Fatalf("replay must return the balance observed originally: %+v", replay)
	}
	if e.balance() != "70.00" || e.st.ledgerCount(e.w) != 3 { // opening + 2 bets, never 3 bets
		t.Fatalf("balance=%s ledger=%d", e.balance(), e.st.ledgerCount(e.w))
	}
	e.reconcile()
}

func TestIdempotencyConflicts(t *testing.T) {
	e := setup(t, "100.00")
	e.do("BET", "bet-1", "25.00", "")
	// same key, different content
	diff := e.cmd("BET", "bet-1", "26.00", "")
	if _, err := e.proc.Process(ctx, diff); !errors.Is(err, ErrIdempotencyKeyConflict) {
		t.Fatalf("expected key conflict, got %v", err)
	}
	// same (provider, external id) under another key
	other := e.cmd("BET", "bet-1", "25.00", "")
	other.Params.IdempotencyKey = "another-key"
	if _, err := e.proc.Process(ctx, other); !errors.Is(err, ErrDuplicateExternalTransaction) {
		t.Fatalf("expected duplicate external transaction, got %v", err)
	}
	if e.balance() != "75.00" {
		t.Fatalf("conflicts must have no financial effect: %s", e.balance())
	}
}

func TestInsufficientFundsDisputeLeavesOneDebit(t *testing.T) {
	e := setup(t, "100.00")
	a := e.do("BET", "bet-a", "80.00", "")
	b := e.do("BET", "bet-b", "80.00", "")
	if a.Status != domain.StatusProcessed || b.Status != domain.StatusRejected || b.FailureCode != domain.FailInsufficientFunds {
		t.Fatalf("a=%+v b=%+v", a, b)
	}
	if e.balance() != "20.00" || e.st.ledgerCount(e.w) != 2 { // opening + one debit
		t.Fatalf("balance=%s ledger=%d", e.balance(), e.st.ledgerCount(e.w))
	}
	// replays do not change the verdict, and the rejection stays auditable
	if r := e.do("BET", "bet-b", "80.00", ""); !r.Replay || r.Status != domain.StatusRejected || r.FailureCode != domain.FailInsufficientFunds {
		t.Fatalf("replay of rejection: %+v", r)
	}
	if got, _ := e.ws.ProviderTransaction(ctx, "provider-a", "bet-b"); got.Status() != domain.StatusRejected {
		t.Fatal("rejection not persisted")
	}
	e.reconcile()
}

func TestLossDoesNotMoveMoney(t *testing.T) {
	e := setup(t, "100.00")
	before, _ := e.ws.Get(ctx, e.w)
	out := e.do("LOSS", "loss-1", "0.00", "")
	after, _ := e.ws.Get(ctx, e.w)
	if out.Status != domain.StatusProcessed || before.Version() != after.Version() || e.st.ledgerCount(e.w) != 1 {
		t.Fatalf("LOSS must not touch version/ledger: %+v v%d->v%d", out, before.Version(), after.Version())
	}
	types := e.st.outboxTypes()
	if types[len(types)-1] != EventTransactionProcessed || types[len(types)-2] == EventWalletBalanceChanged && len(types) > 3 {
		t.Fatalf("LOSS emits Processed only: %v", types)
	}
	// LOSS with a non-zero amount is invalid input
	if _, err := NewCommand(CommandInput{ProviderID: "p", ExternalTransactionID: "x", IdempotencyKey: "k", PlayerID: player,
		WalletID: e.w, RoundID: "r", GameID: "g", Kind: "LOSS", Money: money(t, "1.00")}); err == nil {
		t.Fatal("LOSS 1.00 accepted")
	}
}

func TestReversalRules(t *testing.T) {
	e := setup(t, "100.00")
	e.do("BET", "bet-1", "25.00", "")
	if e.balance() != "75.00" {
		t.Fatal(e.balance())
	}
	// REFUND returns the debit
	rf := e.do("REFUND", "refund-1", "25.00", "bet-1")
	if rf.Status != domain.StatusProcessed || e.balance() != "100.00" {
		t.Fatalf("refund: %+v bal=%s", rf, e.balance())
	}
	// a second refund (new ids) and a rollback of the same BET are both refused
	dup := e.do("REFUND", "refund-2", "25.00", "bet-1")
	rb := e.do("ROLLBACK", "rollback-1", "25.00", "bet-1")
	for _, o := range []Outcome{dup, rb} {
		if o.Status != domain.StatusRejected || o.FailureCode != domain.FailReferenceAlreadyReversed {
			t.Fatalf("double reversal not refused: %+v", o)
		}
	}
	if e.balance() != "100.00" {
		t.Fatalf("no double refund: %s", e.balance())
	}
	// ROLLBACK of the REFUND takes the credit back
	rr := e.do("ROLLBACK", "rollback-2", "25.00", "refund-1")
	if rr.Status != domain.StatusProcessed || e.balance() != "75.00" {
		t.Fatalf("rollback of refund: %+v bal=%s", rr, e.balance())
	}
	// ...and the original BET can never be refunded afterwards (one reversal per transaction)
	if o := e.do("REFUND", "refund-3", "25.00", "bet-1"); o.FailureCode != domain.FailReferenceAlreadyReversed {
		t.Fatalf("%+v", o)
	}
	// partial amounts and wrong kinds
	e.do("BET", "bet-2", "10.00", "")
	if o := e.do("REFUND", "refund-4", "5.00", "bet-2"); o.FailureCode != domain.FailReferenceAmountMismatch {
		t.Fatalf("%+v", o)
	}
	e.do("WIN", "win-1", "10.00", "bet-2")
	if o := e.do("REFUND", "refund-5", "10.00", "win-1"); o.FailureCode != domain.FailReferenceInvalidKind {
		t.Fatalf("%+v", o)
	}
	e.reconcile()
}

func TestRollbackNeedingMoreThanBalanceUsesDistinctCode(t *testing.T) {
	e := setup(t, "100.00")
	e.do("WIN", "win-1", "50.00", "")                 // 150
	e.do("BET", "bet-1", "140.00", "")                // 10
	out := e.do("ROLLBACK", "rb-1", "50.00", "win-1") // would need -40
	if out.Status != domain.StatusRejected || out.FailureCode != domain.FailReversalInsufficientFunds || out.FailureCode == domain.FailInsufficientFunds {
		t.Fatalf("%+v", out)
	}
	if e.balance() != "10.00" {
		t.Fatal(e.balance())
	}
	e.reconcile()
}

func TestReferenceArrivesLate(t *testing.T) {
	e := setup(t, "100.00")
	out := e.do("REFUND", "refund-1", "25.00", "bet-1") // BET not here yet
	if out.Status != domain.StatusPendingReference || out.NextAttemptAt == nil {
		t.Fatalf("expected PENDING_REFERENCE: %+v", out)
	}
	// replay of a pending operation reports the same pending state
	if r := e.do("REFUND", "refund-1", "25.00", "bet-1"); !r.Replay || r.Status != domain.StatusPendingReference {
		t.Fatalf("%+v", r)
	}
	// not due yet: the worker does nothing
	if n, err := e.proc.RetryPending(ctx, 10); err != nil || n != 0 {
		t.Fatalf("n=%d err=%v", n, err)
	}
	e.do("BET", "bet-1", "25.00", "")
	e.st.advance(2 * time.Second)
	if n, err := e.proc.RetryPending(ctx, 10); err != nil || n != 1 {
		t.Fatalf("retry n=%d err=%v", n, err)
	}
	got, _ := e.ws.ProviderTransaction(ctx, "provider-a", "refund-1")
	if got.Status() != domain.StatusProcessed || e.balance() != "100.00" {
		t.Fatalf("status=%s balance=%s", got.Status(), e.balance())
	}
	// processing the same pending item again is a no-op
	if n, _ := e.proc.RetryPending(ctx, 10); n != 0 {
		t.Fatal("terminal transaction was re-processed")
	}
	if types := e.st.outboxTypes(); !contains(types, EventTransactionPendingReference) {
		t.Fatalf("missing pending-reference event: %v", types)
	}
	e.reconcile()
}

func TestReferenceNeverArrivesIsRejectedAfterBackoff(t *testing.T) {
	e := setup(t, "100.00")
	e.do("ROLLBACK", "rb-1", "25.00", "ghost")
	var delays []time.Duration
	for i := 0; i < 10; i++ {
		before := e.st.now()
		got, _ := e.ws.ProviderTransaction(ctx, "provider-a", "rb-1")
		if got.IsTerminal() {
			break
		}
		delays = append(delays, got.NextAttemptAt().Sub(before))
		e.st.advance(got.NextAttemptAt().Sub(before))
		if _, err := e.proc.RetryPending(ctx, 10); err != nil {
			t.Fatal(err)
		}
	}
	got, _ := e.ws.ProviderTransaction(ctx, "provider-a", "rb-1")
	if got.Status() != domain.StatusRejected || got.FailureCode() != domain.FailReferenceNotFound {
		t.Fatalf("status=%s code=%s", got.Status(), got.FailureCode())
	}
	if len(delays) < 3 || delays[1] <= delays[0] || delays[2] <= delays[1] {
		t.Fatalf("backoff must grow exponentially: %v", delays)
	}
	if !contains(e.st.outboxTypes(), EventTransactionRejected) || e.balance() != "100.00" {
		t.Fatal("rejection event missing or money moved")
	}
}

func TestReferenceThatFailedRejectsImmediately(t *testing.T) {
	e := setup(t, "10.00")
	if b := e.do("BET", "bet-1", "50.00", ""); b.Status != domain.StatusRejected {
		t.Fatalf("%+v", b)
	}
	out := e.do("REFUND", "refund-1", "50.00", "bet-1")
	if out.Status != domain.StatusRejected || out.FailureCode != domain.FailReferenceNotSuccessful {
		t.Fatalf("%+v", out)
	}
}

func TestPlayerAndCurrencyMismatchAreAuditedRejections(t *testing.T) {
	e := setup(t, "100.00")
	c := e.cmd("BET", "bet-1", "5.00", "")
	c.Params.PlayerID = "0192f28f-5dc0-7d58-bdb2-814ad6a0f4a2"
	if out, err := e.proc.Process(ctx, c); err != nil || out.FailureCode != domain.FailPlayerMismatch {
		t.Fatalf("%+v %v", out, err)
	}
	usd, _ := domain.ParseMoney("5.00", "USD")
	c = e.cmd("BET", "bet-2", "5.00", "")
	c.Params.Money = usd
	if out, err := e.proc.Process(ctx, c); err != nil || out.FailureCode != domain.FailCurrencyMismatch {
		t.Fatalf("%+v %v", out, err)
	}
	if e.balance() != "100.00" {
		t.Fatal("mismatches must not move money")
	}
}

func TestUnknownWalletPersistsNothing(t *testing.T) {
	e := setup(t, "100.00")
	c := e.cmd("BET", "bet-1", "5.00", "")
	c.Params.WalletID = "0192f291-27dd-7d3f-8071-5f8685deef99"
	if _, err := e.proc.Process(ctx, c); !errors.Is(err, ErrWalletNotFound) {
		t.Fatalf("%v", err)
	}
	if _, err := e.ws.ProviderTransaction(ctx, "provider-a", "bet-1"); !errors.Is(err, ErrNotFound) {
		t.Fatal("a failed operation must leave no trace")
	}
}

func TestTransientFailureHasNoEffectAndCanBeRetried(t *testing.T) {
	e := setup(t, "100.00")
	e.st.failNext = 1
	if _, err := e.proc.Process(ctx, e.cmd("BET", "bet-1", "25.00", "")); !IsTransient(err) {
		t.Fatalf("expected transient error, got %v", err)
	}
	if out := e.do("BET", "bet-1", "25.00", ""); out.Replay || out.Status != domain.StatusProcessed {
		t.Fatalf("%+v", out)
	}
	if e.balance() != "75.00" {
		t.Fatal(e.balance())
	}
}

func TestFiftyParallelIdenticalBetsDebitOnce(t *testing.T) {
	e := setup(t, "100.00")
	cmd := e.cmd("BET", "bet-1", "25.00", "")
	var wg sync.WaitGroup
	var mu sync.Mutex
	replays, firsts := 0, 0
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			out, err := e.proc.Process(ctx, cmd)
			if err != nil {
				t.Errorf("%v", err)
				return
			}
			mu.Lock()
			defer mu.Unlock()
			if out.Replay {
				replays++
			} else {
				firsts++
			}
		}()
	}
	wg.Wait()
	if firsts != 1 || replays != 49 || e.balance() != "75.00" || e.st.ledgerCount(e.w) != 2 {
		t.Fatalf("firsts=%d replays=%d balance=%s ledger=%d", firsts, replays, e.balance(), e.st.ledgerCount(e.w))
	}
}

// ---- hashing ---------------------------------------------------------------

func TestPayloadHashIsCanonical(t *testing.T) {
	e := setup(t, "100.00")
	a := e.cmd("BET", "bet-1", "25.00", "")
	upper := a
	upper.Params.WalletID = strings.ToUpper(a.Params.WalletID)
	upper.Params.PayloadHash = ""
	if b, err := NewCommand(CommandInput{ProviderID: "provider-a", ExternalTransactionID: "bet-1", IdempotencyKey: "OTHER-KEY",
		PlayerID: strings.ToUpper(player), WalletID: strings.ToUpper(e.w), RoundID: "round-1", GameID: "game-1", Kind: "BET",
		Money: money(t, "25.00"), CorrelationID: "x"}); err != nil || b.Params.PayloadHash != a.Params.PayloadHash {
		t.Fatalf("hash must ignore key/case/transport metadata: %v", err)
	}
	for name, mut := range map[string]func(*CommandInput){
		"amount":   func(i *CommandInput) { i.Money = money(t, "25.01") },
		"round":    func(i *CommandInput) { i.RoundID = "round-2" },
		"kind":     func(i *CommandInput) { i.Kind = "WIN" },
		"provider": func(i *CommandInput) { i.ProviderID = "provider-b" },
		"ref":      func(i *CommandInput) { i.Kind = "WIN"; i.ReferenceExternalTransactionID = "bet-0" },
	} {
		in := CommandInput{ProviderID: "provider-a", ExternalTransactionID: "bet-1", IdempotencyKey: "k", PlayerID: player,
			WalletID: e.w, RoundID: "round-1", GameID: "game-1", Kind: "BET", Money: money(t, "25.00")}
		mut(&in)
		c, err := NewCommand(in)
		if err != nil || c.Params.PayloadHash == a.Params.PayloadHash {
			t.Errorf("%s change must change the hash (err=%v)", name, err)
		}
	}
	if _, err := NewCommand(CommandInput{ProviderID: "p", ExternalTransactionID: "x", IdempotencyKey: "k", PlayerID: player,
		WalletID: e.w, RoundID: "r", GameID: "g", Kind: "OPENING", Money: money(t, "1.00")}); err == nil {
		t.Fatal("OPENING accepted from outside")
	}
	var ve *ValidationError
	if _, err := NewCommand(CommandInput{}); !errors.As(err, &ve) {
		t.Fatal("empty input must be a ValidationError")
	}
}

func TestBackoff(t *testing.T) {
	want := []time.Duration{2, 4, 8, 16, 32, 60, 60}
	for i, w := range want {
		if got := Backoff(i, 2*time.Second, time.Minute); got != w*time.Second {
			t.Errorf("Backoff(%d)=%v want %v", i, got, w*time.Second)
		}
	}
	if Backoff(1000, time.Second, time.Hour) != time.Hour || Backoff(-3, time.Second, time.Hour) != time.Second {
		t.Fatal("bounds")
	}
}

// ---- SQS handler -----------------------------------------------------------

func (e *env) msg(id, ext, amount, kind string) []byte {
	b, _ := json.Marshal(MessageEnvelope{MessageID: id, Type: MessageTypeWagerRequested, OccurredAt: time.Now().UTC(),
		Data: MessageData{ProviderID: "provider-a", ExternalTransactionID: ext, IdempotencyKey: "provider-a:" + ext,
			PlayerID: player, WalletID: e.w, RoundID: "round-1", GameID: "game-1", Kind: kind, Money: money(e.t, amount)}})
	return b
}

func TestMessageHandlerRedeliveryAndCrossChannel(t *testing.T) {
	e := setup(t, "100.00")
	body := e.msg("msg-1", "bet-1", "25.00", "BET")
	if d, err := e.h.Handle(ctx, body); d != DispositionAck || err != nil {
		t.Fatalf("%v %v", d, err)
	}
	// broker redelivers after a crash between commit and delete
	if d, err := e.h.Handle(ctx, body); d != DispositionAck || err != nil {
		t.Fatalf("redelivery: %v %v", d, err)
	}
	// same operation arriving over HTTP afterwards is a replay, not a second debit
	if out := e.do("BET", "bet-1", "25.00", ""); !out.Replay || out.Balance.Amount() != "75.00" {
		t.Fatalf("%+v", out)
	}
	// same operation under a new messageId (producer retry) is also deduplicated
	if d, err := e.h.Handle(ctx, e.msg("msg-2", "bet-1", "25.00", "BET")); d != DispositionAck || err != nil {
		t.Fatalf("%v %v", d, err)
	}
	if e.balance() != "75.00" || e.st.ledgerCount(e.w) != 2 {
		t.Fatalf("balance=%s ledger=%d", e.balance(), e.st.ledgerCount(e.w))
	}
	// HTTP first, SQS second
	e.do("BET", "bet-2", "5.00", "")
	if d, _ := e.h.Handle(ctx, e.msg("msg-3", "bet-2", "5.00", "BET")); d != DispositionAck || e.balance() != "70.00" {
		t.Fatalf("cross channel: %v %s", d, e.balance())
	}
	e.reconcile()
}

func TestMessageHandlerDispositions(t *testing.T) {
	e := setup(t, "100.00")
	// business rejection is a committed, terminal outcome: ack
	if d, err := e.h.Handle(ctx, e.msg("m-rej", "bet-big", "500.00", "BET")); d != DispositionAck || err != nil {
		t.Fatalf("rejection must be acked: %v %v", d, err)
	}
	if got, _ := e.ws.ProviderTransaction(ctx, "provider-a", "bet-big"); got.Status() != domain.StatusRejected {
		t.Fatal("rejection not persisted")
	}
	// same messageId, different payload -> poison
	e.h.Handle(ctx, e.msg("m-1", "bet-1", "10.00", "BET"))
	if d, err := e.h.Handle(ctx, e.msg("m-1", "bet-1", "11.00", "BET")); d != DispositionDeadLetter || !errors.Is(err, ErrInboxPayloadMismatch) {
		t.Fatalf("%v %v", d, err)
	}
	// same key, different payload under a new messageId -> conflict -> DLQ
	if d, err := e.h.Handle(ctx, e.msg("m-2", "bet-1", "11.00", "BET")); d != DispositionDeadLetter || !errors.Is(err, ErrIdempotencyKeyConflict) {
		t.Fatalf("%v %v", d, err)
	}
	// the conflicting attempt left no inbox trace: a later valid redelivery of m-2 would still be evaluated
	// invalid messages never succeed
	for name, body := range map[string][]byte{
		"not json":     []byte(`{`),
		"wrong type":   []byte(`{"messageId":"x","type":"Other","data":{}}`),
		"no messageId": []byte(`{"type":"WagerTransactionRequested","data":{}}`),
		"opening":      e.msg("m-op", "op-1", "1.00", "OPENING"),
		"float amount": []byte(strings.Replace(string(e.msg("m-f", "f-1", "1.00", "BET")), `"1.00"`, `1.00`, 1)),
	} {
		if d, err := e.h.Handle(ctx, body); d != DispositionDeadLetter || err == nil {
			t.Errorf("%s: %v %v", name, d, err)
		}
	}
	// infrastructure down -> retry later, nothing committed
	e.st.failNext = 1
	if d, err := e.h.Handle(ctx, e.msg("m-3", "bet-3", "1.00", "BET")); d != DispositionRetry || !IsTransient(err) {
		t.Fatalf("%v %v", d, err)
	}
	if d, _ := e.h.Handle(ctx, e.msg("m-3", "bet-3", "1.00", "BET")); d != DispositionAck {
		t.Fatal("redelivery after outage must succeed")
	}
	// unknown wallet -> DLQ
	var env MessageEnvelope
	json.Unmarshal(e.msg("m-w", "bet-9", "1.00", "BET"), &env)
	env.Data.WalletID = "0192f291-27dd-7d3f-8071-5f8685deef99"
	b, _ := json.Marshal(env)
	if d, err := e.h.Handle(ctx, b); d != DispositionDeadLetter || !errors.Is(err, ErrWalletNotFound) {
		t.Fatalf("%v %v", d, err)
	}
}

// ---- outbox publisher ------------------------------------------------------

type memOutbox struct {
	mu    sync.Mutex
	now   time.Time
	recs  map[string]*outRec
	order []string
}

type outRec struct {
	rec         OutboxRecord
	published   bool
	leasedUntil time.Time
	notBefore   time.Time
}

func newMemOutbox(now time.Time, ids ...string) *memOutbox {
	m := &memOutbox{now: now, recs: map[string]*outRec{}}
	for _, id := range ids {
		m.recs[id] = &outRec{rec: OutboxRecord{EventID: id, AggregateID: "w", EventType: "T", Payload: []byte("{}"), OccurredAt: now}}
		m.order = append(m.order, id)
	}
	return m
}

func (m *memOutbox) Claim(_ context.Context, _ string, limit int, lease time.Duration) ([]OutboxRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []OutboxRecord
	for _, id := range m.order {
		r := m.recs[id]
		if len(out) == limit || r.published || r.notBefore.After(m.now) || r.leasedUntil.After(m.now) {
			continue
		}
		r.leasedUntil = m.now.Add(lease)
		r.rec.Attempts++
		out = append(out, r.rec)
	}
	return out, nil
}

func (m *memOutbox) MarkPublished(_ context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.recs[id].published = true
	return nil
}

func (m *memOutbox) Release(_ context.Context, id string, d time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.recs[id].leasedUntil, m.recs[id].notBefore = time.Time{}, m.now.Add(d)
	return nil
}

func (m *memOutbox) Lag(context.Context) (time.Duration, error) { return 0, nil }

type memSink struct {
	mu     sync.Mutex
	sent   []string
	failOn map[string]int // eventId -> remaining failures
}

func (s *memSink) Publish(_ context.Context, r OutboxRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failOn[r.EventID] > 0 {
		s.failOn[r.EventID]--
		return errors.New("broker unavailable")
	}
	s.sent = append(s.sent, r.EventID)
	return nil
}

func TestOutboxPublisherRetryAndTakeover(t *testing.T) {
	now := time.Date(2026, 10, 16, 12, 0, 0, 0, time.UTC)
	store := newMemOutbox(now, "e1", "e2")
	sink := &memSink{failOn: map[string]int{"e1": 1}}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	opts := PublisherOptions{Batch: 10, Lease: 30 * time.Second, BackoffBase: time.Second, BackoffMax: time.Minute}
	a := NewOutboxPublisher(store, sink, "A", opts, nil, log)
	b := NewOutboxPublisher(store, sink, "B", opts, nil, log)

	// A publishes e2 and fails e1 (released with backoff)
	if n, err := a.RunOnce(ctx); err != nil || n != 1 {
		t.Fatalf("n=%d err=%v", n, err)
	}
	// B sees nothing due: e1 is backing off, e2 is published
	if n, _ := b.RunOnce(ctx); n != 0 {
		t.Fatalf("B must not steal a record in backoff: %d", n)
	}
	store.now = store.now.Add(2 * time.Second)
	if n, _ := b.RunOnce(ctx); n != 1 {
		t.Fatal("B should publish e1 after the backoff")
	}
	if len(sink.sent) != 2 {
		t.Fatalf("sent=%v", sink.sent)
	}

	// Crash between claim and publish: the lease protects the record from other
	// publishers until it expires, then another instance takes over.
	store2 := newMemOutbox(now, "e3")
	if _, err := store2.Claim(ctx, "crashed", 10, 30*time.Second); err != nil { // claimed, never published
		t.Fatal(err)
	}
	sink2 := &memSink{}
	c := NewOutboxPublisher(store2, sink2, "C", opts, nil, log)
	if n, _ := c.RunOnce(ctx); n != 0 {
		t.Fatal("record under a live lease must not be taken")
	}
	store2.now = store2.now.Add(31 * time.Second)
	if n, _ := c.RunOnce(ctx); n != 1 || sink2.sent[0] != "e3" {
		t.Fatalf("takeover failed: %v", sink2.sent)
	}
}

func TestOutboxPublisherTwoPublishersDoNotDuplicate(t *testing.T) {
	now := time.Now()
	ids := make([]string, 40)
	for i := range ids {
		ids[i] = fmt.Sprintf("e%02d", i)
	}
	store := newMemOutbox(now, ids...)
	sink := &memSink{}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	var wg sync.WaitGroup
	for _, owner := range []string{"A", "B", "C"} {
		p := NewOutboxPublisher(store, sink, owner, PublisherOptions{Batch: 7}, nil, log)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 10; i++ {
				p.RunOnce(ctx)
			}
		}()
	}
	wg.Wait()
	seen := map[string]int{}
	for _, id := range sink.sent {
		seen[id]++
	}
	if len(seen) != 40 {
		t.Fatalf("published %d distinct events", len(seen))
	}
	for id, n := range seen {
		if n != 1 {
			t.Errorf("%s published %d times by concurrent publishers", id, n)
		}
	}
}

func contains(xs []string, s string) bool {
	for _, x := range xs {
		if x == s {
			return true
		}
	}
	return false
}
