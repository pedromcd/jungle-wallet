//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/pedro/jungle-wallet/internal/app"
	"github.com/pedro/jungle-wallet/internal/domain"
	"github.com/pedro/jungle-wallet/internal/infra/postgres"
	"github.com/pedro/jungle-wallet/internal/testsupport"
)

var (
	db  *testsupport.DB
	ctx = context.Background()
	log = slog.New(slog.NewTextHandler(io.Discard, nil))
)

func TestMain(m *testing.M) {
	d, ok, err := testsupport.Setup()
	if err != nil {
		fmt.Fprintln(os.Stderr, "setup:", err)
		os.Exit(1)
	}
	if !ok {
		fmt.Println("TEST_DATABASE_URL not set: skipping PostgreSQL integration tests")
		os.Exit(0)
	}
	db = d
	code := m.Run()
	db.Close()
	os.Exit(code)
}

// instance simulates one service replica: its own pool and its own use cases.
type instance struct {
	pool *pgxpool.Pool
	st   *postgres.Store
	proc *app.Processor
	ws   *app.WalletService
	h    *app.MessageHandler
}

func newInstance(t testing.TB) *instance {
	t.Helper()
	pool, err := db.NewPool()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close) // a replica going away
	st := postgres.NewStore(pool, nil)
	proc := app.NewProcessor(st, st, app.Options{MaxPendingAttempts: 3, PendingTTL: time.Hour,
		BackoffBase: 30 * time.Millisecond, BackoffMax: 200 * time.Millisecond}, nil, log)
	return &instance{pool, st, proc, app.NewWalletService(st, st, nil, log), app.NewMessageHandler(st, proc, nil, log)}
}

func money(t testing.TB, amount string) domain.Money {
	t.Helper()
	m, err := domain.ParseMoney(amount, "BRL")
	if err != nil {
		t.Fatal(err)
	}
	return m
}

type scenario struct {
	t        *testing.T
	nodes    []*instance
	wallet   string
	player   string
	provider string // unique per scenario: (provider, key) is globally unique in the schema
}

func uniqueProvider() string { return "prov-" + domain.NewID()[24:] }

// drainOutbox marks everything published so outbox tests only see their own events.
func drainOutbox(t testing.TB) {
	t.Helper()
	if _, err := db.Pool.Exec(ctx, `UPDATE outbox_events SET published_at = now() WHERE published_at IS NULL`); err != nil {
		t.Fatal(err)
	}
}

func newScenario(t *testing.T, initial string, nodes int) *scenario {
	t.Helper()
	s := &scenario{t: t, player: domain.NewID(), provider: uniqueProvider()}
	for i := 0; i < nodes; i++ {
		s.nodes = append(s.nodes, newInstance(t))
	}
	w, err := s.nodes[0].ws.Open(ctx, s.player, money(t, initial), "test")
	if err != nil {
		t.Fatal(err)
	}
	s.wallet = w.ID()
	return s
}

func (s *scenario) cmd(kind, ext, amount, ref string) app.Command {
	s.t.Helper()
	c, err := app.NewCommand(app.CommandInput{ProviderID: s.provider, ExternalTransactionID: ext,
		IdempotencyKey: s.provider + ":" + ext, PlayerID: s.player, WalletID: s.wallet, RoundID: "round-1", GameID: "game-1",
		Kind: kind, Money: money(s.t, amount), ReferenceExternalTransactionID: ref, CorrelationID: "c-" + ext})
	if err != nil {
		s.t.Fatal(err)
	}
	return c
}

func (s *scenario) do(node int, kind, ext, amount, ref string) app.Outcome {
	s.t.Helper()
	out, err := s.nodes[node%len(s.nodes)].proc.Process(ctx, s.cmd(kind, ext, amount, ref))
	if err != nil {
		s.t.Fatalf("%s %s: %v", kind, ext, err)
	}
	return out
}

func (s *scenario) balance() string {
	w, err := s.nodes[0].ws.Get(ctx, s.wallet)
	if err != nil {
		s.t.Fatal(err)
	}
	return w.Balance().Amount()
}

func (s *scenario) requireConsistent() {
	s.t.Helper()
	r, err := s.nodes[0].ws.Reconcile(ctx, s.wallet)
	if err != nil || !r.Consistent {
		s.t.Fatalf("wallet and ledger diverged: %+v %v", r, err)
	}
}

// parallel runs fn(i) for i in [0,n) at the same time.
func parallel(n int, fn func(i int)) {
	var start, done sync.WaitGroup
	start.Add(1)
	for i := 0; i < n; i++ {
		done.Add(1)
		go func(i int) {
			defer done.Done()
			start.Wait()
			fn(i)
		}(i)
	}
	start.Done()
	done.Wait()
}

func TestFiftyParallelIdenticalBetsAcrossThreeInstancesDebitOnce(t *testing.T) {
	s := newScenario(t, "100.00", 3)
	var first, replay atomic.Int32
	parallel(50, func(i int) {
		out, err := s.nodes[i%3].proc.Process(ctx, s.cmd("BET", "bet-1", "25.00", ""))
		if err != nil {
			t.Errorf("worker %d: %v", i, err)
			return
		}
		if out.Status != domain.StatusProcessed || out.Balance.Amount() != "75.00" {
			t.Errorf("worker %d: %+v", i, out)
		}
		if out.Replay {
			replay.Add(1)
		} else {
			first.Add(1)
		}
	})
	if first.Load() != 1 || replay.Load() != 49 || s.balance() != "75.00" {
		t.Fatalf("first=%d replay=%d balance=%s", first.Load(), replay.Load(), s.balance())
	}
	s.requireConsistent()
}

func TestConcurrentDisputeForTheSameBalance(t *testing.T) {
	s := newScenario(t, "100.00", 3)
	var ok, rejected atomic.Int32
	parallel(30, func(i int) {
		out := s.do(i, "BET", fmt.Sprintf("bet-%d", i), "80.00", "")
		switch {
		case out.Status == domain.StatusProcessed:
			ok.Add(1)
		case out.Status == domain.StatusRejected && out.FailureCode == domain.FailInsufficientFunds:
			rejected.Add(1)
		default:
			t.Errorf("unexpected outcome %+v", out)
		}
	})
	if ok.Load() != 1 || rejected.Load() != 29 || s.balance() != "20.00" {
		t.Fatalf("processed=%d rejected=%d balance=%s", ok.Load(), rejected.Load(), s.balance())
	}
	s.requireConsistent()
}

func TestManyWalletsInParallelStayIndependentAndConsistent(t *testing.T) {
	const wallets, bets = 12, 15
	mProvider := uniqueProvider()
	nodes := []*instance{newInstance(t), newInstance(t), newInstance(t)}
	type w struct{ id, player string }
	ws := make([]w, wallets)
	for i := range ws {
		p := domain.NewID()
		wl, err := nodes[0].ws.Open(ctx, p, money(t, "1000.00"), "")
		if err != nil {
			t.Fatal(err)
		}
		ws[i] = w{wl.ID(), p}
	}
	parallel(wallets*bets, func(i int) {
		wi := i % wallets
		c, err := app.NewCommand(app.CommandInput{ProviderID: mProvider, ExternalTransactionID: fmt.Sprintf("m-%d", i),
			IdempotencyKey: fmt.Sprintf("m-key-%d", i), PlayerID: ws[wi].player, WalletID: ws[wi].id, RoundID: "r", GameID: "g",
			Kind: "BET", Money: money(t, "10.00")})
		if err != nil {
			t.Error(err)
			return
		}
		if out, err := nodes[i%3].proc.Process(ctx, c); err != nil || out.Status != domain.StatusProcessed {
			t.Errorf("bet %d: %+v %v", i, out, err)
		}
	})
	for _, x := range ws {
		r, err := nodes[0].ws.Reconcile(ctx, x.id)
		if err != nil || !r.Consistent || r.Stored.Amount() != "850.00" || r.CheckedEntries != bets+1 {
			t.Fatalf("wallet %s: %+v %v", x.id, r, err)
		}
		wl, _ := nodes[0].ws.Get(ctx, x.id)
		if wl.Version() != int64(1+bets) {
			t.Fatalf("version %d, want %d", wl.Version(), 1+bets)
		}
	}
}

func TestRefundAndRollbackRaceOnlyOneReversalWins(t *testing.T) {
	s := newScenario(t, "100.00", 3)
	s.do(0, "BET", "bet-1", "25.00", "")
	var processed, already atomic.Int32
	parallel(12, func(i int) {
		kind := "REFUND"
		if i%2 == 1 {
			kind = "ROLLBACK"
		}
		out := s.do(i, kind, fmt.Sprintf("rev-%d", i), "25.00", "bet-1")
		switch {
		case out.Status == domain.StatusProcessed:
			processed.Add(1)
		case out.FailureCode == domain.FailReferenceAlreadyReversed:
			already.Add(1)
		default:
			t.Errorf("unexpected %+v", out)
		}
	})
	if processed.Load() != 1 || already.Load() != 11 || s.balance() != "100.00" {
		t.Fatalf("processed=%d already=%d balance=%s", processed.Load(), already.Load(), s.balance())
	}
	s.requireConsistent()
}

func TestIdempotencyConflictsAndReplayAfterOtherMovements(t *testing.T) {
	s := newScenario(t, "100.00", 2)
	first := s.do(0, "BET", "bet-1", "25.00", "")
	s.do(1, "BET", "bet-2", "5.00", "")
	if r := s.do(1, "BET", "bet-1", "25.00", ""); !r.Replay || r.Balance.Amount() != "75.00" || r.TransactionID != first.TransactionID {
		t.Fatalf("replay %+v", r)
	}
	if _, err := s.nodes[0].proc.Process(ctx, s.cmd("BET", "bet-1", "26.00", "")); !errors.Is(err, app.ErrIdempotencyKeyConflict) {
		t.Fatalf("%v", err)
	}
	c := s.cmd("BET", "bet-1", "25.00", "")
	c.Params.IdempotencyKey = "another-key"
	if _, err := s.nodes[0].proc.Process(ctx, c); !errors.Is(err, app.ErrDuplicateExternalTransaction) {
		t.Fatalf("%v", err)
	}
	if s.balance() != "70.00" {
		t.Fatal(s.balance())
	}
	s.requireConsistent()
}

func TestPendingReferenceResolvedByWorkerAfterTheBetArrives(t *testing.T) {
	s := newScenario(t, "100.00", 2)
	out := s.do(0, "REFUND", "refund-1", "25.00", "bet-1")
	if out.Status != domain.StatusPendingReference {
		t.Fatalf("%+v", out)
	}
	s.do(1, "BET", "bet-1", "25.00", "")
	deadline := time.Now().Add(5 * time.Second)
	for {
		for _, n := range s.nodes { // both replicas run the worker, as in production
			if _, err := n.proc.RetryPending(ctx, 10); err != nil {
				t.Fatal(err)
			}
		}
		got, err := s.nodes[0].ws.ProviderTransaction(ctx, s.provider, "refund-1")
		if err != nil {
			t.Fatal(err)
		}
		if got.Status() == domain.StatusProcessed {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("refund never resolved: %s", got.Status())
		}
		time.Sleep(50 * time.Millisecond)
	}
	if s.balance() != "100.00" {
		t.Fatal(s.balance())
	}
	s.requireConsistent()
}

func TestPendingReferenceThatNeverArrivesIsRejected(t *testing.T) {
	s := newScenario(t, "100.00", 2)
	s.do(0, "ROLLBACK", "rb-1", "25.00", "ghost")
	deadline := time.Now().Add(10 * time.Second)
	for {
		parallel(4, func(i int) { _, _ = s.nodes[i%2].proc.RetryPending(ctx, 10) }) // workers racing each other
		got, _ := s.nodes[0].ws.ProviderTransaction(ctx, s.provider, "rb-1")
		if got.IsTerminal() {
			if got.Status() != domain.StatusRejected || got.FailureCode() != domain.FailReferenceNotFound {
				t.Fatalf("%s %s", got.Status(), got.FailureCode())
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("never rejected")
		}
		time.Sleep(50 * time.Millisecond)
	}
	if s.balance() != "100.00" {
		t.Fatal(s.balance())
	}
}

func TestInboxDeduplicatesConcurrentRedeliveries(t *testing.T) {
	s := newScenario(t, "100.00", 3)
	body := func(id string) []byte { return sqsBody(t, s, s.provider+"/"+id, "bet-1", "25.00") }
	var acks atomic.Int32
	parallel(20, func(i int) {
		d, err := s.nodes[i%3].h.Handle(ctx, body("msg-1"))
		if d == app.DispositionAck && err == nil {
			acks.Add(1)
		} else {
			t.Errorf("%v %v", d, err)
		}
	})
	if acks.Load() != 20 || s.balance() != "75.00" {
		t.Fatalf("acks=%d balance=%s", acks.Load(), s.balance())
	}
	// same operation under a new messageId, and over HTTP/Process, is still one debit
	if d, _ := s.nodes[0].h.Handle(ctx, body("msg-2")); d != app.DispositionAck {
		t.Fatal(d)
	}
	s.do(1, "BET", "bet-1", "25.00", "")
	// same messageId, different payload: poison, nothing changes
	if d, err := s.nodes[0].h.Handle(ctx, sqsBody(t, s, s.provider+"/msg-1", "bet-1", "26.00")); d != app.DispositionDeadLetter || !errors.Is(err, app.ErrInboxPayloadMismatch) {
		t.Fatalf("%v %v", d, err)
	}
	if s.balance() != "75.00" {
		t.Fatal(s.balance())
	}
	s.requireConsistent()
}

func sqsBody(t *testing.T, s *scenario, messageID, ext, amount string) []byte {
	t.Helper()
	return []byte(fmt.Sprintf(`{"messageId":%q,"type":"WagerTransactionRequested","occurredAt":"2026-10-16T12:00:00Z","data":{
		"providerId":%q,"externalTransactionId":%q,"idempotencyKey":"%s:%s","playerId":%q,"walletId":%q,
		"roundId":"round-1","gameId":"game-1","kind":"BET","money":{"amount":%q,"currency":"BRL"}}}`,
		messageID, s.provider, ext, s.provider, ext, s.player, s.wallet, amount))
}

// ---- outbox ---------------------------------------------------------------

type collectSink struct {
	mu   sync.Mutex
	seen map[string]int
}

func (c *collectSink) Publish(_ context.Context, r app.OutboxRecord) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.seen[r.EventID]++
	return nil
}

func TestOutboxIsAtomicWithBusinessStateAndPublishedOncePerLease(t *testing.T) {
	drainOutbox(t)
	s := newScenario(t, "100.00", 3)
	for i := 0; i < 10; i++ {
		s.do(i, "BET", fmt.Sprintf("o-%d", i), "1.00", "")
	}
	s.do(0, "BET", "o-big", "999.00", "") // rejection also emits an event

	var staged int
	if err := db.Pool.QueryRow(ctx, `SELECT count(*) FROM outbox_events WHERE aggregate_id = $1::uuid AND published_at IS NULL`, s.wallet).Scan(&staged); err != nil {
		t.Fatal(err)
	}
	// opening: 2, each BET: 2 (processed + balance changed), rejection: 1
	if staged != 2+10*2+1 {
		t.Fatalf("staged events = %d", staged)
	}
	sink := &collectSink{seen: map[string]int{}}
	var wg sync.WaitGroup
	for i, n := range s.nodes {
		pub := app.NewOutboxPublisher(n.st, sink, fmt.Sprintf("pub-%d", i), app.PublisherOptions{Batch: 5, Lease: 30 * time.Second}, nil, log)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				if _, err := pub.RunOnce(ctx); err != nil {
					t.Error(err)
				}
			}
		}()
	}
	wg.Wait()
	for id, n := range sink.seen {
		if n != 1 {
			t.Errorf("event %s published %d times by concurrent publishers", id, n)
		}
	}
	var pending int
	_ = db.Pool.QueryRow(ctx, `SELECT count(*) FROM outbox_events WHERE aggregate_id = $1::uuid AND published_at IS NULL`, s.wallet).Scan(&pending)
	if pending != 0 {
		t.Fatalf("%d events left unpublished", pending)
	}
}

func TestOutboxLeaseTakeoverAfterPublisherCrash(t *testing.T) {
	drainOutbox(t)
	s := newScenario(t, "100.00", 2)
	// "crashed" publisher claims everything with a tiny lease and never publishes
	claimed, err := s.nodes[0].st.Claim(ctx, "crashed", 100, 400*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	mine := 0
	for _, r := range claimed {
		if r.AggregateID == s.wallet {
			mine++
		}
	}
	if mine != 2 {
		t.Fatalf("expected the opening events, got %d", mine)
	}
	got, _ := s.nodes[1].st.Claim(ctx, "healthy", 100, time.Minute)
	for _, r := range got {
		if r.AggregateID == s.wallet {
			t.Fatal("record under a live lease must not be stolen")
		}
	}
	time.Sleep(600 * time.Millisecond)
	got, _ = s.nodes[1].st.Claim(ctx, "healthy", 100, time.Minute)
	taken := 0
	for _, r := range got {
		if r.AggregateID == s.wallet {
			taken++
			if r.Attempts != 2 {
				t.Errorf("attempts = %d, want 2 (one per claim)", r.Attempts)
			}
			if err := s.nodes[1].st.MarkPublished(ctx, r.EventID); err != nil {
				t.Fatal(err)
			}
		}
	}
	if taken != 2 {
		t.Fatalf("takeover got %d events", taken)
	}
}

func TestOutboxReleaseBackoffAndLag(t *testing.T) {
	drainOutbox(t)
	s := newScenario(t, "100.00", 1)
	recs, _ := s.nodes[0].st.Claim(ctx, "p", 100, time.Minute)
	for _, r := range recs {
		if err := s.nodes[0].st.Release(ctx, r.EventID, 300*time.Millisecond); err != nil {
			t.Fatal(err)
		}
	}
	again, _ := s.nodes[0].st.Claim(ctx, "p", 100, time.Minute)
	for _, r := range again {
		if r.AggregateID == s.wallet {
			t.Fatal("released record must wait for its backoff")
		}
	}
	if lag, err := s.nodes[0].st.Lag(ctx); err != nil || lag <= 0 {
		t.Fatalf("lag=%v err=%v", lag, err)
	}
	time.Sleep(450 * time.Millisecond)
	again, _ = s.nodes[0].st.Claim(ctx, "p", 100, time.Minute)
	n := 0
	for _, r := range again {
		if r.AggregateID == s.wallet {
			n++
		}
	}
	if n != 2 {
		t.Fatalf("after backoff got %d", n)
	}
}

// ---- reads ----------------------------------------------------------------

func TestLedgerPaginationIsStableAndComplete(t *testing.T) {
	s := newScenario(t, "1000.00", 2)
	for i := 0; i < 24; i++ {
		s.do(i, "BET", fmt.Sprintf("p-%d", i), "1.00", "")
	}
	var seen []string
	cursor := ""
	for pages := 0; ; pages++ {
		p, err := s.nodes[0].ws.Ledger(ctx, s.wallet, cursor, 10)
		if err != nil {
			t.Fatal(err)
		}
		for _, it := range p.Items {
			seen = append(seen, it.ID)
		}
		if p.NextCursor == "" {
			if pages != 2 {
				t.Fatalf("expected 3 pages, got %d", pages+1)
			}
			break
		}
		cursor = p.NextCursor
	}
	set := map[string]bool{}
	for _, id := range seen {
		set[id] = true
	}
	if len(seen) != 25 || len(set) != 25 { // opening + 24 bets
		t.Fatalf("entries=%d distinct=%d", len(seen), len(set))
	}
	first, _ := s.nodes[0].ws.Ledger(ctx, s.wallet, "", 1)
	if first.Items[0].Direction != domain.Credit || first.Items[0].BalanceAfter.Amount() != "1000.00" {
		t.Fatalf("first ledger entry must be the OPENING credit: %+v", first.Items[0])
	}
	var ve *app.ValidationError
	if _, err := s.nodes[0].ws.Ledger(ctx, s.wallet, "garbage", 10); !errors.As(err, &ve) {
		t.Fatalf("bad cursor must be a validation error: %v", err)
	}
	if _, err := s.nodes[0].ws.Ledger(ctx, s.wallet, "", 201); !errors.As(err, &ve) {
		t.Fatalf("limit above max must be rejected: %v", err)
	}
}

func TestReconciliationReportsDivergenceWithoutFixingIt(t *testing.T) {
	s := newScenario(t, "100.00", 1)
	s.do(0, "BET", "bet-1", "10.00", "")
	s.requireConsistent()
	// someone tampers with the balance (version bumped to satisfy the guard trigger)
	if _, err := db.Pool.Exec(ctx, `UPDATE wallets SET balance_minor = balance_minor + 1, version = version + 1 WHERE id = $1::uuid`, s.wallet); err != nil {
		t.Fatal(err)
	}
	r, err := s.nodes[0].ws.Reconcile(ctx, s.wallet)
	if err != nil || r.Consistent || r.Difference.Amount() != "0.01" || r.Stored.Amount() != "90.01" || r.Calculated.Amount() != "90.00" {
		t.Fatalf("%+v %v", r, err)
	}
	if s.balance() != "90.01" { // reconciliation never modifies anything
		t.Fatal("reconciliation changed the wallet")
	}
	if _, err := s.nodes[0].ws.Reconcile(ctx, domain.NewID()); !errors.Is(err, app.ErrWalletNotFound) {
		t.Fatal(err)
	}
}

func TestOpenWalletRules(t *testing.T) {
	n := newInstance(t)
	p := domain.NewID()
	w, err := n.ws.Open(ctx, p, money(t, "50.00"), "")
	if err != nil || w.Version() != 1 {
		t.Fatalf("%v", err)
	}
	if _, err := n.ws.Open(ctx, p, money(t, "1.00"), ""); !errors.Is(err, app.ErrWalletExists) {
		t.Fatalf("%v", err)
	}
	var ledger, events int
	_ = db.Pool.QueryRow(ctx, `SELECT count(*) FROM wallet_ledger_entries WHERE wallet_id = $1::uuid`, w.ID()).Scan(&ledger)
	_ = db.Pool.QueryRow(ctx, `SELECT count(*) FROM outbox_events WHERE aggregate_id = $1::uuid`, w.ID()).Scan(&events)
	if ledger != 1 || events != 2 {
		t.Fatalf("opening produced ledger=%d events=%d", ledger, events)
	}
	// zero balance: wallet only
	z, err := n.ws.Open(ctx, domain.NewID(), money(t, "0.00"), "")
	if err != nil {
		t.Fatal(err)
	}
	_ = db.Pool.QueryRow(ctx, `SELECT count(*) FROM wager_transactions WHERE wallet_id = $1::uuid`, z.ID()).Scan(&ledger)
	if ledger != 0 {
		t.Fatal("zero-balance wallet must not create an OPENING transaction")
	}
	// parallel opens for the same player: exactly one wallet
	pl := domain.NewID()
	var okc, dup atomic.Int32
	parallel(10, func(i int) {
		_, err := n.ws.Open(ctx, pl, money(t, "5.00"), "")
		switch {
		case err == nil:
			okc.Add(1)
		case errors.Is(err, app.ErrWalletExists):
			dup.Add(1)
		default:
			t.Error(err)
		}
	})
	if okc.Load() != 1 || dup.Load() != 9 {
		t.Fatalf("ok=%d dup=%d", okc.Load(), dup.Load())
	}
}

// ---- database-enforced invariants -----------------------------------------

func TestDatabaseRefusesWhatTheApplicationShouldNeverDo(t *testing.T) {
	s := newScenario(t, "100.00", 1)
	out := s.do(0, "BET", "bet-1", "10.00", "")
	var entryID string
	if err := db.Pool.QueryRow(ctx, `SELECT id::text FROM wallet_ledger_entries WHERE wallet_id = $1::uuid ORDER BY seq LIMIT 1`, s.wallet).Scan(&entryID); err != nil {
		t.Fatal(err)
	}
	cases := []struct{ name, sql string }{
		{"ledger UPDATE", fmt.Sprintf(`UPDATE wallet_ledger_entries SET amount_minor = 1 WHERE id = '%s'`, entryID)},
		{"ledger DELETE", fmt.Sprintf(`DELETE FROM wallet_ledger_entries WHERE id = '%s'`, entryID)},
		{"ledger TRUNCATE", `TRUNCATE wallet_ledger_entries`},
		{"negative balance", fmt.Sprintf(`UPDATE wallets SET balance_minor = -1, version = version + 1 WHERE id = '%s'`, s.wallet)},
		{"balance change without version bump", fmt.Sprintf(`UPDATE wallets SET balance_minor = balance_minor + 1 WHERE id = '%s'`, s.wallet)},
		{"version change without balance change", fmt.Sprintf(`UPDATE wallets SET version = version + 1 WHERE id = '%s'`, s.wallet)},
		{"version jump", fmt.Sprintf(`UPDATE wallets SET balance_minor = balance_minor + 1, version = version + 5 WHERE id = '%s'`, s.wallet)},
		{"wallet currency change", fmt.Sprintf(`UPDATE wallets SET currency = 'USD' WHERE id = '%s'`, s.wallet)},
		{"wallet DELETE", fmt.Sprintf(`DELETE FROM wallets WHERE id = '%s'`, s.wallet)},
		{"terminal transaction UPDATE", fmt.Sprintf(`UPDATE wager_transactions SET status = 'REJECTED', failure_code = 'X' WHERE id = '%s'`, out.TransactionID)},
		{"transaction DELETE", fmt.Sprintf(`DELETE FROM wager_transactions WHERE id = '%s'`, out.TransactionID)},
		{"outbox payload rewrite", fmt.Sprintf(`UPDATE outbox_events SET payload = '{}'::jsonb WHERE aggregate_id = '%s'`, s.wallet)},
		{"ledger duplicate (wallet, transaction)", fmt.Sprintf(`INSERT INTO wallet_ledger_entries (id, wallet_id, transaction_id, direction, amount_minor, balance_before_minor, balance_after_minor, created_at)
			VALUES ('%s', '%s', '%s', 'DEBIT', 1, 100, 99, now())`, domain.NewID(), s.wallet, out.TransactionID)},
		{"ledger arithmetic", fmt.Sprintf(`INSERT INTO wallet_ledger_entries (id, wallet_id, transaction_id, direction, amount_minor, balance_before_minor, balance_after_minor, created_at)
			VALUES ('%s', '%s', '%s', 'CREDIT', 5, 100, 50, now())`, domain.NewID(), s.wallet, domain.NewID())},
		{"second OPENING", fmt.Sprintf(`INSERT INTO wager_transactions (id, origin, wallet_id, player_id, kind, amount_minor, currency, status, result_balance_minor, created_at, updated_at)
			VALUES ('%s', 'INTERNAL', '%s', '%s', 'OPENING', 5, 'BRL', 'PROCESSED', 5, now(), now())`, domain.NewID(), s.wallet, s.player)},
		{"LOSS with amount", fmt.Sprintf(`INSERT INTO wager_transactions (id, origin, provider_id, external_transaction_id, idempotency_key, payload_hash, wallet_id, player_id, round_id, game_id, kind, amount_minor, currency, status, created_at, updated_at)
			VALUES ('%s', 'EXTERNAL', 'p', 'e', 'k', '%s', '%s', '%s', 'r', 'g', 'LOSS', 5, 'BRL', 'PENDING', now(), now())`, domain.NewID(), strings.Repeat("a", 64), s.wallet, s.player)},
		{"BET with zero amount", fmt.Sprintf(`INSERT INTO wager_transactions (id, origin, provider_id, external_transaction_id, idempotency_key, payload_hash, wallet_id, player_id, round_id, game_id, kind, amount_minor, currency, status, created_at, updated_at)
			VALUES ('%s', 'EXTERNAL', 'p', 'e2', 'k2', '%s', '%s', '%s', 'r', 'g', 'BET', 0, 'BRL', 'PENDING', now(), now())`, domain.NewID(), strings.Repeat("a", 64), s.wallet, s.player)},
		{"external transaction without provider", fmt.Sprintf(`INSERT INTO wager_transactions (id, origin, wallet_id, player_id, kind, amount_minor, currency, status, created_at, updated_at)
			VALUES ('%s', 'EXTERNAL', '%s', '%s', 'BET', 5, 'BRL', 'PENDING', now(), now())`, domain.NewID(), s.wallet, s.player)},
		{"REFUND without reference", fmt.Sprintf(`INSERT INTO wager_transactions (id, origin, provider_id, external_transaction_id, idempotency_key, payload_hash, wallet_id, player_id, round_id, game_id, kind, amount_minor, currency, status, created_at, updated_at)
			VALUES ('%s', 'EXTERNAL', 'p', 'e3', 'k3', '%s', '%s', '%s', 'r', 'g', 'REFUND', 5, 'BRL', 'PENDING', now(), now())`, domain.NewID(), strings.Repeat("a", 64), s.wallet, s.player)},
		{"unknown status", fmt.Sprintf(`INSERT INTO wager_transactions (id, origin, provider_id, external_transaction_id, idempotency_key, payload_hash, wallet_id, player_id, round_id, game_id, kind, amount_minor, currency, status, created_at, updated_at)
			VALUES ('%s', 'EXTERNAL', 'p', 'e4', 'k4', '%s', '%s', '%s', 'r', 'g', 'BET', 5, 'BRL', 'DONE', now(), now())`, domain.NewID(), strings.Repeat("a", 64), s.wallet, s.player)},
	}
	for _, c := range cases {
		if _, err := db.Pool.Exec(ctx, c.sql); err == nil {
			t.Errorf("%s: the database accepted it", c.name)
		}
	}
	s.requireConsistent()
	if s.balance() != "90.00" {
		t.Fatalf("invariant violations changed data: %s", s.balance())
	}
}

func TestFailedAndPendingStatesAreRecordedForAudit(t *testing.T) {
	s := newScenario(t, "100.00", 1)
	out := s.do(0, "REFUND", "refund-x", "5.00", "bet-x")
	if out.Status != domain.StatusPendingReference {
		t.Fatal(out.Status)
	}
	// permanent internal failure path: FAILED is terminal and keeps its code
	var id string
	_ = db.Pool.QueryRow(ctx, `SELECT id::text FROM wager_transactions WHERE wallet_id=$1::uuid AND status='PENDING_REFERENCE'`, s.wallet).Scan(&id)
	if _, err := db.Pool.Exec(ctx, `UPDATE wager_transactions SET status='FAILED', failure_code='INTERNAL_ERROR', next_attempt_at=NULL WHERE id=$1::uuid`, id); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Pool.Exec(ctx, `UPDATE wager_transactions SET status='PROCESSED', failure_code=NULL, result_balance_minor=1 WHERE id=$1::uuid`, id); err == nil {
		t.Fatal("FAILED must be terminal")
	}
	got, err := s.nodes[0].ws.Transaction(ctx, id)
	if err != nil || got.Status() != domain.StatusFailed || got.FailureCode() != domain.FailInternalError {
		t.Fatalf("%v %v", got, err)
	}
}

// flakyStore loses every publication confirmation: the process "dies" after
// the event reached the broker and before the outbox row was marked published.
type flakyStore struct{ *postgres.Store }

func (f flakyStore) MarkPublished(context.Context, string) error {
	return errors.New("crashed between publishing and confirming")
}

func TestOutboxCrashBetweenPublishAndConfirmRepublishesTheSameEventID(t *testing.T) {
	drainOutbox(t)
	s := newScenario(t, "100.00", 2)
	s.do(0, "BET", "b1", "10.00", "")
	s.do(1, "BET", "b2", "10.00", "")
	opts := app.PublisherOptions{Batch: 50, Lease: 500 * time.Millisecond}

	sinkA, sinkB := &collectSink{seen: map[string]int{}}, &collectSink{seen: map[string]int{}}
	pubA := app.NewOutboxPublisher(flakyStore{s.nodes[0].st}, sinkA, "A", opts, nil, log)
	pubB := app.NewOutboxPublisher(s.nodes[1].st, sinkB, "B", opts, nil, log)

	if n, err := pubA.RunOnce(ctx); err != nil || n != 0 {
		t.Fatalf("A cannot confirm anything: n=%d err=%v", n, err)
	}
	if len(sinkA.seen) != 6 { // opening 2 + two bets 2 each: published but never confirmed
		t.Fatalf("A published %d events", len(sinkA.seen))
	}
	if n, _ := pubB.RunOnce(ctx); n != 0 {
		t.Fatal("B must not take records whose lease is still alive")
	}
	time.Sleep(700 * time.Millisecond) // A's lease expires
	if n, err := pubB.RunOnce(ctx); err != nil || n != 6 {
		t.Fatalf("B takeover: n=%d err=%v", n, err)
	}
	for id := range sinkA.seen {
		if sinkB.seen[id] != 1 {
			t.Errorf("event %s was not republished with the same eventId", id)
		}
	}
	var unpublished int
	_ = db.Pool.QueryRow(ctx, `SELECT count(*) FROM outbox_events WHERE aggregate_id=$1::uuid AND published_at IS NULL`, s.wallet).Scan(&unpublished)
	if unpublished != 0 {
		t.Fatalf("%d events left unpublished", unpublished)
	}
}
