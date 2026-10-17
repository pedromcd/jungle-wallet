package domain

import (
	"errors"
	"strings"
	"testing"
)

const goodHash = "0000000000000000000000000000000000000000000000000000000000000000"

func params(t *testing.T, kind Kind, amount string) ExternalParams {
	t.Helper()
	return ExternalParams{
		ID: NewID(), ProviderID: "provider-a", ExternalID: "tx-1", IdempotencyKey: "provider-a:tx-1",
		PayloadHash: goodHash, WalletID: walletID, PlayerID: playerID, RoundID: "round-1", GameID: "game-1",
		Kind: kind, Money: mustMoney(t, amount, "BRL"),
	}
}

func TestNewExternalTransactionRules(t *testing.T) {
	// OPENING is internal only.
	if _, err := NewExternalTransaction(params(t, KindOpening, "1.00"), t0); !errors.Is(err, ErrOpeningNotAllowed) {
		t.Fatalf("OPENING must be rejected, got %v", err)
	}
	// zero policy: LOSS exactly zero, everything else strictly positive.
	cases := []struct {
		kind   Kind
		amount string
		ref    string
		ok     bool
	}{
		{KindBet, "25.00", "", true}, {KindBet, "0.00", "", false},
		{KindWin, "10.00", "", true}, {KindWin, "10.00", "bet-1", true}, {KindWin, "0.00", "", false},
		{KindLoss, "0.00", "", true}, {KindLoss, "1.00", "", false},
		{KindRefund, "25.00", "bet-1", true}, {KindRefund, "25.00", "", false}, {KindRefund, "0.00", "bet-1", false},
		{KindRollback, "25.00", "bet-1", true}, {KindRollback, "25.00", "", false},
		{KindBet, "25.00", "bet-1", false}, {KindLoss, "0.00", "bet-1", false},
		{"TELEPORT", "1.00", "", false},
	}
	for _, c := range cases {
		p := params(t, c.kind, c.amount)
		p.ReferenceExternalID = c.ref
		_, err := NewExternalTransaction(p, t0)
		if (err == nil) != c.ok {
			t.Errorf("%s %s ref=%q: err=%v, wantOK=%v", c.kind, c.amount, c.ref, err, c.ok)
		}
	}
	// invalid identifiers / hash / money
	p := params(t, KindBet, "1.00")
	p.ProviderID = "bad provider"
	if _, err := NewExternalTransaction(p, t0); err == nil {
		t.Error("identifier with space accepted")
	}
	p = params(t, KindBet, "1.00")
	p.PayloadHash = "xyz"
	if _, err := NewExternalTransaction(p, t0); err == nil {
		t.Error("bad hash accepted")
	}
	p = params(t, KindBet, "1.00")
	p.Money = Money{}
	if _, err := NewExternalTransaction(p, t0); !errors.Is(err, ErrUninitializedMoney) {
		t.Error("uninitialized money accepted")
	}
	p = params(t, KindBet, "1.00")
	p.IdempotencyKey = strings.Repeat("k", 256)
	if _, err := NewExternalTransaction(p, t0); err == nil {
		t.Error("oversized idempotency key accepted")
	}
}

func TestStateMachine(t *testing.T) {
	bal := mustMoney(t, "20.00", "BRL")
	newRef := func() *WagerTransaction {
		p := params(t, KindRefund, "25.00")
		p.ReferenceExternalID = "bet-1"
		tx, err := NewExternalTransaction(p, t0)
		if err != nil {
			t.Fatal(err)
		}
		return tx
	}
	// PENDING -> PENDING_REFERENCE -> PENDING_REFERENCE -> PROCESSED
	tx := newRef()
	if tx.Status() != StatusPending {
		t.Fatal("must start PENDING")
	}
	next := t0.Add(2 * 1e9)
	if err := tx.WaitForReference(0, next, t0); err != nil || tx.Status() != StatusPendingReference {
		t.Fatalf("wait: %v", err)
	}
	if err := tx.WaitForReference(1, next, t0); err != nil || tx.Attempts() != 1 {
		t.Fatalf("re-wait: %v", err)
	}
	if err := tx.MarkProcessed(bal, "ref-id", t0); err != nil || !tx.IsTerminal() || tx.NextAttemptAt() != nil {
		t.Fatalf("processed: %v", err)
	}
	// terminal states never move again
	if err := tx.Reject(FailReferenceNotFound, bal, t0); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("PROCESSED -> REJECTED must fail: %v", err)
	}
	if err := tx.MarkProcessed(bal, "", t0); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("PROCESSED -> PROCESSED must fail: %v", err)
	}
	if err := tx.Fail(FailInternalError, t0); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("PROCESSED -> FAILED must fail: %v", err)
	}
	if err := tx.WaitForReference(0, next, t0); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("PROCESSED -> PENDING_REFERENCE must fail: %v", err)
	}
	// rejection needs code and balance
	tx = newRef()
	if err := tx.Reject("", bal, t0); err == nil {
		t.Fatal("rejection without code accepted")
	}
	if err := tx.Reject(FailReferenceNotFound, bal, t0); err != nil || tx.FailureCode() != FailReferenceNotFound {
		t.Fatalf("reject: %v", err)
	}
	if b, ok := tx.ResultBalance(); !ok || b.Amount() != "20.00" {
		t.Fatal("result balance not recorded")
	}
	// FAILED is terminal too
	tx = newRef()
	if err := tx.Fail(FailInternalError, t0); err != nil || tx.Status() != StatusFailed {
		t.Fatalf("fail: %v", err)
	}
	if err := tx.Reject(FailReferenceNotFound, bal, t0); !errors.Is(err, ErrInvalidTransition) {
		t.Fatal("FAILED must be terminal")
	}
	// a BET has nothing to wait for
	bet, _ := NewExternalTransaction(params(t, KindBet, "1.00"), t0)
	if err := bet.WaitForReference(0, next, t0); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("BET cannot wait for a reference: %v", err)
	}
}

func TestOpeningTransactionAndRehydration(t *testing.T) {
	op, err := NewOpeningTransaction(NewID(), walletID, playerID, mustMoney(t, "1000.00", "BRL"), t0)
	if err != nil || op.Origin() != OriginInternal || op.Kind() != KindOpening || op.ProviderID() != "" || op.ExternalID() != "" {
		t.Fatalf("opening: %v", err)
	}
	if _, err := NewOpeningTransaction(NewID(), walletID, playerID, mustMoney(t, "0.00", "BRL"), t0); err == nil {
		t.Fatal("zero OPENING must not exist")
	}
	// rehydration reproduces the state without triggering transitions
	if err := op.MarkProcessed(mustMoney(t, "1000.00", "BRL"), "", t0); err != nil {
		t.Fatal(err)
	}
	back, err := RehydrateTransaction(op.Snapshot())
	if err != nil || back.Status() != StatusProcessed || back.ID() != op.ID() {
		t.Fatalf("rehydrate: %v", err)
	}
	if err := back.Reject(FailReferenceNotFound, mustMoney(t, "1.00", "BRL"), t0); !errors.Is(err, ErrInvalidTransition) {
		t.Fatal("rehydrated terminal transaction accepted a transition")
	}
	snap := op.Snapshot()
	snap.Status = "WEIRD"
	if _, err := RehydrateTransaction(snap); err == nil {
		t.Fatal("unknown persisted status accepted")
	}
}

func tx(t *testing.T, kind Kind, amount, ext, ref string, mutate func(*ExternalParams)) *WagerTransaction {
	t.Helper()
	p := params(t, kind, amount)
	p.ExternalID, p.ReferenceExternalID = ext, ref
	if mutate != nil {
		mutate(&p)
	}
	x, err := NewExternalTransaction(p, t0)
	if err != nil {
		t.Fatalf("tx %s: %v", kind, err)
	}
	return x
}

func processed(t *testing.T, x *WagerTransaction) *WagerTransaction {
	t.Helper()
	if err := x.MarkProcessed(mustMoney(t, "0.00", "BRL"), "", t0); err != nil {
		t.Fatal(err)
	}
	return x
}

func TestEvaluateReference(t *testing.T) {
	bet := processed(t, tx(t, KindBet, "25.00", "bet-1", "", nil))
	win := processed(t, tx(t, KindWin, "50.00", "win-1", "", nil))
	refundOK := processed(t, tx(t, KindRefund, "25.00", "ref-1", "bet-1", nil))

	type c struct {
		name     string
		op, ref  *WagerTransaction
		reversed bool
		want     Verdict
		code     FailureCode
	}
	pendingBet := tx(t, KindBet, "25.00", "bet-p", "", nil) // still PENDING
	rejBet := tx(t, KindBet, "25.00", "bet-r", "", nil)
	_ = rejBet.Reject(FailInsufficientFunds, mustMoney(t, "0.00", "BRL"), t0)
	otherPlayer := processed(t, tx(t, KindBet, "25.00", "bet-o", "", func(p *ExternalParams) {
		p.PlayerID = "0192f28f-5dc0-7d58-bdb2-814ad6a0f4a2"
	}))
	otherRound := processed(t, tx(t, KindBet, "25.00", "bet-rd", "", func(p *ExternalParams) { p.RoundID = "round-2" }))
	otherCur := processed(t, tx(t, KindBet, "25.00", "bet-c", "", func(p *ExternalParams) { p.Money = mustMoney(t, "25.00", "USD") }))

	cases := []c{
		{"refund of processed bet", tx(t, KindRefund, "25.00", "r", "bet-1", nil), bet, false, Proceed, ""},
		{"rollback of bet", tx(t, KindRollback, "25.00", "r", "bet-1", nil), bet, false, Proceed, ""},
		{"rollback of win", tx(t, KindRollback, "50.00", "r", "win-1", nil), win, false, Proceed, ""},
		{"rollback of refund", tx(t, KindRollback, "25.00", "r", "ref-1", nil), refundOK, false, Proceed, ""},
		{"win referencing bet", tx(t, KindWin, "99.00", "w", "bet-1", nil), bet, false, Proceed, ""},
		{"reference missing", tx(t, KindRefund, "25.00", "r", "bet-x", nil), nil, false, Wait, ""},
		{"reference still pending", tx(t, KindRefund, "25.00", "r", "bet-p", nil), pendingBet, false, Wait, ""},
		{"reference rejected", tx(t, KindRefund, "25.00", "r", "bet-r", nil), rejBet, false, RejectVerdict, FailReferenceNotSuccessful},
		{"refund of win", tx(t, KindRefund, "50.00", "r", "win-1", nil), win, false, RejectVerdict, FailReferenceInvalidKind},
		{"rollback of rollback", tx(t, KindRollback, "25.00", "r", "bet-1", nil),
			processed(t, tx(t, KindRollback, "25.00", "rb", "bet-1", nil)), false, RejectVerdict, FailReferenceInvalidKind},
		{"win referencing a win", tx(t, KindWin, "1.00", "w", "win-1", nil), win, false, RejectVerdict, FailReferenceInvalidKind},
		{"partial reversal", tx(t, KindRefund, "10.00", "r", "bet-1", nil), bet, false, RejectVerdict, FailReferenceAmountMismatch},
		{"different player", tx(t, KindRefund, "25.00", "r", "bet-o", nil), otherPlayer, false, RejectVerdict, FailReferenceMismatch},
		{"different round", tx(t, KindRefund, "25.00", "r", "bet-rd", nil), otherRound, false, RejectVerdict, FailReferenceMismatch},
		{"different currency", tx(t, KindRefund, "25.00", "r", "bet-c", nil), otherCur, false, RejectVerdict, FailReferenceMismatch},
		{"already reversed refund", tx(t, KindRefund, "25.00", "r", "bet-1", nil), bet, true, RejectVerdict, FailReferenceAlreadyReversed},
		{"already reversed -> rollback blocked too", tx(t, KindRollback, "25.00", "r", "bet-1", nil), bet, true, RejectVerdict, FailReferenceAlreadyReversed},
	}
	for _, tc := range cases {
		got := EvaluateReference(tc.op, tc.ref, tc.reversed)
		if got.Verdict != tc.want || got.Code != tc.code {
			t.Errorf("%s: got %+v, want verdict=%d code=%q", tc.name, got, tc.want, tc.code)
		}
	}
}

func TestMovementDirection(t *testing.T) {
	bet := processed(t, tx(t, KindBet, "25.00", "bet-1", "", nil))
	win := processed(t, tx(t, KindWin, "50.00", "win-1", "", nil))
	refund := processed(t, tx(t, KindRefund, "25.00", "ref-1", "bet-1", nil))
	cases := []struct {
		op, ref *WagerTransaction
		want    Direction
	}{
		{bet, nil, Debit},
		{win, nil, Credit},
		{refund, bet, Credit},
		{tx(t, KindRollback, "25.00", "r1", "bet-1", nil), bet, Credit},   // undo a debit
		{tx(t, KindRollback, "50.00", "r2", "win-1", nil), win, Debit},    // undo a credit
		{tx(t, KindRollback, "25.00", "r3", "ref-1", nil), refund, Debit}, // undo a credit
	}
	for i, c := range cases {
		if got := MovementDirection(c.op, c.ref); got != c.want {
			t.Errorf("case %d (%s): got %s want %s", i, c.op.Kind(), got, c.want)
		}
	}
}

func TestNewIDAndParseID(t *testing.T) {
	a, b := NewID(), NewID()
	if a == b {
		t.Fatal("ids must be unique")
	}
	if _, err := ParseID(a); err != nil {
		t.Fatalf("generated id invalid: %v", err)
	}
	if a[14] != '7' {
		t.Fatalf("not a v7 uuid: %s", a)
	}
	if got, err := ParseID(strings.ToUpper(walletID)); err != nil || got != walletID {
		t.Fatalf("canonicalisation: %q %v", got, err)
	}
	for _, bad := range []string{"", "abc", walletID + "0", strings.Repeat("g", 36)} {
		if _, err := ParseID(bad); err == nil {
			t.Errorf("ParseID(%q) accepted", bad)
		}
	}
}
