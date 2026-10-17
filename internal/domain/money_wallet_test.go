package domain

import (
	"encoding/json"
	"errors"
	"math"
	"testing"
	"time"
)

func mustMoney(t *testing.T, amount, cur string) Money {
	t.Helper()
	m, err := ParseMoney(amount, cur)
	if err != nil {
		t.Fatalf("ParseMoney(%q,%q): %v", amount, cur, err)
	}
	return m
}

func TestParseMoney(t *testing.T) {
	good := map[string]int64{"0.00": 0, "0.01": 1, "25.00": 2500, "1000.50": 100050, "92233720368547758.07": math.MaxInt64}
	for in, want := range good {
		m, err := ParseMoney(in, "BRL")
		if err != nil || m.Minor() != want {
			t.Errorf("ParseMoney(%q) = %v, %v; want %d", in, m, err, want)
		}
		if m.Amount() != in {
			t.Errorf("round trip %q -> %q", in, m.Amount())
		}
	}
	bad := []string{"", " ", "25", "25.0", "25.000", "-1.00", "+1.00", "1e3", "1E3", "NaN", "Infinity", "inf",
		".50", "01.00", "1,00", "1.00 ", " 1.00", "0x10", "1.0.0", "٣.00"}
	for _, in := range bad {
		if _, err := ParseMoney(in, "BRL"); !errors.Is(err, ErrInvalidAmount) {
			t.Errorf("ParseMoney(%q) err = %v, want ErrInvalidAmount", in, err)
		}
	}
	if _, err := ParseMoney("92233720368547758.08", "BRL"); !errors.Is(err, ErrMoneyOverflow) {
		t.Errorf("overflow by one cent: %v", err)
	}
	if _, err := ParseMoney("99999999999999999999.00", "BRL"); !errors.Is(err, ErrMoneyOverflow) {
		t.Errorf("huge amount: %v", err)
	}
	for _, c := range []string{"", "brl", "BR", "BRLL", "XXX", "JPY"} {
		if _, err := ParseMoney("1.00", c); !errors.Is(err, ErrInvalidCurrency) {
			t.Errorf("currency %q: %v", c, err)
		}
	}
}

func TestMoneyArithmetic(t *testing.T) {
	a, b := mustMoney(t, "10.00", "BRL"), mustMoney(t, "2.50", "BRL")
	sum, _ := a.Add(b)
	diff, _ := b.Sub(a)
	neg, _ := b.Neg()
	if sum.Amount() != "12.50" || diff.Amount() != "-7.50" || neg.Amount() != "-2.50" {
		t.Fatalf("sum=%v diff=%v neg=%v", sum, diff, neg)
	}
	if c, _ := a.Cmp(b); c != 1 {
		t.Fatalf("cmp = %d", c)
	}
	if !a.Equal(mustMoney(t, "10.00", "BRL")) || a.Equal(mustMoney(t, "10.00", "USD")) {
		t.Fatal("Equal must consider amount and currency")
	}
	usd := mustMoney(t, "1.00", "USD")
	if _, err := a.Add(usd); !errors.Is(err, ErrCurrencyMismatch) {
		t.Fatalf("Add mixed currency: %v", err)
	}
	if _, err := a.Sub(usd); !errors.Is(err, ErrCurrencyMismatch) {
		t.Fatalf("Sub mixed currency: %v", err)
	}
	if _, err := a.Cmp(usd); !errors.Is(err, ErrCurrencyMismatch) {
		t.Fatalf("Cmp mixed currency: %v", err)
	}
	var zero Money
	if _, err := zero.Add(a); !errors.Is(err, ErrUninitializedMoney) {
		t.Fatalf("uninitialized: %v", err)
	}
	if _, err := zero.Neg(); !errors.Is(err, ErrUninitializedMoney) {
		t.Fatalf("uninitialized neg: %v", err)
	}
}

func TestMoneyOverflow(t *testing.T) {
	max, _ := NewMoney(math.MaxInt64, "BRL")
	one, _ := NewMoney(1, "BRL")
	if _, err := max.Add(one); !errors.Is(err, ErrMoneyOverflow) {
		t.Fatalf("add overflow: %v", err)
	}
	min, _ := NewMoney(-math.MaxInt64, "BRL")
	if _, err := min.Sub(one); !errors.Is(err, ErrMoneyOverflow) {
		t.Fatalf("sub overflow: %v", err)
	}
	if _, err := NewMoney(math.MinInt64, "BRL"); !errors.Is(err, ErrMoneyOverflow) {
		t.Fatalf("MinInt64 must be rejected: %v", err)
	}
	negMax, _ := max.Neg()
	if back, err := negMax.Neg(); err != nil || back.Minor() != math.MaxInt64 {
		t.Fatalf("neg round trip: %v %v", back, err)
	}
}

func TestMoneyJSON(t *testing.T) {
	m := mustMoney(t, "25.00", "BRL")
	b, err := json.Marshal(m)
	if err != nil || string(b) != `{"amount":"25.00","currency":"BRL"}` {
		t.Fatalf("marshal: %s %v", b, err)
	}
	var back Money
	if err := json.Unmarshal(b, &back); err != nil || !back.Equal(m) {
		t.Fatalf("unmarshal: %v %v", back, err)
	}
	for _, bad := range []string{
		`{"amount":25.00,"currency":"BRL"}`, // JSON number is never accepted
		`{"amount":"25.00"}`, `{"currency":"BRL"}`, `{}`, `null`, `"25.00"`,
		`{"amount":"25.00","currency":"BRL","x":1}`, `{"amount":"-1.00","currency":"BRL"}`,
	} {
		var x Money
		if err := json.Unmarshal([]byte(bad), &x); err == nil {
			t.Errorf("Unmarshal(%s) must fail", bad)
		}
	}
	var zero Money
	if _, err := json.Marshal(zero); err == nil {
		t.Error("marshal of uninitialized Money must fail")
	}
}

const (
	walletID = "0192f291-27dd-7d3f-8071-5f8685deef37"
	playerID = "0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1"
)

var t0 = time.Date(2026, 10, 16, 12, 0, 0, 0, time.UTC)

func TestWalletInvariants(t *testing.T) {
	w, err := OpenWallet(walletID, playerID, mustMoney(t, "100.00", "BRL"), t0)
	if err != nil || w.Version() != 1 {
		t.Fatalf("open: %v v=%d", err, w.Version())
	}
	mv, err := w.Apply(Debit, mustMoney(t, "80.00", "BRL"), t0)
	if err != nil || w.Balance().Amount() != "20.00" || w.Version() != 2 || mv.Before.Amount() != "100.00" || mv.After.Amount() != "20.00" {
		t.Fatalf("debit: %v %+v", err, mv)
	}
	// insufficient funds must leave the wallet untouched
	if _, err := w.Apply(Debit, mustMoney(t, "80.00", "BRL"), t0); !errors.Is(err, ErrInsufficientFunds) {
		t.Fatalf("expected insufficient funds, got %v", err)
	}
	if w.Balance().Amount() != "20.00" || w.Version() != 2 {
		t.Fatal("failed debit mutated the wallet")
	}
	// exact balance is allowed
	if _, err := w.Apply(Debit, mustMoney(t, "20.00", "BRL"), t0); err != nil || !w.Balance().IsZero() {
		t.Fatalf("debit to zero: %v", err)
	}
	if _, err := w.Apply(Credit, mustMoney(t, "5.00", "USD"), t0); !errors.Is(err, ErrCurrencyMismatch) {
		t.Fatalf("currency mismatch: %v", err)
	}
	if _, err := w.Apply(Credit, mustMoney(t, "0.00", "BRL"), t0); !errors.Is(err, ErrInvalidAmount) {
		t.Fatalf("zero movement: %v", err)
	}
	if _, err := w.Apply("SIDEWAYS", mustMoney(t, "1.00", "BRL"), t0); !errors.Is(err, ErrInvalidEntity) {
		t.Fatalf("direction: %v", err)
	}
	var zero Money
	if _, err := w.Apply(Credit, zero, t0); !errors.Is(err, ErrUninitializedMoney) {
		t.Fatalf("uninitialized: %v", err)
	}
}

func TestWalletConstruction(t *testing.T) {
	if _, err := OpenWallet("nope", playerID, mustMoney(t, "1.00", "BRL"), t0); !errors.Is(err, ErrInvalidEntity) {
		t.Fatal("bad wallet id accepted")
	}
	if _, err := OpenWallet(walletID, "nope", mustMoney(t, "1.00", "BRL"), t0); !errors.Is(err, ErrInvalidEntity) {
		t.Fatal("bad player id accepted")
	}
	if _, err := OpenWallet(walletID, playerID, Money{}, t0); !errors.Is(err, ErrUninitializedMoney) {
		t.Fatal("uninitialized balance accepted")
	}
	neg, _ := NewMoney(-1, "BRL")
	if _, err := OpenWallet(walletID, playerID, neg, t0); !errors.Is(err, ErrInvalidEntity) {
		t.Fatal("negative balance accepted")
	}
	// rehydration does not change version or balance
	w, err := RehydrateWallet(walletID, playerID, mustMoney(t, "7.00", "BRL"), 9, t0, t0)
	if err != nil || w.Version() != 9 || w.Balance().Amount() != "7.00" {
		t.Fatalf("rehydrate: %v", err)
	}
	if _, err := RehydrateWallet(walletID, playerID, mustMoney(t, "7.00", "BRL"), 0, t0, t0); err == nil {
		t.Fatal("version 0 accepted")
	}
}

func TestLedgerEntryValidation(t *testing.T) {
	before, amt, after := mustMoney(t, "100.00", "BRL"), mustMoney(t, "80.00", "BRL"), mustMoney(t, "20.00", "BRL")
	if _, err := NewLedgerEntry("e1", walletID, "t1", Debit, amt, before, after, t0); err != nil {
		t.Fatalf("valid debit entry: %v", err)
	}
	if _, err := NewLedgerEntry("e1", walletID, "t1", Credit, amt, before, after, t0); err == nil {
		t.Fatal("credit with decreasing balance accepted")
	}
	if _, err := NewLedgerEntry("e1", walletID, "t1", Debit, amt, before, mustMoney(t, "21.00", "BRL"), t0); err == nil {
		t.Fatal("inconsistent balanceAfter accepted")
	}
	if _, err := NewLedgerEntry("e1", walletID, "t1", Debit, mustMoney(t, "0.00", "BRL"), before, before, t0); err == nil {
		t.Fatal("zero entry accepted")
	}
	if _, err := NewLedgerEntry("", walletID, "t1", Debit, amt, before, after, t0); err == nil {
		t.Fatal("empty id accepted")
	}
}
