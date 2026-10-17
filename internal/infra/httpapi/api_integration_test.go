//go:build integration

package httpapi_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/pedro/jungle-wallet/internal/app"
	"github.com/pedro/jungle-wallet/internal/domain"
	"github.com/pedro/jungle-wallet/internal/infra/auth"
	"github.com/pedro/jungle-wallet/internal/infra/httpapi"
	"github.com/pedro/jungle-wallet/internal/infra/postgres"
	"github.com/pedro/jungle-wallet/internal/infra/telemetry"
	"github.com/pedro/jungle-wallet/internal/testsupport"
)

var db *testsupport.DB

func TestMain(m *testing.M) {
	d, ok, err := testsupport.Setup()
	if err != nil {
		fmt.Fprintln(os.Stderr, "setup:", err)
		os.Exit(1)
	}
	if !ok {
		fmt.Println("TEST_DATABASE_URL not set: skipping HTTP integration tests")
		os.Exit(0)
	}
	db = d
	code := m.Run()
	db.Close()
	os.Exit(code)
}

type fakeVerifier map[string]*auth.Principal

func (f fakeVerifier) Verify(_ context.Context, raw string) (*auth.Principal, error) {
	if p, ok := f[raw]; ok {
		return p, nil
	}
	return nil, auth.ErrUnauthenticated
}

const (
	tokA        = "tok-provider-a"
	tokB        = "tok-provider-b"
	tokInternal = "tok-internal"
)

type server struct {
	t   *testing.T
	url string
}

func newServer(t *testing.T) *server {
	t.Helper()
	pool, err := db.NewPool()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	st := postgres.NewStore(pool, nil)
	h := httpapi.NewHandler(httpapi.Deps{
		Processor: app.NewProcessor(st, st, app.Options{BackoffBase: 20 * 1e6}, nil, log),
		Wallets:   app.NewWalletService(st, st, nil, log),
		Verifier: fakeVerifier{
			tokA:        auth.NewPrincipal("sa-a", "provider-a", "provider"),
			tokB:        auth.NewPrincipal("sa-b", "provider-b", "provider"),
			tokInternal: auth.NewPrincipal("sa-i", "wallet-internal", "wallet-internal"),
		},
		Checks:       []httpapi.Check{{Name: "postgres", Fn: pool.Ping}},
		Metrics:      telemetry.NewMetrics(),
		Log:          log,
		ProviderRole: "provider", InternalRole: "wallet-internal",
	})
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return &server{t: t, url: srv.URL}
}

type resp struct {
	code int
	hdr  http.Header
	body map[string]any
	raw  string
}

func (s *server) call(method, path, token, idem, body string) resp {
	s.t.Helper()
	req, _ := http.NewRequest(method, s.url+path, strings.NewReader(body))
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if idem != "" {
		req.Header.Set("Idempotency-Key", idem)
	}
	req.Header.Set("Content-Type", "application/json")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		s.t.Fatal(err)
	}
	defer res.Body.Close()
	raw, _ := io.ReadAll(res.Body)
	r := resp{code: res.StatusCode, hdr: res.Header, raw: string(raw), body: map[string]any{}}
	_ = json.Unmarshal(raw, &r.body)
	return r
}

func (r resp) str(path ...string) string {
	var cur any = r.body
	for _, p := range path {
		m, ok := cur.(map[string]any)
		if !ok {
			return ""
		}
		cur = m[p]
	}
	s, _ := cur.(string)
	return s
}

func (s *server) wallet(initial string) (walletID, playerID string) {
	s.t.Helper()
	playerID = domain.NewID()
	r := s.call("POST", "/wallets", tokInternal, "", fmt.Sprintf(`{"playerId":%q,"initialBalance":{"amount":%q,"currency":"BRL"}}`, playerID, initial))
	if r.code != 201 {
		s.t.Fatalf("create wallet: %d %s", r.code, r.raw)
	}
	return r.str("id"), playerID
}

func betBody(provider, ext, wallet, player, kind, amount, ref string) string {
	refField := ""
	if ref != "" {
		refField = fmt.Sprintf(`,"referenceExternalTransactionId":%q`, ref)
	}
	return fmt.Sprintf(`{"providerId":%q,"externalTransactionId":%q,"playerId":%q,"walletId":%q,"roundId":"r1","gameId":"g1","kind":%q,"money":{"amount":%q,"currency":"BRL"}%s}`,
		provider, ext, player, wallet, kind, amount, refField)
}

func TestAuthenticationAndAuthorizationMatrix(t *testing.T) {
	s := newServer(t)
	w, p := s.wallet("100.00")
	prov := "provider-a"
	body := betBody(prov, "auth-1", w, p, "BET", "10.00", "")

	if r := s.call("POST", "/wagering/transactions", "", "k1", body); r.code != 401 || r.hdr.Get("WWW-Authenticate") == "" || r.str("error", "code") != "UNAUTHENTICATED" {
		t.Fatalf("no token: %d %s", r.code, r.raw)
	}
	if r := s.call("POST", "/wagering/transactions", "garbage", "k1", body); r.code != 401 {
		t.Fatalf("bad token: %d", r.code)
	}
	if r := s.call("POST", "/wagering/transactions", tokInternal, "k1", body); r.code != 403 {
		t.Fatalf("internal service must not submit provider operations: %d", r.code)
	}
	if r := s.call("POST", "/wagering/transactions", tokB, "k1", body); r.code != 403 || r.str("error", "code") != "FORBIDDEN" {
		t.Fatalf("provider-b acting as provider-a must be refused: %d %s", r.code, r.raw)
	}
	// wallet operations are internal only
	for _, c := range [][2]string{{"POST", "/wallets"}, {"GET", "/wallets/" + w}, {"GET", "/wallets/" + w + "/ledger"}, {"POST", "/wallets/" + w + "/reconciliation"}} {
		if r := s.call(c[0], c[1], tokA, "", `{}`); r.code != 403 {
			t.Errorf("%s %s with a provider token: %d", c[0], c[1], r.code)
		}
		if r := s.call(c[0], c[1], "", "", `{}`); r.code != 401 {
			t.Errorf("%s %s without token: %d", c[0], c[1], r.code)
		}
	}
	// none of the refused calls had a financial effect
	if r := s.call("GET", "/wallets/"+w, tokInternal, "", ""); r.str("balance", "amount") != "100.00" {
		t.Fatalf("%s", r.raw)
	}
	if r := s.call("GET", "/health/live", "", "", ""); r.code != 200 {
		t.Fatal(r.code)
	}
}

func TestSubmitFlowStatusCodesAndIdempotency(t *testing.T) {
	s := newServer(t)
	w, p := s.wallet("100.00")
	prov := "provider-a"
	ext := func(n string) string { return n + "-" + domain.NewID()[24:] }
	bet1 := ext("bet")

	r := s.call("POST", "/wagering/transactions", tokA, "key-"+bet1, betBody(prov, bet1, w, p, "BET", "25.00", ""))
	if r.code != 200 || r.str("status") != "PROCESSED" || r.str("balance", "amount") != "75.00" || r.body["idempotentReplay"] != false {
		t.Fatalf("first: %d %s", r.code, r.raw)
	}
	txID := r.str("transactionId")
	if r.hdr.Get("X-Correlation-Id") == "" {
		t.Fatal("correlation id header missing")
	}

	// replay: same answer, flagged
	r2 := s.call("POST", "/wagering/transactions", tokA, "key-"+bet1, betBody(prov, bet1, w, p, "BET", "25.00", ""))
	if r2.code != 200 || r2.body["idempotentReplay"] != true || r2.str("transactionId") != txID || r2.str("balance", "amount") != "75.00" {
		t.Fatalf("replay: %d %s", r2.code, r2.raw)
	}
	// same key + different payload; same external id + other key
	if r := s.call("POST", "/wagering/transactions", tokA, "key-"+bet1, betBody(prov, bet1, w, p, "BET", "26.00", "")); r.code != 409 || r.str("error", "code") != "IDEMPOTENCY_KEY_PAYLOAD_MISMATCH" {
		t.Fatalf("%d %s", r.code, r.raw)
	}
	if r := s.call("POST", "/wagering/transactions", tokA, "other-key", betBody(prov, bet1, w, p, "BET", "25.00", "")); r.code != 409 || r.str("error", "code") != "DUPLICATE_EXTERNAL_TRANSACTION" {
		t.Fatalf("%d %s", r.code, r.raw)
	}
	// missing header
	if r := s.call("POST", "/wagering/transactions", tokA, "", betBody(prov, ext("x"), w, p, "BET", "1.00", "")); r.code != 400 || r.str("error", "code") != "MISSING_IDEMPOTENCY_KEY" {
		t.Fatalf("%d %s", r.code, r.raw)
	}
	// insufficient funds: 422 + failure code, persisted and replayable
	big := ext("big")
	r = s.call("POST", "/wagering/transactions", tokA, "key-"+big, betBody(prov, big, w, p, "BET", "500.00", ""))
	if r.code != 422 || r.str("status") != "REJECTED" || r.str("failureCode") != "INSUFFICIENT_FUNDS" {
		t.Fatalf("%d %s", r.code, r.raw)
	}
	if r = s.call("POST", "/wagering/transactions", tokA, "key-"+big, betBody(prov, big, w, p, "BET", "500.00", "")); r.code != 422 || r.body["idempotentReplay"] != true {
		t.Fatalf("%d %s", r.code, r.raw)
	}
	// refund of a BET that does not exist yet: 202 + Location, then it is queryable
	late, refund := ext("late"), ext("refund")
	r = s.call("POST", "/wagering/transactions", tokA, "key-"+refund, betBody(prov, refund, w, p, "REFUND", "7.00", late))
	if r.code != 202 || r.str("status") != "PENDING_REFERENCE" || r.hdr.Get("Location") != "/providers/"+prov+"/wagering/transactions/"+refund {
		t.Fatalf("%d %s %v", r.code, r.raw, r.hdr)
	}
	g := s.call("GET", r.hdr.Get("Location"), tokA, "", "")
	if g.code != 200 || g.str("status") != "PENDING_REFERENCE" || g.str("referenceExternalTransactionId") != late {
		t.Fatalf("%d %s", g.code, g.raw)
	}
	// full refund of the first BET
	rf := ext("rf")
	r = s.call("POST", "/wagering/transactions", tokA, "key-"+rf, betBody(prov, rf, w, p, "REFUND", "25.00", bet1))
	if r.code != 200 || r.str("balance", "amount") != "100.00" {
		t.Fatalf("%d %s", r.code, r.raw)
	}
	// provider isolation also applies to replays: provider-b reusing provider-a's
	// idempotency key and external id gets its own, independent operation
	// (keys are scoped by provider) and never sees provider-a's result.
	bodyB := betBody("provider-b", bet1, w, p, "BET", "1.00", "")
	rb := s.call("POST", "/wagering/transactions", tokB, "key-"+bet1, bodyB)
	if rb.code != 200 || rb.body["idempotentReplay"] != false || rb.str("transactionId") == txID {
		t.Fatalf("provider-b must not hit provider-a's idempotency record: %d %s", rb.code, rb.raw)
	}
	if v := s.call("GET", "/wagering/transactions/"+txID, tokB, "", ""); v.code != 404 {
		t.Fatalf("%d", v.code)
	}
	// wallet, ledger and reconciliation over HTTP (internal only)
	if g := s.call("GET", "/wallets/"+w, tokInternal, "", ""); g.str("balance", "amount") != "99.00" { // 100 - provider-b's own 1.00 BET
		t.Fatalf("%s", g.raw)
	}
	rec := s.call("POST", "/wallets/"+w+"/reconciliation", tokInternal, "", "")
	if rec.code != 200 || rec.body["consistent"] != true || rec.str("difference", "amount") != "0.00" || rec.str("calculatedBalance", "amount") == "" {
		t.Fatalf("%d %s", rec.code, rec.raw)
	}
	led := s.call("GET", "/wallets/"+w+"/ledger?limit=2", tokInternal, "", "")
	items, _ := led.body["items"].([]any)
	if led.code != 200 || len(items) != 2 || led.str("nextCursor") == "" {
		t.Fatalf("%d %s", led.code, led.raw)
	}
	if bad := s.call("GET", "/wallets/"+w+"/ledger?limit=abc", tokInternal, "", ""); bad.code != 400 {
		t.Fatal(bad.code)
	}
	if bad := s.call("GET", "/wallets/"+w+"/ledger?cursor=zzz", tokInternal, "", ""); bad.code != 400 {
		t.Fatal(bad.code)
	}
	if nf := s.call("GET", "/wallets/"+domain.NewID(), tokInternal, "", ""); nf.code != 404 || nf.str("error", "code") != "WALLET_NOT_FOUND" {
		t.Fatalf("%d %s", nf.code, nf.raw)
	}
	if bad := s.call("GET", "/wallets/not-a-uuid", tokInternal, "", ""); bad.code != 400 {
		t.Fatal(bad.code)
	}

	// transaction visibility: providers only see their own operations
	if v := s.call("GET", "/wagering/transactions/"+txID, tokA, "", ""); v.code != 200 || v.str("providerId") != prov {
		t.Fatalf("%d %s", v.code, v.raw)
	}
	if v := s.call("GET", "/wagering/transactions/"+txID, tokB, "", ""); v.code != 404 {
		t.Fatalf("another provider must not see it: %d", v.code)
	}
	if v := s.call("GET", "/providers/"+prov+"/wagering/transactions/"+bet1, tokB, "", ""); v.code != 403 {
		t.Fatalf("%d", v.code)
	}
	if v := s.call("GET", "/wagering/transactions/"+txID, tokInternal, "", ""); v.code != 200 {
		t.Fatalf("internal must see everything: %d", v.code)
	}
	if v := s.call("GET", "/providers/"+prov+"/wagering/transactions/"+bet1, tokInternal, "", ""); v.code != 200 {
		t.Fatalf("%d", v.code)
	}
}

func TestStrictInputValidation(t *testing.T) {
	s := newServer(t)
	w, p := s.wallet("100.00")
	prov := "provider-a"
	good := func(ext string) string { return betBody(prov, ext, w, p, "BET", "1.00", "") }
	cases := map[string]string{
		"amount as JSON number":    strings.Replace(good("v1"), `"1.00"`, `1.00`, 1),
		"amount without decimals":  strings.Replace(good("v2"), `"1.00"`, `"1"`, 1),
		"three decimals":           strings.Replace(good("v3"), `"1.00"`, `"1.005"`, 1),
		"negative amount":          strings.Replace(good("v4"), `"1.00"`, `"-1.00"`, 1),
		"exponent":                 strings.Replace(good("v5"), `"1.00"`, `"1e2"`, 1),
		"zero BET":                 strings.Replace(good("v6"), `"1.00"`, `"0.00"`, 1),
		"unknown currency":         strings.Replace(good("v7"), `"BRL"`, `"XXX"`, 1),
		"unknown field":            strings.Replace(good("v8"), `"kind"`, `"extra":1,"kind"`, 1),
		"unknown kind":             strings.Replace(good("v9"), `"BET"`, `"TELEPORT"`, 1),
		"OPENING from outside":     strings.Replace(good("v10"), `"BET"`, `"OPENING"`, 1),
		"trailing data":            good("v11") + `{}`,
		"not json":                 `nope`,
		"bad wallet id":            strings.Replace(good("v12"), w, "123", 1),
		"REFUND without reference": strings.Replace(good("v13"), `"BET"`, `"REFUND"`, 1),
		"overflowing amount":       strings.Replace(good("v14"), `"1.00"`, `"99999999999999999999.00"`, 1),
	}
	for name, body := range cases {
		r := s.call("POST", "/wagering/transactions", tokA, "k-"+name, body)
		if r.code != 400 || r.str("error", "code") != "INVALID_REQUEST" {
			t.Errorf("%s: %d %s", name, r.code, r.raw)
		}
	}
	if r := s.call("POST", "/wallets", tokInternal, "", fmt.Sprintf(`{"playerId":%q,"initialBalance":{"amount":"-5.00","currency":"BRL"}}`, domain.NewID())); r.code != 400 {
		t.Errorf("negative opening balance: %d", r.code)
	}
	if r := s.call("POST", "/wallets", tokInternal, "", fmt.Sprintf(`{"playerId":%q}`, domain.NewID())); r.code != 400 {
		t.Errorf("missing opening balance: %d", r.code)
	}
	big := bytes.Repeat([]byte("a"), 2<<20)
	if r := s.call("POST", "/wagering/transactions", tokA, "k-big", `{"x":"`+string(big)+`"}`); r.code != 400 {
		t.Errorf("oversized body: %d", r.code)
	}
	// nothing above touched the wallet
	if g := s.call("GET", "/wallets/"+w, tokInternal, "", ""); g.str("balance", "amount") != "100.00" {
		t.Fatalf("%s", g.raw)
	}
	// duplicate wallet for the same player and currency
	if r := s.call("POST", "/wallets", tokInternal, "", fmt.Sprintf(`{"playerId":%q,"initialBalance":{"amount":"1.00","currency":"BRL"}}`, p)); r.code != 409 || r.str("error", "code") != "WALLET_ALREADY_EXISTS" {
		t.Fatalf("%d %s", r.code, r.raw)
	}
}

func TestDatabaseOutageIsA503WithRetryAfterAndNoEffect(t *testing.T) {
	s := newServer(t)
	w, p := s.wallet("100.00")
	// a second server whose pool is already closed: every DB call fails
	pool, _ := db.NewPool()
	pool.Close()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	st := postgres.NewStore(pool, nil)
	h := httpapi.NewHandler(httpapi.Deps{
		Processor: app.NewProcessor(st, st, app.Options{}, nil, log), Wallets: app.NewWalletService(st, st, nil, log),
		Verifier: fakeVerifier{tokA: auth.NewPrincipal("sa-a", "provider-a", "provider")},
		Checks:   []httpapi.Check{{Name: "postgres", Fn: pool.Ping}}, Log: log, ProviderRole: "provider", InternalRole: "wallet-internal",
	})
	down := httptest.NewServer(h)
	defer down.Close()
	body := betBody("provider-a", "out-1", w, p, "BET", "10.00", "")
	req, _ := http.NewRequest("POST", down.URL+"/wagering/transactions", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+tokA)
	req.Header.Set("Idempotency-Key", "out-1")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != 503 || res.Header.Get("Retry-After") == "" {
		t.Fatalf("status %d retry-after %q", res.StatusCode, res.Header.Get("Retry-After"))
	}
	rd, err := http.Get(down.URL + "/health/ready")
	if err != nil {
		t.Fatal(err)
	}
	defer rd.Body.Close()
	if rd.StatusCode != 503 {
		t.Fatalf("readiness during outage: %d", rd.StatusCode)
	}
	// the healthy server shows the retry is safe and nothing was applied
	if g := s.call("GET", "/wallets/"+w, tokInternal, "", ""); g.str("balance", "amount") != "100.00" {
		t.Fatal(g.raw)
	}
	if r := s.call("POST", "/wagering/transactions", tokA, "out-1", body); r.code != 200 || r.body["idempotentReplay"] != false {
		t.Fatalf("%d %s", r.code, r.raw)
	}
	if r := s.call("GET", "/health/ready", "", "", ""); r.code != 200 {
		t.Fatal(r.code)
	}
	if r := s.call("GET", "/metrics", "", "", ""); r.code != 200 || !strings.Contains(r.raw, "wallet_http_requests_total") {
		t.Fatalf("metrics: %d", r.code)
	}
}
