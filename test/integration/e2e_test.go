//go:build e2e

// End-to-end suite. It talks to the real docker-compose stack: three service
// replicas, PostgreSQL, SQS (LocalStack) and Keycloak. Run it with:
//
//	docker compose up -d --build --wait
//	go test -tags e2e -count=1 -timeout 10m ./test/integration/...
//
// E2E_COMPOSE=1 additionally enables the tests that restart containers.
package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/pedro/jungle-wallet/internal/domain"
	sqsinfra "github.com/pedro/jungle-wallet/internal/infra/sqs"
)

var (
	appURLs   []string
	kcURL     string
	pool      *pgxpool.Pool
	sqsc      *awssqs.Client
	queues    sqsinfra.Queues
	tokens    sync.Map // client -> cachedToken
	bgCtx     = context.Background()
	composeOn = os.Getenv("E2E_COMPOSE") == "1"
)

type cachedToken struct {
	value   string
	expires time.Time
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func TestMain(m *testing.M) {
	appURLs = strings.Split(getenv("APP_URLS", "http://localhost:8080,http://localhost:8082,http://localhost:8083"), ",")
	kcURL = getenv("KEYCLOAK_URL", "http://localhost:8081")
	for k, v := range map[string]string{"AWS_ACCESS_KEY_ID": "test", "AWS_SECRET_ACCESS_KEY": "test"} {
		if os.Getenv(k) == "" {
			os.Setenv(k, v)
		}
	}
	var err error
	if pool, err = pgxpool.New(bgCtx, getenv("DATABASE_URL", "postgres://wallet:wallet@localhost:5432/wallet?sslmode=disable")); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if sqsc, err = sqsinfra.NewClient(bgCtx, "us-east-1", getenv("SQS_ENDPOINT", "http://localhost:4566")); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if queues, err = sqsinfra.Resolve(bgCtx, sqsc, sqsinfra.QueueNames{Transactions: "wager-transactions.fifo",
		DLQ: "wager-transactions-dlq.fifo", Events: "wager-events.fifo"}, 60*time.Second); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	waitReady()
	os.Exit(m.Run())
}

func waitReady() {
	deadline := time.Now().Add(150 * time.Second)
	for _, u := range appURLs {
		for {
			res, err := http.Get(u + "/health/ready")
			if err == nil {
				res.Body.Close()
				if res.StatusCode == 200 {
					break
				}
			}
			if time.Now().After(deadline) {
				fmt.Fprintf(os.Stderr, "%s never became ready\n", u)
				os.Exit(1)
			}
			time.Sleep(time.Second)
		}
	}
}

// ---------------------------------------------------------------- helpers

func token(t testing.TB, client string) string {
	t.Helper()
	if v, ok := tokens.Load(client); ok && time.Now().Before(v.(cachedToken).expires) {
		return v.(cachedToken).value
	}
	form := url.Values{"grant_type": {"client_credentials"}, "client_id": {client}, "client_secret": {client + "-secret"}}
	res, err := http.PostForm(kcURL+"/realms/wallet/protocol/openid-connect/token", form)
	if err != nil {
		t.Fatalf("token request: %v", err)
	}
	defer res.Body.Close()
	var body struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.NewDecoder(res.Body).Decode(&body); err != nil || body.AccessToken == "" {
		t.Fatalf("no token for %s (status %d)", client, res.StatusCode)
	}
	tokens.Store(client, cachedToken{body.AccessToken, time.Now().Add(4 * time.Minute)})
	return body.AccessToken
}

type response struct {
	code int
	body map[string]any
	raw  string
}

func (r response) str(path ...string) string {
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

func call(t testing.TB, base, method, path, tok, idem, body string) response {
	t.Helper()
	req, _ := http.NewRequest(method, base+path, strings.NewReader(body))
	if tok != "" {
		req.Header.Set("Authorization", "Bearer "+tok)
	}
	if idem != "" {
		req.Header.Set("Idempotency-Key", idem)
	}
	req.Header.Set("Content-Type", "application/json")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	defer res.Body.Close()
	raw, _ := io.ReadAll(res.Body)
	r := response{code: res.StatusCode, raw: string(raw), body: map[string]any{}}
	_ = json.Unmarshal(raw, &r.body)
	return r
}

func newWallet(t testing.TB, initial string) (wallet, player string) {
	t.Helper()
	player = domain.NewID()
	r := call(t, appURLs[0], "POST", "/wallets", token(t, "wallet-internal"), "",
		fmt.Sprintf(`{"playerId":%q,"initialBalance":{"amount":%q,"currency":"BRL"}}`, player, initial))
	if r.code != 201 {
		t.Fatalf("create wallet: %d %s", r.code, r.raw)
	}
	return r.str("id"), player
}

func operation(provider, ext, wallet, player, kind, amount, ref string) string {
	r := ""
	if ref != "" {
		r = fmt.Sprintf(`,"referenceExternalTransactionId":%q`, ref)
	}
	return fmt.Sprintf(`{"providerId":%q,"externalTransactionId":%q,"playerId":%q,"walletId":%q,"roundId":"r1","gameId":"g1","kind":%q,"money":{"amount":%q,"currency":"BRL"}%s}`,
		provider, ext, player, wallet, kind, amount, r)
}

func uid(prefix string) string { return prefix + "-" + domain.NewID()[24:] }

func balance(t testing.TB, wallet string) string {
	t.Helper()
	return call(t, appURLs[0], "GET", "/wallets/"+wallet, token(t, "wallet-internal"), "", "").str("balance", "amount")
}

func requireConsistent(t testing.TB, wallet string) {
	t.Helper()
	r := call(t, appURLs[1], "POST", "/wallets/"+wallet+"/reconciliation", token(t, "wallet-internal"), "", "")
	if r.code != 200 || r.body["consistent"] != true {
		t.Fatalf("wallet %s diverged from its ledger: %d %s", wallet, r.code, r.raw)
	}
}

func parallel(n int, fn func(i int)) {
	var start, done sync.WaitGroup
	start.Add(1)
	for i := 0; i < n; i++ {
		done.Add(1)
		go func(i int) { defer done.Done(); start.Wait(); fn(i) }(i)
	}
	start.Done()
	done.Wait()
}

func eventually(t testing.TB, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for: %s", what)
		}
		time.Sleep(300 * time.Millisecond)
	}
}

func sendMessage(t testing.TB, messageID, provider, ext, wallet, player, amount string) {
	t.Helper()
	body := fmt.Sprintf(`{"messageId":%q,"type":"WagerTransactionRequested","occurredAt":%q,"data":{"providerId":%q,"externalTransactionId":%q,"idempotencyKey":"%s:%s","playerId":%q,"walletId":%q,"roundId":"r1","gameId":"g1","kind":"BET","money":{"amount":%q,"currency":"BRL"}}}`,
		messageID, time.Now().UTC().Format(time.RFC3339), provider, ext, provider, ext, player, wallet, amount)
	sendRaw(t, body, wallet, messageID)
}

func sendRaw(t testing.TB, body, group, dedup string) {
	t.Helper()
	if _, err := sqsc.SendMessage(bgCtx, &awssqs.SendMessageInput{QueueUrl: aws.String(queues.Transactions),
		MessageBody: aws.String(body), MessageGroupId: aws.String(group), MessageDeduplicationId: aws.String(dedup)}); err != nil {
		t.Fatalf("send: %v", err)
	}
}

func ledgerEntries(t testing.TB, wallet string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(bgCtx, `SELECT count(*) FROM wallet_ledger_entries WHERE wallet_id = $1::uuid`, wallet).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// ------------------------------------------------------------------ tests

func TestRealKeycloakTokens(t *testing.T) {
	a, internal := token(t, "provider-a"), token(t, "wallet-internal")
	w, p := newWallet(t, "100.00")
	if r := call(t, appURLs[0], "POST", "/wagering/transactions", "", "k", operation("provider-a", uid("x"), w, p, "BET", "1.00", "")); r.code != 401 {
		t.Fatalf("no token: %d", r.code)
	}
	tampered := a[:len(a)-3] + "AAA"
	if tampered == a {
		tampered = a[:len(a)-3] + "BBB"
	}
	if r := call(t, appURLs[0], "POST", "/wagering/transactions", tampered, "k", operation("provider-a", uid("x"), w, p, "BET", "1.00", "")); r.code != 401 {
		t.Fatalf("tampered signature must be refused: %d", r.code)
	}
	if r := call(t, appURLs[0], "POST", "/wallets", a, "", `{}`); r.code != 403 {
		t.Fatalf("provider on an internal endpoint: %d", r.code)
	}
	if r := call(t, appURLs[0], "POST", "/wagering/transactions", internal, "k", operation("provider-a", uid("x"), w, p, "BET", "1.00", "")); r.code != 403 {
		t.Fatalf("internal client acting as a provider: %d", r.code)
	}
	b := token(t, "provider-b")
	if r := call(t, appURLs[0], "POST", "/wagering/transactions", b, "k", operation("provider-a", uid("x"), w, p, "BET", "1.00", "")); r.code != 403 {
		t.Fatalf("provider-b impersonating provider-a: %d", r.code)
	}
	ext := uid("own")
	if r := call(t, appURLs[0], "POST", "/wagering/transactions", a, "k-"+ext, operation("provider-a", ext, w, p, "BET", "1.00", "")); r.code != 200 {
		t.Fatalf("legit call: %d %s", r.code, r.raw)
	}
	if r := call(t, appURLs[0], "GET", "/providers/provider-a/wagering/transactions/"+ext, b, "", ""); r.code != 403 {
		t.Fatalf("provider-b reading provider-a data: %d", r.code)
	}
	if balance(t, w) != "99.00" {
		t.Fatal(balance(t, w))
	}
}

func TestFiftyParallelIdenticalRequestsAcrossThreeReplicasDebitOnce(t *testing.T) {
	w, p := newWallet(t, "100.00")
	ext := uid("par")
	var first, replay atomic.Int32
	parallel(50, func(i int) {
		r := call(t, appURLs[i%len(appURLs)], "POST", "/wagering/transactions", token(t, "provider-a"), "key-"+ext, operation("provider-a", ext, w, p, "BET", "25.00", ""))
		if r.code != 200 || r.str("balance", "amount") != "75.00" {
			t.Errorf("%d %s", r.code, r.raw)
			return
		}
		if r.body["idempotentReplay"] == true {
			replay.Add(1)
		} else {
			first.Add(1)
		}
	})
	if first.Load() != 1 || replay.Load() != 49 || balance(t, w) != "75.00" {
		t.Fatalf("first=%d replay=%d balance=%s", first.Load(), replay.Load(), balance(t, w))
	}
	requireConsistent(t, w)
}

func TestConcurrentDisputeAcrossReplicas(t *testing.T) {
	w, p := newWallet(t, "100.00")
	var ok, rejected atomic.Int32
	parallel(30, func(i int) {
		ext := uid("dis")
		r := call(t, appURLs[i%len(appURLs)], "POST", "/wagering/transactions", token(t, "provider-a"), "key-"+ext, operation("provider-a", ext, w, p, "BET", "80.00", ""))
		switch {
		case r.code == 200:
			ok.Add(1)
		case r.code == 422 && r.str("failureCode") == "INSUFFICIENT_FUNDS":
			rejected.Add(1)
		default:
			t.Errorf("%d %s", r.code, r.raw)
		}
	})
	if ok.Load() != 1 || rejected.Load() != 29 || balance(t, w) != "20.00" {
		t.Fatalf("ok=%d rejected=%d balance=%s", ok.Load(), rejected.Load(), balance(t, w))
	}
	requireConsistent(t, w)
}

func TestSameOperationOverHTTPAndSQSDebitsOnce(t *testing.T) {
	w, p := newWallet(t, "100.00")
	ext, prov := uid("mix"), "provider-a"
	parallel(20, func(i int) {
		if i%2 == 0 {
			r := call(t, appURLs[i%len(appURLs)], "POST", "/wagering/transactions", token(t, prov), "key-"+ext, operation(prov, ext, w, p, "BET", "10.00", ""))
			if r.code != 200 {
				t.Errorf("http: %d %s", r.code, r.raw)
			}
		} else {
			sendMessage(t, uid("msg"), prov, ext, w, p, "10.00") // different messageIds, same business operation
		}
	})
	sendMessage(t, "dup-"+ext, prov, ext, w, p, "10.00") // plus a literal duplicate delivery
	time.Sleep(6 * time.Second)                          // let the consumers drain the queue
	if balance(t, w) != "90.00" || ledgerEntries(t, w) != 2 {
		t.Fatalf("balance=%s ledger=%d", balance(t, w), ledgerEntries(t, w))
	}
	requireConsistent(t, w)
}

func TestSQSPurelyAsyncPathProcessesAndPoisonGoesToDLQ(t *testing.T) {
	w, p := newWallet(t, "50.00")
	ext := uid("sqs")
	sendMessage(t, uid("msg"), "provider-a", ext, w, p, "5.00")
	eventually(t, 60*time.Second, "SQS message applied", func() bool { return balance(t, w) == "45.00" })

	poison := fmt.Sprintf(`{"messageId":%q,"type":"WagerTransactionRequested","data":{"providerId":"provider-a","kind":"BET"}}`, uid("poison"))
	sendRaw(t, poison, w, uid("dedup"))
	found := false
	eventually(t, 90*time.Second, "poison message in the DLQ", func() bool {
		out, err := sqsc.ReceiveMessage(bgCtx, &awssqs.ReceiveMessageInput{QueueUrl: aws.String(queues.DLQ), MaxNumberOfMessages: 10,
			WaitTimeSeconds: 2, VisibilityTimeout: 5, MessageAttributeNames: []string{"All"}})
		if err != nil {
			return false
		}
		for _, m := range out.Messages {
			if aws.ToString(m.Body) == poison {
				found = true
				_, _ = sqsc.DeleteMessage(bgCtx, &awssqs.DeleteMessageInput{QueueUrl: aws.String(queues.DLQ), ReceiptHandle: m.ReceiptHandle})
			}
		}
		return found
	})
	if balance(t, w) != "45.00" {
		t.Fatal("poison message changed the balance")
	}
}

func TestEventsAreDeliveredWithEnvelopeAndMatchTheLedger(t *testing.T) {
	w, p := newWallet(t, "100.00")
	for i := 0; i < 5; i++ {
		ext := uid("ev")
		call(t, appURLs[i%3], "POST", "/wagering/transactions", token(t, "provider-a"), "key-"+ext, operation("provider-a", ext, w, p, "BET", "1.00", ""))
	}
	ext := uid("evbig")
	call(t, appURLs[0], "POST", "/wagering/transactions", token(t, "provider-a"), "key-"+ext, operation("provider-a", ext, w, p, "BET", "9999.00", "")) // rejected
	type event struct {
		EventID, EventType, AggregateID, CorrelationID string
		Version                                        int
		OccurredAt                                     time.Time
		Data                                           map[string]any
	}
	seen := map[string]event{}
	eventually(t, 90*time.Second, "all events of the wallet on the events queue", func() bool {
		out, err := sqsc.ReceiveMessage(bgCtx, &awssqs.ReceiveMessageInput{QueueUrl: aws.String(queues.Events), MaxNumberOfMessages: 10,
			WaitTimeSeconds: 2, VisibilityTimeout: 3, MessageAttributeNames: []string{"All"}})
		if err != nil {
			return false
		}
		for _, m := range out.Messages {
			var e event
			if json.Unmarshal([]byte(aws.ToString(m.Body)), &e) == nil && e.AggregateID == w {
				seen[e.EventID] = e // redelivery keeps the same eventId, so the map deduplicates
				_, _ = sqsc.DeleteMessage(bgCtx, &awssqs.DeleteMessageInput{QueueUrl: aws.String(queues.Events), ReceiptHandle: m.ReceiptHandle})
			}
		}
		counts := map[string]int{}
		for _, e := range seen {
			counts[e.EventType]++
		}
		// opening: processed+balance; 5 bets: processed+balance each; 1 rejection
		return counts["WalletBalanceChanged"] == 6 && counts["WagerTransactionProcessed"] == 6 && counts["WagerTransactionRejected"] == 1
	})
	for _, e := range seen {
		if e.EventID == "" || e.Version != 1 || e.CorrelationID == "" || e.OccurredAt.IsZero() || e.Data["walletId"] == nil {
			t.Fatalf("incomplete envelope: %+v", e)
		}
	}
	var changed int
	for _, e := range seen {
		if e.EventType == "WalletBalanceChanged" {
			changed++
		}
	}
	if changed != ledgerEntries(t, w) {
		t.Fatalf("%d balance events vs %d ledger entries", changed, ledgerEntries(t, w))
	}
	var unpublished int
	_ = pool.QueryRow(bgCtx, `SELECT count(*) FROM outbox_events WHERE aggregate_id = $1::uuid AND published_at IS NULL`, w).Scan(&unpublished)
	if unpublished != 0 {
		t.Fatalf("%d events still unpublished", unpublished)
	}
}

func TestLateReferenceIsResolvedAndRejectedWhenItNeverArrives(t *testing.T) {
	w, p := newWallet(t, "100.00")
	tok := token(t, "provider-a")
	bet, refund, ghost := uid("bet"), uid("refund"), uid("ghostref")
	r := call(t, appURLs[0], "POST", "/wagering/transactions", tok, "key-"+refund, operation("provider-a", refund, w, p, "REFUND", "25.00", bet))
	if r.code != 202 || r.str("status") != "PENDING_REFERENCE" {
		t.Fatalf("%d %s", r.code, r.raw)
	}
	call(t, appURLs[1], "POST", "/wagering/transactions", tok, "key-"+bet, operation("provider-a", bet, w, p, "BET", "25.00", ""))
	eventually(t, 60*time.Second, "pending refund resolved", func() bool {
		return call(t, appURLs[2], "GET", "/providers/provider-a/wagering/transactions/"+refund, tok, "", "").str("status") == "PROCESSED"
	})
	if balance(t, w) != "100.00" {
		t.Fatal(balance(t, w))
	}
	// never arrives -> REJECTED(REFERENCE_NOT_FOUND) after the (short, configured) backoff
	rb := uid("rb")
	call(t, appURLs[0], "POST", "/wagering/transactions", tok, "key-"+rb, operation("provider-a", rb, w, p, "ROLLBACK", "25.00", ghost))
	eventually(t, 90*time.Second, "unresolvable reference rejected", func() bool {
		g := call(t, appURLs[0], "GET", "/providers/provider-a/wagering/transactions/"+rb, tok, "", "")
		return g.str("status") == "REJECTED" && g.str("failureCode") == "REFERENCE_NOT_FOUND"
	})
	requireConsistent(t, w)
}

func compose(t *testing.T, args ...string) {
	t.Helper()
	out, err := exec.Command("docker", append([]string{"compose"}, args...)...).CombinedOutput()
	if err != nil {
		t.Fatalf("docker compose %v: %v\n%s", args, err, out)
	}
}

// Needs E2E_COMPOSE=1 (the test restarts the service containers).
func TestPendingStateSurvivesRestartOfEveryReplica(t *testing.T) {
	if !composeOn {
		t.Skip("set E2E_COMPOSE=1 to run tests that restart containers")
	}
	w, p := newWallet(t, "100.00")
	tok := token(t, "provider-a")
	bet, refund := uid("bet"), uid("refund")
	if r := call(t, appURLs[0], "POST", "/wagering/transactions", tok, "key-"+refund, operation("provider-a", refund, w, p, "REFUND", "10.00", bet)); r.code != 202 {
		t.Fatalf("%d %s", r.code, r.raw)
	}
	compose(t, "restart", "app1", "app2", "app3")
	waitReady()
	// the idempotency record survived: replay, not a new operation
	if r := call(t, appURLs[2], "POST", "/wagering/transactions", tok, "key-"+refund, operation("provider-a", refund, w, p, "REFUND", "10.00", bet)); r.body["idempotentReplay"] != true {
		t.Fatalf("%d %s", r.code, r.raw)
	}
	call(t, appURLs[0], "POST", "/wagering/transactions", tok, "key-"+bet, operation("provider-a", bet, w, p, "BET", "10.00", ""))
	eventually(t, 60*time.Second, "pending refund resolved after restart", func() bool {
		return call(t, appURLs[1], "GET", "/providers/provider-a/wagering/transactions/"+refund, tok, "", "").str("status") == "PROCESSED"
	})
	requireConsistent(t, w)
}

func TestGracefulShutdownLosesNoAcknowledgedOperation(t *testing.T) {
	if !composeOn {
		t.Skip("set E2E_COMPOSE=1 to run tests that restart containers")
	}
	w, p := newWallet(t, "1000.00")
	tok := token(t, "provider-a")
	var wg sync.WaitGroup
	var okCount, unavailable atomic.Int32
	acked := sync.Map{}
	stop := make(chan struct{})
	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for n := 0; ; n++ {
				select {
				case <-stop:
					return
				default:
				}
				ext := fmt.Sprintf("gs-%d-%d-%s", i, n, uid("x"))
				req, _ := http.NewRequest("POST", appURLs[2]+"/wagering/transactions", bytes.NewReader([]byte(operation("provider-a", ext, w, p, "BET", "1.00", ""))))
				req.Header.Set("Authorization", "Bearer "+tok)
				req.Header.Set("Idempotency-Key", "key-"+ext)
				res, err := http.DefaultClient.Do(req)
				if err != nil {
					unavailable.Add(1) // connection refused once the replica is down
					time.Sleep(50 * time.Millisecond)
					continue
				}
				io.Copy(io.Discard, res.Body)
				res.Body.Close()
				if res.StatusCode == 200 {
					okCount.Add(1)
					acked.Store(ext, true)
				}
			}
		}(i)
	}
	time.Sleep(2 * time.Second)
	compose(t, "stop", "-t", "30", "app3") // SIGTERM: drain in-flight requests, then exit
	close(stop)
	wg.Wait()
	compose(t, "start", "app3")
	waitReady()
	var n int
	acked.Range(func(_, _ any) bool { n++; return true })
	var stored int
	_ = pool.QueryRow(bgCtx, `SELECT count(*) FROM wager_transactions WHERE wallet_id = $1::uuid AND status = 'PROCESSED' AND kind = 'BET'`, w).Scan(&stored)
	if stored < n {
		t.Fatalf("%d operations were acknowledged with 200 but only %d are stored", n, stored)
	}
	requireConsistent(t, w)
	t.Logf("acknowledged=%d stored=%d connectionErrorsAfterStop=%d", n, stored, unavailable.Load())
}

func TestEveryWalletOfTheRunMatchesItsLedger(t *testing.T) {
	rows, err := pool.Query(bgCtx, `
		SELECT w.id::text, w.balance_minor,
		       COALESCE(SUM(CASE l.direction WHEN 'CREDIT' THEN l.amount_minor ELSE -l.amount_minor END), 0)::bigint
		FROM wallets w LEFT JOIN wallet_ledger_entries l ON l.wallet_id = w.id
		GROUP BY w.id, w.balance_minor`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	n := 0
	for rows.Next() {
		var id string
		var stored, calc int64
		if err := rows.Scan(&id, &stored, &calc); err != nil {
			t.Fatal(err)
		}
		n++
		if stored != calc || stored < 0 {
			t.Errorf("wallet %s: stored %d, ledger %d", id, stored, calc)
		}
	}
	t.Logf("checked %d wallets", n)
}
