//go:build integration

package bootstrap

import (
	"bytes"
	"context"
	"net"
	"net/http"
	"runtime/pprof"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"go.uber.org/fx"

	"github.com/pedro/jungle-wallet/internal/domain"
	sqsinfra "github.com/pedro/jungle-wallet/internal/infra/sqs"
	"github.com/pedro/jungle-wallet/internal/testsupport"
)

// fakeSQS keeps the real PostgreSQL in the loop but replaces the broker, so the
// Fx start/stop sequence can be verified without containers.
type fakeSQS struct {
	mu   sync.Mutex
	sent map[string]bool
}

func (f *fakeSQS) GetQueueUrl(_ context.Context, in *awssqs.GetQueueUrlInput, _ ...func(*awssqs.Options)) (*awssqs.GetQueueUrlOutput, error) {
	return &awssqs.GetQueueUrlOutput{QueueUrl: aws.String("http://fake/" + aws.ToString(in.QueueName))}, nil
}
func (f *fakeSQS) GetQueueAttributes(context.Context, *awssqs.GetQueueAttributesInput, ...func(*awssqs.Options)) (*awssqs.GetQueueAttributesOutput, error) {
	return &awssqs.GetQueueAttributesOutput{}, nil
}
func (f *fakeSQS) ReceiveMessage(ctx context.Context, _ *awssqs.ReceiveMessageInput, _ ...func(*awssqs.Options)) (*awssqs.ReceiveMessageOutput, error) {
	<-ctx.Done() // long poll until the consumer stops fetching
	return nil, ctx.Err()
}
func (f *fakeSQS) DeleteMessage(context.Context, *awssqs.DeleteMessageInput, ...func(*awssqs.Options)) (*awssqs.DeleteMessageOutput, error) {
	return &awssqs.DeleteMessageOutput{}, nil
}
func (f *fakeSQS) ChangeMessageVisibility(context.Context, *awssqs.ChangeMessageVisibilityInput, ...func(*awssqs.Options)) (*awssqs.ChangeMessageVisibilityOutput, error) {
	return &awssqs.ChangeMessageVisibilityOutput{}, nil
}
func (f *fakeSQS) SendMessage(_ context.Context, in *awssqs.SendMessageInput, _ ...func(*awssqs.Options)) (*awssqs.SendMessageOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sent[aws.ToString(in.MessageDeduplicationId)] = true
	return &awssqs.SendMessageOutput{}, nil
}

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

func TestStartServeAndStopReleaseEveryResource(t *testing.T) {
	d, ok, err := testsupport.Setup()
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Skip("TEST_DATABASE_URL not set")
	}
	t.Cleanup(d.Close)
	addr := freeAddr(t)
	dsn := d.DSN
	sep := "?"
	if strings.Contains(dsn, "?") {
		sep = "&"
	}
	for k, v := range map[string]string{
		"DATABASE_URL": dsn + sep + "search_path=" + d.Schema, "HTTP_ADDR": addr,
		"OIDC_ISSUER": "http://127.0.0.1:1/realms/wallet", "OIDC_JWKS_URL": "http://127.0.0.1:1/jwks",
		"SQS_ENDPOINT": "http://127.0.0.1:1", "AWS_ACCESS_KEY_ID": "x", "AWS_SECRET_ACCESS_KEY": "y",
		"OUTBOX_POLL_INTERVAL": "50ms", "PENDING_POLL_INTERVAL": "50ms", "LOG_LEVEL": "warn", "SHUTDOWN_TIMEOUT": "10s",
	} {
		t.Setenv(k, v)
	}
	fake := &fakeSQS{sent: map[string]bool{}}
	a := fx.New(Modules, fx.Decorate(func(sqsinfra.API) sqsinfra.API { return fake }))
	if err := a.Err(); err != nil {
		t.Fatalf("composition: %v", err)
	}
	startCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := a.Start(startCtx); err != nil {
		t.Fatalf("start: %v", err)
	}

	for path, want := range map[string]int{"/health/live": 200, "/health/ready": 200, "/metrics": 200} {
		res, err := http.Get("http://" + addr + path)
		if err != nil || res.StatusCode != want {
			t.Fatalf("%s: %v %v", path, res, err)
		}
		res.Body.Close()
	}
	if res, err := http.Post("http://"+addr+"/wallets", "application/json", strings.NewReader(`{}`)); err != nil || res.StatusCode != 401 {
		t.Fatalf("business endpoints must require authentication: %v %v", res, err)
	}

	// the outbox worker is alive: a staged event gets published and confirmed
	evID := domain.NewID()
	if _, err := d.Pool.Exec(context.Background(), `INSERT INTO outbox_events (event_id, aggregate_id, event_type, payload, occurred_at)
		VALUES ($1::uuid, $2::uuid, 'Probe', '{}'::jsonb, now())`, evID, domain.NewID()); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		var published bool
		_ = d.Pool.QueryRow(context.Background(), `SELECT published_at IS NOT NULL FROM outbox_events WHERE event_id=$1::uuid`, evID).Scan(&published)
		fake.mu.Lock()
		sent := fake.sent[evID]
		fake.mu.Unlock()
		if published && sent {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("outbox worker did not publish the staged event")
		}
		time.Sleep(50 * time.Millisecond)
	}

	stopCtx, cancelStop := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancelStop()
	begin := time.Now()
	if err := a.Stop(stopCtx); err != nil {
		t.Fatalf("stop: %v", err)
	}
	t.Logf("graceful stop took %s", time.Since(begin))

	// new connections are refused and the port is free again
	if res, err := http.Get("http://" + addr + "/health/live"); err == nil {
		res.Body.Close()
		t.Fatal("server still accepting requests after stop")
	}
	l, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("port not released: %v", err)
	}
	l.Close()

	// no component of the service (workers, consumer, pool, key fetcher) may
	// linger; the test's own pools are closed first so only the app's remain
	d.Close()
	http.DefaultClient.CloseIdleConnections()
	var dump string
	for i := 0; i < 50; i++ {
		var buf bytes.Buffer
		_ = pprof.Lookup("goroutine").WriteTo(&buf, 1)
		dump = buf.String()
		if !containsAny(dump, "internal/bootstrap.loop", "infra/sqs.(*Consumer)", "pgxpool.(*Pool)", "puddle", "go-oidc", "http.(*Server).Serve") {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("goroutines still running after Stop:\n%s", dump)
}

func containsAny(s string, subs ...string) bool {
	for _, x := range subs {
		if strings.Contains(s, x) {
			return true
		}
	}
	return false
}

func TestInvalidConfigurationFailsFast(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	t.Setenv("OIDC_ISSUER", "")
	t.Setenv("OIDC_JWKS_URL", "")
	t.Setenv("DB_MAX_CONNS", "-3")
	err := fx.New(Modules).Err()
	if err == nil {
		t.Fatal("the application must refuse to boot with an invalid configuration")
	}
	for _, want := range []string{"DATABASE_URL is required", "OIDC_ISSUER is required", "DB_MAX_CONNS must be a positive integer"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error should mention %q: %v", want, err)
		}
	}
}
