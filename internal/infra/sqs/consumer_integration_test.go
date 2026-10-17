//go:build integration

package sqs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/pedro/jungle-wallet/internal/app"
	"github.com/pedro/jungle-wallet/internal/domain"
	"github.com/pedro/jungle-wallet/internal/infra/postgres"
	"github.com/pedro/jungle-wallet/internal/testsupport"
)

// crashyAPI redelivers a message until a DeleteMessage finally succeeds; the
// first failDeletes deletes fail, which is exactly what a consumer that dies
// after the SQL commit and before the delete looks like to the broker.
type crashyAPI struct {
	mu          sync.Mutex
	msg         types.Message
	failDeletes int
	receives    int
	deleted     bool
}

func (c *crashyAPI) GetQueueUrl(context.Context, *awssqs.GetQueueUrlInput, ...func(*awssqs.Options)) (*awssqs.GetQueueUrlOutput, error) {
	return &awssqs.GetQueueUrlOutput{QueueUrl: aws.String("q")}, nil
}
func (c *crashyAPI) GetQueueAttributes(context.Context, *awssqs.GetQueueAttributesInput, ...func(*awssqs.Options)) (*awssqs.GetQueueAttributesOutput, error) {
	return &awssqs.GetQueueAttributesOutput{}, nil
}
func (c *crashyAPI) ReceiveMessage(ctx context.Context, _ *awssqs.ReceiveMessageInput, _ ...func(*awssqs.Options)) (*awssqs.ReceiveMessageOutput, error) {
	c.mu.Lock()
	if !c.deleted {
		c.receives++
		m := c.msg
		m.Attributes = map[string]string{
			string(types.MessageSystemAttributeNameApproximateReceiveCount): strconv.Itoa(c.receives),
			string(types.MessageSystemAttributeNameMessageGroupId):          "g",
		}
		c.mu.Unlock()
		return &awssqs.ReceiveMessageOutput{Messages: []types.Message{m}}, nil
	}
	c.mu.Unlock()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(20 * time.Millisecond):
		return &awssqs.ReceiveMessageOutput{}, nil
	}
}
func (c *crashyAPI) DeleteMessage(context.Context, *awssqs.DeleteMessageInput, ...func(*awssqs.Options)) (*awssqs.DeleteMessageOutput, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.failDeletes > 0 {
		c.failDeletes--
		return nil, errors.New("consumer died before deleting the message")
	}
	c.deleted = true
	return &awssqs.DeleteMessageOutput{}, nil
}
func (c *crashyAPI) ChangeMessageVisibility(context.Context, *awssqs.ChangeMessageVisibilityInput, ...func(*awssqs.Options)) (*awssqs.ChangeMessageVisibilityOutput, error) {
	return &awssqs.ChangeMessageVisibilityOutput{}, nil
}
func (c *crashyAPI) SendMessage(context.Context, *awssqs.SendMessageInput, ...func(*awssqs.Options)) (*awssqs.SendMessageOutput, error) {
	return &awssqs.SendMessageOutput{}, nil
}

func TestConsumerCrashAfterCommitBeforeDeleteRedeliversSafely(t *testing.T) {
	d, ok, err := testsupport.Setup()
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Skip("TEST_DATABASE_URL not set")
	}
	defer d.Close()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	st := postgres.NewStore(d.Pool, nil)
	proc := app.NewProcessor(st, st, app.Options{}, nil, log)
	ws := app.NewWalletService(st, st, nil, log)
	player := domain.NewID()
	initial, _ := domain.ParseMoney("100.00", "BRL")
	w, err := ws.Open(context.Background(), player, initial, "")
	if err != nil {
		t.Fatal(err)
	}
	body := fmt.Sprintf(`{"messageId":"crash-1","type":"WagerTransactionRequested","occurredAt":"2026-10-16T12:00:00Z","data":{
		"providerId":"provider-a","externalTransactionId":"bet-crash","idempotencyKey":"provider-a:bet-crash","playerId":%q,"walletId":%q,
		"roundId":"r","gameId":"g","kind":"BET","money":{"amount":"30.00","currency":"BRL"}}}`, player, w.ID())

	api := &crashyAPI{failDeletes: 3, msg: types.Message{ReceiptHandle: aws.String("rh"), MessageId: aws.String("sqs-1"), Body: aws.String(body)}}
	c := NewConsumer(api, &Queues{Transactions: "q", DLQ: "dlq"}, app.NewMessageHandler(st, proc, nil, log),
		ConsumerOptions{Concurrency: 1, WaitTime: time.Second}, nil, log)
	c.Start()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		api.mu.Lock()
		done := api.deleted
		api.mu.Unlock()
		if done {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	api.mu.Lock()
	defer api.mu.Unlock()
	if !api.deleted || api.receives != 4 { // 3 failed deletes -> 4 deliveries of the same message
		t.Fatalf("deleted=%v receives=%d", api.deleted, api.receives)
	}
	got, _ := ws.Get(context.Background(), w.ID())
	if got.Balance().Amount() != "70.00" {
		t.Fatalf("redeliveries must not debit again: balance %s", got.Balance().Amount())
	}
	var entries, inbox int
	_ = d.Pool.QueryRow(context.Background(), `SELECT count(*) FROM wallet_ledger_entries WHERE wallet_id=$1::uuid`, w.ID()).Scan(&entries)
	_ = d.Pool.QueryRow(context.Background(), `SELECT count(*) FROM inbox_messages WHERE message_id='crash-1' AND completed_at IS NOT NULL`).Scan(&inbox)
	if entries != 2 || inbox != 1 {
		t.Fatalf("ledger entries=%d (want opening+bet), completed inbox rows=%d", entries, inbox)
	}
}
