package sqs

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/pedro/jungle-wallet/internal/app"
)

type fakeAPI struct {
	mu       sync.Mutex
	inbox    []types.Message
	deleted  []string
	dlq      []string
	visible  map[string]int32
	received int
}

func (f *fakeAPI) GetQueueUrl(_ context.Context, in *awssqs.GetQueueUrlInput, _ ...func(*awssqs.Options)) (*awssqs.GetQueueUrlOutput, error) {
	return &awssqs.GetQueueUrlOutput{QueueUrl: aws.String("http://q/" + aws.ToString(in.QueueName))}, nil
}
func (f *fakeAPI) GetQueueAttributes(context.Context, *awssqs.GetQueueAttributesInput, ...func(*awssqs.Options)) (*awssqs.GetQueueAttributesOutput, error) {
	return &awssqs.GetQueueAttributesOutput{}, nil
}
func (f *fakeAPI) ReceiveMessage(ctx context.Context, _ *awssqs.ReceiveMessageInput, _ ...func(*awssqs.Options)) (*awssqs.ReceiveMessageOutput, error) {
	f.mu.Lock()
	if len(f.inbox) > 0 {
		out := &awssqs.ReceiveMessageOutput{Messages: f.inbox}
		f.inbox = nil
		f.received++
		f.mu.Unlock()
		return out, nil
	}
	f.mu.Unlock()
	select { // behave like long polling
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(20 * time.Millisecond):
		return &awssqs.ReceiveMessageOutput{}, nil
	}
}
func (f *fakeAPI) DeleteMessage(_ context.Context, in *awssqs.DeleteMessageInput, _ ...func(*awssqs.Options)) (*awssqs.DeleteMessageOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deleted = append(f.deleted, aws.ToString(in.ReceiptHandle))
	return &awssqs.DeleteMessageOutput{}, nil
}
func (f *fakeAPI) ChangeMessageVisibility(_ context.Context, in *awssqs.ChangeMessageVisibilityInput, _ ...func(*awssqs.Options)) (*awssqs.ChangeMessageVisibilityOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.visible[aws.ToString(in.ReceiptHandle)] = in.VisibilityTimeout
	return &awssqs.ChangeMessageVisibilityOutput{}, nil
}
func (f *fakeAPI) SendMessage(_ context.Context, in *awssqs.SendMessageInput, _ ...func(*awssqs.Options)) (*awssqs.SendMessageOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if aws.ToString(in.MessageGroupId) == "" {
		return nil, errors.New("FIFO queues require a MessageGroupId")
	}
	f.dlq = append(f.dlq, aws.ToString(in.MessageBody))
	return &awssqs.SendMessageOutput{}, nil
}

// stubUoW lets the handler run without a database.
type stubUoW struct{ err error }

func (s stubUoW) Do(context.Context, func(context.Context, app.Tx) error) error { return s.err }

func msg(handle, body string, receiveCount string) types.Message {
	return types.Message{
		ReceiptHandle: aws.String(handle), MessageId: aws.String("id-" + handle), Body: aws.String(body),
		Attributes: map[string]string{
			string(types.MessageSystemAttributeNameApproximateReceiveCount): receiveCount,
			string(types.MessageSystemAttributeNameMessageGroupId):          "wallet-1",
		},
	}
}

const validBody = `{"messageId":"m1","type":"WagerTransactionRequested","data":{
 "providerId":"p","externalTransactionId":"e","idempotencyKey":"k","playerId":"0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1",
 "walletId":"0192f291-27dd-7d3f-8071-5f8685deef37","roundId":"r","gameId":"g","kind":"BET","money":{"amount":"1.00","currency":"BRL"}}}`

func runConsumer(t *testing.T, uow app.UnitOfWork, messages ...types.Message) *fakeAPI {
	t.Helper()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	api := &fakeAPI{inbox: messages, visible: map[string]int32{}}
	h := app.NewMessageHandler(uow, app.NewProcessor(uow, nil, app.Options{}, nil, log), nil, log)
	c := NewConsumer(api, &Queues{Transactions: "http://q/main", DLQ: "http://q/dlq", Events: "http://q/events"}, h,
		ConsumerOptions{Concurrency: 1, WaitTime: time.Second, RetryBase: 5 * time.Second, RetryMax: time.Minute}, nil, log)
	c.Start()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		api.mu.Lock()
		done := len(api.deleted)+len(api.visible) >= len(messages)
		api.mu.Unlock()
		if done {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := c.Stop(ctx); err != nil {
		t.Fatalf("graceful stop: %v", err)
	}
	return api
}

func TestConsumerAcksCommittedMessages(t *testing.T) {
	api := runConsumer(t, stubUoW{}, msg("h1", validBody, "1"))
	if len(api.deleted) != 1 || api.deleted[0] != "h1" || len(api.dlq) != 0 {
		t.Fatalf("deleted=%v dlq=%v", api.deleted, api.dlq)
	}
}

func TestConsumerSendsPoisonMessagesToTheDLQAndDeletesThem(t *testing.T) {
	api := runConsumer(t, stubUoW{}, msg("bad", `{"not":"a wager"}`, "1"), msg("junk", `{`, "1"))
	if len(api.dlq) != 2 || len(api.deleted) != 2 {
		t.Fatalf("dlq=%v deleted=%v", api.dlq, api.deleted)
	}
}

func TestConsumerBacksOffExponentiallyOnTransientFailures(t *testing.T) {
	uow := stubUoW{err: app.Transient(errors.New("db down"))}
	api := runConsumer(t, uow, msg("r1", validBody, "1"), msg("r3", validBody, "3"))
	if len(api.deleted) != 0 {
		t.Fatalf("transient failures must not delete: %v", api.deleted)
	}
	if api.visible["r1"] != 5 || api.visible["r3"] != 20 { // 5s * 2^(count-1)
		t.Fatalf("visibility = %v", api.visible)
	}
}

func TestConsumerStopWaitsForInFlightWorkThenAborts(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	api := &fakeAPI{inbox: []types.Message{msg("slow", validBody, "1")}, visible: map[string]int32{}}
	started := make(chan struct{})
	uow := blockingUoW{started: started}
	h := app.NewMessageHandler(uow, app.NewProcessor(uow, nil, app.Options{}, nil, log), nil, log)
	c := NewConsumer(api, &Queues{Transactions: "q", DLQ: "d"}, h, ConsumerOptions{Concurrency: 1, WaitTime: time.Second}, nil, log)
	c.Start()
	<-started
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := c.Stop(ctx); err == nil {
		t.Fatal("grace period exceeded: Stop must report it")
	}
	// the in-flight message is released immediately (visibility 0), not deleted
	if len(api.deleted) != 0 || api.visible["slow"] != 0 {
		t.Fatalf("deleted=%v visible=%v", api.deleted, api.visible)
	}
	if _, released := api.visible["slow"]; !released {
		t.Fatal("message was neither acked nor released")
	}
}

type blockingUoW struct{ started chan struct{} }

func (b blockingUoW) Do(ctx context.Context, _ func(context.Context, app.Tx) error) error {
	close(b.started)
	<-ctx.Done() // a long transaction interrupted by the forced shutdown
	return app.Transient(ctx.Err())
}

func TestResolveRetriesUntilQueuesExist(t *testing.T) {
	api := &fakeAPI{visible: map[string]int32{}}
	q, err := Resolve(context.Background(), api, QueueNames{Transactions: "a.fifo", DLQ: "b.fifo", Events: "c.fifo"}, time.Second)
	if err != nil || q.Transactions != "http://q/a.fifo" || q.Events != "http://q/c.fifo" {
		t.Fatalf("%+v %v", q, err)
	}
}
