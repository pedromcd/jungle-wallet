// Package sqs adapts Amazon SQS (or LocalStack / MiniStack) to the app ports.
package sqs

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/pedro/jungle-wallet/internal/app"
)

// API is the subset of *sqs.Client used here (eases faking).
type API interface {
	GetQueueUrl(ctx context.Context, in *awssqs.GetQueueUrlInput, opts ...func(*awssqs.Options)) (*awssqs.GetQueueUrlOutput, error)
	GetQueueAttributes(ctx context.Context, in *awssqs.GetQueueAttributesInput, opts ...func(*awssqs.Options)) (*awssqs.GetQueueAttributesOutput, error)
	ReceiveMessage(ctx context.Context, in *awssqs.ReceiveMessageInput, opts ...func(*awssqs.Options)) (*awssqs.ReceiveMessageOutput, error)
	DeleteMessage(ctx context.Context, in *awssqs.DeleteMessageInput, opts ...func(*awssqs.Options)) (*awssqs.DeleteMessageOutput, error)
	ChangeMessageVisibility(ctx context.Context, in *awssqs.ChangeMessageVisibilityInput, opts ...func(*awssqs.Options)) (*awssqs.ChangeMessageVisibilityOutput, error)
	SendMessage(ctx context.Context, in *awssqs.SendMessageInput, opts ...func(*awssqs.Options)) (*awssqs.SendMessageOutput, error)
}

// NewClient builds the client. endpoint is empty for real AWS. Credentials come
// from the default AWS chain (env vars in docker-compose, IAM role in AWS).
func NewClient(ctx context.Context, region, endpoint string) (*awssqs.Client, error) {
	cfg, err := awscfg.LoadDefaultConfig(ctx, awscfg.WithRegion(region))
	if err != nil {
		return nil, err
	}
	return awssqs.NewFromConfig(cfg, func(o *awssqs.Options) {
		if endpoint != "" {
			o.BaseEndpoint = aws.String(endpoint)
		}
	}), nil
}

// QueueNames are the logical names configured through the environment.
type QueueNames struct{ Transactions, DLQ, Events string }

// Queues holds the resolved URLs.
type Queues struct {
	Transactions, DLQ, Events string
}

// Resolve looks the queues up, retrying while the broker is still starting.
func Resolve(ctx context.Context, api API, names QueueNames, wait time.Duration) (Queues, error) {
	var q Queues
	for _, it := range []struct {
		name string
		dst  *string
	}{{names.Transactions, &q.Transactions}, {names.DLQ, &q.DLQ}, {names.Events, &q.Events}} {
		deadline := time.Now().Add(wait)
		for {
			out, err := api.GetQueueUrl(ctx, &awssqs.GetQueueUrlInput{QueueName: aws.String(it.name)})
			if err == nil {
				*it.dst = aws.ToString(out.QueueUrl)
				break
			}
			if time.Now().After(deadline) || ctx.Err() != nil {
				return q, fmt.Errorf("resolve queue %q: %w", it.name, err)
			}
			time.Sleep(time.Second)
		}
	}
	return q, nil
}

// Ready is the readiness check: the main queue must answer.
func Ready(ctx context.Context, api API, queueURL string) error {
	_, err := api.GetQueueAttributes(ctx, &awssqs.GetQueueAttributesInput{
		QueueUrl: aws.String(queueURL), AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameQueueArn}})
	return err
}

// ---------------------------------------------------------------- consumer

// ConsumerOptions tunes polling.
type ConsumerOptions struct {
	Concurrency       int           // parallel pollers (default 2)
	VisibilityTimeout time.Duration // per receive (default 60s)
	WaitTime          time.Duration // long polling (default 10s)
	RetryBase         time.Duration // visibility backoff for transient failures
	RetryMax          time.Duration
}

func (o ConsumerOptions) withDefaults() ConsumerOptions {
	if o.Concurrency <= 0 {
		o.Concurrency = 2
	}
	if o.VisibilityTimeout <= 0 {
		o.VisibilityTimeout = 60 * time.Second
	}
	if o.WaitTime <= 0 {
		o.WaitTime = 10 * time.Second
	}
	if o.RetryBase <= 0 {
		o.RetryBase = 5 * time.Second
	}
	if o.RetryMax <= 0 {
		o.RetryMax = 5 * time.Minute
	}
	return o
}

// Consumer polls the FIFO transactions queue. FIFO message groups (one per
// wallet, chosen by the producer) keep per-wallet ordering while different
// wallets run in parallel; financial correctness never depends on that order,
// only on the database guarantees.
type Consumer struct {
	api     API
	queues  *Queues // filled during the Fx start phase, read at use time
	handler *app.MessageHandler
	opts    ConsumerOptions
	metrics app.Metrics
	log     *slog.Logger

	fetchCtx, workCtx       context.Context
	cancelFetch, cancelWork context.CancelFunc
	wg                      sync.WaitGroup
}

func NewConsumer(api API, q *Queues, h *app.MessageHandler, opts ConsumerOptions, metrics app.Metrics, log *slog.Logger) *Consumer {
	if metrics == nil {
		metrics = app.NopMetrics{}
	}
	return &Consumer{api: api, queues: q, handler: h, opts: opts.withDefaults(), metrics: metrics, log: log}
}

// Start launches the pollers.
func (c *Consumer) Start() {
	base := context.Background()
	c.fetchCtx, c.cancelFetch = context.WithCancel(base)
	c.workCtx, c.cancelWork = context.WithCancel(base)
	for i := 0; i < c.opts.Concurrency; i++ {
		c.wg.Add(1)
		go c.loop(i)
	}
}

// Stop stops fetching new messages, lets in-flight handlers finish until ctx
// expires, then aborts them (their SQL transaction rolls back and the message
// becomes visible again immediately).
func (c *Consumer) Stop(ctx context.Context) error {
	c.cancelFetch()
	done := make(chan struct{})
	go func() { c.wg.Wait(); close(done) }()
	select {
	case <-done:
		c.cancelWork()
		return nil
	case <-ctx.Done():
		c.log.Warn("sqs consumer grace period exceeded; aborting in-flight messages")
		c.cancelWork()
		<-done
		return ctx.Err()
	}
}

func (c *Consumer) loop(n int) {
	defer c.wg.Done()
	log := c.log.With("component", "sqs-consumer", "worker", n)
	for c.fetchCtx.Err() == nil {
		out, err := c.api.ReceiveMessage(c.fetchCtx, &awssqs.ReceiveMessageInput{
			QueueUrl:            aws.String(c.queues.Transactions),
			MaxNumberOfMessages: 10,
			WaitTimeSeconds:     int32(c.opts.WaitTime.Seconds()),
			VisibilityTimeout:   int32(c.opts.VisibilityTimeout.Seconds()),
			MessageSystemAttributeNames: []types.MessageSystemAttributeName{
				types.MessageSystemAttributeNameApproximateReceiveCount,
				types.MessageSystemAttributeNameMessageGroupId,
			},
		})
		if err != nil {
			if c.fetchCtx.Err() != nil {
				return
			}
			log.Warn("receive failed", "error", err)
			select {
			case <-c.fetchCtx.Done():
				return
			case <-time.After(2 * time.Second):
			}
			continue
		}
		for _, m := range out.Messages { // sequential: keeps FIFO order inside a batch
			c.handle(log, m)
		}
	}
}

func (c *Consumer) handle(log *slog.Logger, m types.Message) {
	disp, err := c.handler.Handle(c.workCtx, []byte(aws.ToString(m.Body)))
	// Settlement calls use their own bounded context: they must still run when
	// the work context was cancelled by a forced shutdown.
	sctx, cancel := context.WithTimeout(context.WithoutCancel(c.workCtx), 10*time.Second)
	defer cancel()
	switch disp {
	case app.DispositionAck:
		if _, derr := c.api.DeleteMessage(sctx, &awssqs.DeleteMessageInput{QueueUrl: aws.String(c.queues.Transactions), ReceiptHandle: m.ReceiptHandle}); derr != nil {
			// harmless: the redelivery is recognised by the inbox
			log.Warn("delete failed; message will be redelivered and deduplicated", "messageId", aws.ToString(m.MessageId), "error", derr)
		}
	case app.DispositionDeadLetter:
		log.Error("message moved to DLQ", "messageId", aws.ToString(m.MessageId), "reason", err)
		if derr := c.deadLetter(sctx, m, err); derr != nil {
			log.Error("could not send to DLQ; the redrive policy will handle it", "error", derr)
			return
		}
		c.metrics.DeadLetter()
	case app.DispositionRetry:
		c.metrics.Retry("sqs")
		count := 1
		if v, ok := m.Attributes[string(types.MessageSystemAttributeNameApproximateReceiveCount)]; ok {
			if n, perr := strconv.Atoi(v); perr == nil && n > 0 {
				count = n
			}
		}
		delay := app.Backoff(count-1, c.opts.RetryBase, c.opts.RetryMax)
		if c.workCtx.Err() != nil {
			delay = 0 // shutting down: release the message right away
		}
		log.Warn("transient failure; will retry", "messageId", aws.ToString(m.MessageId), "receiveCount", count, "delay", delay, "error", err)
		if _, verr := c.api.ChangeMessageVisibility(sctx, &awssqs.ChangeMessageVisibilityInput{
			QueueUrl: aws.String(c.queues.Transactions), ReceiptHandle: m.ReceiptHandle, VisibilityTimeout: int32(delay.Seconds())}); verr != nil {
			log.Warn("could not change visibility", "error", verr)
		}
	}
}

func (c *Consumer) deadLetter(ctx context.Context, m types.Message, reason error) error {
	group := m.Attributes[string(types.MessageSystemAttributeNameMessageGroupId)]
	if group == "" {
		group = "dead-letter"
	}
	text := ""
	if reason != nil {
		text = reason.Error()
		if len(text) > 250 {
			text = text[:250]
		}
	}
	_, err := c.api.SendMessage(ctx, &awssqs.SendMessageInput{
		QueueUrl: aws.String(c.queues.DLQ), MessageBody: m.Body, MessageGroupId: aws.String(group),
		MessageDeduplicationId: m.MessageId,
		MessageAttributes: map[string]types.MessageAttributeValue{
			"failureReason": {DataType: aws.String("String"), StringValue: aws.String(text)},
		},
	})
	if err != nil {
		return err
	}
	_, err = c.api.DeleteMessage(ctx, &awssqs.DeleteMessageInput{QueueUrl: aws.String(c.queues.Transactions), ReceiptHandle: m.ReceiptHandle})
	return err
}

// -------------------------------------------------------------------- sink

// Sink publishes outbox records to the events FIFO queue. Contract:
// MessageGroupId = aggregateId (wallet), MessageDeduplicationId = eventId,
// attributes eventType/eventId, body = the immutable envelope JSON.
type Sink struct {
	api    API
	queues *Queues
}

func NewSink(api API, q *Queues) *Sink { return &Sink{api: api, queues: q} }

func (s *Sink) Publish(ctx context.Context, rec app.OutboxRecord) error {
	_, err := s.api.SendMessage(ctx, &awssqs.SendMessageInput{
		QueueUrl: aws.String(s.queues.Events), MessageBody: aws.String(string(rec.Payload)),
		MessageGroupId: aws.String(rec.AggregateID), MessageDeduplicationId: aws.String(rec.EventID),
		MessageAttributes: map[string]types.MessageAttributeValue{
			"eventType": {DataType: aws.String("String"), StringValue: aws.String(rec.EventType)},
			"eventId":   {DataType: aws.String("String"), StringValue: aws.String(rec.EventID)},
		},
	})
	return err
}
