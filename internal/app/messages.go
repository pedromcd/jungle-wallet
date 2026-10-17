package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/pedro/jungle-wallet/internal/domain"
)

const (
	// ConsumerName identifies this consumer in the inbox.
	ConsumerName = "wager-transactions-consumer"
	// MessageTypeWagerRequested is the only accepted envelope type.
	MessageTypeWagerRequested = "WagerTransactionRequested"
)

// MessageData is the "data" block of the SQS envelope.
type MessageData struct {
	ProviderID                     string       `json:"providerId"`
	ExternalTransactionID          string       `json:"externalTransactionId"`
	IdempotencyKey                 string       `json:"idempotencyKey"`
	PlayerID                       string       `json:"playerId"`
	WalletID                       string       `json:"walletId"`
	RoundID                        string       `json:"roundId"`
	GameID                         string       `json:"gameId"`
	Kind                           string       `json:"kind"`
	Money                          domain.Money `json:"money"`
	ReferenceExternalTransactionID string       `json:"referenceExternalTransactionId,omitempty"`
}

// MessageEnvelope is the SQS message body.
type MessageEnvelope struct {
	MessageID  string      `json:"messageId"`
	Type       string      `json:"type"`
	OccurredAt time.Time   `json:"occurredAt"`
	Data       MessageData `json:"data"`
}

// Disposition tells the SQS adapter what to do with a message.
type Disposition int

const (
	// DispositionAck: durable handling committed (or business rejection
	// committed, or duplicate) - delete the message.
	DispositionAck Disposition = iota
	// DispositionRetry: transient failure - keep the message, back off.
	DispositionRetry
	// DispositionDeadLetter: it can never succeed - move it to the DLQ.
	DispositionDeadLetter
)

// MessageHandler processes one SQS message with at-least-once semantics.
type MessageHandler struct {
	uow       UnitOfWork
	processor *Processor
	metrics   Metrics
	log       *slog.Logger
}

func NewMessageHandler(uow UnitOfWork, p *Processor, metrics Metrics, log *slog.Logger) *MessageHandler {
	if metrics == nil {
		metrics = NopMetrics{}
	}
	if log == nil {
		log = slog.Default()
	}
	return &MessageHandler{uow: uow, processor: p, metrics: metrics, log: log}
}

// Handle parses and applies one message. The inbox registration, the domain
// changes, the ledger and the outbox events share ONE SQL transaction, so a
// crash on either side of the commit is safe: before it nothing happened, after
// it the redelivery is recognised by the inbox (and by the idempotency key).
func (h *MessageHandler) Handle(ctx context.Context, body []byte) (Disposition, error) {
	start := time.Now()
	var env MessageEnvelope
	if err := json.Unmarshal(body, &env); err != nil {
		return DispositionDeadLetter, fmt.Errorf("malformed message: %w", err)
	}
	if env.Type != MessageTypeWagerRequested {
		return DispositionDeadLetter, fmt.Errorf("unsupported message type %q", env.Type)
	}
	if env.MessageID == "" || len(env.MessageID) > 128 {
		return DispositionDeadLetter, errors.New("messageId is required (max 128 chars)")
	}
	d := env.Data
	cmd, err := NewCommand(CommandInput{
		ProviderID: d.ProviderID, ExternalTransactionID: d.ExternalTransactionID, IdempotencyKey: d.IdempotencyKey,
		PlayerID: d.PlayerID, WalletID: d.WalletID, RoundID: d.RoundID, GameID: d.GameID, Kind: d.Kind,
		Money: d.Money, ReferenceExternalTransactionID: d.ReferenceExternalTransactionID, CorrelationID: env.MessageID,
	})
	if err != nil {
		return DispositionDeadLetter, err
	}

	hash := inboxHash(cmd)
	var out Outcome
	duplicate := false
	err = h.uow.Do(ctx, func(ctx context.Context, tx Tx) error {
		out, duplicate = Outcome{}, false
		state, err := tx.Inbox().Begin(ctx, ConsumerName, env.MessageID, hash)
		if err != nil {
			return err
		}
		if state == InboxDuplicate {
			duplicate = true
			return nil
		}
		if out, err = h.processor.processInTx(ctx, tx, cmd); err != nil {
			return err
		}
		return tx.Inbox().Complete(ctx, ConsumerName, env.MessageID)
	})
	switch {
	case err == nil:
		if duplicate {
			h.metrics.Duplicate("sqs")
			h.log.Info("duplicate message ignored", "source", "sqs", "messageId", env.MessageID, "correlationId", env.MessageID,
				"walletId", cmd.Params.WalletID, "providerId", cmd.Params.ProviderID)
			return DispositionAck, nil
		}
		h.processor.record(cmd, out, "sqs", env.MessageID)
		h.metrics.ProcessingSeconds("sqs", time.Since(start))
		return DispositionAck, nil
	case errors.Is(err, ErrInboxPayloadMismatch), errors.Is(err, ErrIdempotencyKeyConflict),
		errors.Is(err, ErrDuplicateExternalTransaction), errors.Is(err, ErrWalletNotFound):
		h.log.Error("message cannot be processed", "messageId", env.MessageID, "correlationId", env.MessageID,
			"walletId", cmd.Params.WalletID, "providerId", cmd.Params.ProviderID, "reason", err.Error())
		return DispositionDeadLetter, err
	case IsTransient(err):
		h.log.Warn("transient failure handling message", "messageId", env.MessageID, "correlationId", env.MessageID,
			"walletId", cmd.Params.WalletID, "providerId", cmd.Params.ProviderID, "error", err.Error())
		return DispositionRetry, err
	default:
		// Unknown failure: retry; the queue's redrive policy (maxReceiveCount)
		// moves a persistent poison message to the DLQ.
		return DispositionRetry, err
	}
}

// PublisherOptions configures the outbox publisher.
type PublisherOptions struct {
	Batch       int
	Lease       time.Duration // must exceed the time to publish a batch
	BackoffBase time.Duration
	BackoffMax  time.Duration
}

func (o PublisherOptions) withDefaults() PublisherOptions {
	if o.Batch <= 0 {
		o.Batch = 25
	}
	if o.Lease <= 0 {
		o.Lease = 30 * time.Second
	}
	if o.BackoffBase <= 0 {
		o.BackoffBase = time.Second
	}
	if o.BackoffMax <= 0 {
		o.BackoffMax = time.Minute
	}
	return o
}

// OutboxPublisher drains the transactional outbox. Several publishers may run
// at once: the store hands each record to a single lease holder, and a lease
// that expires (crashed publisher) lets another instance take over. Delivery is
// at-least-once; republications keep the same eventId.
type OutboxPublisher struct {
	store   OutboxStore
	sink    EventSink
	owner   string
	opts    PublisherOptions
	metrics Metrics
	log     *slog.Logger
}

func NewOutboxPublisher(store OutboxStore, sink EventSink, owner string, opts PublisherOptions, metrics Metrics, log *slog.Logger) *OutboxPublisher {
	if metrics == nil {
		metrics = NopMetrics{}
	}
	if log == nil {
		log = slog.Default()
	}
	return &OutboxPublisher{store: store, sink: sink, owner: owner, opts: opts.withDefaults(), metrics: metrics, log: log}
}

// RunOnce claims and publishes one batch; it returns how many were published.
func (p *OutboxPublisher) RunOnce(ctx context.Context) (int, error) {
	if lag, err := p.store.Lag(ctx); err == nil {
		p.metrics.OutboxLagSeconds(lag.Seconds())
	}
	recs, err := p.store.Claim(ctx, p.owner, p.opts.Batch, p.opts.Lease)
	if err != nil {
		return 0, err
	}
	published := 0
	for _, rec := range recs {
		if err := ctx.Err(); err != nil {
			return published, err
		}
		if err := p.sink.Publish(ctx, rec); err != nil {
			p.metrics.Retry("outbox")
			p.log.Warn("outbox publish failed", "eventId", rec.EventID, "attempts", rec.Attempts, "error", err)
			retryIn := Backoff(rec.Attempts, p.opts.BackoffBase, p.opts.BackoffMax)
			if rerr := p.store.Release(ctx, rec.EventID, retryIn); rerr != nil {
				p.log.Error("outbox release failed (lease will expire)", "eventId", rec.EventID, "error", rerr)
			}
			continue
		}
		// A crash right here republishes the event later with the same eventId.
		if err := p.store.MarkPublished(ctx, rec.EventID); err != nil {
			p.log.Error("outbox mark published failed (will be republished)", "eventId", rec.EventID, "error", err)
			continue
		}
		published++
	}
	return published, nil
}
