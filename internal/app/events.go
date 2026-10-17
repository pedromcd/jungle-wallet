package app

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/pedro/jungle-wallet/internal/domain"
)

const (
	EventTransactionProcessed        = "WagerTransactionProcessed"
	EventTransactionRejected         = "WagerTransactionRejected"
	EventWalletBalanceChanged        = "WalletBalanceChanged"
	EventTransactionPendingReference = "WagerTransactionPendingReference"
)

// eventVersion is set by the constructors (never by callers).
const eventVersion = 1

// Envelope is the contract of every integration event. The aggregate is the
// wallet, which is also the SQS FIFO message group of the destination queue.
type Envelope[T any] struct {
	EventID       string    `json:"eventId"`
	EventType     string    `json:"eventType"`
	AggregateID   string    `json:"aggregateId"`
	CorrelationID string    `json:"correlationId"`
	CausationID   string    `json:"causationId,omitempty"`
	OccurredAt    time.Time `json:"occurredAt"`
	Version       int       `json:"version"`
	Data          T         `json:"data"`
}

// TransactionRef identifies the transaction a transaction-level event is about.
// Provider fields are absent for the internal OPENING credit.
type TransactionRef struct {
	TransactionID         string        `json:"transactionId"`
	Kind                  domain.Kind   `json:"kind"`
	Origin                domain.Origin `json:"origin"`
	WalletID              string        `json:"walletId"`
	PlayerID              string        `json:"playerId"`
	ProviderID            string        `json:"providerId,omitempty"`
	ExternalTransactionID string        `json:"externalTransactionId,omitempty"`
	RoundID               string        `json:"roundId,omitempty"`
	GameID                string        `json:"gameId,omitempty"`
	Money                 domain.Money  `json:"money"`
}

type TransactionProcessedData struct {
	TransactionRef
	Balance domain.Money `json:"balance"`
}

type TransactionRejectedData struct {
	TransactionRef
	FailureCode domain.FailureCode `json:"failureCode"`
	Balance     domain.Money       `json:"balance"`
}

type TransactionPendingReferenceData struct {
	TransactionRef
	ReferenceExternalTransactionID string    `json:"referenceExternalTransactionId"`
	NextAttemptAt                  time.Time `json:"nextAttemptAt"`
}

type BalanceChangedData struct {
	WalletID      string           `json:"walletId"`
	TransactionID string           `json:"transactionId"`
	Direction     domain.Direction `json:"direction"`
	Money         domain.Money     `json:"money"`
	BalanceBefore domain.Money     `json:"balanceBefore"`
	BalanceAfter  domain.Money     `json:"balanceAfter"`
	WalletVersion int64            `json:"walletVersion"`
}

func newEvent[T any](eventType, aggregateID, correlationID, causationID string, now time.Time, data T) (OutboxEvent, error) {
	env := Envelope[T]{
		EventID: domain.NewID(), EventType: eventType, AggregateID: aggregateID,
		CorrelationID: correlationID, CausationID: causationID, OccurredAt: now.UTC(),
		Version: eventVersion, Data: data,
	}
	payload, err := json.Marshal(env) // immutable snapshot taken now
	if err != nil {
		return OutboxEvent{}, fmt.Errorf("marshal %s: %w", eventType, err)
	}
	return OutboxEvent{EventID: env.EventID, AggregateID: aggregateID, EventType: eventType,
		Payload: payload, OccurredAt: env.OccurredAt}, nil
}

func refOf(t *domain.WagerTransaction) TransactionRef {
	return TransactionRef{TransactionID: t.ID(), Kind: t.Kind(), Origin: t.Origin(), WalletID: t.WalletID(),
		PlayerID: t.PlayerID(), ProviderID: t.ProviderID(), ExternalTransactionID: t.ExternalID(),
		RoundID: t.RoundID(), GameID: t.GameID(), Money: t.Money()}
}

func processedEvent(t *domain.WagerTransaction, balance domain.Money, corr string, now time.Time) (OutboxEvent, error) {
	return newEvent(EventTransactionProcessed, t.WalletID(), corr, "", now,
		TransactionProcessedData{TransactionRef: refOf(t), Balance: balance})
}

func rejectedEvent(t *domain.WagerTransaction, code domain.FailureCode, balance domain.Money, corr string, now time.Time) (OutboxEvent, error) {
	return newEvent(EventTransactionRejected, t.WalletID(), corr, "", now,
		TransactionRejectedData{TransactionRef: refOf(t), FailureCode: code, Balance: balance})
}

func pendingReferenceEvent(t *domain.WagerTransaction, next time.Time, corr string, now time.Time) (OutboxEvent, error) {
	return newEvent(EventTransactionPendingReference, t.WalletID(), corr, "", now,
		TransactionPendingReferenceData{TransactionRef: refOf(t),
			ReferenceExternalTransactionID: t.ReferenceExternalID(), NextAttemptAt: next.UTC()})
}

func balanceChangedEvent(w *domain.Wallet, t *domain.WagerTransaction, mv domain.Movement, corr string, now time.Time) (OutboxEvent, error) {
	return newEvent(EventWalletBalanceChanged, w.ID(), corr, t.ID(), now, BalanceChangedData{
		WalletID: w.ID(), TransactionID: t.ID(), Direction: mv.Direction, Money: mv.Money,
		BalanceBefore: mv.Before, BalanceAfter: mv.After, WalletVersion: w.Version()})
}
