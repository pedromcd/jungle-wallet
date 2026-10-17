// Package domain contains the financial model. It depends only on the
// standard library: no Fx, HTTP, SQS or persistence concerns leak in here.
package domain

import "errors"

// Sentinel errors are classifiable with errors.Is. None of them is ever
// signalled through panic.
var (
	ErrInvalidAmount      = errors.New("invalid amount")
	ErrInvalidCurrency    = errors.New("invalid currency")
	ErrCurrencyMismatch   = errors.New("currency mismatch")
	ErrMoneyOverflow      = errors.New("money overflow")
	ErrUninitializedMoney = errors.New("uninitialized money")
	ErrInsufficientFunds  = errors.New("insufficient funds")
	ErrInvalidTransition  = errors.New("invalid state transition")
	ErrInvalidEntity      = errors.New("invalid entity")
	ErrOpeningNotAllowed  = errors.New("OPENING cannot be submitted externally")
)

// FailureCode is a stable, documented identifier for a rejection or failure.
type FailureCode string

const (
	// Definitive business outcomes (persisted, terminal, auditable).
	FailInsufficientFunds         FailureCode = "INSUFFICIENT_FUNDS"
	FailReversalInsufficientFunds FailureCode = "REVERSAL_INSUFFICIENT_FUNDS"
	FailPlayerMismatch            FailureCode = "PLAYER_MISMATCH"
	FailCurrencyMismatch          FailureCode = "CURRENCY_MISMATCH"
	FailReferenceNotFound         FailureCode = "REFERENCE_NOT_FOUND"
	FailReferenceNotSuccessful    FailureCode = "REFERENCE_NOT_SUCCESSFUL"
	FailReferencePendingTimeout   FailureCode = "REFERENCE_PENDING_TIMEOUT"
	FailReferenceMismatch         FailureCode = "REFERENCE_MISMATCH"
	FailReferenceInvalidKind      FailureCode = "REFERENCE_INVALID_KIND"
	FailReferenceAmountMismatch   FailureCode = "REFERENCE_AMOUNT_MISMATCH"
	FailReferenceAlreadyReversed  FailureCode = "REFERENCE_ALREADY_REVERSED"
	// Permanent infrastructure/internal failure (state FAILED).
	FailInternalError FailureCode = "INTERNAL_ERROR"
)
