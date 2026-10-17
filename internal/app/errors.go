// Package app holds the use cases. It depends on the domain and on ports
// (interfaces) only; PostgreSQL, SQS, HTTP and Fx live in adapters.
package app

import (
	"context"
	"errors"
)

var (
	ErrNotFound                     = errors.New("not found")
	ErrWalletNotFound               = errors.New("wallet not found")
	ErrWalletExists                 = errors.New("a wallet already exists for this player and currency")
	ErrIdempotencyKeyConflict       = errors.New("idempotency key reused with a different payload")
	ErrDuplicateExternalTransaction = errors.New("external transaction already registered under another idempotency key")
	ErrInboxPayloadMismatch         = errors.New("message id redelivered with a different payload")
	ErrConcurrentUpdate             = errors.New("wallet version changed concurrently")
)

// ValidationError marks malformed or rule-violating input (HTTP 400; for SQS a
// message that can never succeed).
type ValidationError struct{ Err error }

func (e *ValidationError) Error() string { return e.Err.Error() }
func (e *ValidationError) Unwrap() error { return e.Err }

func invalid(err error) error { return &ValidationError{Err: err} }

type classified struct {
	err       error
	transient bool
}

func (c *classified) Error() string { return c.err.Error() }
func (c *classified) Unwrap() error { return c.err }

// Transient wraps an infrastructure error that is worth retrying
// (connection loss, timeouts, deadlocks, broker unavailable).
func Transient(err error) error {
	if err == nil {
		return nil
	}
	return &classified{err: err, transient: true}
}

// Permanent wraps an infrastructure error that will not go away by retrying.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &classified{err: err, transient: false}
}

// IsTransient reports whether err should be retried later.
func IsTransient(err error) bool {
	var c *classified
	if errors.As(err, &c) {
		return c.transient
	}
	return errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled)
}

// NewValidationError lets adapters flag malformed input (e.g. a bad cursor).
func NewValidationError(err error) error { return invalid(err) }

// IsClassified reports whether err already went through Transient/Permanent.
func IsClassified(err error) bool {
	var c *classified
	return errors.As(err, &c)
}
