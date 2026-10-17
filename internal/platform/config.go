// Package platform holds process-level configuration.
package platform

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config is read once from the environment; invalid values stop the boot.
type Config struct {
	InstanceID string
	LogLevel   string
	HTTPAddr   string

	DatabaseURL string
	DBMaxConns  int

	AWSRegion        string
	SQSEndpoint      string
	QueueTransaction string
	QueueDLQ         string
	QueueEvents      string
	QueueWait        time.Duration

	OIDCIssuer   string
	OIDCJWKSURL  string
	OIDCAudience string
	RoleProvider string
	RoleInternal string

	ConsumerEnabled     bool
	ConsumerConcurrency int
	VisibilityTimeout   time.Duration

	OutboxEnabled      bool
	OutboxPollInterval time.Duration
	OutboxBatch        int
	OutboxLease        time.Duration

	PendingEnabled      bool
	PendingPollInterval time.Duration
	PendingBatch        int
	PendingMaxAttempts  int
	PendingTTL          time.Duration
	PendingBackoffBase  time.Duration
	PendingBackoffMax   time.Duration

	ShutdownTimeout time.Duration
}

// LoadConfig reads and validates the environment.
func LoadConfig() (*Config, error) {
	host, _ := os.Hostname()
	var errs []error
	str := func(k, def string) string {
		if v := os.Getenv(k); v != "" {
			return v
		}
		return def
	}
	need := func(k string) string {
		v := os.Getenv(k)
		if v == "" {
			errs = append(errs, fmt.Errorf("%s is required", k))
		}
		return v
	}
	num := func(k string, def int) int {
		v := os.Getenv(k)
		if v == "" {
			return def
		}
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			errs = append(errs, fmt.Errorf("%s must be a positive integer", k))
			return def
		}
		return n
	}
	dur := func(k string, def time.Duration) time.Duration {
		v := os.Getenv(k)
		if v == "" {
			return def
		}
		d, err := time.ParseDuration(v)
		if err != nil || d <= 0 {
			errs = append(errs, fmt.Errorf("%s must be a positive duration (e.g. 5s)", k))
			return def
		}
		return d
	}
	flag := func(k string, def bool) bool {
		v := strings.ToLower(os.Getenv(k))
		switch v {
		case "":
			return def
		case "1", "true", "yes":
			return true
		case "0", "false", "no":
			return false
		}
		errs = append(errs, fmt.Errorf("%s must be true or false", k))
		return def
	}

	c := &Config{
		InstanceID:  str("INSTANCE_ID", host),
		LogLevel:    str("LOG_LEVEL", "info"),
		HTTPAddr:    str("HTTP_ADDR", ":8080"),
		DatabaseURL: need("DATABASE_URL"),
		DBMaxConns:  num("DB_MAX_CONNS", 20),

		AWSRegion:        str("AWS_REGION", "us-east-1"),
		SQSEndpoint:      os.Getenv("SQS_ENDPOINT"),
		QueueTransaction: str("QUEUE_TRANSACTIONS", "wager-transactions.fifo"),
		QueueDLQ:         str("QUEUE_TRANSACTIONS_DLQ", "wager-transactions-dlq.fifo"),
		QueueEvents:      str("QUEUE_EVENTS", "wager-events.fifo"),
		QueueWait:        dur("QUEUE_RESOLVE_TIMEOUT", 60*time.Second),

		OIDCIssuer:   need("OIDC_ISSUER"),
		OIDCJWKSURL:  need("OIDC_JWKS_URL"),
		OIDCAudience: str("OIDC_AUDIENCE", "wallet-api"),
		RoleProvider: str("ROLE_PROVIDER", "provider"),
		RoleInternal: str("ROLE_INTERNAL", "wallet-internal"),

		ConsumerEnabled:     flag("CONSUMER_ENABLED", true),
		ConsumerConcurrency: num("CONSUMER_CONCURRENCY", 2),
		VisibilityTimeout:   dur("VISIBILITY_TIMEOUT", 60*time.Second),

		OutboxEnabled:      flag("OUTBOX_ENABLED", true),
		OutboxPollInterval: dur("OUTBOX_POLL_INTERVAL", 500*time.Millisecond),
		OutboxBatch:        num("OUTBOX_BATCH", 25),
		OutboxLease:        dur("OUTBOX_LEASE", 30*time.Second),

		PendingEnabled:      flag("PENDING_ENABLED", true),
		PendingPollInterval: dur("PENDING_POLL_INTERVAL", time.Second),
		PendingBatch:        num("PENDING_BATCH", 50),
		PendingMaxAttempts:  num("PENDING_MAX_ATTEMPTS", 8),
		PendingTTL:          dur("PENDING_TTL", 15*time.Minute),
		PendingBackoffBase:  dur("PENDING_BACKOFF_BASE", 2*time.Second),
		PendingBackoffMax:   dur("PENDING_BACKOFF_MAX", 5*time.Minute),

		ShutdownTimeout: ShutdownTimeoutFromEnv(),
	}
	if c.OutboxLease <= 5*time.Second {
		errs = append(errs, errors.New("OUTBOX_LEASE must exceed 5s"))
	}
	if err := errors.Join(errs...); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}
	return c, nil
}

// ShutdownTimeoutFromEnv is needed before Fx starts (StopTimeout option).
func ShutdownTimeoutFromEnv() time.Duration {
	if d, err := time.ParseDuration(os.Getenv("SHUTDOWN_TIMEOUT")); err == nil && d > 0 {
		return d
	}
	return 25 * time.Second
}
