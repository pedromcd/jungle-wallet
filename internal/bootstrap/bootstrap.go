// Package bootstrap wires the service with Uber Fx.
//
// Start order follows constructor dependencies (database pool, broker queues,
// workers, HTTP last); Fx stops in reverse order, so on SIGTERM the service
// first stops accepting HTTP traffic and polling SQS, then lets workers finish,
// and only then closes the database pool.
package bootstrap

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"time"

	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/fx"
	"go.uber.org/fx/fxevent"

	"github.com/pedro/jungle-wallet/internal/app"
	"github.com/pedro/jungle-wallet/internal/infra/auth"
	"github.com/pedro/jungle-wallet/internal/infra/httpapi"
	"github.com/pedro/jungle-wallet/internal/infra/postgres"
	sqsinfra "github.com/pedro/jungle-wallet/internal/infra/sqs"
	"github.com/pedro/jungle-wallet/internal/infra/telemetry"
	"github.com/pedro/jungle-wallet/internal/platform"
)

// Modules is the complete dependency graph (exported so tests can validate it).
var Modules = fx.Options(
	configModule,
	telemetryModule,
	databaseModule,
	messagingModule,
	securityModule,
	applicationModule,
	workersModule,
	httpModule, // last: stops first
	fx.WithLogger(func(l *slog.Logger) fxevent.Logger { return &fxevent.SlogLogger{Logger: l} }),
	fx.StartTimeout(120*time.Second), // the pool and the broker may still be booting
	fx.StopTimeout(platform.ShutdownTimeoutFromEnv()),
)

// New builds the application; extra options may replace pieces in tests.
func New(extra ...fx.Option) *fx.App {
	return fx.New(append([]fx.Option{Modules}, extra...)...)
}

var configModule = fx.Module("config", fx.Provide(platform.LoadConfig))

var telemetryModule = fx.Module("telemetry", fx.Provide(
	func(c *platform.Config) *slog.Logger { return telemetry.NewLogger(c.LogLevel, c.InstanceID) },
	telemetry.NewMetrics,
	func(m *telemetry.Metrics) app.Metrics { return m },
))

var databaseModule = fx.Module("database", fx.Provide(
	newPool,
	func(p *pgxpool.Pool, m app.Metrics) *postgres.Store { return postgres.NewStore(p, m) },
	func(s *postgres.Store) app.UnitOfWork { return s },
	func(s *postgres.Store) app.Reader { return s },
	func(s *postgres.Store) app.OutboxStore { return s },
))

func newPool(lc fx.Lifecycle, c *platform.Config, log *slog.Logger) (*pgxpool.Pool, error) {
	pool, err := postgres.Connect(context.Background(), c.DatabaseURL, int32(c.DBMaxConns))
	if err != nil {
		return nil, err
	}
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			for { // wait for PostgreSQL (compose starts everything together)
				err := pool.Ping(ctx)
				if err == nil {
					log.Info("database ready")
					return nil
				}
				select {
				case <-ctx.Done():
					return err
				case <-time.After(time.Second):
				}
			}
		},
		OnStop: func(context.Context) error { pool.Close(); return nil }, // runs last
	})
	return pool, nil
}

var messagingModule = fx.Module("messaging", fx.Provide(
	func(c *platform.Config) (*awssqs.Client, error) {
		return sqsinfra.NewClient(context.Background(), c.AWSRegion, c.SQSEndpoint)
	},
	func(c *awssqs.Client) sqsinfra.API { return c },
	newQueues,
))

// newQueues resolves queue URLs during the start phase (the broker may still be
// creating them) and exposes them through a pointer filled before any consumer
// or publisher starts.
func newQueues(lc fx.Lifecycle, api sqsinfra.API, c *platform.Config, log *slog.Logger) *sqsinfra.Queues {
	q := &sqsinfra.Queues{}
	lc.Append(fx.Hook{OnStart: func(ctx context.Context) error {
		resolved, err := sqsinfra.Resolve(ctx, api, sqsinfra.QueueNames{
			Transactions: c.QueueTransaction, DLQ: c.QueueDLQ, Events: c.QueueEvents}, c.QueueWait)
		if err != nil {
			return err
		}
		*q = resolved
		log.Info("queues resolved", "transactions", c.QueueTransaction, "dlq", c.QueueDLQ, "events", c.QueueEvents)
		return nil
	}})
	return q
}

var securityModule = fx.Module("security", fx.Provide(
	func(lc fx.Lifecycle, c *platform.Config) *auth.Verifier {
		ctx, cancel := context.WithCancel(context.Background())
		lc.Append(fx.Hook{OnStop: func(context.Context) error { cancel(); return nil }})
		return auth.NewVerifier(ctx, c.OIDCIssuer, c.OIDCJWKSURL, c.OIDCAudience)
	},
))

var applicationModule = fx.Module("application", fx.Provide(
	func(uow app.UnitOfWork, r app.Reader, m app.Metrics, log *slog.Logger, c *platform.Config) *app.Processor {
		return app.NewProcessor(uow, r, app.Options{MaxPendingAttempts: c.PendingMaxAttempts, PendingTTL: c.PendingTTL,
			BackoffBase: c.PendingBackoffBase, BackoffMax: c.PendingBackoffMax}, m, log)
	},
	func(uow app.UnitOfWork, r app.Reader, m app.Metrics, log *slog.Logger) *app.WalletService {
		return app.NewWalletService(uow, r, m, log)
	},
	func(uow app.UnitOfWork, p *app.Processor, m app.Metrics, log *slog.Logger) *app.MessageHandler {
		return app.NewMessageHandler(uow, p, m, log)
	},
))

var workersModule = fx.Module("workers", fx.Invoke(startPendingWorker, startOutboxWorker, startConsumer))

// loop runs step repeatedly. Stopping first stops scheduling; a step already
// running may finish until the stop deadline, then its context is cancelled
// (its SQL transaction rolls back; nothing is lost because state is durable).
func loop(lc fx.Lifecycle, log *slog.Logger, name string, interval time.Duration, step func(ctx context.Context) (int, error)) {
	schedCtx, stopSched := context.WithCancel(context.Background())
	workCtx, abortWork := context.WithCancel(context.Background())
	done := make(chan struct{})
	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			go func() {
				defer close(done)
				timer := time.NewTimer(0)
				defer timer.Stop()
				for {
					select {
					case <-schedCtx.Done():
						return
					case <-timer.C:
					}
					n, err := step(workCtx)
					if err != nil && workCtx.Err() == nil {
						log.Warn("worker step failed", "worker", name, "error", err)
					}
					wait := interval
					if n > 0 && err == nil {
						wait = 0 // busy: drain without sleeping
					}
					timer.Reset(wait)
				}
			}()
			return nil
		},
		OnStop: func(ctx context.Context) error {
			stopSched()
			select {
			case <-done:
				abortWork()
				log.Info("worker stopped", "worker", name)
				return nil
			case <-ctx.Done():
				abortWork()
				<-done
				return ctx.Err()
			}
		},
	})
}

func startPendingWorker(lc fx.Lifecycle, c *platform.Config, p *app.Processor, log *slog.Logger) {
	if !c.PendingEnabled {
		return
	}
	loop(lc, log, "pending-reference", c.PendingPollInterval, func(ctx context.Context) (int, error) {
		return p.RetryPending(ctx, c.PendingBatch)
	})
}

func startOutboxWorker(lc fx.Lifecycle, c *platform.Config, store app.OutboxStore, api sqsinfra.API, q *sqsinfra.Queues, m app.Metrics, log *slog.Logger) {
	if !c.OutboxEnabled {
		return
	}
	pub := app.NewOutboxPublisher(store, sqsinfra.NewSink(api, q), c.InstanceID,
		app.PublisherOptions{Batch: c.OutboxBatch, Lease: c.OutboxLease}, m, log)
	loop(lc, log, "outbox-publisher", c.OutboxPollInterval, pub.RunOnce)
}

func startConsumer(lc fx.Lifecycle, c *platform.Config, api sqsinfra.API, q *sqsinfra.Queues, h *app.MessageHandler, m app.Metrics, log *slog.Logger) {
	if !c.ConsumerEnabled {
		return
	}
	cons := sqsinfra.NewConsumer(api, q, h, sqsinfra.ConsumerOptions{
		Concurrency: c.ConsumerConcurrency, VisibilityTimeout: c.VisibilityTimeout}, m, log)
	lc.Append(fx.Hook{
		OnStart: func(context.Context) error { cons.Start(); return nil },
		OnStop:  cons.Stop,
	})
}

var httpModule = fx.Module("http",
	fx.Provide(newHandler),
	fx.Invoke(serve),
)

func newHandler(c *platform.Config, p *app.Processor, ws *app.WalletService, v *auth.Verifier, pool *pgxpool.Pool,
	api sqsinfra.API, q *sqsinfra.Queues, m *telemetry.Metrics, log *slog.Logger) http.Handler {
	return httpapi.NewHandler(httpapi.Deps{
		Processor: p, Wallets: ws, Verifier: v, Metrics: m, Log: log,
		ProviderRole: c.RoleProvider, InternalRole: c.RoleInternal,
		Checks: []httpapi.Check{
			{Name: "postgres", Fn: pool.Ping},
			{Name: "sqs", Fn: func(ctx context.Context) error { return sqsinfra.Ready(ctx, api, q.Transactions) }},
		},
	})
}

func serve(lc fx.Lifecycle, c *platform.Config, h http.Handler, log *slog.Logger) {
	srv := &http.Server{
		Addr: c.HTTPAddr, Handler: h,
		ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second,
		WriteTimeout: 30 * time.Second, IdleTimeout: 60 * time.Second,
	}
	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			ln, err := net.Listen("tcp", c.HTTPAddr) // bind errors fail the boot
			if err != nil {
				return err
			}
			go func() {
				if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
					log.Error("http server stopped unexpectedly", "error", err)
				}
			}()
			log.Info("http listening", "addr", c.HTTPAddr)
			return nil
		},
		// Shutdown stops accepting connections and waits for in-flight requests.
		OnStop: srv.Shutdown,
	})
}
