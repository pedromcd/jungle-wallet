// Package telemetry provides structured logging and Prometheus metrics.
package telemetry

import (
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// NewLogger returns a JSON logger on stdout. Request/response bodies, tokens
// and secrets are never logged by this service.
func NewLogger(level, instance string) *slog.Logger {
	var lv slog.Level
	if err := lv.UnmarshalText([]byte(strings.ToUpper(level))); err != nil {
		lv = slog.LevelInfo
	}
	h := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lv})
	return slog.New(h).With("service", "wallet", "instance", instance)
}

// Metrics implements app.Metrics on a dedicated registry.
type Metrics struct {
	reg          *prometheus.Registry
	transactions *prometheus.CounterVec
	duplicates   *prometheus.CounterVec
	retries      *prometheus.CounterVec
	deadLetters  prometheus.Counter
	conflicts    prometheus.Counter
	divergences  prometheus.Counter
	processing   *prometheus.HistogramVec
	outboxLag    prometheus.Gauge
	httpRequests *prometheus.CounterVec
	httpLatency  *prometheus.HistogramVec
}

func NewMetrics() *Metrics {
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	f := promauto.With(reg)
	return &Metrics{
		reg: reg,
		transactions: f.NewCounterVec(prometheus.CounterOpts{Name: "wallet_transactions_total",
			Help: "Operations by kind and final status."}, []string{"kind", "status"}),
		duplicates: f.NewCounterVec(prometheus.CounterOpts{Name: "wallet_idempotent_replays_total",
			Help: "Duplicate deliveries answered without re-applying."}, []string{"source"}),
		retries: f.NewCounterVec(prometheus.CounterOpts{Name: "wallet_retries_total",
			Help: "Retries by component."}, []string{"component"}),
		deadLetters: f.NewCounter(prometheus.CounterOpts{Name: "wallet_dead_letters_total", Help: "Messages sent to the DLQ."}),
		conflicts: f.NewCounter(prometheus.CounterOpts{Name: "wallet_concurrency_conflicts_total",
			Help: "Transactions retried after deadlock/serialization/version conflicts."}),
		divergences: f.NewCounter(prometheus.CounterOpts{Name: "wallet_reconciliation_divergences_total",
			Help: "Reconciliations that found stored balance != ledger balance."}),
		processing: f.NewHistogramVec(prometheus.HistogramOpts{Name: "wallet_processing_seconds",
			Help: "Processing latency by source.", Buckets: prometheus.DefBuckets}, []string{"source"}),
		outboxLag: f.NewGauge(prometheus.GaugeOpts{Name: "wallet_outbox_lag_seconds",
			Help: "Age of the oldest unpublished outbox event."}),
		httpRequests: f.NewCounterVec(prometheus.CounterOpts{Name: "wallet_http_requests_total",
			Help: "HTTP requests."}, []string{"route", "method", "code"}),
		httpLatency: f.NewHistogramVec(prometheus.HistogramOpts{Name: "wallet_http_request_seconds",
			Help: "HTTP latency.", Buckets: prometheus.DefBuckets}, []string{"route"}),
	}
}

func (m *Metrics) Handler() http.Handler { return promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{}) }

func (m *Metrics) Transaction(kind, status string) {
	m.transactions.WithLabelValues(kind, status).Inc()
}
func (m *Metrics) Duplicate(source string)    { m.duplicates.WithLabelValues(source).Inc() }
func (m *Metrics) Retry(component string)     { m.retries.WithLabelValues(component).Inc() }
func (m *Metrics) DeadLetter()                { m.deadLetters.Inc() }
func (m *Metrics) ConcurrencyConflict()       { m.conflicts.Inc() }
func (m *Metrics) ReconciliationDivergence()  { m.divergences.Inc() }
func (m *Metrics) OutboxLagSeconds(s float64) { m.outboxLag.Set(s) }
func (m *Metrics) ProcessingSeconds(source string, d time.Duration) {
	m.processing.WithLabelValues(source).Observe(d.Seconds())
}

// HTTP records one request; route is the low-cardinality pattern, not the URL.
func (m *Metrics) HTTP(route, method string, code int, d time.Duration) {
	m.httpRequests.WithLabelValues(route, method, httpClass(code)).Inc()
	m.httpLatency.WithLabelValues(route).Observe(d.Seconds())
}

func httpClass(code int) string {
	switch {
	case code >= 500:
		return "5xx"
	case code >= 400:
		return "4xx"
	case code >= 300:
		return "3xx"
	}
	return "2xx"
}
