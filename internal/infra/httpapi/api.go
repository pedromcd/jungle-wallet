// Package httpapi exposes the REST API. It translates HTTP to use cases and
// back; business rules live in app/domain.
package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"regexp"
	"strconv"
	"time"

	"github.com/pedro/jungle-wallet/internal/app"
	"github.com/pedro/jungle-wallet/internal/domain"
	"github.com/pedro/jungle-wallet/internal/infra/auth"
	"github.com/pedro/jungle-wallet/internal/infra/telemetry"
)

// TokenVerifier validates bearer tokens.
type TokenVerifier interface {
	Verify(ctx context.Context, raw string) (*auth.Principal, error)
}

// Check is one readiness probe (database, broker).
type Check struct {
	Name string
	Fn   func(ctx context.Context) error
}

// Deps are the collaborators of the HTTP layer.
type Deps struct {
	Processor    *app.Processor
	Wallets      *app.WalletService
	Verifier     TokenVerifier
	Checks       []Check
	Metrics      *telemetry.Metrics
	Log          *slog.Logger
	ProviderRole string
	InternalRole string
}

type api struct{ Deps }

type ctxKey int

const (
	keyPrincipal ctxKey = iota
	keyCorrelation
	keyInfo
)

// reqInfo is filled by the route wrapper for logging and metrics.
type reqInfo struct{ route, client string }

var correlationRe = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,64}$`)

// NewHandler builds the full handler chain.
func NewHandler(d Deps) http.Handler {
	a := &api{d}
	mux := http.NewServeMux()
	provider := []string{d.ProviderRole}
	internal := []string{d.InternalRole}
	both := []string{d.ProviderRole, d.InternalRole}

	a.route(mux, "POST /wallets", internal, a.createWallet)
	a.route(mux, "GET /wallets/{walletId}", internal, a.getWallet)
	a.route(mux, "GET /wallets/{walletId}/ledger", internal, a.getLedger)
	a.route(mux, "POST /wallets/{walletId}/reconciliation", internal, a.reconcile)
	a.route(mux, "POST /wagering/transactions", provider, a.submit)
	a.route(mux, "GET /wagering/transactions/{transactionId}", both, a.getTransaction)
	a.route(mux, "GET /providers/{providerId}/wagering/transactions/{externalTransactionId}", both, a.getProviderTransaction)

	a.route(mux, "GET /health/live", nil, func(w http.ResponseWriter, r *http.Request) error {
		writeJSON(w, http.StatusOK, map[string]string{"status": "UP"})
		return nil
	})
	a.route(mux, "GET /health/ready", nil, a.ready)
	if d.Metrics != nil {
		mux.Handle("GET /metrics", d.Metrics.Handler())
	}
	return a.recoverer(a.observe(mux))
}

type handlerFunc func(http.ResponseWriter, *http.Request) error

// route registers a handler; roles == nil means public. Errors returned by the
// handler are mapped to responses in one place.
func (a *api) route(mux *http.ServeMux, pattern string, roles []string, h handlerFunc) {
	mux.HandleFunc(pattern, func(w http.ResponseWriter, r *http.Request) {
		if info, ok := r.Context().Value(keyInfo).(*reqInfo); ok {
			info.route = pattern
		}
		if roles != nil {
			p, err := a.authenticate(r)
			if err != nil {
				w.Header().Set("WWW-Authenticate", `Bearer realm="wallet"`)
				a.writeError(w, r, err)
				return
			}
			if !hasAny(p, roles) {
				a.writeError(w, r, errForbidden)
				return
			}
			if info, ok := r.Context().Value(keyInfo).(*reqInfo); ok {
				info.client = p.ClientID
			}
			r = r.WithContext(context.WithValue(r.Context(), keyPrincipal, p))
		}
		if err := h(w, r); err != nil {
			a.writeError(w, r, err)
		}
	})
}

func hasAny(p *auth.Principal, roles []string) bool {
	for _, r := range roles {
		if p.HasRole(r) {
			return true
		}
	}
	return false
}

func (a *api) authenticate(r *http.Request) (*auth.Principal, error) {
	h := r.Header.Get("Authorization")
	const prefix = "Bearer "
	if len(h) <= len(prefix) || h[:len(prefix)] != prefix {
		return nil, auth.ErrUnauthenticated
	}
	p, err := a.Verifier.Verify(r.Context(), h[len(prefix):])
	if err != nil {
		a.Log.Info("authentication failed", "correlationId", correlationOf(r.Context()), "error", err)
		return nil, auth.ErrUnauthenticated
	}
	return p, nil
}

func principalOf(r *http.Request) *auth.Principal {
	p, _ := r.Context().Value(keyPrincipal).(*auth.Principal)
	return p
}

func correlationOf(ctx context.Context) string {
	s, _ := ctx.Value(keyCorrelation).(string)
	return s
}

// ------------------------------------------------------------- middleware

type statusWriter struct {
	http.ResponseWriter
	code int
}

func (s *statusWriter) WriteHeader(c int) { s.code = c; s.ResponseWriter.WriteHeader(c) }

func (a *api) observe(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		corr := r.Header.Get("X-Correlation-Id")
		if !correlationRe.MatchString(corr) {
			corr = domain.NewID()
		}
		w.Header().Set("X-Correlation-Id", corr)
		info := &reqInfo{route: "unmatched"}
		ctx := context.WithValue(context.WithValue(r.Context(), keyCorrelation, corr), keyInfo, info)
		sw := &statusWriter{ResponseWriter: w, code: http.StatusOK}
		next.ServeHTTP(sw, r.WithContext(ctx))
		d := time.Since(start)
		if a.Metrics != nil {
			a.Metrics.HTTP(info.route, r.Method, sw.code, d)
		}
		if info.route != "GET /health/live" && info.route != "GET /metrics" {
			a.Log.Info("request", "method", r.Method, "route", info.route, "status", sw.code,
				"durationMs", d.Milliseconds(), "correlationId", corr, "client", info.client)
		}
	})
}

func (a *api) recoverer(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				a.Log.Error("panic in handler", "panic", fmt.Sprint(rec), "correlationId", correlationOf(r.Context()))
				writeJSON(w, http.StatusInternalServerError, errorBody("INTERNAL_ERROR", "internal error", correlationOf(r.Context())))
			}
		}()
		next.ServeHTTP(w, r)
	})
}

// ----------------------------------------------------------------- errors

var (
	errForbidden = errors.New("forbidden")
	errMissingID = errors.New("Idempotency-Key header is required")
)

type apiError struct {
	Error struct {
		Code          string `json:"code"`
		Message       string `json:"message"`
		CorrelationID string `json:"correlationId,omitempty"`
	} `json:"error"`
}

func errorBody(code, msg, corr string) apiError {
	var e apiError
	e.Error.Code, e.Error.Message, e.Error.CorrelationID = code, msg, corr
	return e
}

func (a *api) writeError(w http.ResponseWriter, r *http.Request, err error) {
	corr := correlationOf(r.Context())
	status, code, msg := http.StatusInternalServerError, "INTERNAL_ERROR", "internal error"
	var ve *app.ValidationError
	switch {
	case errors.As(err, &ve):
		status, code, msg = http.StatusBadRequest, "INVALID_REQUEST", ve.Error()
	case errors.Is(err, errMissingID):
		status, code, msg = http.StatusBadRequest, "MISSING_IDEMPOTENCY_KEY", err.Error()
	case errors.Is(err, auth.ErrUnauthenticated):
		status, code, msg = http.StatusUnauthorized, "UNAUTHENTICATED", "missing or invalid credentials"
	case errors.Is(err, errForbidden):
		status, code, msg = http.StatusForbidden, "FORBIDDEN", "the authenticated client may not perform this operation"
	case errors.Is(err, app.ErrWalletNotFound):
		status, code, msg = http.StatusNotFound, "WALLET_NOT_FOUND", "wallet not found"
	case errors.Is(err, app.ErrNotFound):
		status, code, msg = http.StatusNotFound, "NOT_FOUND", "resource not found"
	case errors.Is(err, app.ErrWalletExists):
		status, code, msg = http.StatusConflict, "WALLET_ALREADY_EXISTS", err.Error()
	case errors.Is(err, app.ErrIdempotencyKeyConflict):
		status, code, msg = http.StatusConflict, "IDEMPOTENCY_KEY_PAYLOAD_MISMATCH", err.Error()
	case errors.Is(err, app.ErrDuplicateExternalTransaction):
		status, code, msg = http.StatusConflict, "DUPLICATE_EXTERNAL_TRANSACTION", err.Error()
	case app.IsTransient(err):
		w.Header().Set("Retry-After", "1")
		status, code, msg = http.StatusServiceUnavailable, "SERVICE_UNAVAILABLE", "temporarily unavailable, retry with the same Idempotency-Key"
		a.Log.Warn("transient failure", "correlationId", corr, "error", err)
	default:
		a.Log.Error("unexpected failure", "correlationId", corr, "error", err)
	}
	writeJSON(w, status, errorBody(code, msg, corr))
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// decode reads a strict JSON body: size-limited, unknown fields and trailing
// data rejected. Money fields are decoded by domain.Money (strings only).
func decode(w http.ResponseWriter, r *http.Request, dst any) error {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return app.NewValidationError(fmt.Errorf("malformed JSON body: %w", err))
	}
	if _, err := dec.Token(); err != io.EOF {
		return app.NewValidationError(errors.New("unexpected data after the JSON body"))
	}
	return nil
}

// --------------------------------------------------------------- handlers

type walletView struct {
	ID        string       `json:"id"`
	PlayerID  string       `json:"playerId"`
	Balance   domain.Money `json:"balance"`
	Version   int64        `json:"version"`
	CreatedAt time.Time    `json:"createdAt"`
	UpdatedAt time.Time    `json:"updatedAt"`
}

func viewWallet(w *domain.Wallet) walletView {
	return walletView{w.ID(), w.PlayerID(), w.Balance(), w.Version(), w.CreatedAt().UTC(), w.UpdatedAt().UTC()}
}

func (a *api) createWallet(w http.ResponseWriter, r *http.Request) error {
	var req struct {
		PlayerID       string       `json:"playerId"`
		InitialBalance domain.Money `json:"initialBalance"`
	}
	if err := decode(w, r, &req); err != nil {
		return err
	}
	wl, err := a.Wallets.Open(r.Context(), req.PlayerID, req.InitialBalance, correlationOf(r.Context()))
	if err != nil {
		return err
	}
	w.Header().Set("Location", "/wallets/"+wl.ID())
	writeJSON(w, http.StatusCreated, viewWallet(wl))
	return nil
}

func (a *api) getWallet(w http.ResponseWriter, r *http.Request) error {
	wl, err := a.Wallets.Get(r.Context(), r.PathValue("walletId"))
	if err != nil {
		return err
	}
	writeJSON(w, http.StatusOK, viewWallet(wl))
	return nil
}

func (a *api) getLedger(w http.ResponseWriter, r *http.Request) error {
	limit := 0
	if v := r.URL.Query().Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return app.NewValidationError(errors.New("limit must be an integer"))
		}
		limit = n
	}
	page, err := a.Wallets.Ledger(r.Context(), r.PathValue("walletId"), r.URL.Query().Get("cursor"), limit)
	if err != nil {
		return err
	}
	type item struct {
		ID            string           `json:"id"`
		TransactionID string           `json:"transactionId"`
		Direction     domain.Direction `json:"direction"`
		Money         domain.Money     `json:"money"`
		BalanceBefore domain.Money     `json:"balanceBefore"`
		BalanceAfter  domain.Money     `json:"balanceAfter"`
		CreatedAt     time.Time        `json:"createdAt"`
	}
	items := make([]item, 0, len(page.Items))
	for _, it := range page.Items {
		items = append(items, item{it.ID, it.TransactionID, it.Direction, it.Money, it.BalanceBefore, it.BalanceAfter, it.CreatedAt.UTC()})
	}
	resp := map[string]any{"items": items}
	if page.NextCursor != "" {
		resp["nextCursor"] = page.NextCursor
	}
	writeJSON(w, http.StatusOK, resp)
	return nil
}

func (a *api) reconcile(w http.ResponseWriter, r *http.Request) error {
	rec, err := a.Wallets.Reconcile(r.Context(), r.PathValue("walletId"))
	if err != nil {
		return err
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"walletId": rec.WalletID, "storedBalance": rec.Stored, "calculatedBalance": rec.Calculated,
		"difference": rec.Difference, "consistent": rec.Consistent, "checkedEntries": rec.CheckedEntries,
	})
	return nil
}

type submitRequest struct {
	ProviderID                     string       `json:"providerId"`
	ExternalTransactionID          string       `json:"externalTransactionId"`
	PlayerID                       string       `json:"playerId"`
	WalletID                       string       `json:"walletId"`
	RoundID                        string       `json:"roundId"`
	GameID                         string       `json:"gameId"`
	Kind                           string       `json:"kind"`
	Money                          domain.Money `json:"money"`
	ReferenceExternalTransactionID string       `json:"referenceExternalTransactionId,omitempty"`
}

type outcomeView struct {
	TransactionID    string        `json:"transactionId"`
	Status           string        `json:"status"`
	FailureCode      string        `json:"failureCode,omitempty"`
	Balance          *domain.Money `json:"balance,omitempty"`
	IdempotentReplay bool          `json:"idempotentReplay"`
	NextAttemptAt    *time.Time    `json:"nextAttemptAt,omitempty"`
}

// submit: 200 PROCESSED (also for replays), 202 PENDING_REFERENCE, 422 REJECTED.
func (a *api) submit(w http.ResponseWriter, r *http.Request) error {
	key := r.Header.Get("Idempotency-Key")
	if key == "" {
		return errMissingID
	}
	var req submitRequest
	if err := decode(w, r, &req); err != nil {
		return err
	}
	if req.ProviderID != principalOf(r).ClientID {
		return errForbidden // a provider can only act as itself
	}
	cmd, err := app.NewCommand(app.CommandInput{
		ProviderID: req.ProviderID, ExternalTransactionID: req.ExternalTransactionID, IdempotencyKey: key,
		PlayerID: req.PlayerID, WalletID: req.WalletID, RoundID: req.RoundID, GameID: req.GameID, Kind: req.Kind,
		Money: req.Money, ReferenceExternalTransactionID: req.ReferenceExternalTransactionID,
		CorrelationID: correlationOf(r.Context()),
	})
	if err != nil {
		return err
	}
	out, err := a.Processor.Process(r.Context(), cmd)
	if err != nil {
		return err
	}
	v := outcomeView{TransactionID: out.TransactionID, Status: string(out.Status), FailureCode: string(out.FailureCode),
		Balance: out.Balance, IdempotentReplay: out.Replay}
	if out.NextAttemptAt != nil {
		t := out.NextAttemptAt.UTC()
		v.NextAttemptAt = &t
	}
	switch out.Status {
	case domain.StatusProcessed:
		writeJSON(w, http.StatusOK, v)
	case domain.StatusPendingReference, domain.StatusPending:
		w.Header().Set("Location", "/providers/"+req.ProviderID+"/wagering/transactions/"+req.ExternalTransactionID)
		writeJSON(w, http.StatusAccepted, v)
	case domain.StatusRejected:
		writeJSON(w, http.StatusUnprocessableEntity, v)
	default: // FAILED
		writeJSON(w, http.StatusInternalServerError, v)
	}
	return nil
}

type transactionView struct {
	TransactionID                  string             `json:"transactionId"`
	ProviderID                     string             `json:"providerId"`
	ExternalTransactionID          string             `json:"externalTransactionId"`
	WalletID                       string             `json:"walletId"`
	PlayerID                       string             `json:"playerId"`
	RoundID                        string             `json:"roundId"`
	GameID                         string             `json:"gameId"`
	Kind                           domain.Kind        `json:"kind"`
	Money                          domain.Money       `json:"money"`
	Status                         domain.Status      `json:"status"`
	FailureCode                    domain.FailureCode `json:"failureCode,omitempty"`
	Balance                        *domain.Money      `json:"balance,omitempty"`
	ReferenceExternalTransactionID string             `json:"referenceExternalTransactionId,omitempty"`
	NextAttemptAt                  *time.Time         `json:"nextAttemptAt,omitempty"`
	CreatedAt                      time.Time          `json:"createdAt"`
	UpdatedAt                      time.Time          `json:"updatedAt"`
}

func viewTransaction(t *domain.WagerTransaction) transactionView {
	v := transactionView{TransactionID: t.ID(), ProviderID: t.ProviderID(), ExternalTransactionID: t.ExternalID(),
		WalletID: t.WalletID(), PlayerID: t.PlayerID(), RoundID: t.RoundID(), GameID: t.GameID(), Kind: t.Kind(),
		Money: t.Money(), Status: t.Status(), FailureCode: t.FailureCode(),
		ReferenceExternalTransactionID: t.ReferenceExternalID(), CreatedAt: t.CreatedAt().UTC(), UpdatedAt: t.UpdatedAt().UTC()}
	if b, ok := t.ResultBalance(); ok {
		v.Balance = &b
	}
	if n := t.NextAttemptAt(); n != nil && t.Status() == domain.StatusPendingReference {
		u := n.UTC()
		v.NextAttemptAt = &u
	}
	return v
}

func (a *api) isInternal(r *http.Request) bool { return principalOf(r).HasRole(a.InternalRole) }

func (a *api) getTransaction(w http.ResponseWriter, r *http.Request) error {
	t, err := a.Wallets.Transaction(r.Context(), r.PathValue("transactionId"))
	if err != nil {
		return err
	}
	// Providers only see their own operations; for others the resource simply
	// does not exist (no information leak).
	if !a.isInternal(r) && t.ProviderID() != principalOf(r).ClientID {
		return app.ErrNotFound
	}
	writeJSON(w, http.StatusOK, viewTransaction(t))
	return nil
}

func (a *api) getProviderTransaction(w http.ResponseWriter, r *http.Request) error {
	pid := r.PathValue("providerId")
	if !a.isInternal(r) && pid != principalOf(r).ClientID {
		return errForbidden
	}
	t, err := a.Wallets.ProviderTransaction(r.Context(), pid, r.PathValue("externalTransactionId"))
	if err != nil {
		return err
	}
	writeJSON(w, http.StatusOK, viewTransaction(t))
	return nil
}

// ready checks PostgreSQL and the broker (not Keycloak: tokens are verified
// locally with cached keys, so a Keycloak blip must not take the service out).
func (a *api) ready(w http.ResponseWriter, r *http.Request) error {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	failed := []string{}
	for _, c := range a.Checks {
		if err := c.Fn(ctx); err != nil {
			a.Log.Warn("readiness check failed", "check", c.Name, "error", err)
			failed = append(failed, c.Name)
		}
	}
	if len(failed) > 0 {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"status": "DOWN", "failed": failed})
		return nil
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "UP"})
	return nil
}
