package app

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/pedro/jungle-wallet/internal/domain"
)

// CommandInput is the transport-neutral shape shared by HTTP and SQS.
type CommandInput struct {
	ProviderID                     string
	ExternalTransactionID          string
	IdempotencyKey                 string
	PlayerID                       string
	WalletID                       string
	RoundID                        string
	GameID                         string
	Kind                           string
	Money                          domain.Money
	ReferenceExternalTransactionID string
	CorrelationID                  string
}

// Command is a validated, normalized operation ready for the Processor.
type Command struct {
	Params        domain.ExternalParams
	CorrelationID string
}

// NewCommand validates the input, normalizes ids (canonical lower-case UUIDs)
// and computes the deterministic payload hash. Nothing is persisted here.
func NewCommand(in CommandInput) (Command, error) {
	wallet, err := domain.ParseID(in.WalletID)
	if err != nil {
		return Command{}, invalid(fmt.Errorf("walletId: %w", err))
	}
	player, err := domain.ParseID(in.PlayerID)
	if err != nil {
		return Command{}, invalid(fmt.Errorf("playerId: %w", err))
	}
	p := domain.ExternalParams{
		ID: domain.NewID(), ProviderID: in.ProviderID, ExternalID: in.ExternalTransactionID,
		IdempotencyKey: in.IdempotencyKey, WalletID: wallet, PlayerID: player, RoundID: in.RoundID,
		GameID: in.GameID, Kind: domain.Kind(in.Kind), Money: in.Money,
		ReferenceExternalID: in.ReferenceExternalTransactionID,
	}
	p.PayloadHash = PayloadHash(p)
	// Validate every domain rule now so bad input is a 400 before any I/O.
	if _, err := domain.NewExternalTransaction(p, time.Now()); err != nil {
		return Command{}, invalid(err)
	}
	return Command{Params: p, CorrelationID: in.CorrelationID}, nil
}

// PayloadHash is the sha256 (hex) of the canonical JSON of the business fields.
//
// Canonical form: a JSON object with keys in lexicographic order (Go sorts map
// keys), no insignificant whitespace, UTF-8, containing exactly:
// externalTransactionId, gameId, kind, money{amount,currency}, playerId,
// providerId, referenceExternalTransactionId (only when set), roundId,
// walletId. The idempotency key and any transport metadata (headers, SQS
// messageId, correlation id) are excluded, so HTTP and SQS hash identically.
// Money is hashed in canonical form ("25.00"), ids as lower-case UUIDs.
func PayloadHash(p domain.ExternalParams) string {
	m := map[string]any{
		"providerId":            p.ProviderID,
		"externalTransactionId": p.ExternalID,
		"playerId":              p.PlayerID,
		"walletId":              p.WalletID,
		"roundId":               p.RoundID,
		"gameId":                p.GameID,
		"kind":                  string(p.Kind),
		"money":                 map[string]string{"amount": p.Money.Amount(), "currency": p.Money.Currency()},
	}
	if p.ReferenceExternalID != "" {
		m["referenceExternalTransactionId"] = p.ReferenceExternalID
	}
	b, err := json.Marshal(m)
	if err != nil {
		panic("canonical hash: " + err.Error()) // map of strings cannot fail to marshal
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func inboxHash(c Command) string {
	sum := sha256.Sum256([]byte(c.Params.PayloadHash + "|" + c.Params.IdempotencyKey))
	return hex.EncodeToString(sum[:])
}
