// Package auth validates Keycloak-issued JWT access tokens (client credentials).
package auth

import (
	"context"
	"errors"
	"fmt"

	"github.com/coreos/go-oidc/v3/oidc"
)

// ErrUnauthenticated: missing, malformed, expired, wrongly signed or wrongly
// scoped token. Details stay in the logs, never in the response.
var ErrUnauthenticated = errors.New("unauthenticated")

// Principal is the authenticated service account.
type Principal struct {
	Subject  string
	ClientID string // for providers this is the providerId
	roles    map[string]struct{}
}

func (p *Principal) HasRole(role string) bool { _, ok := p.roles[role]; return ok }

// Verifier checks signature (JWKS), issuer, audience and expiry.
type Verifier struct{ v *oidc.IDTokenVerifier }

// NewVerifier builds a verifier that fetches keys from jwksURL (lazily, with
// caching and key rotation handled by go-oidc) and expects the given issuer
// and audience. Issuer and JWKS URL are separate because inside Docker the
// service reaches Keycloak at a different host name than the one baked into
// the token's "iss" claim. ctx must live as long as the process.
func NewVerifier(ctx context.Context, issuer, jwksURL, audience string) *Verifier {
	keys := oidc.NewRemoteKeySet(ctx, jwksURL)
	return &Verifier{v: oidc.NewVerifier(issuer, keys, &oidc.Config{
		ClientID:             audience,
		SupportedSigningAlgs: []string{"RS256"},
	})}
}

type claims struct {
	Azp         string `json:"azp"`
	ClientID    string `json:"client_id"`
	RealmAccess struct {
		Roles []string `json:"roles"`
	} `json:"realm_access"`
}

// Verify validates a raw bearer token.
func (v *Verifier) Verify(ctx context.Context, raw string) (*Principal, error) {
	tok, err := v.v.Verify(ctx, raw)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrUnauthenticated, err)
	}
	var c claims
	if err := tok.Claims(&c); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrUnauthenticated, err)
	}
	client := c.Azp
	if client == "" {
		client = c.ClientID
	}
	if client == "" {
		return nil, fmt.Errorf("%w: token has no client identity", ErrUnauthenticated)
	}
	p := &Principal{Subject: tok.Subject, ClientID: client, roles: map[string]struct{}{}}
	for _, r := range c.RealmAccess.Roles {
		p.roles[r] = struct{}{}
	}
	return p, nil
}

// NewPrincipal builds a Principal directly (used by tests and fake verifiers).
func NewPrincipal(subject, clientID string, roles ...string) *Principal {
	p := &Principal{Subject: subject, ClientID: clientID, roles: map[string]struct{}{}}
	for _, r := range roles {
		p.roles[r] = struct{}{}
	}
	return p
}
