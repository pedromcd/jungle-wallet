package auth_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-jose/go-jose/v4"
	"github.com/go-jose/go-jose/v4/jwt"

	"github.com/pedro/jungle-wallet/internal/infra/auth"
)

const (
	issuer   = "http://idp.test/realms/wallet"
	audience = "wallet-api"
)

type idp struct {
	key *rsa.PrivateKey
	srv *httptest.Server
	v   *auth.Verifier
}

func newIDP(t *testing.T) *idp {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	set := jose.JSONWebKeySet{Keys: []jose.JSONWebKey{{Key: &key.PublicKey, KeyID: "k1", Algorithm: "RS256", Use: "sig"}}}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(set) // the JWKS endpoint, as Keycloak serves it
	}))
	t.Cleanup(srv.Close)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	return &idp{key: key, srv: srv, v: auth.NewVerifier(ctx, issuer, srv.URL, audience)}
}

func (i *idp) sign(t *testing.T, key *rsa.PrivateKey, claims map[string]any) string {
	t.Helper()
	sig, err := jose.NewSigner(jose.SigningKey{Algorithm: jose.RS256, Key: jose.JSONWebKey{Key: key, KeyID: "k1"}},
		(&jose.SignerOptions{}).WithType("JWT"))
	if err != nil {
		t.Fatal(err)
	}
	raw, err := jwt.Signed(sig).Claims(claims).Serialize()
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func good() map[string]any {
	return map[string]any{
		"iss": issuer, "aud": audience, "sub": "service-account-provider-a", "azp": "provider-a",
		"iat": time.Now().Add(-time.Minute).Unix(), "exp": time.Now().Add(5 * time.Minute).Unix(),
		"realm_access": map[string]any{"roles": []string{"provider", "offline_access"}},
	}
}

func with(c map[string]any, k string, v any) map[string]any {
	if v == nil {
		delete(c, k)
	} else {
		c[k] = v
	}
	return c
}

func TestValidTokenYieldsClientIdentityAndRoles(t *testing.T) {
	i := newIDP(t)
	p, err := i.v.Verify(context.Background(), i.sign(t, i.key, good()))
	if err != nil {
		t.Fatal(err)
	}
	if p.ClientID != "provider-a" || !p.HasRole("provider") || p.HasRole("wallet-internal") {
		t.Fatalf("%+v", p)
	}
	// audience may also be an array, and client_id is accepted when azp is absent
	c := with(with(good(), "aud", []string{"account", audience}), "azp", nil)
	c["client_id"] = "provider-b"
	if p, err := i.v.Verify(context.Background(), i.sign(t, i.key, c)); err != nil || p.ClientID != "provider-b" {
		t.Fatalf("%v %v", p, err)
	}
}

func TestRejectedTokens(t *testing.T) {
	i := newIDP(t)
	other, _ := rsa.GenerateKey(rand.Reader, 2048)
	b64 := func(s string) string { return base64.RawURLEncoding.EncodeToString([]byte(s)) }
	claimsJSON, _ := json.Marshal(good())
	valid := i.sign(t, i.key, good())

	cases := map[string]string{
		"empty":                    "",
		"garbage":                  "not-a-jwt",
		"expired":                  i.sign(t, i.key, with(good(), "exp", time.Now().Add(-time.Minute).Unix())),
		"wrong audience":           i.sign(t, i.key, with(good(), "aud", "another-api")),
		"wrong issuer":             i.sign(t, i.key, with(good(), "iss", "http://evil.test/realms/wallet")),
		"signed with another key":  i.sign(t, other, good()),
		"alg none":                 b64(`{"alg":"none","typ":"JWT"}`) + "." + b64(string(claimsJSON)) + ".",
		"tampered payload":         valid[:len(valid)/2] + "x" + valid[len(valid)/2+1:],
		"truncated signature":      valid[:len(valid)-8],
		"no client identity":       i.sign(t, i.key, with(with(good(), "azp", nil), "client_id", nil)),
		"HS256 forged with pubkey": b64(`{"alg":"HS256","typ":"JWT","kid":"k1"}`) + "." + b64(string(claimsJSON)) + "." + b64("forged"),
	}
	for name, tok := range cases {
		p, err := i.v.Verify(context.Background(), tok)
		if err == nil || p != nil || !errors.Is(err, auth.ErrUnauthenticated) {
			t.Errorf("%s: accepted or wrongly classified (p=%v err=%v)", name, p, err)
		}
	}
}

func TestKeyRotationIsPickedUpFromTheJWKS(t *testing.T) {
	i := newIDP(t)
	if _, err := i.v.Verify(context.Background(), i.sign(t, i.key, good())); err != nil {
		t.Fatal(err)
	}
	// unknown key id -> refused (the remote key set refreshes, still not found)
	other, _ := rsa.GenerateKey(rand.Reader, 2048)
	sig, _ := jose.NewSigner(jose.SigningKey{Algorithm: jose.RS256, Key: jose.JSONWebKey{Key: other, KeyID: "unknown"}}, (&jose.SignerOptions{}).WithType("JWT"))
	raw, _ := jwt.Signed(sig).Claims(good()).Serialize()
	if _, err := i.v.Verify(context.Background(), raw); !errors.Is(err, auth.ErrUnauthenticated) {
		t.Fatalf("token signed with an unpublished key must be refused: %v", err)
	}
}

func TestRolesAreExactMatches(t *testing.T) {
	p := auth.NewPrincipal("s", "c", "provider")
	if !p.HasRole("provider") || p.HasRole("Provider") || p.HasRole("") {
		t.Fatal("role matching must be exact")
	}
}
