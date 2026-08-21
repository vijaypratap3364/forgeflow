package security

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

func TestJWTAuthenticatorValidatesSignatureAndClaims(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, time.August, 21, 12, 0, 0, 0, time.UTC)
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey() error = %v", err)
	}
	authenticator := testJWTAuthenticator(t, publicKey, now)

	tests := []struct {
		name   string
		claims forgeFlowClaims
		key    any
		method jwt.SigningMethod
		wantID UserID
	}{
		{
			name: "valid",
			claims: forgeFlowClaims{Name: "Alice", RegisteredClaims: jwt.RegisteredClaims{
				Subject: "user-alice", Issuer: "forgeflow-tests",
				Audience: jwt.ClaimStrings{"forgeflow-api"}, ExpiresAt: jwt.NewNumericDate(now.Add(time.Hour)),
			}},
			key: privateKey, method: jwt.SigningMethodEdDSA, wantID: "user-alice",
		},
		{
			name: "expired",
			claims: forgeFlowClaims{RegisteredClaims: jwt.RegisteredClaims{
				Subject: "user-alice", Issuer: "forgeflow-tests",
				Audience: jwt.ClaimStrings{"forgeflow-api"}, ExpiresAt: jwt.NewNumericDate(now.Add(-time.Second)),
			}},
			key: privateKey, method: jwt.SigningMethodEdDSA,
		},
		{
			name: "wrong audience",
			claims: forgeFlowClaims{RegisteredClaims: jwt.RegisteredClaims{
				Subject: "user-alice", Issuer: "forgeflow-tests",
				Audience: jwt.ClaimStrings{"another-api"}, ExpiresAt: jwt.NewNumericDate(now.Add(time.Hour)),
			}},
			key: privateKey, method: jwt.SigningMethodEdDSA,
		},
		{
			name: "wrong issuer",
			claims: forgeFlowClaims{RegisteredClaims: jwt.RegisteredClaims{
				Subject: "user-alice", Issuer: "another-issuer",
				Audience: jwt.ClaimStrings{"forgeflow-api"}, ExpiresAt: jwt.NewNumericDate(now.Add(time.Hour)),
			}},
			key: privateKey, method: jwt.SigningMethodEdDSA,
		},
		{
			name: "missing expiration",
			claims: forgeFlowClaims{RegisteredClaims: jwt.RegisteredClaims{
				Subject: "user-alice", Issuer: "forgeflow-tests", Audience: jwt.ClaimStrings{"forgeflow-api"},
			}},
			key: privateKey, method: jwt.SigningMethodEdDSA,
		},
		{
			name: "missing subject",
			claims: forgeFlowClaims{RegisteredClaims: jwt.RegisteredClaims{
				Issuer: "forgeflow-tests", Audience: jwt.ClaimStrings{"forgeflow-api"},
				ExpiresAt: jwt.NewNumericDate(now.Add(time.Hour)),
			}},
			key: privateKey, method: jwt.SigningMethodEdDSA,
		},
		{
			name: "wrong algorithm",
			claims: forgeFlowClaims{RegisteredClaims: jwt.RegisteredClaims{
				Subject: "user-alice", Issuer: "forgeflow-tests",
				Audience: jwt.ClaimStrings{"forgeflow-api"}, ExpiresAt: jwt.NewNumericDate(now.Add(time.Hour)),
			}},
			key: []byte("not-used-by-verifier"), method: jwt.SigningMethodHS256,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			token := jwt.NewWithClaims(test.method, test.claims)
			signed, err := token.SignedString(test.key)
			if err != nil {
				t.Fatalf("SignedString() error = %v", err)
			}
			principal, err := authenticator.Authenticate(context.Background(), signed)
			if test.wantID == "" {
				if !errors.Is(err, ErrUnauthenticated) {
					t.Fatalf("Authenticate() error = %v, want ErrUnauthenticated", err)
				}
				return
			}
			if err != nil || principal.UserID != test.wantID || principal.DisplayName != "Alice" {
				t.Fatalf("Authenticate() = %#v, %v", principal, err)
			}
		})
	}
}

func TestJWTAuthenticatorRejectsWrongSignature(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, time.August, 21, 12, 0, 0, 0, time.UTC)
	trustedPublic, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey(trusted) error = %v", err)
	}
	_, untrustedPrivate, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey(untrusted) error = %v", err)
	}
	authenticator := testJWTAuthenticator(t, trustedPublic, now)
	claims := forgeFlowClaims{RegisteredClaims: jwt.RegisteredClaims{
		Subject: "user-alice", Issuer: "forgeflow-tests", Audience: jwt.ClaimStrings{"forgeflow-api"},
		ExpiresAt: jwt.NewNumericDate(now.Add(time.Hour)),
	}}
	token, err := jwt.NewWithClaims(jwt.SigningMethodEdDSA, claims).SignedString(untrustedPrivate)
	if err != nil {
		t.Fatalf("SignedString() error = %v", err)
	}
	if _, err := authenticator.Authenticate(context.Background(), token); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("Authenticate() error = %v, want ErrUnauthenticated", err)
	}
}

func testJWTAuthenticator(t *testing.T, publicKey ed25519.PublicKey, now time.Time) *JWTAuthenticator {
	t.Helper()
	encoded, err := x509.MarshalPKIXPublicKey(publicKey)
	if err != nil {
		t.Fatalf("MarshalPKIXPublicKey() error = %v", err)
	}
	block := pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: encoded})
	authenticator, err := NewJWTAuthenticator(JWTConfig{
		PublicKeyPEM: string(block),
		Issuer:       "forgeflow-tests",
		Audience:     "forgeflow-api",
		Now:          func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("NewJWTAuthenticator() error = %v", err)
	}
	return authenticator
}
