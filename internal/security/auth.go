package security

import (
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// Principal is the authenticated identity established from a verified token.
// Authorization roles are intentionally absent and come from persisted state.
type Principal struct {
	UserID      UserID
	DisplayName string
}

// Authenticator verifies one bearer token and returns its stable principal.
type Authenticator interface {
	Authenticate(context.Context, string) (Principal, error)
}

// JWTConfig defines the trust policy for externally issued ForgeFlow tokens.
type JWTConfig struct {
	PublicKeyPEM string
	Issuer       string
	Audience     string
	Leeway       time.Duration
	Now          func() time.Time
}

// JWTAuthenticator validates Ed25519 signatures and registered JWT claims.
type JWTAuthenticator struct {
	publicKey ed25519.PublicKey
	parser    *jwt.Parser
}

type forgeFlowClaims struct {
	Name string `json:"name,omitempty"`
	jwt.RegisteredClaims
}

// NewJWTAuthenticator creates a verifier that accepts only EdDSA tokens with
// the configured issuer, audience, subject, and expiration.
func NewJWTAuthenticator(config JWTConfig) (*JWTAuthenticator, error) {
	if strings.TrimSpace(config.PublicKeyPEM) == "" {
		return nil, errors.New("create JWT authenticator: public key is empty")
	}
	if strings.TrimSpace(config.Issuer) == "" || strings.TrimSpace(config.Audience) == "" {
		return nil, errors.New("create JWT authenticator: issuer and audience are required")
	}
	if config.Leeway < 0 {
		return nil, errors.New("create JWT authenticator: leeway must not be negative")
	}
	key, err := jwt.ParseEdPublicKeyFromPEM([]byte(config.PublicKeyPEM))
	if err != nil {
		return nil, fmt.Errorf("create JWT authenticator: parse Ed25519 public key: %w", err)
	}
	publicKey, ok := key.(ed25519.PublicKey)
	if !ok {
		return nil, errors.New("create JWT authenticator: public key is not Ed25519")
	}
	options := []jwt.ParserOption{
		jwt.WithValidMethods([]string{jwt.SigningMethodEdDSA.Alg()}),
		jwt.WithExpirationRequired(),
		jwt.WithIssuer(config.Issuer),
		jwt.WithAudience(config.Audience),
		jwt.WithLeeway(config.Leeway),
		jwt.WithStrictDecoding(),
	}
	if config.Now != nil {
		options = append(options, jwt.WithTimeFunc(config.Now))
	}
	return &JWTAuthenticator{
		publicKey: append(ed25519.PublicKey(nil), publicKey...),
		parser:    jwt.NewParser(options...),
	}, nil
}

// Authenticate validates token and returns a principal without exposing why
// hostile credentials failed.
func (authenticator *JWTAuthenticator) Authenticate(ctx context.Context, token string) (Principal, error) {
	if ctx == nil || ctx.Err() != nil || strings.TrimSpace(token) == "" {
		return Principal{}, ErrUnauthenticated
	}
	claims := &forgeFlowClaims{}
	parsed, err := authenticator.parser.ParseWithClaims(token, claims, func(candidate *jwt.Token) (any, error) {
		if candidate.Method != jwt.SigningMethodEdDSA {
			return nil, ErrUnauthenticated
		}
		return authenticator.publicKey, nil
	})
	if err != nil || !parsed.Valid || !validIdentifier(claims.Subject) {
		return Principal{}, ErrUnauthenticated
	}
	displayName := strings.TrimSpace(claims.Name)
	if !validName(displayName) {
		displayName = claims.Subject
	}
	return Principal{UserID: UserID(claims.Subject), DisplayName: displayName}, nil
}

var _ Authenticator = (*JWTAuthenticator)(nil)
