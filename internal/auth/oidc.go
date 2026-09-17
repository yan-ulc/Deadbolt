package auth

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// Identity represents verified human identity returned from OIDC
type Identity struct {
	Issuer    string                 `json:"issuer"`
	Subject   string                 `json:"subject"`
	Email     string                 `json:"email,omitempty"`
	Name      string                 `json:"name,omitempty"`
	RawClaims map[string]interface{} `json:"raw_claims"`
}

// OpenIDConfiguration represents OIDC discovery document
type OpenIDConfiguration struct {
	Issuer                string `json:"issuer"`
	AuthorizationEndpoint string `json:"authorization_endpoint"`
	TokenEndpoint         string `json:"token_endpoint"`
	JWKSURI               string `json:"jwks_uri"`
	UserinfoEndpoint      string `json:"userinfo_endpoint,omitempty"`
}

// JSONWebKey represents a public key in JWKS
type JSONWebKey struct {
	Kty string `json:"kty"`
	Alg string `json:"alg"`
	Use string `json:"use"`
	Kid string `json:"kid"`
	N   string `json:"n"`
	E   string `json:"e"`
	Crv string `json:"crv"`
	X   string `json:"x"`
	Y   string `json:"y"`
}

// JSONWebKeySet represents a set of JWKS keys
type JSONWebKeySet struct {
	Keys []JSONWebKey `json:"keys"`
}

// PKCEParams contains values generated for an OIDC authorization request
type PKCEParams struct {
	CodeVerifier        string
	CodeChallenge       string
	CodeChallengeMethod string
	State               string
	Nonce               string
}

// GeneratePKCE creates cryptographically secure PKCE and state parameters
func GeneratePKCE() (*PKCEParams, error) {
	// 32 random bytes for code verifier
	verifierBytes := make([]byte, 32)
	if _, err := rand.Read(verifierBytes); err != nil {
		return nil, fmt.Errorf("failed to generate code verifier: %w", err)
	}
	codeVerifier := base64.RawURLEncoding.EncodeToString(verifierBytes)

	// S256 challenge = base64url(sha256(code_verifier))
	h := sha256.Sum256([]byte(codeVerifier))
	codeChallenge := base64.RawURLEncoding.EncodeToString(h[:])

	// 32 random bytes for state
	stateBytes := make([]byte, 32)
	if _, err := rand.Read(stateBytes); err != nil {
		return nil, fmt.Errorf("failed to generate state: %w", err)
	}
	state := base64.RawURLEncoding.EncodeToString(stateBytes)

	// 32 random bytes for nonce
	nonceBytes := make([]byte, 32)
	if _, err := rand.Read(nonceBytes); err != nil {
		return nil, fmt.Errorf("failed to generate nonce: %w", err)
	}
	nonce := base64.RawURLEncoding.EncodeToString(nonceBytes)

	return &PKCEParams{
		CodeVerifier:        codeVerifier,
		CodeChallenge:       codeChallenge,
		CodeChallengeMethod: "S256",
		State:               state,
		Nonce:               nonce,
	}, nil
}

// OIDCClient handles standards-compliant OpenID Connect operations
type OIDCClient struct {
	cfg        OIDCConfig
	httpClient *http.Client

	mu        sync.RWMutex
	discovery *OpenIDConfiguration
	jwks      *JSONWebKeySet
	jwksExp   time.Time
}

// NewOIDCClient initializes a new OIDC client
func NewOIDCClient(cfg OIDCConfig, httpClient *http.Client) *OIDCClient {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 15 * time.Second}
	}
	return &OIDCClient{
		cfg:        cfg,
		httpClient: httpClient,
	}
}

// Discover fetches the OpenID configuration document
func (c *OIDCClient) Discover(ctx context.Context) (*OpenIDConfiguration, error) {
	c.mu.RLock()
	if c.discovery != nil {
		defer c.mu.RUnlock()
		return c.discovery, nil
	}
	c.mu.RUnlock()

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.discovery != nil {
		return c.discovery, nil
	}

	wellKnownURL := strings.TrimRight(c.cfg.Issuer, "/") + "/.well-known/openid-configuration"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, wellKnownURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to build discovery request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch OIDC discovery document: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("OIDC discovery failed with status %d: %s", resp.StatusCode, string(body))
	}

	var discovery OpenIDConfiguration
	if err := json.NewDecoder(resp.Body).Decode(&discovery); err != nil {
		return nil, fmt.Errorf("failed to decode OIDC discovery document: %w", err)
	}

	c.discovery = &discovery
	return c.discovery, nil
}

// GetJWKS fetches or returns cached JWKS keys
func (c *OIDCClient) GetJWKS(ctx context.Context, forceRefresh bool) (*JSONWebKeySet, error) {
	c.mu.RLock()
	if !forceRefresh && c.jwks != nil && time.Now().Before(c.jwksExp) {
		defer c.mu.RUnlock()
		return c.jwks, nil
	}
	c.mu.RUnlock()

	discovery, err := c.Discover(ctx)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, discovery.JWKSURI, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to build JWKS request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch JWKS: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("JWKS request failed with status %d: %s", resp.StatusCode, string(body))
	}

	var jwks JSONWebKeySet
	if err := json.NewDecoder(resp.Body).Decode(&jwks); err != nil {
		return nil, fmt.Errorf("failed to decode JWKS: %w", err)
	}

	c.mu.Lock()
	c.jwks = &jwks
	c.jwksExp = time.Now().Add(1 * time.Hour)
	c.mu.Unlock()

	return &jwks, nil
}

// BuildAuthorizationURL constructs the authorization redirect URL with PKCE
func (c *OIDCClient) BuildAuthorizationURL(ctx context.Context, pkce *PKCEParams) (string, error) {
	discovery, err := c.Discover(ctx)
	if err != nil {
		return "", err
	}

	authURL, err := url.Parse(discovery.AuthorizationEndpoint)
	if err != nil {
		return "", fmt.Errorf("invalid authorization endpoint: %w", err)
	}

	q := authURL.Query()
	q.Set("client_id", c.cfg.ClientID)
	q.Set("redirect_uri", c.cfg.RedirectURL)
	q.Set("response_type", "code")
	q.Set("scope", "openid profile email")
	q.Set("state", pkce.State)
	q.Set("nonce", pkce.Nonce)
	q.Set("code_challenge", pkce.CodeChallenge)
	q.Set("code_challenge_method", pkce.CodeChallengeMethod)

	authURL.RawQuery = q.Encode()
	return authURL.String(), nil
}

// ExchangeAndVerify exchanges the authorization code for tokens and verifies the ID token
func (c *OIDCClient) ExchangeAndVerify(ctx context.Context, code, codeVerifier, expectedNonce string) (*Identity, error) {
	return c.exchangeAndVerify(ctx, code, codeVerifier, expectedNonce, c.cfg.ClientID, c.cfg.ClientSecret, c.cfg.RedirectURL)
}

// ExchangePublicCode verifies an authorization-code exchange for the dedicated
// public CLI client. Public clients intentionally send no client secret.
func (c *OIDCClient) ExchangePublicCode(ctx context.Context, code, codeVerifier, redirectURI, expectedNonce, clientID string) (*Identity, error) {
	publicClient := NewOIDCClient(OIDCConfig{Issuer: c.cfg.Issuer, ClientID: clientID}, c.httpClient)
	return publicClient.exchangeAndVerify(ctx, code, codeVerifier, expectedNonce, clientID, "", redirectURI)
}

func (c *OIDCClient) exchangeAndVerify(ctx context.Context, code, codeVerifier, expectedNonce, clientID, clientSecret, redirectURI string) (*Identity, error) {
	discovery, err := c.Discover(ctx)
	if err != nil {
		return nil, err
	}

	form := url.Values{}
	form.Set("grant_type", "authorization_code")
	form.Set("client_id", clientID)
	if clientSecret != "" {
		form.Set("client_secret", clientSecret)
	}
	form.Set("code", code)
	form.Set("redirect_uri", redirectURI)
	form.Set("code_verifier", codeVerifier)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, discovery.TokenEndpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return nil, fmt.Errorf("failed to build token request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to execute token request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("token exchange failed with status %d: %s", resp.StatusCode, string(body))
	}

	var tokenResp struct {
		IDToken     string `json:"id_token"`
		AccessToken string `json:"access_token"`
		TokenType   string `json:"token_type"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&tokenResp); err != nil {
		return nil, fmt.Errorf("failed to decode token response: %w", err)
	}

	if tokenResp.IDToken == "" {
		return nil, errors.New("token response did not contain id_token")
	}

	return c.VerifyIDToken(ctx, tokenResp.IDToken, expectedNonce)
}

// VerifyIDToken parses and verifies an OIDC ID token JWT
func (c *OIDCClient) VerifyIDToken(ctx context.Context, rawJWT, expectedNonce string) (*Identity, error) {
	parts := strings.Split(rawJWT, ".")
	if len(parts) != 3 {
		return nil, errors.New("malformed JWT: expected 3 parts")
	}

	headerBytes, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return nil, fmt.Errorf("invalid JWT header encoding: %w", err)
	}
	var header struct {
		Alg string `json:"alg"`
		Kid string `json:"kid"`
	}
	if err := json.Unmarshal(headerBytes, &header); err != nil {
		return nil, fmt.Errorf("failed to parse JWT header: %w", err)
	}

	payloadBytes, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return nil, fmt.Errorf("invalid JWT payload encoding: %w", err)
	}
	var claims struct {
		Iss   string      `json:"iss"`
		Sub   string      `json:"sub"`
		Aud   interface{} `json:"aud"`
		Exp   int64       `json:"exp"`
		Nbf   int64       `json:"nbf"`
		Nonce string      `json:"nonce"`
		Email string      `json:"email"`
		Name  string      `json:"name"`
	}
	if err := json.Unmarshal(payloadBytes, &claims); err != nil {
		return nil, fmt.Errorf("failed to parse JWT claims: %w", err)
	}

	// 1. Verify Issuer
	if strings.TrimSpace(claims.Iss) == "" {
		return nil, errors.New("missing required issuer (iss) claim in ID token")
	}
	expectedIssuer := strings.TrimRight(c.cfg.Issuer, "/")
	tokenIssuer := strings.TrimRight(claims.Iss, "/")
	if tokenIssuer != expectedIssuer {
		return nil, fmt.Errorf("issuer mismatch: expected %q, got %q", expectedIssuer, tokenIssuer)
	}

	// 2. Verify Subject
	if strings.TrimSpace(claims.Sub) == "" {
		return nil, errors.New("missing required subject (sub) claim in ID token")
	}

	// 3. Verify Audience
	if !audienceContains(claims.Aud, c.cfg.ClientID) {
		return nil, fmt.Errorf("audience mismatch: %v does not contain client ID %q", claims.Aud, c.cfg.ClientID)
	}

	// 4. Verify Expiry (mandatory claim with 1-minute clock skew allowance)
	if claims.Exp <= 0 {
		return nil, errors.New("missing or invalid expiration (exp) claim in ID token")
	}
	now := time.Now().Unix()
	if now > claims.Exp+60 {
		return nil, fmt.Errorf("token expired at %s (now: %s)", time.Unix(claims.Exp, 0), time.Unix(now, 0))
	}

	// 5. Verify Nonce
	if expectedNonce != "" && claims.Nonce != expectedNonce {
		return nil, fmt.Errorf("nonce mismatch: expected %q, got %q", expectedNonce, claims.Nonce)
	}

	// 5. Verify Cryptographic Signature against JWKS
	sigBytes, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return nil, fmt.Errorf("invalid JWT signature encoding: %w", err)
	}

	signedContent := []byte(parts[0] + "." + parts[1])
	jwks, err := c.GetJWKS(ctx, false)
	if err != nil {
		return nil, err
	}

	key, found := findKey(jwks, header.Kid)
	if !found {
		// Key not found; attempt one forced refresh in case of key rotation
		jwks, err = c.GetJWKS(ctx, true)
		if err != nil {
			return nil, err
		}
		key, found = findKey(jwks, header.Kid)
		if !found {
			return nil, fmt.Errorf("key ID %q not found in JWKS", header.Kid)
		}
	}

	if err := verifySignature(header.Alg, key, signedContent, sigBytes); err != nil {
		return nil, fmt.Errorf("signature verification failed: %w", err)
	}

	var rawClaims map[string]interface{}
	_ = json.Unmarshal(payloadBytes, &rawClaims)

	return &Identity{
		Issuer:    claims.Iss,
		Subject:   claims.Sub,
		Email:     claims.Email,
		Name:      claims.Name,
		RawClaims: rawClaims,
	}, nil
}

func audienceContains(aud interface{}, clientID string) bool {
	switch v := aud.(type) {
	case string:
		return v == clientID
	case []interface{}:
		for _, item := range v {
			if s, ok := item.(string); ok && s == clientID {
				return true
			}
		}
	}
	return false
}

func findKey(jwks *JSONWebKeySet, kid string) (*JSONWebKey, bool) {
	if jwks == nil {
		return nil, false
	}
	for i := range jwks.Keys {
		if kid == "" || jwks.Keys[i].Kid == kid {
			return &jwks.Keys[i], true
		}
	}
	return nil, false
}

func verifySignature(alg string, key *JSONWebKey, signedContent, sigBytes []byte) error {
	switch alg {
	case "RS256":
		pubKey, err := parseRSAPublicKey(key)
		if err != nil {
			return err
		}
		hasher := sha256.New()
		hasher.Write(signedContent)
		hashed := hasher.Sum(nil)
		return rsa.VerifyPKCS1v15(pubKey, crypto.SHA256, hashed, sigBytes)
	case "ES256":
		pubKey, err := parseECDSAPublicKey(key)
		if err != nil {
			return err
		}
		hasher := sha256.New()
		hasher.Write(signedContent)
		hashed := hasher.Sum(nil)

		// IEEE P1363 signature format: r || s (each 32 bytes)
		if len(sigBytes) == 64 {
			r := new(big.Int).SetBytes(sigBytes[:32])
			s := new(big.Int).SetBytes(sigBytes[32:])
			if !ecdsa.Verify(pubKey, hashed, r, s) {
				return errors.New("invalid ECDSA signature")
			}
			return nil
		}
		return errors.New("invalid ECDSA signature byte length")
	default:
		return fmt.Errorf("unsupported JWS algorithm %q", alg)
	}
}

func parseRSAPublicKey(key *JSONWebKey) (*rsa.PublicKey, error) {
	nBytes, err := base64.RawURLEncoding.DecodeString(key.N)
	if err != nil {
		return nil, fmt.Errorf("failed to decode RSA modulus N: %w", err)
	}
	eBytes, err := base64.RawURLEncoding.DecodeString(key.E)
	if err != nil {
		return nil, fmt.Errorf("failed to decode RSA exponent E: %w", err)
	}

	var eInt int
	for _, b := range eBytes {
		eInt = (eInt << 8) | int(b)
	}

	return &rsa.PublicKey{
		N: new(big.Int).SetBytes(nBytes),
		E: eInt,
	}, nil
}

func parseECDSAPublicKey(key *JSONWebKey) (*ecdsa.PublicKey, error) {
	xBytes, err := base64.RawURLEncoding.DecodeString(key.X)
	if err != nil {
		return nil, fmt.Errorf("failed to decode ECDSA X coordinate: %w", err)
	}
	yBytes, err := base64.RawURLEncoding.DecodeString(key.Y)
	if err != nil {
		return nil, fmt.Errorf("failed to decode ECDSA Y coordinate: %w", err)
	}

	return &ecdsa.PublicKey{
		Curve: elliptic.P256(),
		X:     new(big.Int).SetBytes(xBytes),
		Y:     new(big.Int).SetBytes(yBytes),
	}, nil
}
