package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/Ryanakml/Deadbolt/internal/auth"
	"github.com/Ryanakml/Deadbolt/internal/auth/oidcfixture"
	"github.com/Ryanakml/Deadbolt/internal/controlplane"
)

func TestPublicCLIOIDCExchangesPKCEForHumanBearer(t *testing.T) {
	tc := setupTenantContext(t)
	defer tc.cleanup()
	fixture, err := oidcfixture.NewFixtureServer("deadbolt-cli")
	if err != nil {
		t.Fatal(err)
	}
	defer fixture.Close()
	tc.authCfg.RuntimeMode = auth.ModeHosted
	tc.authCfg.OIDC = auth.OIDCConfig{Issuer: fixture.URL(), ClientID: "dashboard-bff", CLIClientID: "deadbolt-cli"}
	tc.authCfg.AllowedOrigins = []string{"https://dashboard.example"}
	tc.authCfg.CookieSecure = true
	server := httptest.NewServer(controlplane.BuildMux(tc.authCfg, tc.runtimePool, nil, nil))
	defer server.Close()

	var metadata struct {
		Issuer   string `json:"issuer"`
		ClientID string `json:"clientId"`
	}
	getJSON(t, http.MethodGet, server.URL+"/api/auth/cli/config", nil, &metadata, http.StatusOK)
	if metadata.Issuer != fixture.URL() || metadata.ClientID != "deadbolt-cli" {
		t.Fatalf("unexpected public metadata: %#v", metadata)
	}

	requestCode := func(stateOverride string) (string, *auth.PKCEParams, string) {
		pkce, err := auth.GeneratePKCE()
		if err != nil {
			t.Fatal(err)
		}
		callback := httptest.NewServer(http.NewServeMux())
		defer callback.Close()
		codeCh := make(chan string, 1)
		callback.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			codeCh <- r.URL.Query().Get("code")
			w.WriteHeader(http.StatusOK)
		})
		client := auth.NewOIDCClient(auth.OIDCConfig{Issuer: metadata.Issuer, ClientID: metadata.ClientID, RedirectURL: callback.URL + "/callback"}, fixture.Client())
		authorizeURL, err := client.BuildAuthorizationURL(context.Background(), pkce)
		if err != nil {
			t.Fatal(err)
		}
		if stateOverride != "" {
			u, _ := url.Parse(authorizeURL)
			q := u.Query()
			q.Set("state", stateOverride)
			u.RawQuery = q.Encode()
			authorizeURL = u.String()
		}
		resp, err := fixture.Client().Get(authorizeURL)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return <-codeCh, pkce, callback.URL + "/callback"
	}

	code, pkce, redirect := requestCode("")
	wrong := postCLIToken(t, server.URL, code, "wrong-verifier", redirect, pkce.Nonce)
	if wrong.Code != http.StatusUnauthorized {
		t.Fatalf("wrong verifier status=%d", wrong.Code)
	}
	code, pkce, redirect = requestCode("")
	valid := postCLIToken(t, server.URL, code, pkce.CodeVerifier, redirect, pkce.Nonce)
	if valid.Code != http.StatusOK {
		t.Fatalf("valid exchange status=%d: %s", valid.Code, valid.Body.String())
	}
	var token struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.Unmarshal(valid.Body.Bytes(), &token); err != nil || len(token.AccessToken) < 7 || token.AccessToken[:6] != "dbcli_" {
		t.Fatalf("expected dbcli bearer")
	}

	create, _ := http.NewRequest(http.MethodPost, server.URL+"/v1/organizations", bytes.NewBufferString(`{"name":"CLI OIDC Org"}`))
	create.Header.Set("Authorization", "Bearer "+token.AccessToken)
	create.Header.Set("Idempotency-Key", "cli-oidc-create-org")
	create.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(create)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusCreated {
		t.Fatalf("dbcli human bearer could not call /v1: %d", response.StatusCode)
	}
}

func postCLIToken(t *testing.T, base, code, verifier, redirect, nonce string) *httptest.ResponseRecorder {
	t.Helper()
	b, _ := json.Marshal(map[string]string{"code": code, "code_verifier": verifier, "redirect_uri": redirect, "nonce": nonce})
	request, _ := http.NewRequest(http.MethodPost, base+"/api/auth/cli/token", bytes.NewReader(b))
	request.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	recorder := httptest.NewRecorder()
	recorder.Code = response.StatusCode
	_, _ = recorder.Body.ReadFrom(response.Body)
	return recorder
}

func getJSON(t *testing.T, method, target string, body *bytes.Buffer, dest any, expected int) {
	t.Helper()
	var requestBody io.Reader
	if body != nil {
		requestBody = body
	}
	request, _ := http.NewRequest(method, target, requestBody)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != expected {
		t.Fatalf("%s %s got %d", method, target, response.StatusCode)
	}
	if err := json.NewDecoder(response.Body).Decode(dest); err != nil {
		t.Fatal(err)
	}
}
