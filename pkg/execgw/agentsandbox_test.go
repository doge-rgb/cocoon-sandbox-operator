// Copyright 2026 The CocoonStack Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package execgw

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/doge-rgb/cocoon-sandbox-operator/pkg/scale"
)

// stubResolver answers for one sandbox and denies everything else.
type stubResolver struct{ id, addr string }

func (s stubResolver) Resolve(_ context.Context, id string) (string, string, error) {
	if id != s.id {
		return "", "", ErrUnknownSandbox
	}
	return s.id, s.addr, nil
}

// TestRouterRejectsBeforeTouchingAGuest pins the failure modes a caller can
// tell apart. The upstream SDK reports whatever status it gets, so a sandbox on
// a drained node and a sandbox this gateway has no credential for must not both
// arrive as the same opaque 502 — one is retryable elsewhere, the other needs a
// token the caller has to supply.
func TestRouterRejectsBeforeTouchingAGuest(t *testing.T) {
	gw := New(stubResolver{id: "sb_known", addr: "10.0.0.1:7777"}, logr.Discard())
	h := gw.AgentSandboxHandler()

	do := func(id string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(http.MethodPost, "/execute", strings.NewReader(`{"command":"true"}`))
		if id != "" {
			r.Header.Set(sandboxIDHeader, id)
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w
	}

	assert.Equal(t, http.StatusBadRequest, do("").Code,
		"a request that names no sandbox is the caller's error, not a gateway failure")
	assert.Equal(t, http.StatusNotFound, do("sb_missing").Code,
		"an id no node holds must read as absent, not as a broken data plane")
	assert.Equal(t, http.StatusUnauthorized, do("sb_known").Code,
		"a sandbox whose credential this process never saw needs a token from the caller, "+
			"and saying so is the only way the caller can fix it")

	// A malformed body must not reach the resolver at all.
	r := httptest.NewRequest(http.MethodPost, "/execute", strings.NewReader("{"))
	r.Header.Set(sandboxIDHeader, "sb_known")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

// TestRememberedTokenSurvivesATokenlessCaller pins why the gateway keeps
// credentials at all. The agent-sandbox protocol has nowhere to put one, so a
// claim's token has to be recoverable from the claim itself or that whole
// surface can only ever reach sandboxes created through some other API.
func TestRememberedTokenSurvivesATokenlessCaller(t *testing.T) {
	gw := New(stubResolver{id: "sb_known", addr: "10.0.0.1:7777"}, logr.Discard())

	_, ok := gw.token("sb_known", "")
	assert.False(t, ok, "nothing is known before a claim happens")

	gw.RememberToken(scale.ClaimInfo{ClaimID: "sb_known", Namespace: "default", Name: "demo",
		Token: "tok-from-claim", NodeAddr: "10.0.0.1:7777"})
	got, ok := gw.token("sb_known", "")
	require.True(t, ok)
	assert.Equal(t, "tok-from-claim", got)

	// A caller that does carry a token — every e2b client does, because it was
	// handed one as envdAccessToken — wins over the remembered one, so a
	// restarted gateway still serves them.
	got, ok = gw.token("sb_known", "tok-from-caller")
	require.True(t, ok)
	assert.Equal(t, "tok-from-caller", got)

	// The Kubernetes SDK addresses the data plane by the object name it created,
	// so that spelling has to resolve to the same credential.
	got, ok = gw.token("demo", "")
	require.True(t, ok, "the object name must reach the same sandbox")
	assert.Equal(t, "tok-from-claim", got)

	gw.ForgetToken(scale.ClaimInfo{ClaimID: "sb_known", Namespace: "default", Name: "demo"})
	_, ok = gw.token("sb_known", "")
	assert.False(t, ok, "a released sandbox must not leave its credential behind")
}

// TestBearerAcceptsWhatEachClientActuallySends guards the header list. e2b
// clients present the sandbox token in their own way; agent-sandbox clients
// present nothing. Getting this wrong looks like an auth failure against a
// perfectly healthy sandbox.
func TestBearerAcceptsWhatEachClientActuallySends(t *testing.T) {
	for name, set := range map[string]func(*http.Request){
		"Authorization: Bearer": func(r *http.Request) { r.Header.Set("Authorization", "Bearer abc") },
		"X-Access-Token":        func(r *http.Request) { r.Header.Set("X-Access-Token", "abc") },
		"X-Sandbox-Token":       func(r *http.Request) { r.Header.Set("X-Sandbox-Token", "abc") },
	} {
		t.Run(name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodGet, "/", nil)
			set(r)
			assert.Equal(t, "abc", bearer(r))
		})
	}
	assert.Empty(t, bearer(httptest.NewRequest(http.MethodGet, "/", nil)),
		"no header means no token, which is the agent-sandbox case")
}

// TestResolveMatchesEitherSpellingOfAnID guards the seam between the two
// surfaces. e2b publishes a DNS-safe rendering because a claim id becomes a
// hostname label there; the node only ever knows the raw one. A gateway that
// compared them literally would answer 404 for a sandbox that plainly exists.
func TestResolveMatchesEitherSpellingOfAnID(t *testing.T) {
	assert.True(t, sameID("sb_abc123", "sb_abc123"))
	assert.True(t, sameID("sb_abc123", "sb-abc123"), "the e2b rendering must resolve")
	assert.False(t, sameID("sb_abc123", "sb_abc124"))
	assert.False(t, sameID("", "sb_abc123"))
	assert.False(t, sameID("sb_abc123", ""))
}

// TestExecuteRunsAShellForABareCommand pins the upstream calling convention.
// Its SDK marshals one string — the whole command line, pipes and all — and
// nothing else. Passing that straight to exec makes the guest look for a binary
// whose name contains spaces, which comes back as "No such file or directory"
// and reads like a broken sandbox rather than a protocol mismatch.
func TestExecuteRunsAShellForABareCommand(t *testing.T) {
	assert.Equal(t, []string{"/bin/sh", "-lc", "echo hi && uname -s"},
		argvFor(ExecuteRequest{Command: "echo hi && uname -s"}),
		"a bare command line needs a shell to mean what the caller wrote")

	// An explicit args list is this implementation's own extension: the caller
	// has already split the argv, so putting a shell in the way would change
	// what runs.
	assert.Equal(t, []string{"go", "build", "./..."},
		argvFor(ExecuteRequest{Command: "go", Args: []string{"build", "./..."}}),
		"an explicit argv must reach the guest unchanged")
}
