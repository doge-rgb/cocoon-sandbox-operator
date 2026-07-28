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
	"time"

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

func (s stubResolver) EntryAddr(context.Context) (string, error) { return s.addr, nil }

// TestRouterRejectsBeforeTouchingAGuest pins the failure modes a caller can
// tell apart. The upstream SDK reports whatever status it gets, so a sandbox on
// a drained node and a sandbox this gateway has no credential for must not both
// arrive as the same opaque 502 — one is retryable elsewhere, the other needs a
// token the caller has to supply.
func TestRouterRejectsBeforeTouchingAGuest(t *testing.T) {
	gw := New(stubResolver{id: "sb_known", addr: "10.0.0.1:7777"}, "fleet-token", logr.Discard())
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
	gw := New(stubResolver{id: "sb_known", addr: "10.0.0.1:7777"}, "fleet-token", logr.Discard())

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

// meshResolver knows a sandbox exists but deliberately refuses to say where,
// the way inventory does for one claimed seconds ago.
type meshResolver struct{ claimID, entry string }

func (m meshResolver) Resolve(_ context.Context, id string) (string, string, error) {
	if id != m.claimID {
		return "", "", ErrUnknownSandbox
	}
	return m.claimID, "", nil
}
func (m meshResolver) EntryAddr(context.Context) (string, error) { return m.entry, nil }

// TestUnknownLocationFallsThroughToTheMesh pins why this gateway does not need
// to know which node holds a sandbox. Every sandboxd can route a lookup to the
// owner — it answers for its own and scatters to its peers otherwise — so the
// gateway enters the mesh anywhere and lets it resolve. Depending on published
// node state instead would make a sandbox unreachable for most of a minute
// after it is claimed, which is exactly when a caller wants it.
func TestUnknownLocationFallsThroughToTheMesh(t *testing.T) {
	gw := New(meshResolver{claimID: "sb_here", entry: "10.0.0.9:7777"}, "fleet-token", logr.Discard())

	// No remembered address and none from the resolver: the only way to reach
	// the guest is through the mesh, so a client must be built for the entry
	// node rather than the request being refused.
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	_, err := gw.Open(ctx, "sb_here", "caller-token")
	require.Error(t, err, "no node is actually listening in this test")
	assert.NotContains(t, err.Error(), "no credential",
		"a caller that supplied a token must not be told it lacked one")

	gw.mu.Lock()
	_, built := gw.clients["10.0.0.9:7777"]
	gw.mu.Unlock()
	assert.True(t, built, "the gateway must enter the mesh at the address the resolver named")
}

// TestSandboxFromHostReadsTheOnlyIdentifierE2BSends pins the e2b addressing
// scheme. Its client puts the sandbox nowhere in the request but the hostname —
// "{port}-{sandboxID}.{domain}" — so a gateway that looked at paths or headers
// would serve every call against whichever sandbox it guessed, or none.
func TestSandboxFromHostReadsTheOnlyIdentifierE2BSends(t *testing.T) {
	assert.Equal(t, "sb-abc123", sandboxFromHost("49983-sb-abc123.example.com"))
	assert.Equal(t, "sb-abc123", sandboxFromHost("49983-sb-abc123.example.com:8081"),
		"a port on the Host header must not become part of the id")
	assert.Equal(t, "sb-abc123", sandboxFromHost("49983-sb-abc123"),
		"a bare label, as a direct-to-gateway caller would send")
	assert.Empty(t, sandboxFromHost("example.com"), "a host with no port prefix names no sandbox")
	assert.Empty(t, sandboxFromHost(""))
}

// TestDataPlaneRefusesUncredentialedCallersWhenGated pins what makes this
// listener publishable. Its protocols carry no credential of their own — the
// agent-sandbox SDK sends none at all — so an ungated listener is safe only
// while it is unreachable, and nothing tells you when that stops being true.
func TestDataPlaneRefusesUncredentialedCallersWhenGated(t *testing.T) {
	gw := New(stubResolver{id: "sb_known", addr: "10.0.0.1:7777"}, "fleet-token",
		logr.Discard(), WithAPIKeys([]string{"fleet-key"}))
	h := gw.Authenticated(gw.AgentSandboxHandler())

	// A short deadline: the addresses here answer nothing, and what this test
	// asserts is the verdict at the gate, not what happens past it.
	do := func(set func(*http.Request)) int {
		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		defer cancel()
		r := httptest.NewRequest(http.MethodPost, "/execute",
			strings.NewReader(`{"command":"true"}`)).WithContext(ctx)
		r.Header.Set(sandboxIDHeader, "sb_known")
		if set != nil {
			set(r)
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w.Code
	}

	assert.Equal(t, http.StatusUnauthorized, do(nil), "no credential must not reach a guest")
	assert.Equal(t, http.StatusUnauthorized,
		do(func(r *http.Request) { r.Header.Set("X-API-Key", "wrong") }))

	// A fleet key gets past the gate; the sandbox itself is still authorized
	// downstream by its own token, which this gateway has not been told.
	assert.Equal(t, http.StatusUnauthorized,
		do(func(r *http.Request) { r.Header.Set("X-API-Key", "fleet-key") }),
		"past the gate, but still no per-sandbox credential to reach the guest with")

	// A caller holding one sandbox's token needs no fleet key.
	gw.RememberToken(scale.ClaimInfo{ClaimID: "sb_known", Token: "sandbox-token", NodeAddr: "10.0.0.1:7777"})
	assert.NotEqual(t, http.StatusUnauthorized,
		do(func(r *http.Request) { r.Header.Set("Authorization", "Bearer sandbox-token") }),
		"a per-sandbox token authorizes exactly its own sandbox")
}
