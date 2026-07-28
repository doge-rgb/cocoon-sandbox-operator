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

// Package execgw serves the two data-plane protocols the ecosystem's clients
// speak — upstream agent-sandbox's REST and e2b's envd — over the one execution
// channel these sandboxes actually have.
//
// A sandbox here runs silkd, reached by upgrading a request to its node's
// sandboxd into a raw connection onto the guest's vsock. Neither foreign client
// knows how to do that: the agent-sandbox SDK expects a router that takes
// /execute with an X-Sandbox-ID header, and the e2b SDK expects envd's
// Connect-RPC at a per-sandbox hostname. Both are thin re-spellings of the same
// operations, so this package terminates them and re-issues each through the
// CocoonStack SDK, which owns the silkd wire protocol.
//
// Authorization stays where it already is: every call reaches the guest with
// that sandbox's own token, so a caller can only drive a sandbox whose token it
// already holds. The gateway mints nothing and, for the e2b surface, keeps
// nothing — the e2b client hands the token back on every request because it was
// given it as envdAccessToken at create time.
package execgw

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	sdk "github.com/cocoonstack/sandbox/sdk/go"
	"github.com/doge-rgb/cocoon-sandbox-operator/pkg/scale"
	"github.com/go-logr/logr"
)

// Resolver maps the id a client used to the claim id the fleet knows it by, and
// names a node to enter the mesh through. It does not have to find the owner:
// the mesh does that, and a lookup that only consulted published inventory
// would answer "no such sandbox" for one claimed seconds ago.
type Resolver interface {
	// Resolve accepts the raw sandboxd claim id, the DNS-safe rendering the e2b
	// surface publishes, or the Kubernetes object name, and returns the claim id.
	Resolve(ctx context.Context, id string) (claimID, nodeAddr string, err error)
	// EntryAddr returns any node's sandboxd address. Which one does not matter —
	// every node can route a lookup to the owner — so a caller that already has
	// a live address should keep using it rather than asking again.
	EntryAddr(ctx context.Context) (string, error)
}

// ErrUnknownSandbox is returned when no node claims the requested id.
var ErrUnknownSandbox = errors.New("execgw: no node holds that sandbox")

// Gateway turns a (sandbox id, token) pair into a live handle on the guest.
type Gateway struct {
	resolver   Resolver
	fleetToken string
	log        logr.Logger

	// clients are per-node SDK clients. The SDK's Client is a thin wrapper over
	// an http.Client, and building one per request would discard every pooled
	// connection to a node this gateway talks to constantly.
	mu      sync.Mutex
	clients map[string]*sdk.Client

	// tokens remembers the per-sandbox credential for callers that have no
	// place to carry one. The agent-sandbox protocol has no token field
	// anywhere — its router is expected to be inside the trust boundary — so
	// without this the only sandboxes reachable over that surface would be ones
	// whose token the caller could somehow already produce. Populated at claim
	// time by the aggregated apiserver, which is the only component that ever
	// sees a token. Process-local on purpose: a credential written to etcd is a
	// credential leaked to everything that can read it.
	tokens sync.Map // claimID -> claimed

	// byToken identifies a sandbox from the credential alone. An e2b client
	// pointed at a fixed gateway URL — the deployment that needs no wildcard DNS
	// — sends a Host header naming no sandbox, and then the per-sandbox token is
	// the only thing in the request that says which one it means. Tokens are
	// unguessable and issued one per sandbox, so this identifies as precisely as
	// it authorizes.
	byToken sync.Map // token -> claimID
}

// claimed is what the claim path told us about a sandbox.
type claimed struct {
	claimID  string
	token    string
	nodeAddr string
}

// New builds a Gateway over resolver. fleetToken is the uniform node credential
// the mesh's peer discovery is read under; without it a lookup cannot scatter
// and only sandboxes on the entry node are reachable.
func New(resolver Resolver, fleetToken string, log logr.Logger) *Gateway {
	return &Gateway{
		resolver:   resolver,
		fleetToken: fleetToken,
		log:        log,
		clients:    map[string]*sdk.Client{},
	}
}

// RememberToken records the credential handed out when a sandbox was claimed,
// so the token-less agent-sandbox surface can reach it later. Callers that do
// carry a token (the e2b surface) never consult this.
func (g *Gateway) RememberToken(info scale.ClaimInfo) {
	if info.Token == "" {
		return
	}
	c := claimed{claimID: info.ClaimID, token: info.Token, nodeAddr: info.NodeAddr}
	// A caller names the sandbox with whichever identity its own API gave it:
	// the Kubernetes clients only ever知道 the object name they created, e2b
	// clients only the node's claim id. Index every spelling so neither has to
	// learn the other's.
	for _, k := range identities(info) {
		g.tokens.Store(k, c)
	}
	g.byToken.Store(info.Token, info.ClaimID)
}

// identities lists the keys a sandbox can be addressed by.
func identities(info scale.ClaimInfo) []string {
	var keys []string
	if info.ClaimID != "" {
		keys = append(keys, info.ClaimID, dnsSafe(info.ClaimID))
	}
	if info.Name != "" {
		keys = append(keys, info.Name)
		if info.Namespace != "" {
			keys = append(keys, info.Namespace+"/"+info.Name)
		}
	}
	return keys
}

// ForgetToken drops a released sandbox's credential under every identity.
func (g *Gateway) ForgetToken(info scale.ClaimInfo) {
	for _, k := range identities(info) {
		g.tokens.Delete(k)
	}
	if info.Token != "" {
		g.byToken.Delete(info.Token)
	}
}

// remembered returns what the claim path recorded for a sandbox, if anything.
func (g *Gateway) remembered(claimID string) (claimed, bool) {
	v, ok := g.tokens.Load(claimID)
	if !ok {
		return claimed{}, false
	}
	return v.(claimed), true
}

// token returns the credential to use: the one the caller supplied if it did,
// otherwise the one remembered from the claim.
func (g *Gateway) token(claimID, supplied string) (string, bool) {
	if supplied != "" {
		return supplied, true
	}
	c, ok := g.remembered(claimID)
	if !ok {
		return "", false
	}
	return c.token, true
}

// clientFor returns the pooled SDK client for a node.
func (g *Gateway) clientFor(addr string) (*sdk.Client, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if c, ok := g.clients[addr]; ok {
		return c, nil
	}
	c, err := sdk.Connect(addr, sdk.WithAPIToken(g.fleetToken))
	if err != nil {
		return nil, fmt.Errorf("execgw: connect to node %q: %w", addr, err)
	}
	g.clients[addr] = c
	return c, nil
}

// Open binds a handle to the sandbox a request names. suppliedToken may be
// empty, in which case the credential recorded at claim time is used.
func (g *Gateway) Open(ctx context.Context, id, suppliedToken string) (*sdk.Sandbox, error) {
	if id == "" {
		return nil, fmt.Errorf("execgw: no sandbox id in request")
	}
	claimID, addr := id, ""
	if c, ok := g.remembered(id); ok {
		// The claim itself said where the sandbox landed, which costs nothing to
		// reuse and is correct the instant it is made.
		claimID, addr = c.claimID, c.nodeAddr
	} else if cid, _, err := g.resolver.Resolve(ctx, id); err == nil {
		claimID = cid
	} else if suppliedToken == "" {
		// Nothing remembered, nothing in the fleet, and no credential offered.
		// Which of those it is decides what the caller can do about it, so
		// report the absence rather than the missing credential: a caller told
		// to supply a token it has no way to obtain learns nothing.
		return nil, err
	}

	tok, ok := g.token(claimID, suppliedToken)
	if !ok {
		return nil, fmt.Errorf("execgw: no credential for sandbox %q; pass it as a bearer token", id)
	}

	if addr != "" {
		c, err := g.clientFor(addr)
		if err != nil {
			return nil, err
		}
		return c.Attach(addr, claimID, tok), nil
	}

	// Nothing local to go on, so ask the mesh. Any node answers: it checks
	// whether it owns the sandbox and otherwise scatters across its peers,
	// first confirmation winning. That is what makes a sandbox reachable
	// through this gateway no matter which node it landed on, and without
	// waiting for node state to be republished.
	entry, err := g.resolver.EntryAddr(ctx)
	if err != nil {
		return nil, err
	}
	c, err := g.clientFor(entry)
	if err != nil {
		return nil, err
	}
	sb, err := c.Lookup(ctx, claimID, tok)
	if err != nil {
		return nil, fmt.Errorf("execgw: sandbox %q: %w", id, ErrUnknownSandbox)
	}
	return sb, nil
}

// sandboxForToken names the sandbox a credential belongs to, for callers whose
// request carries no identifier of its own.
func (g *Gateway) sandboxForToken(token string) (string, bool) {
	if token == "" {
		return "", false
	}
	v, ok := g.byToken.Load(token)
	if !ok {
		return "", false
	}
	return v.(string), true
}

// bearer pulls a token out of the usual places a client might put one. The e2b
// SDK sends the value it was given as envdAccessToken; agent-sandbox sends
// nothing, which is what the remembered token covers.
func bearer(r *http.Request) string {
	if v := r.Header.Get("Authorization"); v != "" {
		if after, ok := strings.CutPrefix(v, "Bearer "); ok {
			return strings.TrimSpace(after)
		}
	}
	for _, h := range []string{"X-Access-Token", "X-Sandbox-Token", "X-Envd-Access-Token"} {
		if v := r.Header.Get(h); v != "" {
			return strings.TrimSpace(v)
		}
	}
	return ""
}

// execTimeout bounds a single data-plane call. A command may legitimately run
// for a long time, so this is generous; it exists to stop a wedged guest from
// pinning a gateway goroutine forever, which is the failure that turns one bad
// sandbox into a bad gateway.
const execTimeout = 30 * time.Minute

// withTimeout applies execTimeout unless the caller already set a deadline.
func withTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, execTimeout)
}
