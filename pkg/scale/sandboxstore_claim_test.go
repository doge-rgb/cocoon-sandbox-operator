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

package scale

import (
	"context"
	"fmt"
	sandboxv1beta1 "github.com/doge-rgb/cocoon-sandbox-operator/api/v1beta1"
	extv1beta1 "github.com/doge-rgb/cocoon-sandbox-operator/extensions/api/v1beta1"
	"k8s.io/apimachinery/pkg/watch"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/doge-rgb/cocoon-sandbox-operator/pkg/scale/sandboxd"
)

// poolInv builds a NodeInventory advertising a sandboxd address and pool capacities.
func poolInv(node, addr string, pools ...PoolCapacity) *NodeInventory {
	return &NodeInventory{
		ObjectMeta: metav1.ObjectMeta{Name: node},
		Node:       node,
		Address:    addr,
		Pools:      pools,
	}
}

// recordingFactory captures how the store built and called the per-node sandboxd
// client, so tests can assert claim/release routing (address + uniform token) and
// the derived claim spec without a live node.
type recordingFactory struct {
	builtAddr    string
	builtToken   string
	claimSpec    sandboxd.ClaimSpec
	claimCalls   int
	releaseID    string
	releaseToken string
	releaseCalls int

	claimResult sandboxd.ClaimResult
	claimErr    error
	releaseErr  error
}

func (f *recordingFactory) factory() SandboxdClientFactory {
	return func(addr, token string) SandboxdClient {
		f.builtAddr, f.builtToken = addr, token
		return &recordingClient{f: f}
	}
}

type recordingClient struct{ f *recordingFactory }

var _ SandboxdClient = (*recordingClient)(nil)

func (c *recordingClient) Claim(_ context.Context, spec sandboxd.ClaimSpec) (sandboxd.ClaimResult, error) {
	c.f.claimCalls++
	c.f.claimSpec = spec
	if c.f.claimErr != nil {
		return sandboxd.ClaimResult{}, c.f.claimErr
	}
	return c.f.claimResult, nil
}

func (c *recordingClient) Release(_ context.Context, id, token string) error {
	c.f.releaseCalls++
	c.f.releaseID, c.f.releaseToken = id, token
	return c.f.releaseErr
}

func TestStoreClaim_RoutesToAWarmNode(t *testing.T) {
	src := NewStaticInventorySource()
	src.Put(poolInv("n1", "10.0.0.1:7777", PoolCapacity{Template: "img", Warm: 0, Target: 5}))
	src.Put(poolInv("n2", "10.0.0.2:7777", PoolCapacity{Template: "img", Warm: 4, Target: 5}))
	f := &recordingFactory{claimResult: sandboxd.ClaimResult{ID: "sb-abc", Token: "sbtok", OwnerAddr: "10.0.0.2:9000"}}
	store := NewScatterGatherStore(src, WithLogger(logr.Discard()), WithClaimRouting("uniform-token", f.factory()))

	a, err := store.Claim(context.Background(), "ns", "s1", PoolKey{Template: "img"})
	require.NoError(t, err)
	// n1 advertises no warm capacity, so the only viable node is n2, and the
	// assignment carries the sandboxd id + address.
	assert.Equal(t, "n2", a.Node)
	assert.Equal(t, "sb-abc", a.SandboxName)
	assert.Equal(t, "10.0.0.2:9000", a.Address)
	// The claim routed to the picked node's advertise address with the uniform token.
	assert.Equal(t, "10.0.0.2:7777", f.builtAddr)
	assert.Equal(t, "uniform-token", f.builtToken)
	assert.Equal(t, "img", f.claimSpec.Template)
	// The claim carries the k8s "<namespace>/<name>" so the node echoes it into
	// its operator index and the aggregated read path can resolve this sandbox.
	assert.Equal(t, "ns/s1", f.claimSpec.ClaimRef)
	assert.Equal(t, 1, f.claimCalls)
}

// TestStoreClaim_SpreadsAcrossNodesWithinOneInventoryGeneration pins the
// property that makes concurrent claims usable: the warm counts come from a
// summary republished every ~30s, so a burst inside one generation reads
// identical numbers. Routing by "largest wins" would send all of it to one node
// until that node drained and answered no-capacity, with the rest of the fleet
// untouched — which is exactly what a 100-sandbox burst did before this.
func TestStoreClaim_SpreadsAcrossNodesWithinOneInventoryGeneration(t *testing.T) {
	src := NewStaticInventorySource()
	for _, n := range []string{"n1", "n2", "n3", "n4"} {
		src.Put(poolInv(n, "10.0.0."+n[1:]+":7777", PoolCapacity{Template: "img", Warm: 5, Target: 5}))
	}
	f := &recordingFactory{claimResult: sandboxd.ClaimResult{ID: "sb-abc", Token: "sbtok"}}
	store := NewScatterGatherStore(src, WithLogger(logr.Discard()), WithClaimRouting("tok", f.factory()))

	// The inventory never changes, so every claim below sees Warm: 5 on all four.
	picked := map[string]int{}
	for i := range 20 {
		a, err := store.Claim(context.Background(), "ns", fmt.Sprintf("s%d", i), PoolKey{Template: "img"})
		require.NoError(t, err)
		picked[a.Node]++
	}
	assert.Len(t, picked, 4, "every node with warm capacity should take a share, got %v", picked)
	for node, got := range picked {
		assert.LessOrEqual(t, got, 5, "node %q took %d of 20 claims but advertised only 5 warm", node, got)
	}

	// Exhausting the advertised capacity answers no-capacity rather than
	// overcommitting a node whose count this generation is already spent.
	_, err := store.Claim(context.Background(), "ns", "overflow", PoolKey{Template: "img"})
	assert.ErrorIs(t, err, ErrNoWarmCapacity)
}

func TestStoreClaim_NoWarmCapacityIsRetryable(t *testing.T) {
	src := NewStaticInventorySource()
	// Warm==0 everywhere: no node can hand over a microVM.
	src.Put(poolInv("n1", "10.0.0.1:7777", PoolCapacity{Template: "img", Warm: 0, Target: 5}))
	f := &recordingFactory{}
	store := NewScatterGatherStore(src, WithLogger(logr.Discard()), WithClaimRouting("t", f.factory()))

	_, err := store.Claim(context.Background(), "ns", "s1", PoolKey{Template: "img"})
	require.Error(t, err)
	assert.True(t, IsNoWarmCapacity(err), "want ErrNoWarmCapacity, got %v", err)
	assert.Equal(t, 0, f.claimCalls, "must not call sandboxd when no node is warm")
}

func TestStoreClaim_PoolKeyMatchingNormalizesDefaults(t *testing.T) {
	src := NewStaticInventorySource()
	// A pool advertised with unset net/size serves the default-named ("none"/"small") key.
	src.Put(poolInv("n1", "10.0.0.1:7777", PoolCapacity{Template: "img", Warm: 2, Target: 2}))
	f := &recordingFactory{claimResult: sandboxd.ClaimResult{ID: "sb-1"}}
	store := NewScatterGatherStore(src, WithLogger(logr.Discard()), WithClaimRouting("t", f.factory()))

	_, err := store.Claim(context.Background(), "ns", "s1", PoolKey{Template: "img", Net: "none", Size: "small"})
	require.NoError(t, err)

	// A different net finds no matching pool.
	_, err = store.Claim(context.Background(), "ns", "s2", PoolKey{Template: "img", Net: "egress"})
	require.Error(t, err)
	assert.True(t, IsNoWarmCapacity(err), "net mismatch must be no-capacity, got %v", err)
}

func TestStoreClaim_SandboxdCapacityRaceIsRetryable(t *testing.T) {
	src := NewStaticInventorySource()
	src.Put(poolInv("n1", "10.0.0.1:7777", PoolCapacity{Template: "img", Warm: 1, Target: 5}))
	// The advertised warm count raced to zero: sandboxd answers at-capacity.
	f := &recordingFactory{claimErr: sandboxd.ErrNodeAtCapacity}
	store := NewScatterGatherStore(src, WithLogger(logr.Discard()), WithClaimRouting("t", f.factory()))

	_, err := store.Claim(context.Background(), "ns", "s1", PoolKey{Template: "img"})
	require.Error(t, err)
	assert.True(t, IsNoWarmCapacity(err), "sandboxd 429 must map to no-capacity, got %v", err)
}

func TestStoreRelease_RoutesToNodeAddressWithUniformToken(t *testing.T) {
	src := NewStaticInventorySource()
	src.Put(poolInv("n2", "10.0.0.2:7777", PoolCapacity{Template: "img", Warm: 3, Target: 5}))
	f := &recordingFactory{}
	store := NewScatterGatherStore(src, WithLogger(logr.Discard()), WithClaimRouting("uniform-token", f.factory()))

	err := store.Release(context.Background(), "n2", "sb-abc")
	require.NoError(t, err)
	assert.Equal(t, "10.0.0.2:7777", f.builtAddr, "release must route to the node's advertise address")
	assert.Equal(t, "sb-abc", f.releaseID)
	assert.Equal(t, "uniform-token", f.releaseToken, "release authenticates with the uniform token")
	assert.Equal(t, 1, f.releaseCalls)
}

func TestStoreClaimRelease_FailClosedWithoutRouting(t *testing.T) {
	src := NewStaticInventorySource()
	src.Put(poolInv("n1", "10.0.0.1:7777", PoolCapacity{Template: "img", Warm: 1, Target: 1}))
	store := NewScatterGatherStore(src, WithLogger(logr.Discard())) // no WithClaimRouting

	_, err := store.Claim(context.Background(), "ns", "s1", PoolKey{Template: "img"})
	require.Error(t, err)
	assert.False(t, IsNoWarmCapacity(err), "unconfigured routing is a config error, not no-capacity")

	require.Error(t, store.Release(context.Background(), "n1", "sb-1"))
}

// The lifecycle verbs are not exercised here; they satisfy the SandboxdClient
// port so the recorder stays a drop-in.
func (c *recordingClient) Hibernate(context.Context, string) error { return nil }
func (c *recordingClient) Wake(context.Context, string) error      { return nil }
func (c *recordingClient) Fork(context.Context, string, sandboxd.ForkSpec) (sandboxd.ForkResult, error) {
	return sandboxd.ForkResult{}, nil
}
func (c *recordingClient) Checkpoint(context.Context, string, sandboxd.CheckpointSpec) (sandboxd.Checkpoint, error) {
	return sandboxd.Checkpoint{}, nil
}
func (c *recordingClient) Checkpoints(context.Context) ([]sandboxd.Checkpoint, error) {
	return nil, nil
}
func (c *recordingClient) DeleteCheckpoint(context.Context, string) error { return nil }
func (c *recordingClient) ClaimCheckpoint(context.Context, string, sandboxd.CheckpointClaimSpec) (sandboxd.ClaimResult, error) {
	return sandboxd.ClaimResult{}, nil
}
func (c *recordingClient) Promote(context.Context, string, sandboxd.PromoteSpec) (sandboxd.PoolKey, error) {
	return sandboxd.PoolKey{}, nil
}
func (c *recordingClient) Stats(context.Context, string) (sandboxd.SandboxStats, error) {
	return sandboxd.SandboxStats{}, nil
}

// TestListPublishesResourceVersionAndWatchBookmarks pins what an informer needs
// from a synthesized collection. Without a List resourceVersion an informer has
// nothing to resume from; without bookmarks its cursor never advances, the
// apiserver eventually rejects it as expired, and the informer never reports
// synced — which silently keeps every controller watching Sandbox from starting
// at all. That is exactly how the SandboxClaim controller sat dead while looking
// healthy, so both halves are asserted here.
func TestListPublishesResourceVersionAndWatchBookmarks(t *testing.T) {
	src := NewStaticInventorySource()
	inv := poolInv("n1", "10.0.0.1:7777", PoolCapacity{Template: "img", Warm: 1, Target: 1})
	inv.ResourceVersion = "4242"
	inv.Entries = []InventoryEntry{{Name: "default/sb1", ID: "sb_1", Phase: "Running"}}
	src.Put(inv)
	store := NewScatterGatherStore(src, WithLogger(logr.Discard()), WithWatchPollInterval(10*time.Millisecond))

	list, err := store.List(context.Background(), ListOptions{})
	require.NoError(t, err)
	assert.Equal(t, "4242", list.ResourceVersion,
		"a List with no resourceVersion leaves an informer nothing to resume a watch from")

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	w, err := store.Watch(ctx, ListOptions{AllowWatchBookmarks: true})
	require.NoError(t, err)
	defer w.Stop()

	var sawBookmark bool
	for !sawBookmark {
		select {
		case ev, ok := <-w.ResultChan():
			if !ok {
				t.Fatal("watch closed before emitting a bookmark")
			}
			if ev.Type == watch.Bookmark {
				sawBookmark = true
				assert.Equal(t, "4242", ev.Object.(*sandboxv1beta1.Sandbox).ResourceVersion,
					"a bookmark must quote the inventory generation so the client cursor advances")
			}
		case <-ctx.Done():
			t.Fatal("no bookmark within the deadline; an informer would never finish syncing")
		}
	}
}

// TestSynthesizedSandboxCarriesClaimOwnership pins the ownerReference a
// claim-created Sandbox must appear to have. The upstream SandboxClaim
// controller creates a Sandbox and then re-reads it, rejecting anything
// metav1.IsControlledBy says it does not own — so a synthesized collection that
// drops ownerReferences makes the controller reject the sandbox it just made
// ("sandbox X is not controlled by claim X") and retry until the caller times
// out, which is exactly how the official Go SDK hung for three minutes.
func TestSynthesizedSandboxCarriesClaimOwnership(t *testing.T) {
	const uid = "8f14e45f-ceea-467a-9e5a-1b2c3d4e5f60"
	sb := entryToSandbox("n1", InventoryEntry{
		Name: "default/sandbox-claim-abc", ID: "sb_1", Phase: "Running",
		ClaimRef: formatClaimRef("default", "sandbox-claim-abc", uid),
	})
	claim := &extv1beta1.SandboxClaim{ObjectMeta: metav1.ObjectMeta{
		Name: "sandbox-claim-abc", Namespace: "default", UID: types.UID(uid)}}
	assert.True(t, metav1.IsControlledBy(sb, claim),
		"the claim controller re-reads the sandbox it created and rejects it unless it owns it")
	assert.Equal(t, "sandbox-claim-abc", sb.Labels[ClaimLabel],
		"the claim label must be the claim name, not the uid-carrying ref")

	// No UID recorded means no reference at all. A forged UID would fail the very
	// check it imitates, and would read to the garbage collector as an owner that
	// no longer exists — which deletes the live sandbox.
	noUID := entryToSandbox("n1", InventoryEntry{Name: "default/sb", Phase: "Running", ClaimRef: "default/sb"})
	assert.Empty(t, noUID.OwnerReferences, "an unknown claim UID must not be invented")

	// ownerReferences are namespace-local.
	cross := entryToSandbox("n1", InventoryEntry{
		Name: "default/sb", Phase: "Running", ClaimRef: formatClaimRef("other", "claim", uid)})
	assert.Empty(t, cross.OwnerReferences, "cross-namespace claimRef must not become an ownerReference")

	// A warm sandbox has no claim at all.
	warm := entryToSandbox("n1", InventoryEntry{Name: "default/sb", Phase: "Running"})
	assert.Empty(t, warm.OwnerReferences, "an unclaimed sandbox has no owner")
}

func TestClaimIsVisibleAndIdempotentBeforeInventoryCatchesUp(t *testing.T) {
	ctx := context.Background()
	src := NewStaticInventorySource()
	src.Put(poolInv("n1", "10.0.0.1:7777", PoolCapacity{Template: "img", Warm: 5, Target: 5}))
	f := &recordingFactory{claimResult: sandboxd.ClaimResult{ID: "sb-abc", OwnerAddr: "10.0.0.1:7777"}}
	now := time.Now()
	store := NewScatterGatherStore(src, WithLogger(logr.Discard()),
		WithClaimRouting("tok", f.factory()), WithClock(func() time.Time { return now }))

	pool := PoolKey{Template: "img"}
	_, err := store.Claim(ctx, "default", "sb", pool, WithOwnerUID("11111111-2222-4333-8444-555555555555"))
	require.NoError(t, err)

	// Visible to Get and List even though no inventory mentions it.
	got, err := store.Get(ctx, "default", "sb")
	assert.NoError(t, err, "a just-claimed sandbox must not read as NotFound")
	assert.Equal(t, "n1", got.Status.NodeName)
	assert.True(t, meta.IsStatusConditionTrue(got.Status.Conditions, string(sandboxv1beta1.SandboxConditionReady)),
		"a claimed warm microVM is already running, so it reports Ready")
	assert.NotEmpty(t, got.OwnerReferences, "the claim that made it still owns it before inventory confirms")
	list, err := store.List(ctx, ListOptions{Namespace: "default"})
	assert.NoError(t, err)
	assert.Len(t, list.Items, 1, "List must include claims the inventory has not echoed yet")

	// A retry of the same create must not consume a second warm microVM.
	_, err = store.Claim(ctx, "default", "sb", pool)
	assert.True(t, IsClaimExists(err), "repeat claim must be refused, got %v", err)
	assert.Equal(t, 1, f.claimCalls, "the warm pool must have been asked exactly once")

	// Once the node publishes the entry it is served from real node state, and
	// the pending entry is dropped rather than duplicating the sandbox.
	inv := poolInv("n1", "10.0.0.1:7777", PoolCapacity{Template: "img", Warm: 4, Target: 5})
	inv.Entries = []InventoryEntry{{Name: "default/sb", ID: "sb-abc", Phase: "Running", ClaimRef: "default/sb"}}
	src.Put(inv)
	list, err = store.List(ctx, ListOptions{Namespace: "default"})
	assert.NoError(t, err)
	assert.Len(t, list.Items, 1, "confirmed claim must appear once, not twice")

	// A claim the fleet never confirms stops being advertised after the TTL, so a
	// node that died mid-claim cannot pin a phantom sandbox forever.
	_, err = store.Claim(ctx, "default", "ghost", pool)
	require.NoError(t, err)
	now = now.Add(pendingTTL + time.Second)
	_, err = store.Get(ctx, "default", "ghost")
	assert.True(t, k8serrors.IsNotFound(err), "an unconfirmed claim must expire, got %v", err)
}

// TestEntryNameCarryingClaimUIDResolvesToTheObjectName guards the coupling that
// broke this once: a node names the inventory entry after the claim_ref string
// it was handed, so a uid-carrying claim_ref lands in InventoryEntry.Name too.
// Reading that field as a plain "<ns>/<name>" serves the sandbox under
// "probe-1#<uid>" — a name no client can Get, so every read but List 404s.
func TestEntryNameCarryingClaimUIDResolvesToTheObjectName(t *testing.T) {
	const uid = "8ace193f-899b-45b0-9c66-5e6859dbc447"
	ref := formatClaimRef("default", "probe-1", uid)
	sb := entryToSandbox("n1", InventoryEntry{Name: ref, ClaimRef: ref, ID: "sb_1", Phase: "Running"})
	assert.Equal(t, "probe-1", sb.Name, "the uid suffix must not leak into the object name")
	assert.Equal(t, "default", sb.Namespace)
	assert.Equal(t, uid, string(sb.OwnerReferences[0].UID))
}

// TestSynthesizedSandboxHasAStableUID guards the field whose absence made every
// PATCH answer 404 for an object GET had just returned: Kubernetes reads a
// missing metadata.uid as "never persisted" and refuses to patch it. The UID
// must also be stable, or each publish would look like a different object to
// every watch cache in the cluster.
func TestSynthesizedSandboxHasAStableUID(t *testing.T) {
	e := InventoryEntry{Name: "default/sb", ID: "sb_c63240d9002b1af1", Phase: "Running"}
	first := entryToSandbox("n1", e)
	require.NotEmpty(t, first.UID, "a sandbox with no UID cannot be patched")
	assert.Regexp(t, `^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`, string(first.UID))

	// Same sandbox, later publish: phase and address moved on, identity did not.
	e.Phase, e.Address = "Paused", "10.0.0.9:1234"
	assert.Equal(t, first.UID, entryToSandbox("n1", e).UID, "identity must survive a phase change")

	// A different microVM is a different object.
	assert.NotEqual(t, first.UID, entryToSandbox("n1", InventoryEntry{Name: "default/sb2", ID: "sb_other"}).UID)
}
