package scale

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"

	sandboxv1beta1 "github.com/doge-rgb/cocoon-sandbox-operator/api/v1beta1"
)

// benchInventory builds a fleet of `nodes` inventories holding `total` claimed
// sandboxes between them — the shape the read path faces at scale.
func benchInventory(nodes, total int, withClaimUID bool) *StaticInventorySource {
	src := NewStaticInventorySource()
	per := total / nodes
	for n := range nodes {
		node := fmt.Sprintf("node-%02d", n)
		inv := &NodeInventory{
			ObjectMeta: metav1.ObjectMeta{Name: node, ResourceVersion: "100"},
			Node:       node, Address: fmt.Sprintf("10.0.%d.1:7777", n),
		}
		for i := range per {
			name := fmt.Sprintf("default/sb-%02d-%06d", n, i)
			ref := name
			if withClaimUID {
				ref = formatClaimRef("default", fmt.Sprintf("sb-%02d-%06d", n, i), "11111111-2222-4333-8444-555555555555")
			}
			inv.Entries = append(inv.Entries, InventoryEntry{
				Name: ref, ID: fmt.Sprintf("sb_%02d%06d", n, i), Phase: "Running",
				ClaimRef: ref, Address: fmt.Sprintf("172.16.%d.%d:8080", n, i%250),
			})
		}
		src.Put(inv)
	}
	return src
}

// BenchmarkListAtScale measures one full read of the synthesized collection —
// the exact work a watch tick repeats every poll interval, and what an
// informer's initial LIST costs.
func BenchmarkListAtScale(b *testing.B) {
	for _, total := range []int{1_000, 10_000, 100_000} {
		b.Run(fmt.Sprintf("total=%d", total), func(b *testing.B) {
			store := NewScatterGatherStore(benchInventory(20, total, true), WithLogger(logr.Discard()))
			ctx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				l, err := store.List(ctx, ListOptions{Namespace: "default"})
				if err != nil || len(l.Items) != total {
					b.Fatalf("list: %v items=%d", err, len(l.Items))
				}
			}
		})
	}
}

// BenchmarkGetAtScale measures a single-object read, the hot path for
// pause/resume/delete: it scans inventories rather than indexing.
func BenchmarkGetAtScale(b *testing.B) {
	for _, total := range []int{1_000, 100_000} {
		b.Run(fmt.Sprintf("total=%d", total), func(b *testing.B) {
			store := NewScatterGatherStore(benchInventory(20, total, true), WithLogger(logr.Discard()))
			ctx := context.Background()
			name := fmt.Sprintf("sb-19-%06d", total/20-1) // worst case: last node, last entry
			b.ResetTimer()
			for b.Loop() {
				if _, err := store.Get(ctx, "default", name); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// countingSource counts how often a watch actually re-reads inventories, so a
// test can prove the steady-state tick does not re-synthesize the collection.
type countingSource struct {
	InventorySource
	reads atomic.Int64
}

func (c *countingSource) NodeInventory(ctx context.Context, node string) (*NodeInventory, error) {
	c.reads.Add(1)
	return c.InventorySource.NodeInventory(ctx, node)
}

// TestWatchSkipsMaterializationWhileNothingChanges pins the cost model of an
// idle watch. Re-synthesizing the collection is O(sandboxes) in CPU and
// allocation — at fleet scale hundreds of megabytes of garbage per tick, per
// watcher — so a poll that finds nothing changed must not pay it. The gate is
// the inventories' newest resourceVersion, which moves on any node's publish.
func TestWatchSkipsMaterializationWhileNothingChanges(t *testing.T) {
	src := &countingSource{InventorySource: benchInventory(4, 400, false)}
	store := NewScatterGatherStore(src, WithLogger(logr.Discard()), WithWatchPollInterval(10*time.Millisecond))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	w, err := store.Watch(ctx, ListOptions{Namespace: "default"})
	require.NoError(t, err)
	defer w.Stop()

	// Drain the initial Added burst.
	for range 400 {
		select {
		case <-w.ResultChan():
		case <-time.After(5 * time.Second):
			t.Fatal("initial burst did not arrive")
		}
	}

	// Let a dozen idle ticks pass. Each one may read inventories to check the
	// fingerprint (O(nodes)) but must not rebuild the collection (O(sandboxes)).
	src.reads.Store(0)
	time.Sleep(150 * time.Millisecond)
	reads := src.reads.Load()
	assert.Less(t, reads, int64(400),
		"an idle watch re-read inventories %d times in ~15 ticks; it is re-synthesizing instead of comparing a fingerprint", reads)

	// A claim the store makes itself moves no inventory, so the fingerprint has
	// to cover the pending index too — otherwise a just-created sandbox stays
	// invisible to every watcher until the owning node's next publish.
	store.rememberClaim("default", "fresh", "node-00", InventoryEntry{
		Name: "default/fresh", ID: "sb_fresh", Phase: "Running", ClaimRef: "default/fresh"})
	select {
	case ev := <-w.ResultChan():
		assert.Equal(t, watch.Added, ev.Type)
		assert.Equal(t, "fresh", ev.Object.(*sandboxv1beta1.Sandbox).Name)
	case <-time.After(5 * time.Second):
		t.Fatal("a claim the store just made never reached the watch stream")
	}
}

// BenchmarkWatchIdleTick measures what one poll costs when nothing changed —
// the steady state a fleet spends almost all of its time in.
func BenchmarkWatchIdleTick(b *testing.B) {
	for _, total := range []int{1_000, 100_000} {
		b.Run(fmt.Sprintf("total=%d", total), func(b *testing.B) {
			store := NewScatterGatherStore(benchInventory(20, total, true), WithLogger(logr.Discard()))
			ctx := context.Background()
			// Prime the fingerprint the way the loop does.
			gen, trusted := store.collectionGeneration(ctx)
			if !trusted {
				b.Fatal("benchmark fixture must publish resourceVersions")
			}
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				cur, trusted := store.collectionGeneration(ctx)
				if !trusted {
					b.Fatal("fingerprint became untrusted on an idle store")
				}
				if cur != gen {
					b.Fatal("fingerprint moved on an idle store")
				}
			}
		})
	}
}

// TestWatchPollsWhenGenerationIsUnknowable guards the failure direction of the
// fingerprint gate. An inventory published without a usable resourceVersion
// makes the fingerprint constant; treating that as "nothing changed" would stop
// event delivery permanently and silently. Paying for a poll is the safe error.
func TestWatchPollsWhenGenerationIsUnknowable(t *testing.T) {
	src := NewStaticInventorySource()
	inv := poolInv("n1", "10.0.0.1:7777") // poolInv sets no resourceVersion
	src.Put(inv)
	store := NewScatterGatherStore(src, WithLogger(logr.Discard()), WithWatchPollInterval(10*time.Millisecond))
	if _, known := store.collectionGeneration(context.Background()); known {
		t.Fatal("an inventory with no resourceVersion must not produce a trusted fingerprint")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w, err := store.Watch(ctx, ListOptions{Namespace: "default"})
	require.NoError(t, err)
	defer w.Stop()

	// No resourceVersion anywhere, so the only way this event arrives is if the
	// watch fell back to polling.
	updated := poolInv("n1", "10.0.0.1:7777")
	updated.Entries = []InventoryEntry{{Name: "default/sb", ID: "sb_1", Phase: "Running"}}
	src.Put(updated)
	select {
	case ev := <-w.ResultChan():
		assert.Equal(t, watch.Added, ev.Type)
	case <-time.After(5 * time.Second):
		t.Fatal("watch went silent when the fingerprint was unknowable")
	}
}
