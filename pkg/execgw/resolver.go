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
	"fmt"
	"strings"

	"github.com/doge-rgb/cocoon-sandbox-operator/pkg/scale"
)

// InventoryResolver answers "which node holds this sandbox, and how do I reach
// it" from the same node inventories the read view is built from.
//
// It scans rather than indexes. The alternative is a map kept warm by the
// publish stream, which is a cache to invalidate for a lookup that happens once
// per data-plane session — an exec opens a connection and then reuses it — and
// the scan is over nodes, not sandboxes: each inventory is read once and its
// entries walked in memory.
type InventoryResolver struct {
	src scale.InventorySource
}

// NewInventoryResolver builds a resolver over src.
func NewInventoryResolver(src scale.InventorySource) *InventoryResolver {
	return &InventoryResolver{src: src}
}

// Resolve accepts either the raw sandboxd claim id or the DNS-safe rendering
// the e2b surface publishes, and returns the claim id the node knows together
// with that node's sandboxd address.
func (r *InventoryResolver) Resolve(ctx context.Context, id string) (string, string, error) {
	if r.src == nil {
		return "", "", fmt.Errorf("execgw: no inventory source configured")
	}
	nodes, err := r.src.ListNodes(ctx)
	if err != nil {
		return "", "", fmt.Errorf("execgw: enumerate nodes: %w", err)
	}
	for _, node := range nodes {
		inv, err := r.src.NodeInventory(ctx, node)
		if err != nil {
			// A node whose inventory is momentarily unreadable is skipped, not
			// fatal: the sandbox may well be on another one.
			continue
		}
		for i := range inv.Entries {
			if !sameID(inv.Entries[i].ID, id) && !sameName(inv.Entries[i].Name, id) {
				continue
			}
			if inv.Address == "" {
				return "", "", fmt.Errorf("execgw: node %q holds %q but advertises no address: %w",
					node, id, ErrUnknownSandbox)
			}
			return inv.Entries[i].ID, inv.Address, nil
		}
	}
	return "", "", fmt.Errorf("execgw: sandbox %q: %w", id, ErrUnknownSandbox)
}

// sameID compares a node's claim id against what a client asked for. The e2b
// surface publishes ids with the underscore rewritten, because a claim id has
// to survive being used as a DNS label there, so both spellings must match the
// one id the node actually holds.
func sameID(nodeID, want string) bool {
	if nodeID == "" || want == "" {
		return false
	}
	if nodeID == want {
		return true
	}
	return dnsSafe(nodeID) == dnsSafe(want)
}

// sameName matches the Kubernetes identity a node records for an entry —
// "<namespace>/<name>", sometimes carrying a uid suffix — against what a client
// asked for. The upstream Kubernetes SDK addresses the data plane by the object
// name it created, never by the node's claim id, so without this every call
// from that client resolves to nothing.
func sameName(entryName, want string) bool {
	if entryName == "" || want == "" {
		return false
	}
	ns, name, _ := splitEntryName(entryName)
	return want == name || want == ns+"/"+name
}

// splitEntryName parses "<ns>/<name>[#uid]" as a node records it.
func splitEntryName(s string) (ns, name, uid string) {
	if i := strings.Index(s, "#"); i >= 0 {
		s, uid = s[:i], s[i+1:]
	}
	if i := strings.Index(s, "/"); i >= 0 {
		return s[:i], s[i+1:], uid
	}
	return "", s, uid
}

// dnsSafe lowercases and replaces characters that cannot appear in a DNS label.
// It mirrors the rendering the e2b surface publishes; keep the two in step.
func dnsSafe(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	for _, r := range strings.ToLower(s) {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == '-':
			b.WriteRune(r)
		default:
			b.WriteByte('-')
		}
	}
	return b.String()
}
