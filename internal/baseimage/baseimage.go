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

// Package baseimage names the sandbox image this cluster runs.
package baseimage

// Default is the cluster's sandbox base image.
//
// It is the upstream rootfs plus one systemd-networkd drop-in, because guests
// here reach the internet over IPv6 and neither half of that is the stock
// default: the prefix and default route arrive by router advertisement, which
// the stock image ignores, and names must resolve through the node's DNS64
// resolvers so a v4-only host is reached through the IDC's NAT64 at
// 64:ff9b::/96. Public IPv4 egress is deliberately rejected on these nodes, so
// a guest without that drop-in resolves a name fine and then hangs on connect —
// which reads as a broken sandbox, not a missing image setting.
//
// The drop-in also turns DHCP off entirely. The guest bridge's IPv4 subnet is a
// single /24 shared with the desktop VMs on the same node, and a sandbox that
// takes a lease there spends an address a desktop may need — for nothing, since
// the data plane addresses the node's sandboxd rather than the guest and public
// v4 egress is rejected anyway. IPv6 is a /64, so the egress lane grows without
// ever competing for one.
//
// Pinned by digest on purpose. The pool key is the image string itself, so a
// moving tag quietly becomes a second pool: the warm capacity sits under the
// old key while every new claim asks for the new one and gets 503.
const Default = "mindset-ap-southeast-1.cr.volces.com/g0031/sandbox-rt@sha256:389ce7f4a21ece1b798a8820881c4a797c0992bd7f76da70a513cc067cfba063"
