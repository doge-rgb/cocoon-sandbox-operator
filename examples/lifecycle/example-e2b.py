#!/usr/bin/env python3
# Copyright 2026 The CocoonStack Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Drive this operator's e2b-compatible surface with the *official* e2b SDK.

`example.go` speaks the e2b REST contract directly, because e2b ships no Go
client — which proves the wire format but not that a real SDK is satisfied by
it. This script closes that gap: it imports `e2b` from PyPI, unmodified, and
runs the whole control-plane lifecycle against a live cluster.

    pip install e2b
    export E2B_API_URL=http://localhost:8080 E2B_API_KEY=e2b_...
    python examples/lifecycle/example-e2b.py --template <image> --count 100

Two deployment facts shape every call below. Neither is a defect in the SDK;
both are things an operator has to know before pointing a real client at this
surface.

  * `allow_internet_access` selects the warm pool, and the SDK defaults it to
    True. A True picks the egress network lane; the pools most clusters run
    (and the ones this repo's benchmarks use) are on the no-egress lane, so the
    SDK's *default* create answers 503 "no warm sandbox available". Pass
    allow_internet_access=False to claim from a no-egress pool, or provision an
    egress-lane SandboxWarmPool.

  * Reads are eventually consistent. Create returns as soon as the node-local
    claim completes, but every other verb resolves the sandbox through a view
    synthesized from NodeInventory. The apiserver keeps a read-your-writes index
    of the claims it made, so a create is visible to the very next call and a
    create immediately followed by a pause works; no settling wait is needed.

Data-plane methods (`commands`, `files`) run against the gateway, which serves
envd's protocol and re-issues each call into the guest.

Where the SDK looks for that gateway depends on the deployment. With a wildcard
domain routed to it the SDK finds it unaided — it dials `{port}-{id}.{domain}`.
Without one, set `E2B_SANDBOX_URL` to the gateway; the Host header then names no
sandbox, and the per-sandbox token the SDK already carries identifies it
instead.
"""

import argparse
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor

from e2b import Sandbox
from e2b.exceptions import SandboxException

# MAX_SANDBOXES caps how many sandboxes this example holds at once. The scale
# pass claims real microVMs on a shared cluster; keeping the ceiling explicit
# means a mistyped --count cannot turn a demo into a load test.
MAX_SANDBOXES = 100

# The publish cadence is ~30 s, so this is that plus room for a slow node.


def step(name, detail=""):
    print(f"  {name:<18} {detail}", flush=True)



def release_all():
    """Kill every sandbox the API still reports, so a failed run leaves none."""
    killed = 0
    page = Sandbox.list()
    while True:
        for info in page.next_items():
            try:
                Sandbox.connect(info.sandbox_id).kill()
                killed += 1
            except SandboxException:
                pass
        if not page.has_next:
            break
    return killed


def lifecycle_pass(template):
    """Exercise every control-plane verb on one sandbox, including fork."""
    print("\n=== lifecycle: every verb on one sandbox ===")
    started = time.monotonic()
    sandbox = Sandbox.create(template, timeout=900, allow_internet_access=False)
    step("create", f"{sandbox.sandbox_id} in {(time.monotonic()-started)*1000:.0f} ms")

    info = sandbox.get_info()
    step("get_info", f"template={info.template_id}")

    # The data plane. Everything else here delivers a sandbox; this is the part
    # that uses one.
    started = time.monotonic()
    result = sandbox.commands.run("echo from-the-sandbox && uname -s")
    step("commands.run", f"{(time.monotonic()-started)*1000:.0f} ms "
                         f"exit={result.exit_code} stdout={result.stdout!r}")
    started = time.monotonic()
    sandbox.files.write("/tmp/example.txt", "written-by-example")
    got = sandbox.files.read("/tmp/example.txt")
    step("files", f"{(time.monotonic()-started)*1000:.0f} ms wrote and read back {got!r}")
    step("list", f"{sum(1 for _ in Sandbox.list().next_items())} sandbox(es)")

    sandbox.set_timeout(900)
    step("set_timeout", "extended")

    metrics = sandbox.get_metrics()
    step("get_metrics", f"cpu_count={metrics[0].cpu_count}" if metrics else "no samples yet")

    snapshot = sandbox.create_snapshot(name="example-e2b")
    step("create_snapshot", snapshot.snapshot_id)
    step("list_snapshots", f"{sum(1 for _ in sandbox.list_snapshots().next_items())} fleet-wide")

    children = sandbox.fork(count=1)
    forked = [c for c in children if not isinstance(c, Exception)]
    step("fork", f"{len(forked)} child: {forked[0].sandbox_id}" if forked else "no child")

    paused = time.monotonic()
    sandbox.pause()
    step("pause", f"{(time.monotonic()-paused)*1000:.0f} ms (writes guest memory out)")

    resumed = time.monotonic()
    Sandbox.connect(sandbox.sandbox_id)
    step("connect", f"{(time.monotonic()-resumed)*1000:.0f} ms (mmap restore fast path)")

    sandbox.kill()
    for child in forked:
        child.kill()
    step("kill", f"released {1 + len(forked)}")


def scale_pass(template, count):
    """Claim `count` sandboxes at once, pause/resume each, then release them."""
    print(f"\n=== scale: {count} concurrent sandboxes ===")
    started = time.monotonic()
    with ThreadPoolExecutor(max_workers=16) as pool:
        sandboxes = list(
            pool.map(
                lambda _: Sandbox.create(
                    template, timeout=900, allow_internet_access=False
                ),
                range(count),
            )
        )
    step("create", f"{len(sandboxes)} in {time.monotonic()-started:.1f} s")

    paused = time.monotonic()
    with ThreadPoolExecutor(max_workers=16) as pool:
        list(pool.map(lambda s: s.pause(), sandboxes))
    step("pause", f"{len(sandboxes)} in {time.monotonic()-paused:.1f} s")

    resumed = time.monotonic()
    with ThreadPoolExecutor(max_workers=16) as pool:
        list(pool.map(lambda s: Sandbox.connect(s.sandbox_id), sandboxes))
    step("connect", f"{len(sandboxes)} in {time.monotonic()-resumed:.1f} s")

    killed = time.monotonic()
    with ThreadPoolExecutor(max_workers=16) as pool:
        list(pool.map(lambda s: s.kill(), sandboxes))
    step("kill", f"{len(sandboxes)} in {time.monotonic()-killed:.1f} s")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--template",
        required=True,
        help="template id (an image reference the fleet's warm pools advertise)",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=10,
        help=f"sandboxes for the scale pass (max {MAX_SANDBOXES})",
    )
    parser.add_argument(
        "--skip-scale", action="store_true", help="run only the single-sandbox pass"
    )
    args = parser.parse_args()

    if args.count > MAX_SANDBOXES:
        parser.error(f"--count {args.count} exceeds the {MAX_SANDBOXES} ceiling")
    if not os.environ.get("E2B_API_URL"):
        parser.error("set E2B_API_URL to this operator's e2b surface")

    dataplane = os.environ.get("E2B_SANDBOX_URL")
    print(f"e2b SDK against {os.environ['E2B_API_URL']}")
    print(f"  data plane: {dataplane or 'via {port}-{id}.{domain} (needs a wildcard domain)'}")
    try:
        leftover = release_all()
        if leftover:
            step("pre-clean", f"released {leftover} leftover sandbox(es)")

        lifecycle_pass(args.template)
        if not args.skip_scale:
            scale_pass(args.template, args.count)
        print("\nAll operations completed.")
    finally:
        remaining = release_all()
        if remaining:
            step("cleanup", f"released {remaining} sandbox(es)")


if __name__ == "__main__":
    sys.exit(main())
