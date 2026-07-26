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
    synthesized from NodeInventory, which nodes republish on a ~30 s cadence.
    The SDK does not retry on its own, so a create immediately followed by a
    pause raises SandboxNotFoundException. `wait_visible` below polls with
    ordinary SDK calls; see the README's L3 follow-up for the fix that removes
    the window.

Data-plane methods (`is_running`, `commands`, `files`, `pty`) are deliberately
not exercised: they reach envd inside the guest at `{port}-{id}.{domain}`, which
needs a wildcard domain routed to the sandboxes. Configure the server's
`--e2b-domain` and that DNS before expecting them to work.
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

# VISIBILITY_TIMEOUT bounds the wait for the read view to publish a sandbox.
# The publish cadence is ~30 s, so this is that plus room for a slow node.
VISIBILITY_TIMEOUT = 90.0


def step(name, detail=""):
    print(f"  {name:<18} {detail}", flush=True)


def wait_visible(sandbox, timeout=VISIBILITY_TIMEOUT):
    """Poll until the read view publishes the sandbox, using only SDK calls.

    Returns the seconds waited. Raises if the sandbox never appears, which
    means something worse than the publish lag went wrong.
    """
    deadline = time.monotonic() + timeout
    started = time.monotonic()
    while True:
        try:
            sandbox.get_info()
            return time.monotonic() - started
        except SandboxException:
            if time.monotonic() > deadline:
                raise TimeoutError(
                    f"sandbox {sandbox.sandbox_id} not visible within {timeout}s"
                )
            time.sleep(0.5)


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

    step("wait visible", f"{wait_visible(sandbox):.1f} s (NodeInventory publish lag)")

    info = sandbox.get_info()
    step("get_info", f"template={info.template_id}")
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

    waited = time.monotonic()
    with ThreadPoolExecutor(max_workers=16) as pool:
        list(pool.map(wait_visible, sandboxes))
    step("wait visible", f"all {len(sandboxes)} in {time.monotonic()-waited:.1f} s")

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

    print(f"e2b SDK against {os.environ['E2B_API_URL']}")
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
