"""Model of Caerus's sequence merger under more than K region failures.

Pre-registered: docs/superpowers/notes/2026-10-11-caerus-contagion-preregistration.md

Caerus (Hildred, Abebe, Daudjee, PVLDB 17(3) 2023) orders transactions by
having each region sequence the transactions that touch its primary items
(a *partial sequence*) and having every region merge all partial sequences
through a shared, deterministically built conflict graph (sections 4.2-4.5).
A transaction is released into the merged order once it is *complete*
(seen in the partial sequence of every region it touches, Property 4.5) and
its SCC has no outgoing edges to unreleased transactions (Algorithm 2).

This module models one observing merger at a live region. It reproduces
Algorithm 1's write-follows-write edge rule (the workload is write-only, so
the read rules never fire) and Algorithm 2's release rule, and kills a set
of regions at ``t_fail`` with no recovery. Everything else in Caerus -- the
scheduler, storage, batching, throttling, recovery -- is out of scope; see
the pre-registration for why each omission cannot manufacture the effect
under test.
"""

from __future__ import annotations

import heapq
import random
from dataclasses import dataclass, field
from typing import Any

# Paper section 5.1 regions; one-way latency = half of Table 1 RTT bounds.
REGIONS = ("us-east", "us-east-2", "france-c", "eu-west", "se-asia", "east-asia")
_CONTINENT = ("NA", "NA", "EU", "EU", "AP", "AP")
_ONE_WAY_MS = {
    ("NA", "NA"): 3.0, ("EU", "EU"): 6.0, ("AP", "AP"): 17.0,
    ("NA", "EU"): 41.0, ("EU", "AP"): 80.0, ("NA", "AP"): 100.0,
}


def latency_s(a: int, b: int) -> float:
    if a == b:
        return 0.0
    ca, cb = _CONTINENT[a], _CONTINENT[b]
    key = (ca, cb) if (ca, cb) in _ONE_WAY_MS else (cb, ca)
    return _ONE_WAY_MS[key] / 1000.0


@dataclass
class Txn:
    tid: int
    origin: int
    submit_t: float
    items: tuple[tuple[int, int], ...]  # (primary region, item index), all writes
    regions: frozenset[int] = field(init=False)
    seen: int = 0
    released_t: float | None = None
    out: set[int] = field(default_factory=set)  # tids this txn must follow

    def __post_init__(self) -> None:
        self.regions = frozenset(r for r, _ in self.items)

    @property
    def complete(self) -> bool:
        return self.seen == len(self.regions)


@dataclass
class Config:
    p_mr: float = 0.1
    policy: str = "submit"  # "submit" or "reject"
    rate: float = 1000.0  # transactions per second, all origins
    items_per_region: int = 20
    t_fail: float = 2.0
    horizon: float = 10.0
    drain: float = 1.0
    dead: tuple[int, ...] = (2, 5)  # france-c, east-asia
    observer: int = 0
    tick: float = 0.01
    seed: int = 0


class Merger:
    """One region's sequence merger: Algorithm 1 inserts, Algorithm 2 releases."""

    def __init__(self) -> None:
        self.txns: dict[int, Txn] = {}
        self.unreleased: set[int] = set()
        self.last_writer: dict[tuple[int, int], int] = {}

    def insert(self, region: int, t: Txn) -> None:
        if t.tid not in self.txns:
            self.txns[t.tid] = t
            self.unreleased.add(t.tid)
        for item in t.items:
            if item[0] != region:
                continue  # only this partial sequence's primary items
            prev = self.last_writer.get(item)
            if prev is not None and prev != t.tid and prev in self.unreleased:
                t.out.add(prev)
            self.last_writer[item] = t.tid
        t.seen += 1

    def release(self, now: float) -> int:
        """Release every complete SCC whose out-edges all lead to released txns.

        Iterative Tarjan over the unreleased subgraph. Tarjan emits an SCC only
        after every SCC reachable from it, so one pass in emission order
        decides releasability with all successors already decided.
        """
        txns, unrel = self.txns, self.unreleased
        index: dict[int, int] = {}
        low: dict[int, int] = {}
        on_stack: set[int] = set()
        stack: list[int] = []
        releasable_scc: dict[int, bool] = {}  # vertex -> its SCC releasable
        counter = 0
        released = 0
        for root in list(unrel):
            if root in index:
                continue
            work = [(root, iter([o for o in txns[root].out if o in unrel]))]
            index[root] = low[root] = counter
            counter += 1
            stack.append(root)
            on_stack.add(root)
            while work:
                v, it = work[-1]
                advanced = False
                for w in it:
                    if w not in index:
                        index[w] = low[w] = counter
                        counter += 1
                        stack.append(w)
                        on_stack.add(w)
                        work.append((w, iter([o for o in txns[w].out if o in unrel])))
                        advanced = True
                        break
                    if w in on_stack:
                        low[v] = min(low[v], index[w])
                if advanced:
                    continue
                work.pop()
                if work:
                    parent = work[-1][0]
                    low[parent] = min(low[parent], low[v])
                if low[v] == index[v]:
                    scc = []
                    while True:
                        w = stack.pop()
                        on_stack.discard(w)
                        scc.append(w)
                        if w == v:
                            break
                    members = set(scc)
                    ok = all(txns[m].complete for m in scc) and all(
                        o in members or releasable_scc.get(o, False)
                        for m in scc
                        for o in txns[m].out
                        if o in unrel
                    )
                    for m in scc:
                        releasable_scc[m] = ok
        for tid, ok in releasable_scc.items():
            if ok:
                txns[tid].released_t = now
                unrel.discard(tid)
                released += 1
        return released


def run(cfg: Config) -> list[Txn]:
    rng = random.Random(cfg.seed)
    n = len(REGIONS)
    dead = set(cfg.dead)
    merger = Merger()
    events: list[tuple[float, int, str, Any]] = []
    seq = 0

    def push(t: float, kind: str, payload: Any) -> None:
        nonlocal seq
        heapq.heappush(events, (t, seq, kind, payload))
        seq += 1

    # Poisson arrivals, origins uniform over regions alive at submit time.
    all_txns: list[Txn] = []
    t = 0.0
    tid = 0
    while True:
        t += rng.expovariate(cfg.rate)
        if t >= cfg.horizon:
            break
        alive = [r for r in range(n) if not (t >= cfg.t_fail and r in dead)]
        origin = rng.choice(alive)
        items = [(origin, rng.randrange(cfg.items_per_region))]
        if rng.random() < cfg.p_mr:
            other = rng.choice([r for r in range(n) if r != origin])
            items.append((other, rng.randrange(cfg.items_per_region)))
        txn = Txn(tid, origin, t, tuple(items))
        tid += 1
        if cfg.policy == "reject" and t >= cfg.t_fail and txn.regions & dead:
            continue
        all_txns.append(txn)
        for r in txn.regions:
            push(t + latency_s(origin, r), "sequence", (r, txn))

    tick = cfg.tick
    while tick < cfg.horizon + cfg.drain:
        push(tick, "release", None)
        tick += cfg.tick

    while events:
        now, _, kind, payload = heapq.heappop(events)
        if kind == "sequence":
            r, txn = payload
            if now >= cfg.t_fail and r in dead:
                continue  # dead sequencer: never appended, never forwarded
            push(now + latency_s(r, cfg.observer), "merge", (r, txn))
        elif kind == "merge":
            r, txn = payload
            merger.insert(r, txn)
        else:
            merger.release(now)
    return all_txns


def unaffected_goodput(txns: list[Txn], cfg: Config, window: float = 1.0) -> list[tuple[float, float]]:
    """Per submit-time window: fraction of live-only transactions ever released.

    Live-only means every touched region is outside ``cfg.dead``. Windows
    before ``t_fail`` are included as the pre-failure baseline.
    """
    dead = set(cfg.dead)
    out = []
    start = 0.0
    while start < cfg.horizon:
        bucket = [
            x for x in txns
            if start <= x.submit_t < start + window and not (x.regions & dead)
        ]
        if bucket:
            ok = sum(1 for x in bucket if x.released_t is not None)
            out.append((start, ok / len(bucket)))
        start += window
    return out
