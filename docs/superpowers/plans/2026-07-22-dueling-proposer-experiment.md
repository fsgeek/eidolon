# Dueling-Proposer Hazard Experiment Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Measure the offset-dependent cost that a topology-trapped (R1=1, R2=0) LEO proposer imposes on an Earth proposer contending for the same slot, under the design contract of the 2026-07-22 pre-mortem.

**Architecture:** A new root module `duel.py` builds a minimal duel system (10 acceptors on the standard 5/1/1/3 topology, exactly two `PriorityProposer`s sharing ONE `CrumblingWallQuorum` instance) and runs single-slot trials at a swept start offset. `experiments/duel_sweep.py` drives a two-stage offset grid across condition combinations (ballot polarity × phase2 threshold k × Earth retry budget), producing a deterministic offset→outcome map (primary) and a separate jitter-robustness sweep with Wilson intervals (secondary). Two small, behavior-preserving upgrades to existing modules: opt-in per-link RNG isolation in `datacenter.py`, and additive instrumentation on `paxos.Proposer`/`ConsensusResult`.

**Tech Stack:** Python ≥ 3.14, SimPy, uv, pytest. No new dependencies.

## Global Constraints

- Governing documents: `docs/superpowers/notes/2026-07-22-dueling-proposer-premortem.md` (§A design contract, §B framing contract) and `docs/superpowers/specs/2026-07-19-four-layer-capability-model-design.md` (experiment section ~line 164; success criteria 5, 9, 10).
- Use `uv run pytest` / `uv run python`; never pip.
- **Legacy RNG behavior must not change.** Existing sweeps' reproducibility depends on module-global `random` draws in exact call order. Per-link RNG is strictly opt-in (`enable_per_link_rng()`); with it off, the exact same functions are called on the module-global `random` in the same order as today.
- All additions to `ConsensusResult` take defaults so existing constructors and CSV writers are unaffected.
- Full test suite (`uv run pytest`) must be green before every commit.
- Prose bans carried from the spec (repeat in every results artifact): no FLP claims, no Multi-Paxos authority claims, no backoff-policy study. Results language: the lemma establishes reachability of the hazard; the experiment measures its cost.
- The full sweep runs ONLY after the pre-registration document (Task 6) is committed.
- Commits trigger GPG signing (pinentry); the executor should not be surprised by the prompt.

---

### Task 1: Opt-in per-link RNG isolation in DatacenterNetwork

Contract items: A3 (RNG isolation), A5 (enables the paired baseline), A10.

**Files:**
- Modify: `datacenter.py` (`DatacenterNetwork.__init__` ~line 39; `_deliver` lines ~172, ~186, ~191)
- Test: `tests/test_duel.py` (new file)

**Interfaces:**
- Produces: `DatacenterNetwork.enable_per_link_rng() -> None`; `DatacenterNetwork._rng_for(src_id: int | None, dst_id: int) -> random.Random | module` — legacy default returns the `random` module itself (so `.random()` / `.uniform()` calls are byte-identical to today); after `enable_per_link_rng()`, returns a dedicated `random.Random(f"{seed}|{src_id}->{dst_id}")` per ordered pair, created lazily.

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_duel.py
"""Tests for the dueling-proposer experiment harness (duel.py) and its
supporting upgrades (per-link RNG, proposer instrumentation).

Design contract: docs/superpowers/notes/2026-07-22-dueling-proposer-premortem.md
"""
import random

import simpy

from datacenter import DatacenterNetwork, five_dc_topology
from network import NetworkConfig


def _mk_net(seed=7):
    env = simpy.Environment()
    net = DatacenterNetwork(env, NetworkConfig(seed=seed))
    return net


def test_legacy_rng_is_module_global():
    net = _mk_net()
    assert net._rng_for(1, 2) is random


def test_per_link_rng_isolated_from_other_traffic():
    # Same seed, two networks. Consume DIFFERENT amounts of module-global
    # randomness in each; the per-link stream for (1, 2) must be identical.
    net_a = _mk_net(seed=7)
    net_a.enable_per_link_rng()
    net_b = _mk_net(seed=7)
    net_b.enable_per_link_rng()

    random.random()  # perturb global stream (only before net_b's draws)
    _ = [net_b._rng_for(3, 4).random() for _ in range(5)]  # other-link traffic

    draws_a = [net_a._rng_for(1, 2).random() for _ in range(8)]
    draws_b = [net_b._rng_for(1, 2).random() for _ in range(8)]
    assert draws_a == draws_b


def test_per_link_rng_directional_and_distinct():
    net = _mk_net(seed=7)
    net.enable_per_link_rng()
    a = [net._rng_for(1, 2).random() for _ in range(4)]
    net2 = _mk_net(seed=7)
    net2.enable_per_link_rng()
    b = [net2._rng_for(2, 1).random() for _ in range(4)]
    assert a != b  # ordered pairs get distinct streams
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_duel.py -v`
Expected: FAIL with `AttributeError: 'DatacenterNetwork' object has no attribute '_rng_for'`

- [ ] **Step 3: Implement the RNG hook**

In `datacenter.py`, add to `DatacenterNetwork.__init__` (after the partition-support lines):

```python
        # Opt-in per-link RNG isolation (pre-mortem contract A3).
        # None = legacy behavior: all draws come from the module-global
        # `random` in packet-send order. Enabled: each ordered (src, dst)
        # entity pair gets its own stream seeded from (config seed, pair),
        # so one seed defines a reproducible noise field independent of
        # event interleaving.
        self._per_link_rng: dict[tuple[int | None, int], random.Random] | None = None
```

Add these methods to `DatacenterNetwork`:

```python
    def enable_per_link_rng(self):
        """Give each ordered (src, dst) entity pair an independent RNG stream."""
        self._per_link_rng = {}

    def _rng_for(self, src_id, dst_id):
        if self._per_link_rng is None:
            return random  # legacy: module-global, byte-identical call order
        key = (src_id, dst_id)
        rng = self._per_link_rng.get(key)
        if rng is None:
            rng = random.Random(f"{self.config.seed}|{src_id}->{dst_id}")
            self._per_link_rng[key] = rng
        return rng
```

In `_deliver`, replace the three global draws:

```python
        # Check link-level loss
        if link and link.loss > 0 and self._rng_for(source_id, destination_id).random() < link.loss:
```

```python
            if link.jitter > 0:
                delay += self._rng_for(source_id, destination_id).uniform(-link.jitter, link.jitter)
```

```python
            if self.config.delay_jitter > 0:
                delay += self._rng_for(source_id, destination_id).uniform(
                    -self.config.delay_jitter, self.config.delay_jitter)
```

- [ ] **Step 4: Run new tests and the FULL suite (regression gate)**

Run: `uv run pytest tests/test_duel.py -v && uv run pytest`
Expected: new tests PASS; full suite green (legacy path calls the same module-global functions in the same order, so no existing result can shift).

- [ ] **Step 5: Commit**

```bash
git add datacenter.py tests/test_duel.py
git commit -m "feat: opt-in per-link RNG isolation in DatacenterNetwork (premortem A3)"
```

---

### Task 2: Proposer instrumentation — per-phase NACKs, round log, late responses

Contract items: A8 (NACK attribution, outcome taxonomy inputs), A9 (overlap diagnostic inputs). Fixes the harness critic's H7 (late NACKs silently dropped by `_collect_responses` cleanup at `paxos.py:431-434`).

**Files:**
- Modify: `paxos.py` (`ConsensusResult` ~line 278; `Proposer.__init__` stats ~line 334; `_receiver` ~line 346; `_propose` ~line 445)
- Test: `tests/test_duel.py`

**Interfaces:**
- Produces (all additive, defaulted): `ConsensusResult.phase1_nacks: int`, `ConsensusResult.phase2_nacks: int`, `ConsensusResult.phase1_quorums: int` (rounds whose Phase 1 achieved quorum), `ConsensusResult.phase2_failures: int` (rounds whose Phase 1 succeeded but Phase 2 did not), `ConsensusResult.round_log: list[dict]` with per-round entries `{"round", "proposal_number", "p1_start", "p1_end", "p1_quorum", "p1_nacks", "p2_start", "p2_end", "p2_quorum", "p2_nacks"}` (`p2_*` are `None` when Phase 1 failed). `Proposer.stats` gains `"late_responses"` and `"late_nacks"` (responses arriving after their transaction was cleaned up).

- [ ] **Step 1: Write the failing test** (append to `tests/test_duel.py`)

```python
from entity import EntityRegistry
from paxos import Acceptor, MajorityQuorum, Proposer


def _tiny_consensus(max_rounds=3):
    env = simpy.Environment()
    net = DatacenterNetwork(env, NetworkConfig(seed=1))
    net.add_location("dc")
    reg = EntityRegistry()
    accs = []
    for i in range(3):
        e = reg.create(name=f"a{i}")
        net.assign_entity(e.id, "dc")
        accs.append(Acceptor(env, e, net))
    pe = reg.create(name="prop")
    net.assign_entity(pe.id, "dc")
    prop = Proposer(env, pe, net, [a.entity.id for a in accs],
                    MajorityQuorum([a.entity.id for a in accs]),
                    timeout=0.5, max_rounds=max_rounds)
    holder = {}

    def go():
        holder["r"] = yield prop.propose(slot=0, value="v")
    env.process(go())
    env.run(until=10.0)
    return holder["r"], prop


def test_consensus_result_round_log_and_counters():
    r, prop = _tiny_consensus()
    assert r.success
    assert len(r.round_log) == r.rounds
    first = r.round_log[0]
    for key in ("round", "proposal_number", "p1_start", "p1_end",
                "p1_quorum", "p1_nacks", "p2_start", "p2_end",
                "p2_quorum", "p2_nacks"):
        assert key in first
    assert first["p1_quorum"] is True
    assert first["p2_quorum"] is True
    assert r.phase1_quorums == 1
    assert r.phase2_failures == 0
    assert r.phase1_nacks == 0 and r.phase2_nacks == 0
    assert "late_responses" in prop.stats and "late_nacks" in prop.stats
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_duel.py::test_consensus_result_round_log_and_counters -v`
Expected: FAIL with `AttributeError: 'ConsensusResult' object has no attribute 'round_log'`

- [ ] **Step 3: Implement**

In `paxos.py`, extend `ConsensusResult` (note `field` is already imported at the top of the module):

```python
@dataclass
class ConsensusResult:
    """Result of a Paxos consensus attempt."""
    success: bool
    slot: int
    value: Any = None
    proposal_number: int = 0
    phase1_responses: int = 0
    phase2_responses: int = 0
    nacks: int = 0
    rounds: int = 0  # How many proposal rounds needed
    total_time: float = 0.0
    packets_sent: int = 0
    # Duel instrumentation (premortem A8/A9). Additive; defaults keep
    # every existing constructor and CSV writer unchanged.
    phase1_nacks: int = 0
    phase2_nacks: int = 0
    phase1_quorums: int = 0
    phase2_failures: int = 0
    round_log: list = field(default_factory=list)
```

In `Proposer.__init__`, extend `_stats`:

```python
        self._stats = {
            "proposals_started": 0,
            "proposals_succeeded": 0,
            "proposals_failed": 0,
            "phase1_rounds": 0,
            "phase2_rounds": 0,
            "total_packets_sent": 0,
            "late_responses": 0,
            "late_nacks": 0,
        }
```

In `Proposer._receiver`, count orphaned responses instead of dropping them silently:

```python
    def _receiver(self):
        """Background process receiving VMTP responses."""
        while True:
            packet = yield self.mailbox.get()
            if isinstance(packet, Response):
                txn_id = packet.transaction_id
                if txn_id in self._pending:
                    self._responses[txn_id] = packet
                    self._pending[txn_id].succeed()
                else:
                    # Response arrived after its round was cleaned up
                    # (e.g. a preemption NACK landing post-quorum). Count
                    # it so contention evidence is not silently dropped.
                    self._stats["late_responses"] += 1
                    if (isinstance(packet.payload, PaxosPayload)
                            and packet.payload.phase == PaxosPhase.NACK):
                        self._stats["late_nacks"] += 1
```

In `Proposer._propose`, thread the new accounting. Add accumulators after `packets = 0`:

```python
        total_p1_nacks = 0
        total_p2_nacks = 0
        phase1_quorums = 0
        phase2_failures = 0
        round_log = []
```

Immediately after `proposal_number = self._next_proposal_number()`:

```python
            entry = {
                "round": round_num,
                "proposal_number": proposal_number,
                "p1_start": self.env.now, "p1_end": None,
                "p1_quorum": False, "p1_nacks": 0,
                "p2_start": None, "p2_end": None,
                "p2_quorum": None, "p2_nacks": None,
            }
            round_log.append(entry)
```

After the Phase 1 parse loop (`total_phase1 += len(promises)` / `total_nacks += phase1_nacks`):

```python
            total_p1_nacks += phase1_nacks
            entry["p1_end"] = self.env.now
            entry["p1_nacks"] = phase1_nacks
```

In the Phase-1-failure branch, before `continue`:

```python
                entry["p1_quorum"] = False
```

After the Phase 1 quorum check passes (right before the highest-accepted scan):

```python
            entry["p1_quorum"] = True
            phase1_quorums += 1
```

Right before the Phase 2 sends (`self._stats["phase2_rounds"] += 1` block), add:

```python
            entry["p2_start"] = self.env.now
```

After the Phase 2 parse loop (`total_phase2 += len(accepteds)` / `total_nacks += phase2_nacks`):

```python
            total_p2_nacks += phase2_nacks
            entry["p2_end"] = self.env.now
            entry["p2_nacks"] = phase2_nacks
```

In the success return path, set `entry["p2_quorum"] = True` immediately after the `if self.quorum.is_phase2_quorum(phase2_respondents):` line, and extend the returned `ConsensusResult` (both return sites) with:

```python
                    phase1_nacks=total_p1_nacks,
                    phase2_nacks=total_p2_nacks,
                    phase1_quorums=phase1_quorums,
                    phase2_failures=phase2_failures,
                    round_log=round_log,
```

In the Phase-2-failure path (just before the final `yield self.env.timeout(0.010 * (round_num + 1))` retry backoff):

```python
            entry["p2_quorum"] = False
            phase2_failures += 1
```

- [ ] **Step 4: Run new test and full suite**

Run: `uv run pytest tests/test_duel.py -v && uv run pytest`
Expected: all PASS. (`write_summary_csv` and the sweep scripts read named attributes only; the new fields are invisible to them.)

- [ ] **Step 5: Commit**

```bash
git add paxos.py tests/test_duel.py
git commit -m "feat: per-phase NACK, round-log, and late-response instrumentation on Proposer (premortem A8/A9)"
```

---

### Task 3: `duel.py` — PriorityProposer, wiring with startup gates, jitter scaling

Contract items: A1 (shared wall), A2 (explicit ballot priority), A7 (capability gate, derived reachability, time-budget validation), A10 (determinism hygiene).

**Files:**
- Create: `duel.py`
- Test: `tests/test_duel.py`

**Interfaces:**
- Consumes: `demo_step_9.build_topology(env, mars_base_latency_s, seed)`; `capability.classify(wall, initiator_tier, reachable) -> CapabilityReport`; `capability.Hazard`; `time_budget.phase_time(d_max, p_max)`; Task 1's `enable_per_link_rng()`; Task 2's instrumented `Proposer`.
- Produces: `PriorityProposer(Proposer)` with keyword-only `ballot_rank: int` (ballot = `counter*1000 + ballot_rank`); `scale_jitter(network, scale: float) -> None`; `DuelSystem` dataclass (`env, network, wall, earth_prop, leo_prop, acceptors, earth_ids, all_ids, k, polarity`); `wire_duel(env, *, k: int, polarity: str, earth_max_rounds: int, leo_max_rounds: int, jitter_scale: float, seed: int, timeout: float = 1.0) -> DuelSystem`; `required_d_max(network, proposer_entity_id, node_ids) -> float`.

- [ ] **Step 1: Write the failing tests** (append to `tests/test_duel.py`)

```python
import pytest

from capability import Hazard, classify


def test_priority_proposer_ballot_uses_rank():
    from duel import PriorityProposer
    env = simpy.Environment()
    net = DatacenterNetwork(env, NetworkConfig(seed=1))
    net.add_location("dc")
    reg = EntityRegistry()
    e = reg.create(name="p")
    net.assign_entity(e.id, "dc")
    p = PriorityProposer(env, e, net, [e.id], MajorityQuorum([e.id]),
                         ballot_rank=501)
    assert p._next_proposal_number() == 1 * 1000 + 501
    assert p._next_proposal_number() == 2 * 1000 + 501


def test_priority_proposer_rejects_bad_rank():
    from duel import PriorityProposer
    env = simpy.Environment()
    net = DatacenterNetwork(env, NetworkConfig(seed=1))
    reg = EntityRegistry()
    e = reg.create(name="p")
    with pytest.raises(AssertionError):
        PriorityProposer(env, e, net, [e.id], MajorityQuorum([e.id]),
                         ballot_rank=1000)


def test_scale_jitter_zeroes_all_links():
    from duel import scale_jitter
    env = simpy.Environment()
    net = five_dc_topology(env, seed=3)
    scale_jitter(net, 0.0)
    assert all(l.jitter == 0.0 for l in net._links.values())
    assert net._default_local_link.jitter == 0.0
    assert net.config.delay_jitter == 0.0


def test_wire_duel_gates_and_shared_wall():
    from duel import wire_duel
    env = simpy.Environment()
    sys_ = wire_duel(env, k=5, polarity="leo_high", earth_max_rounds=1,
                     leo_max_rounds=8, jitter_scale=0.0, seed=0)
    # A1: literally the same quorum object.
    assert sys_.earth_prop.quorum is sys_.leo_prop.quorum
    # A2: explicit distinct ranks; leo_high means LEO wins equal-counter ties.
    assert sys_.leo_prop.ballot_rank > sys_.earth_prop.ballot_rank
    # A7: LEO really is in the hazard state, derived from actual links.
    leo_reach = {a for a in sys_.all_ids
                 if sys_.network.get_link(sys_.leo_prop.entity.id, a) is not None}
    rep = classify(sys_.wall, 2, leo_reach)
    assert rep.r1 and not rep.r2
    assert Hazard.DISRUPTIVE_ELECTION in rep.hazards


def test_wire_duel_k3_is_failover_regime():
    from duel import wire_duel
    env = simpy.Environment()
    sys_ = wire_duel(env, k=3, polarity="earth_high", earth_max_rounds=1,
                     leo_max_rounds=8, jitter_scale=0.0, seed=0)
    leo_reach = {a for a in sys_.all_ids
                 if sys_.network.get_link(sys_.leo_prop.entity.id, a) is not None}
    rep = classify(sys_.wall, 2, leo_reach)
    assert rep.r1 and rep.r2 and not rep.hazards
    assert sys_.earth_prop.ballot_rank > sys_.leo_prop.ballot_rank
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_duel.py -v -k "priority or scale_jitter or wire_duel"`
Expected: FAIL with `ModuleNotFoundError: No module named 'duel'`

- [ ] **Step 3: Implement `duel.py` (wiring half)**

```python
"""Dueling-proposer hazard experiment core.

Runs an Earth-initiated and a sparse-LEO-initiated proposer against the
SAME slot of the SAME CrumblingWallQuorum, at a controlled start offset.
Under strict Phase 2 (k=|E|) the LEO proposer is in the (R1=1, R2=0)
capability state: it can complete elections but can never commit — a
pure spoiler. The lemma fixes THAT disruption is reachable; this
experiment measures its COST as a function of relative schedule offset.

Design contract (binding):
  docs/superpowers/notes/2026-07-22-dueling-proposer-premortem.md  §A
Spec:
  docs/superpowers/specs/2026-07-19-four-layer-capability-model-design.md
Prose bans carried from the spec: no FLP claims, no Multi-Paxos
authority claims, no backoff-policy study.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import simpy

from capability import Hazard, classify
from demo_step_9 import build_topology
from entity import EntityRegistry
from paxos import Acceptor, ConsensusResult, Proposer
from quorums import CrumblingWallQuorum
from time_budget import phase_time

# Explicit ballot priority (premortem A2): who wins an equal-counter tie
# is an experimental condition, never an accident of entity-creation
# order. Ranks sit far above every entity id (asserted at wiring time).
LOW_RANK = 501
HIGH_RANK = 502

ACCEPTOR_PROCESS_TIME_MAX = 0.001  # p_max for time-budget checks


class PriorityProposer(Proposer):
    """Proposer whose ballot low-bits come from an explicit rank."""

    def __init__(self, *args, ballot_rank: int, **kwargs):
        assert 0 < ballot_rank < 1000, (
            f"ballot_rank must fit the counter*1000 scheme, got {ballot_rank}")
        super().__init__(*args, **kwargs)
        self.ballot_rank = ballot_rank

    def _next_proposal_number(self) -> int:
        self._proposal_counter += 1
        return self._proposal_counter * 1000 + self.ballot_rank


def scale_jitter(network, scale: float) -> None:
    """Scale every jitter source in the network (0.0 = deterministic).

    add_link stores one Link object under both direction keys, so
    deduplicate by identity before scaling.
    """
    for link in {id(l): l for l in network._links.values()}.values():
        link.jitter *= scale
    network._default_local_link.jitter *= scale
    network.config.delay_jitter *= scale


def required_d_max(network, proposer_entity_id: int,
                   node_ids: set[int]) -> float:
    """Worst one-way latency (incl. jitter bound) to a REQUIRED node set."""
    worst = 0.0
    for nid in node_ids:
        link = network.get_link(proposer_entity_id, nid)
        assert link is not None, f"no route to required node {nid}"
        worst = max(worst, link.latency + link.jitter)
    return worst


@dataclass
class DuelSystem:
    env: simpy.Environment
    network: Any
    wall: CrumblingWallQuorum
    earth_prop: PriorityProposer
    leo_prop: PriorityProposer
    acceptors: list[Acceptor]
    earth_ids: list[int]
    all_ids: list[int]
    k: int
    polarity: str


def wire_duel(env: simpy.Environment, *, k: int, polarity: str,
              earth_max_rounds: int, leo_max_rounds: int,
              jitter_scale: float, seed: int,
              timeout: float = 1.0) -> DuelSystem:
    """Build the duel system with every premortem §A startup gate armed."""
    assert polarity in ("leo_high", "earth_high"), polarity
    assert k in (5, 4, 3), k

    registry = EntityRegistry()
    network = build_topology(env, mars_base_latency_s=186.0, seed=seed)
    scale_jitter(network, jitter_scale)
    if jitter_scale > 0:
        network.enable_per_link_rng()

    earth_locs = ["na-west", "europe", "asia", "sa-east", "africa"]
    earth_entities = []
    for loc in earth_locs:
        entity = registry.create(name=f"earth-{loc}")
        network.assign_entity(entity.id, loc)
        earth_entities.append(entity)
    moon = registry.create(name="moon")
    network.assign_entity(moon.id, "moon")
    leo = registry.create(name="leo")
    network.assign_entity(leo.id, "leo-sat")
    mars_entities = []
    for i in range(3):
        entity = registry.create(name=f"mars-{i}")
        network.assign_entity(entity.id, f"mars-{i}")
        mars_entities.append(entity)

    acceptors = []
    for entity in earth_entities + [moon, leo] + mars_entities:
        pt = 0.0005 if "earth" in entity.name or "leo" in entity.name else 0.001
        acceptors.append(Acceptor(env, entity, network, process_time=pt))

    earth_ids = [e.id for e in earth_entities]
    mars_ids = [e.id for e in mars_entities]
    all_ids = earth_ids + [leo.id, moon.id] + mars_ids

    # ONE wall instance for BOTH proposers (premortem A1). demo_step_9's
    # earth_prop (FlexibleQuorum) must never be reused here: under a
    # mismatched pair, LEO's Phase 1 poisons only its 3 reachable Earth
    # nodes and a q2=2 Earth commit slips through the other 2 — a false
    # null manufactured by configuration.
    wall = CrumblingWallQuorum(
        [mars_ids, [moon.id], [leo.id], earth_ids],
        phase2_threshold=k)

    earth_rank, leo_rank = ((LOW_RANK, HIGH_RANK) if polarity == "leo_high"
                            else (HIGH_RANK, LOW_RANK))

    earth_prop_entity = registry.create(name="earth-proposer")
    network.assign_entity(earth_prop_entity.id, "na-west")
    earth_prop = PriorityProposer(
        env, earth_prop_entity, network, all_ids, wall,
        timeout=timeout, max_rounds=earth_max_rounds,
        initiator_tier=3, ballot_rank=earth_rank)

    leo_prop_entity = registry.create(name="leo-proposer")
    network.assign_entity(leo_prop_entity.id, "leo-sat")
    leo_prop = PriorityProposer(
        env, leo_prop_entity, network, all_ids, wall,
        timeout=timeout, max_rounds=leo_max_rounds,
        initiator_tier=2, ballot_rank=leo_rank)

    # --- Startup gates (premortem A1/A2/A7/A10) ---
    assert earth_prop.quorum is leo_prop.quorum, "proposers must share the wall"
    assert earth_prop.quorum.phase2_threshold == k
    assert earth_prop.ballot_rank != leo_prop.ballot_rank
    assert max(e.id for e in registry._entities.values()) < 1000, (
        "entity ids must stay below the ballot multiplier")

    # Capability gate: derive reachability from ACTUAL links, then demand
    # the classifier certify the regime this trial claims to exercise.
    leo_reach = {a for a in all_ids
                 if network.get_link(leo_prop_entity.id, a) is not None}
    leo_report = classify(wall, 2, leo_reach)
    if k in (5, 4):
        assert leo_report.r1 and not leo_report.r2, (
            f"k={k} must put sparse LEO in (1,0); got "
            f"({leo_report.r1},{leo_report.r2})")
        assert Hazard.DISRUPTIVE_ELECTION in leo_report.hazards
    else:  # k == 3: relaxation converts the spoiler into a failover peer
        assert leo_report.r1 and leo_report.r2 and not leo_report.hazards
    earth_reach = {a for a in all_ids
                   if network.get_link(earth_prop_entity.id, a) is not None}
    earth_report = classify(wall, 3, earth_reach)
    assert earth_report.r1 and earth_report.r2

    # Time-budget gate: each proposer's timeout must exceed the worst
    # request-response path over its REQUIRED nodes; both share one
    # timeout, so the clocks are commensurate by construction.
    earth_d = required_d_max(network, earth_prop_entity.id, set(earth_ids))
    leo_required = {leo.id} | (leo_reach & set(earth_ids))
    leo_d = required_d_max(network, leo_prop_entity.id, leo_required)
    for name, d in (("earth", earth_d), ("leo", leo_d)):
        assert timeout > phase_time(d, ACCEPTOR_PROCESS_TIME_MAX), (
            f"{name} timeout {timeout}s inside worst phase "
            f"{phase_time(d, ACCEPTOR_PROCESS_TIME_MAX)}s")

    return DuelSystem(env=env, network=network, wall=wall,
                      earth_prop=earth_prop, leo_prop=leo_prop,
                      acceptors=acceptors, earth_ids=earth_ids,
                      all_ids=all_ids, k=k, polarity=polarity)
```

- [ ] **Step 4: Run tests**

Run: `uv run pytest tests/test_duel.py -v`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add duel.py tests/test_duel.py
git commit -m "feat: duel wiring with shared-wall, ballot-priority, capability and time-budget gates (premortem A1/A2/A7)"
```

---

### Task 4: Trial runner — decision certificate, outcome taxonomy, overlap diagnostic

Contract items: A5 (baseline arm), A7 (same slot), A8 (certificate-based attribution, livelock criterion, competing-risks outcomes), A9 (overlap diagnostic), A10 (nonzero offsets).

**Files:**
- Modify: `duel.py` (append)
- Test: `tests/test_duel.py`

**Interfaces:**
- Produces: `decision_certificate(acceptors, wall, slot) -> tuple[int, Any] | None` (the decided-or-inevitable value: a Phase 2 quorum whose members' latest acceptances all carry one value); `classify_outcome(cert, earth_r, leo_r, leo_enabled, preempted_earth, preempted_leo, decided_by) -> str` (pure outcome taxonomy, unit-testable in isolation); `DuelTrialResult` dataclass (fields listed in the code below); `run_duel_trial(*, offset, polarity, k, earth_max_rounds=1, leo_max_rounds=8, jitter_scale=0.0, seed=0, leo_enabled=True, earth_start=5.0, tail=45.0) -> DuelTrialResult`. Offset convention: `offset = leo_start - earth_start`; negative = LEO first. `offset == 0.0` is rejected (A10). Baseline arm = `leo_enabled=False`: the LEO proposer is constructed identically but never proposes — under per-link RNG (and trivially under zero jitter) Earth's noise field and Paxos state are then identical to a LEO-free run, which is the paired counterfactual A5 requires without any message-suppression machinery.
- LIVELOCK_MIN_PREEMPTED_ROUNDS = 3 (the explicit criterion; a constant, reported in every CSV row via the config columns).

- [ ] **Step 1: Write the failing tests** (append to `tests/test_duel.py`)

```python
def test_far_offset_earth_commits_clean():
    from duel import run_duel_trial
    t = run_duel_trial(offset=30.0, polarity="leo_high", k=5)
    assert t.outcome == "earth_commit"
    assert t.decided_by == "earth"
    assert t.earth_result.phase2_nacks == 0
    assert not t.rounds_overlapped
    # LEO completed elections but could never commit (R2=0).
    assert t.leo_result.phase1_quorums >= 1
    assert t.leo_result.phase2_failures == t.leo_result.phase1_quorums
    assert not t.leo_result.success


def test_overlap_offset_leo_high_single_shot_preempts_earth():
    # The lemma-predicted regime: Earth starts 0.5s into LEO's ~9s span
    # of repeated election attempts; LEO's higher ballot has already
    # poisoned its 3 reachable Earth nodes, so single-shot Earth cannot
    # commit (k=5 needs all five). The offset sits deep inside the
    # collision band, robust to link-latency tweaks — the map sweep
    # charts the razor edges; unit tests must not sit on them. (A +0.05
    # offset misses the window by ~7.5ms given concrete link latencies.)
    from duel import run_duel_trial
    t = run_duel_trial(offset=-0.5, polarity="leo_high", k=5,
                       earth_max_rounds=1)
    assert t.rounds_overlapped
    assert t.outcome in ("no_decision", "leo_blocked")
    assert not t.earth_result.success
    assert (t.earth_result.phase1_nacks + t.earth_result.phase2_nacks
            + t.earth_late_nacks) >= 1


def test_baseline_arm_has_no_leo_interference():
    from duel import run_duel_trial
    t = run_duel_trial(offset=0.05, polarity="leo_high", k=5,
                       leo_enabled=False)
    assert t.outcome == "earth_commit"
    assert t.leo_result is None


def test_zero_offset_rejected():
    from duel import run_duel_trial
    import pytest as _pytest
    with _pytest.raises(AssertionError):
        run_duel_trial(offset=0.0, polarity="leo_high", k=5)


def test_k3_someone_commits_and_safety_holds():
    from duel import run_duel_trial
    t = run_duel_trial(offset=-0.5, polarity="leo_high", k=3,
                       earth_max_rounds=5)
    assert t.outcome in ("earth_commit", "leo_commit")
    assert t.decided_value is not None


def test_censored_outcome_on_short_horizon():
    from duel import run_duel_trial
    t = run_duel_trial(offset=-0.5, polarity="leo_high", k=5, tail=0.05)
    assert t.outcome == "censored"


def test_classify_outcome_branches():
    # Pure-function coverage of the full A8 taxonomy (as amended: mutual
    # livelock, total-not-consecutive counts) without razor-edge dynamics.
    from duel import classify_outcome
    from paxos import ConsensusResult
    done_ok = ConsensusResult(success=True, slot=1)
    done_fail = ConsensusResult(success=False, slot=1)
    assert classify_outcome(None, None, done_ok, True, 0, 0, None) == "censored"
    assert classify_outcome(None, done_fail, None, True, 0, 0, None) == "censored"
    assert classify_outcome((5502, "leo-1"), done_fail, done_ok, True, 1, 0,
                            "leo") == "leo_commit"
    assert classify_outcome(None, done_fail, done_fail, True, 3, 1,
                            None) == "livelock"
    assert classify_outcome(None, done_fail, done_fail, True, 3, 0,
                            None) == "leo_blocked"
    assert classify_outcome(None, done_fail, None, False, 0, 0,
                            None) == "no_decision"
```

Note for the implementer: late NACKs are a per-PROPOSER stat (`Proposer.stats["late_nacks"]`), not a `ConsensusResult` field — `DuelTrialResult.earth_late_nacks` carries it, and the test above already uses `t.earth_late_nacks`.

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_duel.py -v -k "far_offset or overlap_offset or baseline_arm or zero_offset or k3"`
Expected: FAIL with `ImportError: cannot import name 'run_duel_trial'`

- [ ] **Step 3: Implement (append to `duel.py`)**

```python
# Explicit livelock criterion (premortem A8): declared, not horizon-implied.
LIVELOCK_MIN_PREEMPTED_ROUNDS = 3


def decision_certificate(acceptors: list[Acceptor], wall: CrumblingWallQuorum,
                         slot: int):
    """Return (ballot, value) for the decided-or-inevitable value, or None.

    This is the four-layer model's learner discipline: a value is chosen
    only when quorum evidence says so — never inferred from a proposer's
    own success flag (which is cross-checked against this certificate).

    Groups by VALUE, not exact ballot: value carry-over means a spoiler's
    higher-ballot re-accept can legitimately advance part of a quorum
    past the ballot the value was first chosen at (premortem A8 expects
    exactly this). A value v held as the LATEST acceptance by a full
    Phase 2 quorum is unique (any two Q2s intersect, and an acceptor has
    one latest value), and by carry-over every future decision must be
    v — i.e. v is decided or inevitable. The returned ballot is the
    highest among v's certifying acceptances.
    """
    by_value: dict[Any, set[int]] = {}
    ballots: dict[Any, int] = {}
    for acc in acceptors:
        got = acc._accepted.get(slot)
        if got is not None:
            ballot, value = got
            by_value.setdefault(value, set()).add(acc.entity.id)
            ballots[value] = max(ballots.get(value, 0), ballot)
    for value, voters in by_value.items():
        if wall.is_phase2_quorum(voters):
            return ballots[value], value
    return None


def _intervals(result: ConsensusResult) -> list[tuple[float, float]]:
    spans = []
    for e in result.round_log:
        end = e["p2_end"] if e["p2_end"] is not None else e["p1_end"]
        if end is None:
            end = e["p1_start"]  # round censored mid-phase at horizon
        spans.append((e["p1_start"], end))
    return spans


def _overlapped(a: ConsensusResult, b: ConsensusResult) -> bool:
    return any(s1 < e2 and s2 < e1
               for (s1, e1) in _intervals(a)
               for (s2, e2) in _intervals(b))


def _preempted_rounds(result: ConsensusResult | None) -> int:
    if result is None:
        return 0
    return sum(1 for e in result.round_log
               if (e["p1_nacks"] or 0) + (e["p2_nacks"] or 0) > 0)


def classify_outcome(cert, earth_r, leo_r, leo_enabled,
                     preempted_earth, preempted_leo, decided_by) -> str:
    """Outcome taxonomy (premortem A8, as amended 2026-07-22).

    'livelock' requires MUTUAL starvation: Earth preempted in >=
    LIVELOCK_MIN_PREEMPTED_ROUNDS rounds AND LEO preempted in >= 1 round,
    with no decision and both proposers finished. Documented
    simplification: preempted rounds are counted in TOTAL, not
    consecutively — with earth_max_rounds <= 5 the two coincide in
    practice, and consecutive-run detection buys nothing at this scale.
    One-sided starvation (only Earth preempted) is 'leo_blocked': the
    spoiler prevented a decision — the asymmetric regime the premortem's
    §B3 names, where classic mutual livelock is not even available to LEO.
    """
    if earth_r is None or (leo_enabled and leo_r is None):
        return "censored"
    if cert is not None:
        return f"{decided_by}_commit"
    if (leo_enabled and not earth_r.success and not leo_r.success
            and preempted_earth >= LIVELOCK_MIN_PREEMPTED_ROUNDS
            and preempted_leo >= 1):
        return "livelock"
    if leo_enabled and not earth_r.success and preempted_earth > 0:
        return "leo_blocked"
    return "no_decision"


@dataclass
class DuelTrialResult:
    offset: float
    polarity: str
    k: int
    earth_max_rounds: int
    leo_max_rounds: int
    jitter_scale: float
    seed: int
    leo_enabled: bool
    earth_start: float
    tail: float
    outcome: str          # earth_commit | leo_commit | leo_blocked |
                          # no_decision | livelock | censored
    decided_value: str | None
    decided_by: str | None    # "earth" | "leo" | None (from value provenance)
    decided_ballot: int | None
    rounds_overlapped: bool
    preempted_earth_rounds: int
    preempted_leo_rounds: int
    earth_result: ConsensusResult | None   # None = censored at horizon
    leo_result: ConsensusResult | None     # None = disabled or censored
    earth_late_nacks: int
    leo_late_nacks: int
    earth_commit_latency: float | None


def run_duel_trial(*, offset: float, polarity: str, k: int,
                   earth_max_rounds: int = 1, leo_max_rounds: int = 8,
                   jitter_scale: float = 0.0, seed: int = 0,
                   leo_enabled: bool = True, earth_start: float = 5.0,
                   tail: float = 45.0) -> DuelTrialResult:
    """One single-slot duel. offset = leo_start - earth_start (s)."""
    assert offset != 0.0, "offset must be strictly nonzero (premortem A10)"
    leo_start = earth_start + offset
    assert min(earth_start, leo_start) > 0.0, (
        "both start times must be positive; widen earth_start")

    env = simpy.Environment()
    sys_ = wire_duel(env, k=k, polarity=polarity,
                     earth_max_rounds=earth_max_rounds,
                     leo_max_rounds=leo_max_rounds,
                     jitter_scale=jitter_scale, seed=seed)
    slot = 1
    results: dict[str, ConsensusResult] = {}

    def drive(name: str, prop: PriorityProposer, start: float, value: str):
        yield env.timeout(start)
        results[name] = yield prop.propose(slot=slot, value=value)

    env.process(drive("earth", sys_.earth_prop, earth_start, f"earth-{slot}"))
    if leo_enabled:
        env.process(drive("leo", sys_.leo_prop, leo_start, f"leo-{slot}"))

    env.run(until=max(earth_start, leo_start) + tail)

    earth_r = results.get("earth")
    leo_r = results.get("leo") if leo_enabled else None
    cert = decision_certificate(sys_.acceptors, sys_.wall, slot)
    decided_ballot, decided_value = cert if cert else (None, None)
    decided_by = (decided_value.split("-")[0] if decided_value else None)

    # Cross-check proposer claims against quorum evidence.
    if earth_r is not None and earth_r.success:
        assert cert is not None and decided_value == earth_r.value, (
            "earth proposer claims success without a matching certificate")
    if leo_r is not None and leo_r.success:
        assert cert is not None and decided_value == leo_r.value, (
            "leo proposer claims success without a matching certificate")

    preempted = _preempted_rounds(earth_r)
    preempted_leo = _preempted_rounds(leo_r)
    outcome = classify_outcome(cert, earth_r, leo_r, leo_enabled,
                               preempted, preempted_leo, decided_by)

    return DuelTrialResult(
        offset=offset, polarity=polarity, k=k,
        earth_max_rounds=earth_max_rounds, leo_max_rounds=leo_max_rounds,
        jitter_scale=jitter_scale, seed=seed, leo_enabled=leo_enabled,
        earth_start=earth_start, tail=tail,
        outcome=outcome, decided_value=decided_value, decided_by=decided_by,
        decided_ballot=decided_ballot,
        rounds_overlapped=(earth_r is not None and leo_r is not None
                           and _overlapped(earth_r, leo_r)),
        preempted_earth_rounds=preempted,
        preempted_leo_rounds=preempted_leo,
        earth_result=earth_r, leo_result=leo_r,
        earth_late_nacks=sys_.earth_prop.stats["late_nacks"],
        leo_late_nacks=(sys_.leo_prop.stats["late_nacks"]
                        if leo_enabled else 0),
        earth_commit_latency=(earth_r.total_time
                              if earth_r is not None and earth_r.success
                              else None),
    )
```

- [ ] **Step 4: Run tests, then full suite**

Run: `uv run pytest tests/test_duel.py -v && uv run pytest`
Expected: all PASS. If `test_overlap_offset_leo_high_single_shot_preempts_earth` fails on the NACK assertion, the NACK may have landed post-cleanup — that is exactly what `earth_late_nacks` counts; the committed test asserts on the sum including it. If it fails because Earth *succeeded*, STOP: that is potentially finding H1/H2 territory (shared wall or ballot polarity not doing its job) — debug with the round_log before touching the test.

- [ ] **Step 5: Commit**

```bash
git add duel.py tests/test_duel.py
git commit -m "feat: duel trial runner with decision certificate, outcome taxonomy, overlap diagnostic (premortem A5/A8/A9)"
```

---

### Task 5: Sweep driver — two-stage grid, deterministic map, jitter sweep with Wilson CIs

Contract items: A4 (deterministic primary presented as a map; degenerate-cell flags; Wilson intervals), A6 (retry budget as labeled condition), A9 (two-stage grid, full-curve reporting, overlap diagnostic in every row).

**Files:**
- Create: `experiments/duel_sweep.py`
- Test: `tests/test_duel.py`

**Interfaces:**
- Consumes: `duel.run_duel_trial(...) -> DuelTrialResult` (Task 4 signature).
- Produces: `offset_grid(fine_lo=-12.0, fine_hi=6.0, fine_step=0.05, coarse_step=5.0, coarse_hi=118.0) -> list[float]` (fine band at sub-round resolution around 0, exact 0 excluded, plus coarse tail across the 120 s reconcile cadence — the fraction-of-round_time step A9 demands: Earth-scale round ≈ 0.37 s, step 0.05 ≈ round/7); `wilson_ci(successes: int, n: int, z: float = 1.96) -> tuple[float | None, float | None]`; `trial_row(t: DuelTrialResult) -> dict` (flat CSV row, every config param included); CLI `uv run python experiments/duel_sweep.py --mode {map,jitter} --output PATH [--aggregate-output PATH] [--seeds CSV]`.
- Conditions matrix: polarity ∈ {leo_high, earth_high} × k ∈ {5, 3} × earth_max_rounds ∈ {1, 5}; leo_max_rounds fixed at 8. k=4 is omitted as capability-identical to k=5 (both (1,0); the classifier table covers it). Baseline rows (`leo_enabled=False`): one per (k, earth_max_rounds) in map mode (deterministic — offset-invariant by construction); one per (k, earth_max_rounds, seed) in jitter mode.

- [ ] **Step 1: Write the failing tests** (append to `tests/test_duel.py`)

```python
def test_offset_grid_shape():
    import sys as _sys
    from pathlib import Path as _P
    _sys.path.insert(0, str(_P(__file__).resolve().parents[1] / "experiments"))
    from duel_sweep import offset_grid
    grid = offset_grid()
    assert 0.0 not in grid
    assert min(grid) == -12.0
    assert max(grid) <= 118.0
    fine = [o for o in grid if -12.0 <= o <= 6.0]
    diffs = {round(b - a, 3) for a, b in zip(fine, fine[1:])}
    assert diffs == {0.05} or diffs == {0.05, 0.1}  # 0.1 gap where 0.0 was removed
    assert all(o > 6.0 for o in grid if o not in fine)


def test_wilson_ci_known_values():
    from duel_sweep import wilson_ci
    lo, hi = wilson_ci(0, 0)
    assert lo is None and hi is None
    lo, hi = wilson_ci(50, 50)
    assert hi == 1.0 and lo > 0.9   # no fake ±0.0 certainty
    lo, hi = wilson_ci(0, 50)
    assert lo == 0.0 and hi < 0.1
    lo, hi = wilson_ci(25, 50)
    assert 0.36 < lo < 0.5 < hi < 0.64


def test_trial_row_carries_full_config():
    from duel import run_duel_trial
    from duel_sweep import trial_row
    t = run_duel_trial(offset=30.0, polarity="leo_high", k=5)
    row = trial_row(t)
    for col in ("offset", "polarity", "k", "earth_max_rounds",
                "leo_max_rounds", "jitter_scale", "seed", "leo_enabled",
                "earth_start", "tail",
                "livelock_min_preempted_rounds", "outcome", "decided_by",
                "rounds_overlapped", "earth_success", "earth_rounds",
                "earth_p1_nacks", "earth_p2_nacks", "earth_late_nacks",
                "leo_p1_quorums", "leo_p2_failures",
                "earth_commit_latency_s"):
        assert col in row, col
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_duel.py -v -k "grid or wilson or trial_row"`
Expected: FAIL with `ModuleNotFoundError: No module named 'duel_sweep'`

- [ ] **Step 3: Implement `experiments/duel_sweep.py`**

```python
"""Dueling-proposer sweep: offset -> outcome map plus jitter robustness.

Primary (--mode map): fully deterministic (jitter 0, nothing random ever
drawn). One trial per (condition, offset). Presented as a MAP — never as
mean±CI: 50 identical deterministic replicas would manufacture ±0.0%
precision (premortem A4).

Secondary (--mode jitter): per-link-RNG stochastic sweep over seeds, on a
reduced offset set, aggregated with Wilson intervals and degenerate-cell
flags.

Result language (binding, from the spec's prohibitions): this measures
contention COST vs offset. No FLP claims, no Multi-Paxos authority
claims, no backoff-policy study.
"""
from __future__ import annotations

import argparse
import csv
import math
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from duel import LIVELOCK_MIN_PREEMPTED_ROUNDS, DuelTrialResult, run_duel_trial

POLARITIES = ("leo_high", "earth_high")
KS = (5, 3)                 # k=4 omitted: capability-identical to k=5
EARTH_RETRIES = (1, 5)      # single-shot vs bounded-retry (premortem A6)
LEO_MAX_ROUNDS = 8
# Both start times must be strictly positive (run_duel_trial asserts it,
# premortem A10) and the fine grid reaches offset -12.0, so the sweep
# starts Earth well past |min offset|. Deterministic dynamics depend only
# on relative times; translation invariance is verified empirically in
# the Task 7 preamble, not assumed.
EARTH_START = 20.0

# Reduced offsets for the jitter sweep: full fine coverage is the
# deterministic map's job; here we sample the collision band and a few
# far points to bound jitter sensitivity.
JITTER_OFFSETS = [-10.0, -5.0, -2.0, -1.0, -0.5, -0.2, -0.05,
                  0.05, 0.2, 0.5, 1.0, 2.0, 30.0, 90.0]


def offset_grid(fine_lo: float = -12.0, fine_hi: float = 6.0,
                fine_step: float = 0.05, coarse_step: float = 5.0,
                coarse_hi: float = 118.0) -> list[float]:
    """Two-stage grid (premortem A9): fine band at sub-round resolution
    around offset 0 (exact 0 excluded, A10), coarse tail across the
    reconcile cadence to confirm the flat no-interaction region."""
    n = int(round((fine_hi - fine_lo) / fine_step))
    fine = [round(fine_lo + i * fine_step, 3) for i in range(n + 1)]
    fine = [o for o in fine if abs(o) >= fine_step / 2]
    start = fine_hi + coarse_step
    coarse = []
    o = start
    while o <= coarse_hi:
        coarse.append(round(o, 3))
        o += coarse_step
    return fine + coarse


def wilson_ci(successes: int, n: int, z: float = 1.96):
    """Wilson score interval — no ±0.0% certainty at the 0/1 boundary."""
    if n == 0:
        return (None, None)
    p = successes / n
    denom = 1 + z * z / n
    centre = (p + z * z / (2 * n)) / denom
    half = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / denom
    return (max(0.0, centre - half), min(1.0, centre + half))


def trial_row(t: DuelTrialResult) -> dict:
    e, l = t.earth_result, t.leo_result
    return {
        "offset": t.offset,
        "polarity": t.polarity,
        "k": t.k,
        "earth_max_rounds": t.earth_max_rounds,
        "leo_max_rounds": t.leo_max_rounds,
        "jitter_scale": t.jitter_scale,
        "seed": t.seed,
        "leo_enabled": int(t.leo_enabled),
        "earth_start": t.earth_start,
        "tail": t.tail,
        "livelock_min_preempted_rounds": LIVELOCK_MIN_PREEMPTED_ROUNDS,
        "outcome": t.outcome,
        "decided_by": t.decided_by or "",
        "decided_ballot": t.decided_ballot if t.decided_ballot is not None else "",
        "rounds_overlapped": int(t.rounds_overlapped),
        "preempted_earth_rounds": t.preempted_earth_rounds,
        "preempted_leo_rounds": t.preempted_leo_rounds,
        "earth_success": int(bool(e and e.success)),
        "earth_rounds": e.rounds if e else "",
        "earth_p1_nacks": e.phase1_nacks if e else "",
        "earth_p2_nacks": e.phase2_nacks if e else "",
        "earth_late_nacks": t.earth_late_nacks,
        "earth_commit_latency_s": (f"{t.earth_commit_latency:.6f}"
                                   if t.earth_commit_latency is not None else ""),
        "leo_success": int(bool(l and l.success)),
        "leo_p1_quorums": l.phase1_quorums if l else "",
        "leo_p2_failures": l.phase2_failures if l else "",
        "leo_p1_nacks": l.phase1_nacks if l else "",
        "leo_p2_nacks": l.phase2_nacks if l else "",
        "leo_late_nacks": t.leo_late_nacks,
    }


FIELDNAMES = list(trial_row(run_duel_trial.__wrapped__).keys()) if False else [
    "offset", "polarity", "k", "earth_max_rounds", "leo_max_rounds",
    "jitter_scale", "seed", "leo_enabled", "earth_start", "tail",
    "livelock_min_preempted_rounds",
    "outcome", "decided_by", "decided_ballot", "rounds_overlapped",
    "preempted_earth_rounds", "preempted_leo_rounds", "earth_success", "earth_rounds",
    "earth_p1_nacks", "earth_p2_nacks", "earth_late_nacks",
    "earth_commit_latency_s", "leo_success", "leo_p1_quorums",
    "leo_p2_failures", "leo_p1_nacks", "leo_p2_nacks", "leo_late_nacks",
]


def _write_rows(path: Path, rows: list[dict]):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDNAMES)
        w.writeheader()
        w.writerows(rows)


def run_map(output: Path):
    grid = offset_grid()
    rows = []
    total = len(POLARITIES) * len(KS) * len(EARTH_RETRIES)
    done = 0
    for polarity in POLARITIES:
        for k in KS:
            for retries in EARTH_RETRIES:
                for off in grid:
                    rows.append(trial_row(run_duel_trial(
                        offset=off, polarity=polarity, k=k,
                        earth_max_rounds=retries,
                        leo_max_rounds=LEO_MAX_ROUNDS,
                        earth_start=EARTH_START)))
                done += 1
                print(f"  map {done}/{total}: {polarity} k={k} "
                      f"retries={retries} ({len(grid)} offsets)")
    # Baselines: offset-invariant under determinism -> one per condition.
    for k in KS:
        for retries in EARTH_RETRIES:
            rows.append(trial_row(run_duel_trial(
                offset=30.0, polarity="leo_high", k=k,
                earth_max_rounds=retries, leo_max_rounds=LEO_MAX_ROUNDS,
                earth_start=EARTH_START, leo_enabled=False)))
    _write_rows(output, rows)
    print(f"Wrote map: {output} ({len(rows)} rows)")


def run_jitter(output: Path, aggregate_output: Path, seeds: list[int]):
    rows = []
    for polarity in POLARITIES:
        for k in KS:
            for retries in EARTH_RETRIES:
                for off in JITTER_OFFSETS:
                    for seed in seeds:
                        rows.append(trial_row(run_duel_trial(
                            offset=off, polarity=polarity, k=k,
                            earth_max_rounds=retries,
                            leo_max_rounds=LEO_MAX_ROUNDS,
                            earth_start=EARTH_START,
                            jitter_scale=1.0, seed=seed)))
    for k in KS:
        for retries in EARTH_RETRIES:
            for seed in seeds:
                rows.append(trial_row(run_duel_trial(
                    offset=30.0, polarity="leo_high", k=k,
                    earth_max_rounds=retries, leo_max_rounds=LEO_MAX_ROUNDS,
                    earth_start=EARTH_START,
                    jitter_scale=1.0, seed=seed, leo_enabled=False)))
    _write_rows(output, rows)
    print(f"Wrote raw: {output} ({len(rows)} rows)")

    # Aggregate: earth_success rate per cell with Wilson interval and an
    # explicit degenerate flag instead of fake ±0.0 (premortem A4).
    cells: dict[tuple, list[dict]] = {}
    for r in rows:
        key = (r["polarity"], r["k"], r["earth_max_rounds"],
               r["leo_enabled"], r["offset"])
        cells.setdefault(key, []).append(r)
    agg_rows = []
    for (polarity, k, retries, enabled, off), cell in sorted(cells.items()):
        n = len(cell)
        s = sum(r["earth_success"] for r in cell)
        lo, hi = wilson_ci(s, n)
        lats = [float(r["earth_commit_latency_s"]) for r in cell
                if r["earth_commit_latency_s"] != ""]
        agg_rows.append({
            "polarity": polarity, "k": k, "earth_max_rounds": retries,
            "leo_enabled": enabled, "offset": off, "n": n,
            "earth_success_rate": f"{s / n:.6f}",
            "wilson_lo": f"{lo:.6f}", "wilson_hi": f"{hi:.6f}",
            "degenerate": int(s == 0 or s == n),
            "commit_latency_mean_s": (f"{sum(lats) / len(lats):.6f}"
                                      if lats else ""),
            "commit_latency_n": len(lats),
            "livelock_count": sum(r["outcome"] == "livelock" for r in cell),
            "censored_count": sum(r["outcome"] == "censored" for r in cell),
        })
    aggregate_output.parent.mkdir(parents=True, exist_ok=True)
    with aggregate_output.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(agg_rows[0].keys()))
        w.writeheader()
        w.writerows(agg_rows)
    print(f"Wrote aggregate: {aggregate_output} ({len(agg_rows)} cells)")


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--mode", choices=["map", "jitter"], required=True)
    ap.add_argument("--output", type=Path, required=True)
    ap.add_argument("--aggregate-output", type=Path, default=None)
    ap.add_argument("--seeds", type=str, default="")
    args = ap.parse_args()
    if args.mode == "map":
        run_map(args.output)
    else:
        seeds = [int(s) for s in args.seeds.split(",") if s.strip()]
        assert seeds, "--seeds required for jitter mode"
        assert args.aggregate_output is not None
        run_jitter(args.output, args.aggregate_output, seeds)


if __name__ == "__main__":
    main()
```

Delete the vestigial `FIELDNAMES = list(...) if False else [` construction if it bothers you — the plain list literal is the intended code; keep only the explicit list.

- [ ] **Step 4: Run tests and a smoke sweep**

Run: `uv run pytest tests/test_duel.py -v && uv run pytest`
Expected: all PASS.

Smoke (temporary, not committed output): verify the CLI runs end-to-end on a truncated grid by running one condition manually:

```bash
uv run python -c "
import sys; sys.path.insert(0, 'experiments')
from duel_sweep import trial_row
from duel import run_duel_trial
for off in (-0.5, 0.05, 30.0):
    r = trial_row(run_duel_trial(offset=off, polarity='leo_high', k=5, earth_max_rounds=1))
    print(off, r['outcome'], r['rounds_overlapped'], r['preempted_earth_rounds'])
"
```
Expected: three lines; offset 30.0 shows `earth_commit` with `rounds_overlapped=0`.

- [ ] **Step 5: Commit**

```bash
git add experiments/duel_sweep.py tests/test_duel.py
git commit -m "feat: duel sweep driver — two-stage offset grid, deterministic map, jitter sweep with Wilson CIs (premortem A4/A6/A9)"
```

---

### Task 6: Pre-registration document (BEFORE any full sweep runs)

Contract item: D2 (pre-register the lemma-predicted outcome; deviation = harness-bug signal first). This is a document task; it gates Task 7.

**Files:**
- Create: `docs/superpowers/notes/2026-07-22-duel-preregistration.md`

**Interfaces:**
- Produces: the committed prediction record Task 7 compares against.

- [ ] **Step 1: Write the document** with exactly these sections (predictions are derived from the lemma + code analysis; write them BEFORE looking at any sweep output):

```markdown
# Pre-registration: dueling-proposer sweep predictions

Committed BEFORE the first full sweep run. Deviations from these
predictions are treated as harness-bug signals FIRST (premortem D2);
only after the harness is exonerated do they become findings.

## Grid and conditions (fixed)
Offsets: fine [-12.0, +6.0] step 0.05 (0 excluded) + coarse to 118.0
step 5.0. Conditions: polarity {leo_high, earth_high} x k {5, 3} x
earth_max_rounds {1, 5}; leo_max_rounds 8. Deterministic map primary;
jitter sweep seeds 40-59 on the reduced offset set. The FULL curve is
reported for every condition (no selected offsets).

## Predictions

P1 (k=5, leo_high, earth retries=1): Earth fails exactly on the offsets
where a LEO round overlaps Earth's single round; elsewhere commits
~0.4s. The failure band edges align with LEO's Phase 1 span (~9.3s of
repeated attempts for offsets < 0) and Earth's round span (< ~1.5s for
offsets > 0). rounds_overlapped=1 on every failed row (a failure with
rounds_overlapped=0 is a harness bug).

P2 (k=5, leo_high, earth retries=5): Earth ultimately commits at ALL
offsets (its ~1.2s escalation cycle out-paces LEO's ~1.1s+ cycle enough
to land a fresh higher counter), with commit latency elevated by one to
a few retry cycles inside the collision band; livelock count 0 or
near-0. If livelock appears, report the offsets; do not tune backoff.

P3 (k=5, earth_high, retries=1): strictly weaker disruption than P1 -
equal-counter ties go to Earth, so only a LEO promise from a LATER
counter blocks. The failure band is narrower than P1's; if it is not,
suspect ballot-rank wiring (premortem A2 gate).

P4 (k=3, any polarity): the capability gate certifies LEO (1,1); LEO
becomes a legitimate failover peer. Some offsets end leo_commit (with
Earth's value carried where Earth's Phase 2 partially landed first).
decided_value is NEVER two different values across acceptors' final
quorum certificate (safety; a violation fails the trial's assert).

P5 (baselines, leo_enabled=False): earth_commit at every condition,
zero NACKs, latency ~0.4s (retries=1 and =5 identical - no contention).

P6 (jitter sweep): no cell's Wilson interval contradicts the
deterministic map's outcome at the same offset except inside the
collision band's edge cells (edge offsets may flip under +-jitter of
link latencies); degenerate cells appear only deep inside or far
outside the band.

## What would make us stop and audit the harness
- Any failed Earth row with rounds_overlapped=0 (P1 violation).
- Any commit without a decision certificate, or certificate/proposer
  disagreement (trips an assert).
- earth_high band wider than leo_high band (P3 violation).
- Baseline rows differing across retries (P5 violation).
- k=3 rows still showing DISRUPTIVE_ELECTION-style pure spoiling
  (capability gate should have made this impossible).
```

- [ ] **Step 2: Commit**

```bash
git add docs/superpowers/notes/2026-07-22-duel-preregistration.md
git commit -m "docs: pre-register dueling-proposer sweep predictions (premortem D2)"
```

---

### Task 7: Run the sweeps, verify against pre-registration, record results

Contract items: A4 (map + separated jitter sweep), D2 (compare to pre-registration), D6 (prose bans in the results note). Runs ONLY after Task 6 is committed.

**Files:**
- Create: `results/duel/duel_map.csv`, `results/duel/duel_jitter.csv`, `results/duel/duel_jitter_ci.csv` (generated)
- Create: `docs/superpowers/notes/2026-07-22-duel-results.md`
- Modify: `docs/step9-repro.md` (append reproduction commands)

**Interfaces:**
- Consumes: Task 5 CLI; Task 6 predictions.

- [ ] **Step 1: Run the deterministic map**

Run: `uv run python experiments/duel_sweep.py --mode map --output results/duel/duel_map.csv`
Expected: progress lines for 8 conditions, then `Wrote map: results/duel/duel_map.csv (...)` with 8×|grid|+4 rows. Runtime: minutes.

- [ ] **Step 2: Run the jitter sweep**

Run: `uv run python experiments/duel_sweep.py --mode jitter --output results/duel/duel_jitter.csv --aggregate-output results/duel/duel_jitter_ci.csv --seeds "40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59"`
Expected: `Wrote raw:` and `Wrote aggregate:` lines.

- [ ] **Step 3: Verify against the pre-registration, mechanically**

Run each check; record PASS/FAIL in the results note:

```bash
uv run python - <<'EOF'
import csv
rows = list(csv.DictReader(open("results/duel/duel_map.csv")))
duel = [r for r in rows if r["leo_enabled"] == "1"]
# P1 guard, AMENDED 2026-07-22 after gate-triggered audit: the original
# "no failure without temporal overlap" premise is FALSIFIED physics —
# Paxos promises are durable state, so a spoiler's denial outlives its
# activity (verified: k=5 offset -12, LEO quiesces t=16.6, Earth starts
# t=20.0, still NACKed by standing ballot 8502 at the 3 poisoned nodes).
# Correct harness-bug detector: no Earth failure without PREEMPTION
# EVIDENCE (some higher-ballot NACK, in-round or late).
bad = [r for r in duel if r["earth_success"] == "0"
       and r["outcome"] != "censored"
       and int(r["earth_p1_nacks"]) + int(r["earth_p2_nacks"])
           + int(r["earth_late_nacks"]) == 0]
print("P1 preemption-evidence guard:", "PASS" if not bad else f"FAIL {len(bad)} rows")
# Descriptive census (headline stat, not a guard): failures split by
# whether the rounds temporally overlapped — the no-overlap band is the
# durable-poison result.
no_ov = sum(1 for r in duel if r["earth_success"] == "0"
            and r["outcome"] != "censored" and r["rounds_overlapped"] == "0")
with_ov = sum(1 for r in duel if r["earth_success"] == "0"
              and r["outcome"] != "censored" and r["rounds_overlapped"] == "1")
print(f"earth failures: {with_ov} with temporal overlap, {no_ov} from standing promises alone")
# P3: earth_high failure band must not exceed leo_high's (retries=1, k=5).
def band(pol):
    return {float(r["offset"]) for r in duel
            if r["polarity"] == pol and r["k"] == "5"
            and r["earth_max_rounds"] == "1" and r["earth_success"] == "0"}
lh, eh = band("leo_high"), band("earth_high")
print("P3 band subset:", "PASS" if eh <= lh else
      f"FAIL earth_high has {sorted(eh - lh)[:5]} extra")
# P5: baselines all commit.
base = [r for r in rows if r["leo_enabled"] == "0"]
print("P5 baselines:", "PASS" if all(r["earth_success"] == "1" for r in base)
      else "FAIL")
# Livelock and censoring census (report, not assert):
from collections import Counter
print("outcomes:", Counter(r["outcome"] for r in duel))
EOF
```

Expected: three PASS lines and an outcome census. **Any FAIL: stop, treat as harness bug (D2), debug with `superpowers:systematic-debugging`, and do not proceed to Step 4 until the deviation is explained in writing (either a fixed bug or a documented, exonerated surprise).**

- [ ] **Step 4: Write `docs/superpowers/notes/2026-07-22-duel-results.md`**

Contents (structure fixed; numbers from the actual CSVs):
1. Header block repeating the three prose bans verbatim (premortem D6) plus the one-line role sentence: "The lemma establishes reachability of the hazard; this experiment measures its cost as a function of relative schedule offset."
2. Per-prediction verdict table (P1–P6: predicted / observed / verdict). P1 MUST be
   recorded as FALSIFIED AS WRITTEN: its temporal-overlap premise was wrong physics.
   Report the actual mechanism with the audit evidence — standing promises persist
   after the spoiler quiesces (k=5, offset −12: LEO's last round ends t≈16.6, Earth
   starts t=20.0, and is still denied by ballot 8502 held at the 3 LEO-reachable
   Earth acceptors); the no-overlap failure band ends at offset −8.65 ≈ LEO's
   8-round attempt span, identically in ALL FOUR k=5 conditions (polarity-independent,
   as durable-state physics requires). The k=3 negative-offset `leo_commit` rows are
   legitimate failover race wins (LEO commits before Earth starts), not disruption.
   Frame the durable-poison result in §B2's language: the hazard's denial window is
   not the spoiler's activity window; it persists until superseded by a higher ballot.
3. The offset→outcome map summarized per condition: band boundaries (with grid resolution ±0.05 s stated), cost magnitude inside the band (latency delta, retry counts), livelock/censored counts.
4. Deviations section — every deviation with its exoneration or bug-fix commit hash.
5. Explicit statement of which rows are cadence-quantized bounds (per the standing directive, if any recovery-style quantity appears).

- [ ] **Step 5: Append reproduction commands to `docs/step9-repro.md`**

```markdown
## Dueling-proposer experiment (duel.py)

# Deterministic offset->outcome map (primary)
uv run python experiments/duel_sweep.py --mode map --output results/duel/duel_map.csv

# Jitter robustness sweep (secondary; per-link RNG, Wilson CIs)
uv run python experiments/duel_sweep.py --mode jitter \
  --output results/duel/duel_jitter.csv \
  --aggregate-output results/duel/duel_jitter_ci.csv \
  --seeds "40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59"
```

- [ ] **Step 6: Commit (data + note + repro docs together, house style)**

```bash
git add results/duel/ docs/superpowers/notes/2026-07-22-duel-results.md docs/step9-repro.md
git commit -m "data: dueling-proposer offset map + jitter sweep, verified against pre-registration"
```

---

### Task 8: Reading plot (working figure, not yet paper-final)

Purpose: let humans read the map during review; the paper figure/table decision (premortem D4) comes after Tony sees this.

**Files:**
- Create: `experiments/plot_duel.py`
- Output: `results/duel/plots/duel_map.svg`

**Interfaces:**
- Consumes: `results/duel/duel_map.csv` columns from Task 5's `FIELDNAMES`.

- [ ] **Step 1: Implement `experiments/plot_duel.py`**

```python
"""Working plot for the duel map: outcome and cost vs offset, per condition.

Reading aid for review — NOT the paper figure. Follows the house style of
experiments/plot_step9.py (matplotlib, SVG out).
"""
from __future__ import annotations

import argparse
import csv
from collections import defaultdict
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

OUTCOME_Y = {"earth_commit": 3, "leo_commit": 2, "leo_blocked": 1,
             "no_decision": 1, "livelock": 0.5, "censored": 0}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", type=Path,
                    default=Path("results/duel/duel_map.csv"))
    ap.add_argument("--output", type=Path,
                    default=Path("results/duel/plots/duel_map.svg"))
    ap.add_argument("--zoom", type=float, nargs=2, default=(-12.0, 6.0),
                    help="offset window to display")
    args = ap.parse_args()

    rows = [r for r in csv.DictReader(args.input.open())
            if r["leo_enabled"] == "1"]
    conds = sorted({(r["polarity"], r["k"], r["earth_max_rounds"])
                    for r in rows})
    fig, axes = plt.subplots(len(conds), 1, figsize=(9, 2.2 * len(conds)),
                             sharex=True)
    if len(conds) == 1:
        axes = [axes]
    lo, hi = args.zoom
    for ax, cond in zip(axes, conds):
        pol, k, retries = cond
        sel = sorted((float(r["offset"]), r) for r in rows
                     if (r["polarity"], r["k"], r["earth_max_rounds"]) == cond
                     and lo <= float(r["offset"]) <= hi)
        xs = [o for o, _ in sel]
        ys = [OUTCOME_Y.get(r["outcome"], 0) for _, r in sel]
        ax.step(xs, ys, where="mid", lw=0.8)
        lat_x = [o for o, r in sel if r["earth_commit_latency_s"]]
        lat_y = [float(r["earth_commit_latency_s"]) for _, r in sel
                 if r["earth_commit_latency_s"]]
        ax2 = ax.twinx()
        ax2.plot(lat_x, lat_y, ".", ms=2, alpha=0.5)
        ax2.set_ylabel("commit s", fontsize=7)
        ax.set_yticks(list(set(OUTCOME_Y.values())))
        ax.set_ylim(-0.3, 3.3)
        ax.set_ylabel(f"{pol}\nk={k} r={retries}", fontsize=7)
    axes[-1].set_xlabel("offset = leo_start - earth_start (s)")
    fig.suptitle("Duel map: outcome (step) and Earth commit latency (dots)")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    fig.tight_layout()
    fig.savefig(args.output)
    print(f"Wrote {args.output}")


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Generate and eyeball**

Run: `uv run python experiments/plot_duel.py`
Expected: `Wrote results/duel/plots/duel_map.svg`; open it and confirm the collision band is visible in the retries=1 rows and absent in baselines.

- [ ] **Step 3: Commit**

```bash
git add experiments/plot_duel.py results/duel/plots/
git commit -m "feat: working plot for the duel offset map"
```

---

## Self-Review

1. **Spec coverage.** Spec §experiment: same slots ✓ (Task 4, single shared slot, `earth_slot == leo_slot` by construction); explicit offset sweep across one reconciliation period ✓ (coarse tail to 118 s; fine band per A9); seeds/jitter secondary ✓ (jitter mode separated); reports Earth success/latency/retries/NACKs and LEO completed-P1/failed-P2 grouped by offset ✓ (`trial_row`); explicit scheduling/ballots ✓ (PriorityProposer, fixed start times); collision/livelock reported with escape offsets ✓ (outcome taxonomy + full-curve reporting); no FLP/Multi-Paxos/backoff claims ✓ (bans repeated in module docstring, sweep docstring, pre-registration, results-note template). Success criteria 9 (time budget) ✓ Task 3 gate; 10 (transition/censoring) ✓ censored outcome class.
2. **Pre-mortem §A coverage.** A1 ✓ T3; A2 ✓ T3 (+P3 check T7); A3 ✓ T1; A4 ✓ T5 (map presentation, Wilson, degenerate flags); A5 ✓ T4 (leo_enabled=False rationale documented); A6 ✓ T5 conditions + every row carries the budget; A7 ✓ T3 gates + T4 same-slot; A8 ✓ T2+T4 (certificate, taxonomy, per-phase NACKs, late NACKs, carry-over attribution via certificate ballot); A9 ✓ T5 grid + rounds_overlapped in every row; A10 ✓ nonzero offsets asserted, both polarities as the same-timestamp audit. §D: D2 ✓ T6 gates T7; D6 ✓ bans embedded. B6 (k=3 extension) ✓ in the condition matrix.
3. **Placeholder scan.** One flagged in-code: the vestigial `FIELDNAMES ... if False else` in Task 5 carries an explicit deletion instruction. Task 7 Step 4's results note intentionally takes numbers from generated CSVs — structure is fully specified.
4. **Type consistency.** `run_duel_trial` keyword signature matches all test call sites; `trial_row` columns match `FIELDNAMES` exactly (26 entries both); `DuelTrialResult.earth_late_nacks` used in tests per the noted correction; `wilson_ci` return shape matches both call sites.

## Execution

Plan complete and saved to `docs/superpowers/plans/2026-07-22-dueling-proposer-experiment.md`. Tasks 1→2→3→4→5 are strictly ordered; 6 gates 7; 8 is last.
