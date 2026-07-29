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

    # Capability gate: derive reachability from ACTUAL deliverability —
    # links AND current partition state — then demand the classifier
    # certify the regime this trial claims to exercise.
    #
    # This used to read get_link, which is partition-blind, so the gate
    # kept certifying (1,1) after any partition was applied.  Harmless
    # for the original duel campaign, which partitioned nothing; fatal
    # for any trial that flips connectivity mid-round.
    leo_reach = {a for a in all_ids
                 if network.is_reachable(leo_prop_entity.id, a)}
    leo_report = classify(wall, 2, leo_reach)
    if k in (5, 4):
        assert leo_report.r1 and not leo_report.r2, (
            f"k={k} must put sparse LEO in (1,0); got "
            f"({leo_report.r1},{leo_report.r2})")
        assert Hazard.DISRUPTIVE_ELECTION in leo_report.hazards
    else:  # k == 3: relaxation converts the spoiler into a failover peer
        assert leo_report.r1 and leo_report.r2 and not leo_report.hazards
    earth_reach = {a for a in all_ids
                   if network.is_reachable(earth_prop_entity.id, a)}
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
