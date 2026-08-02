"""Mid-round capability flip: does a (1,0) incumbent cost anything?

Registered in advance:
  docs/superpowers/notes/2026-07-29-midround-flip-preregistration.md

The registered harm is NOT that a (1,0) proposer raises the ballot
high-water mark — paxos.py does that on any prepare, quorum or not.  The
registered harm is that a (1,0) proposer is SUCCESS-SIGNALED: it completes
Phase 1, believes it holds authority, stands its failover logic down, and
(re-preparing on commit timeout) becomes a contender that can win rounds
but never end the game, because only committing ends it.

Design: two proposers over one shared wall.

  incumbent  Moon-tier (code tier 1), at "moon".  Phase 1 needs one node
             from Moon, LEO and Earth; Phase 2 needs k-of-Earth.
  healthy    Earth-tier (code tier 3), at "na-west".  (1,1) throughout.

Arms differ ONLY in which link is cut and when.  Same incumbent, same
location, same latency profile, same start times.

  TREATMENT  cut moon<->africa mid-round  ->  incumbent (1,1) -> (1,0)
  C1         no incumbent                     baseline
  C2         cut moon<->leo-sat at t=0    ->  incumbent (0,1): Phase 1
             fails for want of a LEO witness, but its prepares still reach
             Earth, so the high-water mark still rises.  This holds ballot
             disruption constant and varies only the belief-and-retry
             state -- the registered purpose of C2.
  C3         no cut                       ->  incumbent (1,1) throughout
  P4         cut moon<->leo-sat MID-ROUND ->  incumbent (1,1) -> (0,1).
             Tests P4's intent: does the TIMING of the Phase-1-failing state
             matter, or only the state?  (P4 as registered assumed the
             treatment cut could yield (0,x); it cannot -- it removes one
             Earth node and Phase 1 needs only one.)

Why the incumbent is Moon-tier and not Earth-tier: an Earth-tier proposer
at an Earth location has a colocated Earth acceptor, and its Phase 1 needs
only one Earth node, so no partition can fail its Phase 1 while still
letting it poison Earth.  C2 is unrealizable for an Earth-tier incumbent.
Verified: tests/test_flip_arms.py.
"""
from dataclasses import dataclass

import simpy

from capability import classify
from demo_step_9 import build_topology
from duel import (LOW_RANK, HIGH_RANK, PriorityProposer,
                  decision_certificate, required_d_max, scale_jitter)
from entity import EntityRegistry
from paxos import Acceptor
from quorums import CrumblingWallQuorum
from time_budget import phase_time

EARTH_LOCS = ["na-west", "europe", "asia", "sa-east", "africa"]

#: The Earth location severed from the incumbent in the TREATMENT arm.
TREATMENT_CUT = ("moon", "africa")
#: The link severed in C2, chosen so Phase 1 fails while Earth is still
#: reachable (and therefore still poisonable).
C2_CUT = ("moon", "leo-sat")

ARMS = ("treatment", "c1_absent", "c2_phase1_fails", "c3_healthy_incumbent",
        "p4_late_phase1_fail")


@dataclass
class FlipSystem:
    env: simpy.Environment
    network: object
    registry: EntityRegistry
    wall: CrumblingWallQuorum
    acceptors: list
    incumbent: PriorityProposer
    healthy: PriorityProposer
    all_ids: list[int]
    earth_ids: list[int]
    #: incumbent's worst required one-way latency (sets the poison delay)
    d_incumbent: float
    #: shared per-phase timeout actually used
    timeout: float


@dataclass
class FlipTrialResult:
    arm: str
    seed: int
    k: int
    incumbent_max_rounds: int
    flip_time: float | None
    flip_applied: bool
    #: capability state of the incumbent before / after the flip, as
    #: certified by classify() against ACTUAL deliverability
    cap_before: tuple[bool, bool] | None
    cap_after: tuple[bool, bool] | None
    #: AXIS 1 — did the healthy proposer's ATTEMPT succeed (it completed a
    #: round and a value was chosen).  W1: this is NOT the same as its own
    #: value winning; Paxos value adoption means a successful healthy
    #: attempt can carry the INCUMBENT's value.
    healthy_committed: bool
    #: AXIS 2 — whose VALUE was decided: "healthy" | "incumbent" | None.
    #: Every table and figure must report both axes separately.
    decided_by: str | None
    #: seconds from the healthy proposer's start to its first commit;
    #: None if it never committed within the horizon
    healthy_ttfc: float | None
    healthy_rounds: int
    incumbent_p1_quorums: int
    incumbent_committed: bool
    decided_value: str | None
    nacks_seen_by_healthy: int


ACCEPTOR_PROCESS_TIME_MAX = 0.001


def wire_flip(env, *, k: int, seed: int, jitter_scale: float = 0.0,
              incumbent_max_rounds: int = 8, healthy_max_rounds: int = 8,
              timeout: float | None = None,
              incumbent_high: bool = True) -> FlipSystem:
    """Wire the flip topology with the duel's startup gates re-armed.

    timeout=None computes a shared per-phase budget from the incumbent's
    worst required one-way latency.  A hardcoded budget is how this
    experiment manufactures a false null: the Moon-tier incumbent needs
    ~2.56s per phase, so any timeout below that means it NEVER completes
    Phase 1, every arm looks identical, and the null is an artifact of
    configuration rather than a finding (premortem A7).
    """
    assert k in (5, 4, 3), k

    registry = EntityRegistry()
    network = build_topology(env, mars_base_latency_s=186.0, seed=seed)
    scale_jitter(network, jitter_scale)
    if jitter_scale > 0:
        network.enable_per_link_rng()

    earth_entities = []
    for loc in EARTH_LOCS:
        e = registry.create(name=f"earth-{loc}")
        network.assign_entity(e.id, loc)
        earth_entities.append(e)
    moon = registry.create(name="moon")
    network.assign_entity(moon.id, "moon")
    leo = registry.create(name="leo")
    network.assign_entity(leo.id, "leo-sat")
    mars_entities = []
    for i in range(3):
        e = registry.create(name=f"mars-{i}")
        network.assign_entity(e.id, f"mars-{i}")
        mars_entities.append(e)

    acceptors = []
    for e in earth_entities + [moon, leo] + mars_entities:
        pt = 0.0005 if ("earth" in e.name or "leo" in e.name) else 0.001
        acceptors.append(Acceptor(env, e, network, process_time=pt))

    earth_ids = [e.id for e in earth_entities]
    mars_ids = [e.id for e in mars_entities]
    all_ids = earth_ids + [leo.id, moon.id] + mars_ids

    # ONE wall for both proposers (premortem A1).
    wall = CrumblingWallQuorum(
        [mars_ids, [moon.id], [leo.id], earth_ids], phase2_threshold=k)

    inc_rank, well_rank = ((HIGH_RANK, LOW_RANK) if incumbent_high
                           else (LOW_RANK, HIGH_RANK))

    # Time-budget gate. The incumbent's Phase 1 spans Moon + LEO + Earth;
    # its worst one-way leg sets the shared per-phase budget. Both
    # proposers share one timeout so their clocks stay commensurate.
    moon_leo_ids = {i for i in all_ids
                    if network._entity_location[i] in ("moon", "leo-sat")}
    inc_required = moon_leo_ids | set(earth_ids)
    d_inc = required_d_max(network, moon.id, inc_required)
    required_budget = phase_time(d_inc, ACCEPTOR_PROCESS_TIME_MAX)
    if timeout is None:
        timeout = required_budget * 1.25
    assert timeout > required_budget, (
        f"per-phase timeout {timeout:.4f}s must exceed the incumbent's "
        f"worst request-response path {required_budget:.4f}s, or the "
        f"incumbent never completes Phase 1 and every arm is identical")

    inc_entity = registry.create(name="incumbent-proposer")
    network.assign_entity(inc_entity.id, "moon")
    incumbent = PriorityProposer(
        env, inc_entity, network, all_ids, wall, timeout=timeout,
        max_rounds=incumbent_max_rounds, initiator_tier=1,
        ballot_rank=inc_rank)

    well_entity = registry.create(name="healthy-proposer")
    network.assign_entity(well_entity.id, "na-west")
    healthy = PriorityProposer(
        env, well_entity, network, all_ids, wall, timeout=timeout,
        max_rounds=healthy_max_rounds, initiator_tier=3,
        ballot_rank=well_rank)

    assert incumbent.quorum is healthy.quorum, "proposers must share the wall"
    assert incumbent.quorum.phase2_threshold == k
    assert incumbent.ballot_rank != healthy.ballot_rank
    assert max(e.id for e in registry._entities.values()) < 1000

    return FlipSystem(env=env, network=network, registry=registry, wall=wall,
                      acceptors=acceptors, incumbent=incumbent,
                      healthy=healthy, all_ids=all_ids, earth_ids=earth_ids,
                      d_incumbent=d_inc, timeout=timeout)


def capability_of(sys_: FlipSystem, prop: PriorityProposer):
    """(r1, r2) for prop, derived from ACTUAL deliverability."""
    reach = {a for a in sys_.all_ids
             if sys_.network.is_reachable(prop.entity.id, a)}
    rep = classify(sys_.wall, prop.initiator_tier, reach)
    return (rep.r1, rep.r2)


def run_flip_trial(*, arm: str, seed: int, k: int = 5,
                   incumbent_max_rounds: int = 8,
                   healthy_max_rounds: int = 8,
                   jitter_scale: float = 0.0,
                   incumbent_start: float = 5.0,
                   healthy_offset: float | None = None,
                   flip_delay: float = 0.5,
                   tail: float = 120.0,
                   incumbent_high: bool = True) -> FlipTrialResult:
    """One trial.

    flip_delay is measured from incumbent_start.  Deviation D1 (recorded in
    the pre-registration): the flip lands BEFORE the incumbent's Phase 2
    fan-out, not during the Phase 2 return leg.  A return-leg flip lets the
    Accept reach the severed acceptor and only loses its response, which is
    an acknowledgment gap, not a capability state -- the very phenomenon the
    deployed-systems census disqualified.  Pre-fan-out is the only timing
    that makes Q2 genuinely unformable.

    healthy_offset=None starts the healthy proposer just after the
    incumbent's prepares have reached Earth (d_incumbent + 0.05s).  Starting
    it earlier is the second way to manufacture a false null: the Earth-tier
    proposer completes a whole round in 0.181s, so if it starts before the
    incumbent's ballot is standing at Earth it commits unopposed and the arms
    are indistinguishable for reasons that have nothing to do with (1,0).
    """
    assert arm in ARMS, arm
    env = simpy.Environment()
    sys_ = wire_flip(env, k=k, seed=seed, jitter_scale=jitter_scale,
                     incumbent_max_rounds=incumbent_max_rounds,
                     healthy_max_rounds=healthy_max_rounds,
                     incumbent_high=incumbent_high)
    net = sys_.network
    slot = 1
    if healthy_offset is None:
        healthy_offset = sys_.d_incumbent + 0.05
    healthy_start = incumbent_start + healthy_offset
    assert 0.0 < flip_delay < sys_.timeout, (
        "flip must land after the incumbent's prepares are sent and before "
        "its Phase 2 fan-out")

    if arm == "c2_phase1_fails":
        net.partition_locations(*C2_CUT)

    cap_before = (None if arm == "c1_absent"
                  else capability_of(sys_, sys_.incumbent))
    state = {"flip_applied": False, "flip_time": None, "cap_after": None}

    def flipper():
        yield env.timeout(incumbent_start + flip_delay)
        net.partition_locations(
            *(C2_CUT if arm == "p4_late_phase1_fail" else TREATMENT_CUT))
        state["flip_applied"] = True
        state["flip_time"] = env.now
        state["cap_after"] = capability_of(sys_, sys_.incumbent)

    results = {}

    def drive(name, prop, start, value):
        yield env.timeout(start)
        results[name] = yield prop.propose(slot=slot, value=value)

    if arm != "c1_absent":
        env.process(drive("incumbent", sys_.incumbent, incumbent_start,
                          f"incumbent-{slot}"))
    env.process(drive("healthy", sys_.healthy, healthy_start,
                      f"healthy-{slot}"))
    if arm in ("treatment", "p4_late_phase1_fail"):
        env.process(flipper())

    env.run(until=max(incumbent_start, healthy_start) + tail)

    if arm == "c2_phase1_fails":
        state["cap_after"] = cap_before

    hr = results.get("healthy")
    ir = results.get("incumbent")

    ttfc = None
    if hr is not None and hr.success:
        # last round's p2_end is the commit instant
        ends = [e.get("p2_end") for e in hr.round_log
                if e.get("p2_end") is not None]
        if ends:
            ttfc = max(ends) - healthy_start

    inc_p1 = 0
    if ir is not None:
        inc_p1 = sum(1 for e in ir.round_log if e.get("p1_quorum"))

    nacks = 0
    if hr is not None:
        nacks = sum(e.get("p1_nacks", 0) for e in hr.round_log)

    # Two-axis outcome (W1 / rikuy condition (a)): the decided value comes
    # from the quorum certificate, never from a proposer's own claim.
    cert = decision_certificate(sys_.acceptors, sys_.wall, slot)
    decided = cert[1] if cert else None
    decided_by = decided.split("-")[0] if decided else None

    if hr is not None and hr.success:
        assert cert is not None, (
            "healthy proposer claims success without a certificate")
    if ir is not None and ir.success:
        assert cert is not None, (
            "incumbent claims success without a certificate")

    return FlipTrialResult(
        arm=arm, seed=seed, k=k,
        incumbent_max_rounds=incumbent_max_rounds,
        flip_time=state["flip_time"], flip_applied=state["flip_applied"],
        cap_before=cap_before, cap_after=state["cap_after"],
        healthy_committed=bool(hr is not None and hr.success),
        decided_by=decided_by,
        healthy_ttfc=ttfc,
        healthy_rounds=len(hr.round_log) if hr is not None else 0,
        incumbent_p1_quorums=inc_p1,
        incumbent_committed=bool(ir is not None and ir.success),
        decided_value=decided,
        nacks_seen_by_healthy=nacks,
    )
