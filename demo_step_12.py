"""Step 12: Recovery semantics under conflicting independent decisions.

The hard problem: during blackout, Earth and Mars independently make
decisions about shared resources via their local Paxos instances.
When connectivity returns, these decisions may conflict.

This experiment measures:
1. How many conflicts arise during recovery?
2. How long is the user-visible inconsistency window?
3. What resolution policy works, and what does it cost?

Three conflict resolution strategies:
- HIERARCHY: Earth always wins (tier priority)
- LWW: Last-writer-wins by timestamp (clock-dependent)
- DETECT: Detect conflicts, flag for manual resolution (safest)

The key insight from Tony's observation: recovery is what matters.
A system that "works" during blackout but produces unresolvable
conflicts during recovery is worse than one that stops.
"""

from __future__ import annotations

import argparse
import random
from dataclasses import dataclass, field
from enum import Enum, auto

import simpy

from datacenter import five_dc_topology
from entity import EntityRegistry
from paxos import Acceptor, FlexibleQuorum, MajorityQuorum, Proposer
from demo_step_10 import build_topology


class ResolutionPolicy(Enum):
    HIERARCHY = auto()  # Earth always wins
    LWW = auto()        # Last-writer-wins by timestamp
    DETECT = auto()     # Flag conflicts for manual resolution


@dataclass
class ResourceClaim:
    """A claim on a shared resource."""
    resource_id: str
    claimant_tier: str
    value: str
    timestamp: float
    slot: int  # Paxos slot that decided this
    decided_locally: bool = True  # Was this decided during disconnection?


@dataclass
class Conflict:
    """A detected conflict between tiers."""
    resource_id: str
    earth_claim: ResourceClaim
    mars_claim: ResourceClaim
    resolution: str  # "earth", "mars", or "unresolved"
    resolution_time: float
    staleness_at_detection: float  # How stale was the losing claim?


@dataclass
class RecoveryMetrics:
    """What actually happens during recovery."""
    # Claims
    earth_claims_total: int = 0
    mars_claims_total: int = 0
    earth_claims_during_blackout: int = 0
    mars_claims_during_blackout: int = 0
    # Conflicts
    conflicts: list[Conflict] = field(default_factory=list)
    conflicts_during_blackout: int = 0  # Conflicting claims made during blackout
    # Resolution
    resolved_earth_wins: int = 0
    resolved_mars_wins: int = 0
    unresolved: int = 0
    # Timing
    first_conflict_detected_s: float | None = None
    last_conflict_resolved_s: float | None = None
    inconsistency_window_s: float | None = None
    # User-visible
    claims_overwritten_during_recovery: int = 0
    # Resource state
    final_resource_states: dict[str, ResourceClaim] = field(default_factory=dict)


@dataclass(frozen=True)
class Step12Config:
    mars_base_latency_s: float = 186.0
    blackout_start_s: float = 600.0
    blackout_duration_s: float = 900.0
    sim_end_s: float = 4000.0
    num_shared_resources: int = 20  # Resources both tiers can claim
    claim_interval_s: float = 30.0  # How often each tier claims a resource
    merge_interval_s: float = 60.0  # How often tiers sync state
    seed: int = 42
    policy: ResolutionPolicy = ResolutionPolicy.HIERARCHY


def run_recovery_experiment(
    with_repeater: bool,
    cfg: Step12Config,
    verbose: bool = True,
) -> RecoveryMetrics:
    env = simpy.Environment()
    rng = random.Random(cfg.seed)
    network = build_topology(env, cfg.mars_base_latency_s, seed=cfg.seed)
    registry = EntityRegistry()

    # Setup entities
    earth_locs = ["na-west", "europe", "asia", "sa-east", "africa"]
    earth_entities = []
    for loc in earth_locs:
        entity = registry.create(name=f"earth-{loc}")
        network.assign_entity(entity.id, loc)
        earth_entities.append(entity)

    moon_entity = registry.create(name="moon")
    network.assign_entity(moon_entity.id, "moon")
    leo_entity = registry.create(name="leo")
    network.assign_entity(leo_entity.id, "leo-sat")

    mars_entities = []
    for i in range(3):
        entity = registry.create(name=f"mars-{i}")
        network.assign_entity(entity.id, f"mars-{i}")
        mars_entities.append(entity)

    earth_ids = [e.id for e in earth_entities]
    mars_ids = [e.id for e in mars_entities]

    for entity in earth_entities + [moon_entity, leo_entity] + mars_entities:
        pt = 0.0005 if "earth" in entity.name or "leo" in entity.name else 0.001
        Acceptor(env, entity, network, process_time=pt)

    ep = registry.create(name="earth-proposer")
    network.assign_entity(ep.id, "na-west")
    earth_prop = Proposer(env, ep, network, earth_ids,
        FlexibleQuorum(earth_ids, phase1_size=4, phase2_size=2), timeout=1.0)

    mp = registry.create(name="mars-proposer")
    network.assign_entity(mp.id, "mars-0")
    mars_prop = Proposer(env, mp, network, mars_ids,
        MajorityQuorum(mars_ids), timeout=1.0)

    # Shared resource state
    resource_ids = [f"resource-{i}" for i in range(cfg.num_shared_resources)]
    earth_state: dict[str, ResourceClaim] = {}
    mars_state: dict[str, ResourceClaim] = {}

    metrics = RecoveryMetrics()
    blackout_end = cfg.blackout_start_s + cfg.blackout_duration_s
    blackout_active = [False]

    def earth_claims():
        slot = 0
        while env.now < cfg.sim_end_s:
            # Pick a random resource to claim
            rid = rng.choice(resource_ids)
            value = f"earth-{rid}-{slot}"
            result = yield earth_prop.propose(slot=slot, value=value)
            if result.success:
                claim = ResourceClaim(
                    resource_id=rid,
                    claimant_tier="earth",
                    value=value,
                    timestamp=env.now,
                    slot=slot,
                    decided_locally=blackout_active[0],
                )
                earth_state[rid] = claim
                metrics.earth_claims_total += 1
                if blackout_active[0]:
                    metrics.earth_claims_during_blackout += 1
            slot += 1
            yield env.timeout(cfg.claim_interval_s)

    def mars_claims():
        slot = 10000
        while env.now < cfg.sim_end_s:
            rid = rng.choice(resource_ids)
            value = f"mars-{rid}-{slot}"
            result = yield mars_prop.propose(slot=slot, value=value)
            if result.success:
                claim = ResourceClaim(
                    resource_id=rid,
                    claimant_tier="mars",
                    value=value,
                    timestamp=env.now,
                    slot=slot,
                    decided_locally=blackout_active[0],
                )
                mars_state[rid] = claim
                metrics.mars_claims_total += 1
                if blackout_active[0]:
                    metrics.mars_claims_during_blackout += 1
            slot += 1
            yield env.timeout(cfg.claim_interval_s)

    def merge_states():
        """Periodically sync resource state between tiers."""
        while env.now < cfg.sim_end_s:
            yield env.timeout(cfg.merge_interval_s)

            if blackout_active[0] and not with_repeater:
                continue  # Can't sync during hard blackout

            # Merge: compare Earth and Mars state for each resource
            all_rids = set(earth_state.keys()) | set(mars_state.keys())
            for rid in all_rids:
                e_claim = earth_state.get(rid)
                m_claim = mars_state.get(rid)

                if e_claim is None and m_claim is not None:
                    # Mars has it, Earth doesn't — propagate
                    earth_state[rid] = m_claim
                elif m_claim is None and e_claim is not None:
                    # Earth has it, Mars doesn't — propagate
                    mars_state[rid] = e_claim
                elif e_claim is not None and m_claim is not None:
                    # Both have claims — check for conflict
                    if e_claim.value == m_claim.value:
                        continue  # Same claim, no conflict

                    # CONFLICT! Both tiers independently decided differently
                    if e_claim.decided_locally and m_claim.decided_locally:
                        metrics.conflicts_during_blackout += 1

                    # Resolve based on policy
                    if cfg.policy == ResolutionPolicy.HIERARCHY:
                        winner = "earth"
                        earth_state[rid] = e_claim
                        mars_state[rid] = e_claim
                        metrics.resolved_earth_wins += 1
                        metrics.claims_overwritten_during_recovery += 1
                    elif cfg.policy == ResolutionPolicy.LWW:
                        if e_claim.timestamp >= m_claim.timestamp:
                            winner = "earth"
                            mars_state[rid] = e_claim
                        else:
                            winner = "mars"
                            earth_state[rid] = m_claim
                        metrics.claims_overwritten_during_recovery += 1
                        if winner == "earth":
                            metrics.resolved_earth_wins += 1
                        else:
                            metrics.resolved_mars_wins += 1
                    else:  # DETECT
                        winner = "unresolved"
                        metrics.unresolved += 1

                    conflict = Conflict(
                        resource_id=rid,
                        earth_claim=e_claim,
                        mars_claim=m_claim,
                        resolution=winner,
                        resolution_time=env.now,
                        staleness_at_detection=abs(e_claim.timestamp - m_claim.timestamp),
                    )
                    metrics.conflicts.append(conflict)

                    if metrics.first_conflict_detected_s is None:
                        metrics.first_conflict_detected_s = env.now
                    metrics.last_conflict_resolved_s = env.now

    def conjunction_controller():
        mars_locs = [f"mars-{i}" for i in range(3)]
        earth_path_locs = ["na-west", "europe", "moon"]
        yield env.timeout(cfg.blackout_start_s)
        blackout_active[0] = True

        if with_repeater:
            for src in earth_path_locs:
                for dst in mars_locs:
                    network.update_link(src, dst, latency=240.0, jitter=12.0)
        else:
            for src in earth_path_locs:
                for dst in mars_locs:
                    network.partition_locations(src, dst)

        yield env.timeout(cfg.blackout_duration_s)
        blackout_active[0] = False

        if with_repeater:
            for src in earth_path_locs:
                for dst in mars_locs:
                    base = cfg.mars_base_latency_s + (1.28 if src == "moon" else 0.0)
                    network.update_link(src, dst, latency=base, jitter=5.0)
        else:
            network.heal_all()

    env.process(earth_claims())
    env.process(mars_claims())
    env.process(merge_states())
    env.process(conjunction_controller())
    env.run(until=cfg.sim_end_s)

    # Compute final state
    metrics.final_resource_states = dict(earth_state)

    # Inconsistency window
    if metrics.first_conflict_detected_s and metrics.last_conflict_resolved_s:
        metrics.inconsistency_window_s = (
            metrics.last_conflict_resolved_s - metrics.first_conflict_detected_s
        )

    if verbose:
        label = ("REPEATER" if with_repeater else "HARD BLACKOUT") + f" [{cfg.policy.name}]"
        print("=" * 74)
        print(f"  {label}")
        print("=" * 74)
        print()
        print(f"  Claims: Earth {metrics.earth_claims_total} "
              f"({metrics.earth_claims_during_blackout} during blackout)")
        print(f"  Claims: Mars  {metrics.mars_claims_total} "
              f"({metrics.mars_claims_during_blackout} during blackout)")
        print()
        print(f"  Total conflicts detected: {len(metrics.conflicts)}")
        print(f"    Caused by blackout decisions: {metrics.conflicts_during_blackout}")
        print(f"    Resolved Earth wins: {metrics.resolved_earth_wins}")
        print(f"    Resolved Mars wins:  {metrics.resolved_mars_wins}")
        print(f"    Unresolved:          {metrics.unresolved}")
        print(f"    Claims overwritten:  {metrics.claims_overwritten_during_recovery}")
        print()
        if metrics.first_conflict_detected_s:
            print(f"  First conflict at:     t={metrics.first_conflict_detected_s:.1f}s")
            print(f"  Last conflict at:      t={metrics.last_conflict_resolved_s:.1f}s")
            print(f"  Inconsistency window:  {metrics.inconsistency_window_s:.1f}s")
        else:
            print("  No conflicts detected.")
        print()

        # Show some example conflicts
        if metrics.conflicts:
            print("  Sample conflicts:")
            for c in metrics.conflicts[:5]:
                print(f"    {c.resource_id}: "
                      f"Earth({c.earth_claim.value[:30]}, t={c.earth_claim.timestamp:.0f}) vs "
                      f"Mars({c.mars_claim.value[:30]}, t={c.mars_claim.timestamp:.0f}) "
                      f"-> {c.resolution} "
                      f"(staleness={c.staleness_at_detection:.0f}s)")
            if len(metrics.conflicts) > 5:
                print(f"    ... and {len(metrics.conflicts) - 5} more")
        print()

    return metrics


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mars-latency-s", type=float, default=186.0)
    parser.add_argument("--blackout-duration-s", type=float, default=900.0)
    parser.add_argument("--num-resources", type=int, default=20)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--quiet", action="store_true")
    args = parser.parse_args()

    print("\n" + "=" * 74)
    print("STEP 12: RECOVERY SEMANTICS UNDER CONFLICTING DECISIONS")
    print("=" * 74 + "\n")

    for policy in ResolutionPolicy:
        for with_rep in [False, True]:
            cfg = Step12Config(
                mars_base_latency_s=args.mars_latency_s,
                blackout_duration_s=args.blackout_duration_s,
                num_shared_resources=args.num_resources,
                seed=args.seed,
                policy=policy,
            )
            run_recovery_experiment(with_rep, cfg, verbose=not args.quiet)

    if not args.quiet:
        print("=" * 74)
        print("THE RECOVERY QUESTION")
        print("=" * 74)
        print()
        print("  During blackout, both tiers independently claim shared resources.")
        print("  When connectivity returns, conflicting claims must be resolved.")
        print()
        print("  HIERARCHY (Earth wins): Deterministic, fast, but Mars work is lost.")
        print("  LWW (timestamp): Non-deterministic, clock-dependent, but fair.")
        print("  DETECT (flag): Safe, but requires human/application intervention.")
        print()
        print("  The fundamental tension: a system that 'works' during blackout")
        print("  but produces unresolvable conflicts during recovery may be worse")
        print("  than one that stops and waits for connectivity.")
        print()
        print("  Paxos says: stop and wait. MRDTs say: keep going and merge.")
        print("  The right answer depends on what the resources represent.")
        print()


if __name__ == "__main__":
    main()
