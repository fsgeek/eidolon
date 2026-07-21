"""Static capability classifier for the crumbling-wall construction.

Implements the structural half of the four-layer capability model for
`CrumblingWallQuorum` specifically (a generic quorum-family interface
is deliberately not attempted). Layers 1 and 2 of the model (quorum
obligation, effective reachability) are decidable from static
configuration plus a connectivity summary; that is what `classify`
computes. Layer 3 (protocol authority) is runtime state — the
classifier reports only whether structurally available progress
*depends* on it. Layer 4 (service contract) is a policy declaration,
never a classifier output.

The connectivity input is Reach(i, C): the set of acceptors the
initiating tier can exchange protocol traffic with inside the
experiment's liveness assumptions, including its own colocated
acceptors when they are up. The classifier does not decide *why* a
node is absent (scheduled disconnection, failed relay, short timeout);
the caller must report the cause.

Spec: docs/superpowers/specs/2026-07-19-four-layer-capability-model-design.md
"""

from __future__ import annotations

from dataclasses import dataclass

from quorums import CrumblingWallQuorum


@dataclass(frozen=True)
class TierObligation:
    """One obligation row: what a phase requires from one tier.

    phase is 1 or 2. required is 1 for intermediate Phase 1 rows,
    min_earth_in_q1 for the Phase 1 fast-tier row (the |E|-k+1
    hitting-set bound), and the phase2 threshold k for the Phase 2
    obligation. witnesses are the reachable candidates; unreachable
    are the candidates connectivity has removed.
    """

    phase: int
    tier_index: int
    required: int
    witnesses: frozenset[int]
    unreachable: frozenset[int]

    @property
    def satisfied(self) -> bool:
        return len(self.witnesses) >= self.required


@dataclass(frozen=True)
class CapabilityReport:
    """Structural capability envelope for one initiating tier."""

    initiator_tier: int
    r1: bool
    r2: bool
    r1_obligations: tuple[TierObligation, ...]
    r2_obligation: TierObligation


def classify(wall: CrumblingWallQuorum, initiator_tier: int,
             reachable: set[int]) -> CapabilityReport:
    """Compute R1(i, C) and R2(i, C) with per-obligation evidence."""
    if not 0 <= initiator_tier < wall.num_tiers:
        raise ValueError(
            f"initiator_tier must be in [0, {wall.num_tiers - 1}], "
            f"got {initiator_tier}")
    reachable = set(reachable)
    fast_index = wall.num_tiers - 1

    obligations = []
    for j in range(initiator_tier, wall.num_tiers):
        tier_set = set(wall.tiers[j])
        required = wall.min_earth_in_q1 if j == fast_index else 1
        obligations.append(TierObligation(
            phase=1,
            tier_index=j,
            required=required,
            witnesses=frozenset(reachable & tier_set),
            unreachable=frozenset(tier_set - reachable),
        ))
    r1 = all(o.satisfied for o in obligations)

    fast_set = set(wall.fast_tier)
    r2_obligation = TierObligation(
        phase=2,
        tier_index=fast_index,
        required=wall.phase2_threshold,
        witnesses=frozenset(reachable & fast_set),
        unreachable=frozenset(fast_set - reachable),
    )
    r2 = r2_obligation.satisfied

    return CapabilityReport(
        initiator_tier=initiator_tier,
        r1=r1,
        r2=r2,
        r1_obligations=tuple(obligations),
        r2_obligation=r2_obligation,
    )
