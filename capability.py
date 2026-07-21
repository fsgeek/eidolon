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
from enum import Enum

from quorums import CrumblingWallQuorum


class EvidenceChannel(Enum):
    """Kinds of evidence a conclusion can rest on (stratified legibility).

    RUNTIME denotes FRESH runtime evidence — e.g. that an incumbent's
    ballot is still un-preempted now, not merely that it once was.
    """

    CONFIGURATION = "configuration"
    CONNECTIVITY = "connectivity"
    RUNTIME = "runtime"
    POLICY = "policy"


class Hazard(Enum):
    """Hazardous-action labels from the R1 x R2 matrix.

    DISRUPTIVE_ELECTION marks where the disruptive action is
    structurally REACHABLE: actually disrupting an incumbent further
    requires completing a higher-ballot Phase 1 whose slot/epoch scope
    covers the incumbent's — runtime facts the classifier cannot see.
    INCUMBENT_ONLY marks that progress continues only while some
    incumbent's authority remains valid — likewise runtime state.
    """

    DISRUPTIVE_ELECTION = "disruptive-election"
    INCUMBENT_ONLY = "incumbent-only"


#: Evidence channels each conclusion JOINTLY requires. Structural
#: conclusions need the quorum configuration AND a connectivity
#: summary. Two boundary markers extend beyond computed report fields,
#: as the spec requires provenance to mark where structural legibility
#: ends: operational progress additionally needs fresh runtime
#: authority evidence, and the client-visible contract is a service
#: policy declaration, never a classifier output.
_STRUCTURAL = frozenset({EvidenceChannel.CONFIGURATION,
                         EvidenceChannel.CONNECTIVITY})
PROVENANCE: dict[str, frozenset[EvidenceChannel]] = {
    "quorum_families": frozenset({EvidenceChannel.CONFIGURATION}),
    "r1": _STRUCTURAL,
    "r2": _STRUCTURAL,
    "r1_witness": _STRUCTURAL,
    "r2_witness": _STRUCTURAL,
    "missing": _STRUCTURAL,
    "hazards": _STRUCTURAL,
    "requires_preexisting_authority": _STRUCTURAL,
    "operational_progress": _STRUCTURAL | {EvidenceChannel.RUNTIME},
    "service_contract": frozenset({EvidenceChannel.POLICY}),
}


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
    r1_witness: frozenset[int] | None
    r2_witness: frozenset[int] | None
    hazards: tuple[Hazard, ...]
    requires_preexisting_authority: bool

    @property
    def missing(self) -> tuple[TierObligation, ...]:
        """The unsatisfied obligations, as typed records."""
        unmet = [o for o in self.r1_obligations if not o.satisfied]
        if not self.r2_obligation.satisfied:
            unmet.append(self.r2_obligation)
        return tuple(unmet)

    @property
    def can_acquire_or_recover_authority(self) -> bool:
        """Structural: a Phase 1 quorum is reachable (spec Layer 3)."""
        return self.r1

    @property
    def can_exercise_existing_authority(self) -> bool:
        """Structural precondition ONLY: a Phase 2 quorum is reachable.
        Actually exercising it also requires a valid incumbent
        authority, which is runtime evidence the classifier never sees.
        """
        return self.r2

    @property
    def provenance(self) -> dict[str, frozenset[EvidenceChannel]]:
        return dict(PROVENANCE)


def _minimal_witness(obligations: list[TierObligation]) -> frozenset[int]:
    """Deterministic minimal quorum: lowest node IDs satisfying each row."""
    picked: set[int] = set()
    for o in obligations:
        picked.update(sorted(o.witnesses)[:o.required])
    return frozenset(picked)


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

    r1_witness = _minimal_witness(obligations) if r1 else None
    if r1_witness is not None:
        assert wall.is_phase1_quorum(set(r1_witness), initiator_tier)
    r2_witness = (frozenset(sorted(r2_obligation.witnesses)[:wall.phase2_threshold])
                  if r2 else None)
    if r2_witness is not None:
        assert wall.is_phase2_quorum(set(r2_witness))

    hazards = []
    if r1 and not r2:
        hazards.append(Hazard.DISRUPTIVE_ELECTION)
    if r2 and not r1:
        hazards.append(Hazard.INCUMBENT_ONLY)
    requires_preexisting_authority = r2 and not r1

    return CapabilityReport(
        initiator_tier=initiator_tier,
        r1=r1,
        r2=r2,
        r1_obligations=tuple(obligations),
        r2_obligation=r2_obligation,
        r1_witness=r1_witness,
        r2_witness=r2_witness,
        hazards=tuple(hazards),
        requires_preexisting_authority=requires_preexisting_authority,
    )


def format_missing(report: CapabilityReport) -> tuple[str, ...]:
    """Human-readable rendering of missing obligations (CLI boundary)."""
    return tuple(
        f"Phase {o.phase} obligation at tier {o.tier_index}: require "
        f"{o.required}, reachable {len(o.witnesses)}; "
        f"unreachable candidates {sorted(o.unreachable)}"
        for o in report.missing)
