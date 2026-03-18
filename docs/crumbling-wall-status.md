# Crumbling Wall Implementation: Status and Next Steps

**Date**: 2026-03-16
**Status**: Core implementation complete, partially verified, paper needs rewrite

---

## What changed and why

The prior implementation flattened the crumbling wall into "Phase 1 requires all tiers" — which is just Flexible Paxos with a topology constraint. Reviewers correctly flagged this as "Howard already did this."

The real crumbling wall (Peleg & Wool 1995) has **per-tier Phase 1 quorum families**. A proposer reads DOWN through the wall:

| Initiating tier | Phase 1 needs | Phase 1 scope |
|---|---|---|
| Mars (top) | Mars + Moon + LEO + Earth | All 4 tiers |
| Moon | Moon + LEO + Earth | 3 tiers |
| LEO | LEO + Earth | 2 tiers |
| Earth (bottom) | Earth only | 1 tier |

Phase 2 is always the full Earth row (strict) or k-of-Earth (relaxed).

**Intersection guarantee**: Every tier's Q1 includes at least one Earth node (everyone reads down to Earth). Phase 2 is Earth-only. So every Q1 intersects every Q2.

**Liveness consequence**: During Mars blackout, only Mars-initiated Phase 1 is blocked. Moon, LEO, and Earth can still complete Phase 1 because they never needed Mars. The liveness failure is scoped to the unreachable tier, not the whole system.

**Key result**: Under hard blackout, the old implementation produced 0% during-blackout global success. The corrected implementation produces **100% during-blackout global success at 0.2s latency** — because the Earth-based global proposer only needs Earth for both Phase 1 and Phase 2.

---

## What was done

### Python (`quorums.py`, `paxos.py`)

- `CrumblingWallQuorum.is_phase1_quorum` now takes `initiator_tier` parameter
- Checks that respondents include at least one node from each tier at or below the initiator
- For relaxed Q2 (k-of-Earth), enforces minimum Earth nodes in Q1 via pigeonhole (e.g., 2 Earth nodes when Q2 is 4-of-5)
- `CrumblingWallQuorum.tier_of(node_id)` returns a node's tier index
- `QuorumSystem.is_phase1_quorum` base signature updated for compatibility
- `Proposer` accepts `initiator_tier` parameter, threads it through quorum checks

### Demo steps updated

- `demo_step_8.py`: global proposer gets `initiator_tier=3` (Earth)
- `demo_step_9.py`: global proposer gets `initiator_tier=3` (Earth)
- `demo_step_10.py`: global proposer gets `initiator_tier=3` (Earth)
- `experiments/step10_sweep.py`: global proposer gets `initiator_tier=3`

### TLA+ specs updated

- `ExhaustiveIntersection.tla`: Rewritten with per-tier Q1 families. Checks strict Q2, relaxed Q2, and crash-fault variants for all four tiers' Q1 families. **Passes** (all ASSUMEs satisfied).
- `QuorumIntersection.tla`: Rewritten with `initiator` variable ranging over 1..4. Explores all (construction × tier × Q1 × Q2) combinations. **11,789 states, passes.**

### TLA+ quorum counts (from ExhaustiveIntersection)

| Tier | Strict Q1 count | Relaxed Q1 count |
|---|---|---|
| Mars (top) | 217 | 182 |
| Moon | 248 | — |
| LEO | 496 | — |
| Earth (bottom) | 992 | 832 |
| Strict Q2 | 1 | — |
| Relaxed Q2 | — | 6 |

---

## What the next instance needs to do

### 1. Update remaining TLA+ specs

**CrashFault.tla**: Currently checks the old "all tiers required" construction. Needs to be rewritten with per-tier Q1 families and `initiator_tier`. Should verify that under one Earth crash with relaxed Q2:
- Each tier's Q1 quorums still exist (or document which don't)
- Intersection holds for all surviving Q1 × Q2 pairs

**PaxosSmall.tla**: Currently uses `IsQ1(S)` requiring all tiers + size >= 5. Needs per-tier `IsQ1(S, tier)` that checks tier-spanning from `tier` downward. The reduced topology is 3E+1L+1U+1M — structurally equivalent, so the mapping is:
- Mars Q1: spans all 4 tiers
- Moon Q1: spans Moon + LEO + Earth
- LEO Q1: spans LEO + Earth
- Earth Q1: Earth only (2-of-3 Earth for the reduced model)

The Agreement invariant should still hold because the intersection property is preserved (verified by ExhaustiveIntersection).

**PaxosRelaxed.tla / PaxosFull.tla**: These never completed at 10 nodes and won't now either. Update them for correctness but don't expect them to finish. The safety argument rests on ExhaustiveIntersection (complete) + Flexible Paxos theorem + PaxosSmall (belt-and-suspenders).

### 2. Ensure TLA+ and Python are consistent

The TLA+ specs and Python code define quorum membership independently. A disciplined check:
- For each tier i in {0,1,2,3}: enumerate all subsets S of the 10-node universe where `is_phase1_quorum(S, initiator_tier=i)` returns True in Python
- Compare against the TLA+ Q1 family for that tier
- They should match exactly

This could be a Python test that imports both the quorum logic and a reference implementation of the TLA+ definitions.

### 3. Rewrite the paper

The paper's framing, formal model, results, and conclusions all assume the flat "all tiers required" construction. With the crumbling wall:

**Abstract**: Lead with tier-scoped liveness, not the liveness envelope. The headline result is that Earth-initiated global consensus works through hard blackout — no relay needed.

**Formal Model (Section 2)**: The quorum family definitions change fundamentally. Instead of one Q1 family, there's a family per tier. The intersection proof is per-tier. The notation needs to reflect this.

**Related Work (Section 3)**: The positioning against Flexible Paxos changes. This is no longer "Flexible Paxos applied to space" — it's "crumbling walls mapped to physical topology, giving tier-scoped liveness." Peleg & Wool becomes much more central. Howard's work is the underlying safety theorem, not the construction.

**Results (Section 5)**: The sweep results will be completely different. The hard blackout vs repeater distinction largely disappears for Earth-initiated consensus. The interesting questions become:
- What can each tier do independently during blackout?
- How long does it take Mars to learn Earth's decisions after blackout?
- What happens when Mars wants to write? (Needs Multi-Paxos / leader)

**Discussion (Section 7)**: Topology-scoped consistency gets much richer. Instead of "Earth is consistent, Mars is stale," it's "each tier's consistency scope is determined by its position in the wall."

**New content needed**: The paper should discuss the Multi-Paxos / leader question. Single-decree Paxos with crumbling walls handles reads beautifully. Writes from slow tiers need a leader to amortize Phase 1 cost. The leader placement question (which tier?) determines the write latency characteristics. This is future work but should be discussed.

### 4. Re-run experiments

All sweep results (step9, step10) need to be regenerated with the corrected quorum implementation. The step 9 sweep will show dramatically different results:
- Hard blackout: 100% during-blackout global success (not 0%)
- The repeater becomes irrelevant for Earth-initiated consensus
- The interesting parameter is now timeout budget for Mars-initiated consensus

Step 10 (crash tolerance) results may also change — the interaction between crashed Earth nodes and the per-tier Q1 minimum Earth count is worth exploring.

### 5. Step 11/12 implications

**Step 11 (MRDT)**: The motivation changes. Under the old construction, MRDTs solved the "blackout kills global consensus" problem. Under crumbling walls, Earth-initiated global consensus works through blackout. MRDTs become relevant specifically for Mars-tier writes — where the crumbling wall's Phase 1 cost is prohibitive.

**Step 12 (conflict resolution)**: Still relevant. Even with crumbling walls, Mars can make local decisions that conflict with Earth's global decisions. Recovery semantics matter.

---

## Architecture summary (for the next instance)

```
Crumbling Wall (Peleg & Wool 1995)
  Tiers: Mars (top) → Moon → LEO → Earth (bottom)

  Phase 1 (prepare/promise): read DOWN the wall
    Mars proposer:  needs Mars + Moon + LEO + Earth
    Moon proposer:  needs Moon + LEO + Earth
    LEO proposer:   needs LEO + Earth
    Earth proposer: needs Earth only

  Phase 2 (accept/commit): Earth row only
    Strict:  all 5 Earth
    Relaxed: k-of-5 Earth (requires wider Q1 base for intersection)

  Intersection: every Q1 contains ≥1 Earth node, Q2 is Earth-only → Q1 ∩ Q2 ≠ ∅

  Liveness during blackout:
    Mars Phase 1: BLOCKED (can't reach Moon/LEO/Earth)
    Moon Phase 1: works (doesn't need Mars)
    LEO Phase 1:  works (doesn't need Mars or Moon)
    Earth Phase 1: works (doesn't need anyone else)
    Phase 2: always works (Earth-only)

  Mars writes: need Multi-Paxos leader to amortize Phase 1 RTT
  Mars reads: linearizable from local accepted state
```

---

## Files modified in this session

- `quorums.py` — CrumblingWallQuorum rewritten for real crumbling wall
- `paxos.py` — QuorumSystem and Proposer accept initiator_tier
- `demo_step_8.py` — initiator_tier=3
- `demo_step_9.py` — initiator_tier=3
- `demo_step_10.py` — initiator_tier=3
- `experiments/step10_sweep.py` — initiator_tier=3
- `demo_step_11.py` — convergence detector bug fix (from earlier in session)
- `tla/ExhaustiveIntersection.tla` — rewritten for per-tier Q1
- `tla/QuorumIntersection.tla` — rewritten for per-tier Q1
- `tla/QuorumIntersection.cfg` — updated for new variables
- `docs/paper/main.tex` — TLA+ numbers updated (will need full rewrite)
- `docs/steps-10-12-status.md` — status doc from earlier in session
- `docs/crumbling-wall-status.md` — this document
