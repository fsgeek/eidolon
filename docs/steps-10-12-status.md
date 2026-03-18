# Steps 10–12 and TLA+ Verification: Status Report

**Date**: 2026-03-15
**Author**: Claude (picking up from prior instance)
**Status**: Code complete, partially validated

---

## What was built

The prior instance extended the Eidolon simulator in three directions beyond the Step 9 paper results:

### 1. Step 10: Relaxed Phase 2 quorums under crash faults

**File**: `demo_step_10.py`
**Question**: What if we relax Q2 from "all 5 Earth" to "4-of-5 Earth"?

This trades Phase 1 cost (Q1 minimum rises from 6 to 7 to maintain intersection) for crash fault tolerance: one Earth node can fail without killing Phase 2 liveness.

Three regimes compared:
- **Strict Q2** = {E} (all 5 Earth). One Earth crash → global Phase 2 dead.
- **Relaxed Q2** = 4-of-5 Earth. Tolerates one Earth crash.
- **Relaxed Q2 + repeater**. Tolerates crash + blackout.

**Sweep results** (`results/step10/step10_sweep_ci.csv`, 50 seeds):

| Scenario | Earth crash | Earth local | Global during | Global post | Recovery lag (s) |
|---|---|---|---|---|---|
| strict, 0 crash | 0 | 100% | 100% | 100% | 489 |
| strict, 1 crash | 1 | 100% | — | 0% | — |
| relaxed, 0 crash | 0 | 100% | 100% | 100% | 490 |
| relaxed, 1 crash | 1 | 100% | 100% | 100% | 489 |

**Key finding**: Strict Q2 is a single point of failure for the Earth tier. One crash during blackout kills global consensus permanently (no recovery). Relaxed Q2 handles it with zero degradation. The additional Phase 1 cost (7 vs 6) is irrelevant during blackout because Phase 1 can't succeed anyway (Mars unreachable).

### 2. Step 11: MRDT-based reconciliation

**Files**: `demo_step_11.py`, `mrdt.py`
**Question**: What if we stop fighting the blackout?

Instead of global Paxos reconciliation (requiring tier-spanning Phase 1), use CRDTs (here called MRDTs — Mergeable Replicated Data Types) for cross-tier state. Each tier runs strong local consensus; cross-tier state is eventually consistent via automatic merge.

MRDT library (`mrdt.py`) implements:
- **GCounter**: grow-only counter, one slot per tier, merge = max per slot
- **PNCounter**: increment/decrement via two GCounters
- **LWWRegister**: last-writer-wins by timestamp
- **VersionVector**: causal ordering tracker with staleness measurement

**Single-run results** (seed 42, 186s Mars latency, 900s blackout):

| Metric | Hard blackout | Repeater |
|---|---|---|
| Earth ops during blackout | 400 | 400 |
| Mars ops during blackout | 446 | 446 |
| Max Earth/Mars staleness | 455 ops | 10 ops |
| Convergence after blackout | **30s** | **1s** |
| Paxos recovery lag (Step 9) | ~490s | ~490s |

Both tiers continue operating independently during blackout. The convergence metric measures how long after connectivity returns until blackout-accumulated divergence is absorbed (staleness returns to steady-state levels).

**Bug fix**: The prior instance's convergence detector checked for zero staleness, which is impossible during active operation (ops happen every 2s, merges every 30s). Fixed to measure return to pre-blackout steady-state staleness levels.

**Key findings**:
- MRDTs eliminate the blackout liveness problem entirely — both tiers make full progress.
- Convergence is 16x faster than Paxos reconciliation (30s vs 490s under hard blackout).
- With repeater, divergence barely accumulates (10 ops max) and resolves in 1 second.
- The cost: eventual consistency instead of strong consistency across tiers.

### 3. Step 12: Recovery conflict resolution

**File**: `demo_step_12.py`
**Question**: When Earth and Mars independently decide about shared resources during blackout, what happens at recovery?

Three resolution policies:
- **HIERARCHY**: Earth always wins. Deterministic, fast, but Mars work is lost.
- **LWW**: Last-writer-wins by timestamp. Fair but clock-dependent.
- **DETECT**: Flag conflicts for manual resolution. Safest but requires intervention.

**Single-run results** (seed 42, 10 shared resources):

| Policy | Network | Conflicts | Blackout-caused | Inconsistency window |
|---|---|---|---|---|
| HIERARCHY | hard blackout | 178 | 12 | 3840s |
| HIERARCHY | repeater | 221 | 30 | 3840s |
| LWW | hard blackout | 178 | 12 | 3840s |
| LWW | repeater | 221 | 41 | 3840s |
| DETECT | hard blackout | 873 | 49 | 3840s |
| DETECT | repeater | 1133 | 163 | 3840s |

**Notable**: The repeater *increases* conflict count. More connectivity during blackout means more opportunities for tiers to see and disagree about each other's claims. DETECT mode accumulates unresolved conflicts (873–1133) because it never overwrites.

**The fundamental tension** (quoted from the code, attributed to Tony's observation): "A system that 'works' during blackout but produces unresolvable conflicts during recovery is worse than one that stops."

---

## TLA+ Formal Verification

**Directory**: `tla/`

Six specifications, forming a verification hierarchy:

| Spec | What it checks | States | Result |
|---|---|---|---|
| ExhaustiveIntersection | Q1∩Q2≠∅ for strict, relaxed, and crash-fault constructions via ASSUME | 1 | ✅ Pass |
| QuorumIntersection | Same property via state exploration | 619 | ✅ Pass |
| CrashFault | Relaxed construction with one Earth crashed: quorums exist and intersect | 150 | ✅ Pass |
| PaxosSmall | Full Paxos protocol, structurally equivalent reduced topology (3E+1L+1U+1M) | 67,426,637 | ✅ Pass |
| PaxosRelaxed | Full Paxos, 10-node topology, relaxed quorums (2 proposers, 2 ballots) | 2.8B generated, 358M distinct (depth 20) | Terminated (resource bounds) |
| PaxosFull | Full Paxos, 10-node topology, optimized with p2bEarth tracking | 6.5B generated, 731M distinct (depth 20) | Terminated (resource bounds) |

The quorum counts are: 157 strict Q1 quorums, 1 strict Q2 quorum, 92 relaxed Q1 quorums, 5 relaxed Q2 quorums.

**What this proves**: The topology-aware quorum construction satisfies the Flexible Paxos cross-intersection requirement under exhaustive enumeration. PaxosSmall proves that agreement holds for the full protocol (not just quorum intersection) under a structurally equivalent reduced model. PaxosRelaxed/Full will confirm this for the actual 10-node topology.

---

## Open questions

1. ~~**Step 11 convergence**~~: Fixed. Was a measurement bug — convergence detector expected zero staleness, which is impossible during active operation. Now measures return to steady-state. Result: 30s (hard blackout), 1s (repeater).

2. **Step 12 sweep**: Only single-seed results exist. A parameter sweep (like step 9/10) would show how conflict counts scale with blackout duration and Mars latency.

3. **Step 12 DETECT policy**: 873 unresolved conflicts for 10 resources over one blackout is operationally untenable. Is there a hybrid policy (auto-resolve obvious cases, flag genuinely ambiguous ones)?

4. ~~**PaxosRelaxed/Full completion**~~: Terminated after ~24 hours. PaxosFull explored 6.5B states (731M distinct, BFS depth 20, 157GB on disk) with no violation. PaxosRelaxed explored 2.8B states (358M distinct). The state space at 10 nodes is infeasible for exhaustive model checking — the BFS frontier was still expanding at termination. Safety argument rests on ExhaustiveIntersection (complete) + Flexible Paxos theorem, with PaxosSmall (67M states, complete) and the partial 10-node runs as additional evidence.

5. **Paper integration**: Steps 10–12 go beyond the current paper scope. The TLA+ results could strengthen the paper (replacing the hand-wavy Proposition 1 with "verified by model checker"). Steps 10–12 are future work or a second paper.

---

## Reproduction

```bash
# Step 10 (single run)
uv run python demo_step_10.py --seed 42

# Step 10 (sweep, 50 seeds)
uv run python experiments/step10_sweep.py --seeds "$(seq -s, 40 89)" --output results/step10/step10_sweep.csv

# Step 11
uv run python demo_step_11.py --seed 42

# Step 12
uv run python demo_step_12.py --seed 42

# TLA+ (requires tla2tools.jar)
java -jar /path/to/tla2tools.jar -workers auto QuorumIntersection.tla
java -jar /path/to/tla2tools.jar -workers auto ExhaustiveIntersection.tla
java -jar /path/to/tla2tools.jar -workers auto CrashFault.tla
java -jar /path/to/tla2tools.jar -workers auto PaxosSmall.tla
```
