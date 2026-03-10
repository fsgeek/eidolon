# Workshop Paper Roadmap (Interplanetary Quorum Systems)

## Positioning

The Mars setting is a concrete case study for a broader class of systems:
high-latency, intermittently disconnected quorum deployments.

Core claim:
Paxos is governed by quorum intersection constraints, not simplistic
"majority voting" intuition. Quorum structure can encode topology.

## Paper-Ready Questions

1. What safety properties remain invariant under extreme propagation delay and scheduled blackouts?
2. How should quorum structure separate steady-state commit path from rare election/reconfiguration path?
3. What does a control-plane refinement (Lagrange repeater) change quantitatively?
4. How much autonomy can local clusters preserve during global disconnection?

## Implemented in Repository

- Hierarchical Earth/Mars simulation (`demo_step_7.py`)
- Crumbling wall quorum construction (`quorums.py`)
- Topology-aware network model (`datacenter.py`)
- Conjunction blackout vs repeater experiment (`demo_step_9.py`)

## Critical Modeling Fix Applied

Structured quorum shape is now enforced in protocol execution:
- Proposer checks `is_phase1_quorum` / `is_phase2_quorum` on responders.
- Crumbling wall phase constraints enforce tier-spanning in Phase 1 and fast-tier membership in Phase 2.

This prevents topology-aware quorums from silently collapsing to size-only quorums.

## Experiment Set for the Paper

1. Flat majority across planets (negative control).
2. Hierarchical local autonomy + global reconciliation.
3. Conjunction blackout (hard partition window).
4. Repeater refinement (degraded continuity instead of hard outage).
5. Sensitivity sweep:
   - Mars latency (closest/average/farthest)
   - Blackout duration
   - Reconciliation interval
   - Phase-1 timeout budget

## Metrics

- Local commit success rate (Earth, Mars)
- Global reconciliation success rate by phase:
  - pre-blackout
  - blackout
  - post-blackout
- First-success-after-blackout recovery lag
- Global reconciliation latency distribution (P50/P95/P99)
- Message volume per decided value

## Draft Structure (Workshop Length)

1. Motivation and thesis
2. Model and assumptions
3. Quorum constructions and safety constraints
4. Simulation methodology
5. Results (blackout and repeater)
6. Discussion: autonomy, liveness tradeoffs, and practical deployment
7. Related work
8. Reproducibility appendix

## Immediate Next Build Tasks

1. Add fairness/load metrics (messages and wait time by tier).
2. Add a liveness envelope plot over timeout and blackout parameters.
3. Draft the methods/results sections directly against generated figures.
4. Add hypothesis-to-result traceability table for the paper narrative.
