# Generic Quorum Auditor Pre-registration

**Date:** 2026-07-30
**Status:** Registered before implementation
**Design:** `docs/superpowers/specs/2026-07-30-nines-capability-gaps-revision-design.md`

This note fixes the expected outputs for the generic quorum-family auditor
before `quorum_audit.py`, its CLI, or its tests exist. The commit containing
this note is the registration boundary. Later code scores these predictions;
it does not revise them to match observed output.

## Model and Classification

For a finite node universe `N` and explicit nonempty quorum family `Q`, define

```text
Form(Q) = {C subseteq N | exists q in Q: q subseteq C}.
```

`R1(C)` and `R2(C)` mean membership in `Form(Q1)` and `Form(Q2)`. The mixed
states are `(1,0) = Form(Q1) \ Form(Q2)` and
`(0,1) = Form(Q2) \ Form(Q1)`.

An optional pinned set `P` restricts the analyzed domain to connectivity
states containing every pinned node:

```text
D_P = {C subseteq N | P subseteq C}.
```

Equivalently, for capability classification only, replace each quorum `q`
with `q union P` and minimize the resulting family again. Pinning restricts
the connectivity states under consideration; it does not alter the Paxos
configuration. Cross-phase safety is therefore evaluated on the original
families, not the lifted families.

The four mutually exclusive predicate relations and gap profiles are:

| Predicate relation | `(1,0)` | `(0,1)` |
|---|---:|---:|
| `equal` | no | no |
| `r1-strictly-implies-r2` | no | yes |
| `r2-strictly-implies-r1` | yes | no |
| `incomparable` | yes | yes |

Equality is checked before either non-strict implication in an implementation.
Witnesses below are minimum-cardinality connectivity states, with ties broken
lexicographically over sorted string node identifiers.

## Registered Generic Cases

All universes are `N={a,b,c}` and all pinned sets are empty.

| Case | `Q1` | `Q2` | Safe | Relation | `(1,0)` witness | `(0,1)` witness |
|---|---|---|---:|---|---|---|
| Semantic equality, syntactically different | `{{a,b},{a,b,c}}` | `{{a,b}}` | yes | `equal` | none | none |
| R1 strictly implies R2 | `{{a,b}}` | `{{a}}` | yes | `r1-strictly-implies-r2` | none | `{a}` |
| R2 strictly implies R1 | `{{a}}` | `{{a,b}}` | yes | `r2-strictly-implies-r1` | `{a}` | none |
| Incomparable | `{{a,b}}` | `{{a,c}}` | yes | `incomparable` | `{a,b}` | `{a,c}` |
| Threshold `q1=1,q2=3` | `{{a},{b},{c}}` | `{{a,b,c}}` | yes | `r2-strictly-implies-r1` | `{a}` | none |
| Threshold `q1=2,q2=2` | all 2-subsets | all 2-subsets | yes | `equal` | none | none |
| Threshold `q1=3,q2=1` | `{{a,b,c}}` | `{{a},{b},{c}}` | yes | `r1-strictly-implies-r2` | none | `{a}` |
| Unsafe negative control | `{{a}}` | `{{b}}` | no | `incomparable` | `{a}` | `{b}` |

For the semantic-equality case, both canonical minimal antichains must be
`{{a,b}}`. For the unsafe control, the deterministic disjoint safety witness
must be `Q1={a}, Q2={b}`. Unsafe input is classified rather than rejected, but
the safety failure must be prominent in both text and JSON output.

## Registered Wall Cases

The wall uses code-tier order and identifiers:

```text
Mars  = {0,1,2}
Moon  = {3}
LEO   = {4}
Earth = {5,6,7,8,9}
```

For Phase 2 threshold `k`, every minimal Phase 2 quorum is a `k`-subset of
Earth. Every minimal Phase 1 quorum contains one node from each required
non-anchor tier and `|Earth|-k+1` Earth nodes.

The unconstrained reading has `P={}`. The self-reachable reading pins the
first node of the initiating tier: Mars `{0}`, Moon `{3}`, LEO `{4}`, Earth
`{5}`. The existing gradient CSV instead filters for at least one reachable
node from the initiating tier. Pinning one representative produces the same
gap profile here because wall predicates are symmetric within each tier. This
equivalence is wall-specific; it is not asserted for asymmetric deployments.

For both `k=4` and `k=5`, the registered relations are:

| Initiator | Unconstrained | Self-reachable | Registered gap profile |
|---|---|---|---|
| Mars | `incomparable` | `incomparable` | both gaps under both readings |
| Moon | `incomparable` | `incomparable` | both gaps under both readings |
| LEO | `incomparable` | `r2-strictly-implies-r1` | both unpinned; only `(1,0)` pinned |
| Earth | `r2-strictly-implies-r1` | `r2-strictly-implies-r1` | only `(1,0)` under both readings |

### Deterministic witnesses at `k=4`

The Phase 1 Earth floor is two nodes and Phase 2 requires four.

| Initiator | `(1,0)` witness | Unconstrained `(0,1)` witness | Pinned `(0,1)` witness |
|---|---|---|---|
| Mars | `{0,3,4,5,6}` | `{5,6,7,8}` | `{0,5,6,7,8}` |
| Moon | `{3,4,5,6}` | `{5,6,7,8}` | `{3,5,6,7,8}` |
| LEO | `{4,5,6}` | `{5,6,7,8}` | none |
| Earth | `{5,6}` | none | none |

### Deterministic witnesses at `k=5`

The Phase 1 Earth floor is one node and Phase 2 requires all five.

| Initiator | `(1,0)` witness | Unconstrained `(0,1)` witness | Pinned `(0,1)` witness |
|---|---|---|---|
| Mars | `{0,3,4,5}` | `{5,6,7,8,9}` | `{0,5,6,7,8,9}` |
| Moon | `{3,4,5}` | `{5,6,7,8,9}` | `{3,5,6,7,8,9}` |
| LEO | `{4,5}` | `{5,6,7,8,9}` | none |
| Earth | `{5}` | none | none |

## Scoring and Invalidation Rules

1. Generic cases are scored against the relation, both gap flags, witnesses,
   canonical antichains where stated, and safety result.
2. Wall cases are compared only with the matching model reading in
   `results/capability/dual_gradient_map.csv`: unconstrained to
   `reachable_unconstrained`, pinned to `reachable_self_reachable`.
3. A difference between unconstrained and pinned output is expected model
   sensitivity, not a finding by itself.
4. Any disagreement under the same reading stops implementation scoring. It
   is recorded as a discrepancy and investigated as a possible registration,
   model, implementation, or prior-artifact defect before anything is changed.
5. The independent exhaustive self-check must be able to reject a deliberately
   mutated report; a self-check that only confirms the production computation
   is invalid.
6. The scarcity lemma and every unregistered aggregate or design conclusion
   are fenced off. The auditor may expose raw classifications from which later
   work could derive them, but this implementation round does not score or
   claim them.
