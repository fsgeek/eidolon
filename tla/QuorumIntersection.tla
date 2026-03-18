--------------------------- MODULE QuorumIntersection ---------------------------
\* Verify cross-intersection property for crumbling wall quorum construction.
\* For safety: every Q1 quorum (from any tier) must intersect every Q2 quorum.
\*
\* Crumbling wall topology: 10 nodes in 4 tiers
\*   Tier 1 - Mars  (M): m1..m3   (3 nodes, top of wall)
\*   Tier 2 - Moon  (U): u1       (1 node)
\*   Tier 3 - LEO   (L): l1       (1 node)
\*   Tier 4 - Earth (E): e1..e5   (5 nodes, bottom of wall)
\*
\* Phase 1: proposer at tier i reads down — needs one node from
\*          each tier j where j >= i.
\* Phase 2: full Earth row (strict) or k-of-Earth (relaxed).

EXTENDS Integers, FiniteSets

CONSTANTS Strict, Relaxed

VARIABLES construction, initiator, q1, q2, result

\* Node sets
Earth == {"e1","e2","e3","e4","e5"}
LEO   == {"l1"}
Moon  == {"u1"}
Mars  == {"m1","m2","m3"}
AllNodes == Earth \union LEO \union Moon \union Mars

\* Tiers as a tuple, indexed 1..4 (top to bottom)
Tiers == <<Mars, Moon, LEO, Earth>>
NumTiers == 4

\* Does set S contain at least one node from each tier j >= i?
SpansTiersFrom(S, i) == \A j \in i..NumTiers : S \cap Tiers[j] /= {}

\* ------ Strict Construction ------
\* Q2: ALL 5 Earth nodes
StrictQ2 == {Earth}
\* Q1 per tier: spans tiers from initiator down, at least 1 Earth
StrictQ1(tier) == {S \in SUBSET AllNodes : SpansTiersFrom(S, tier)}

\* ------ Relaxed Construction ------
\* Q2: any 4-of-5 Earth
RelaxedQ2 == {S \in SUBSET Earth : Cardinality(S) >= 4}
\* Q1 per tier: spans tiers from initiator down, at least 2 Earth (pigeonhole)
RelaxedQ1(tier) == {S \in SUBSET AllNodes : SpansTiersFrom(S, tier) /\ Cardinality(S \cap Earth) >= 2}

vars == <<construction, initiator, q1, q2, result>>

Init ==
    /\ construction \in {Strict, Relaxed}
    /\ initiator \in 1..NumTiers
    /\ q1 = {}
    /\ q2 = {}
    /\ result = "init"

PickStrict ==
    /\ construction = Strict
    /\ result = "init"
    /\ \E s1 \in StrictQ1(initiator), s2 \in StrictQ2 :
        /\ q1' = s1
        /\ q2' = s2
        /\ result' = "checked"
    /\ UNCHANGED <<construction, initiator>>

PickRelaxed ==
    /\ construction = Relaxed
    /\ result = "init"
    /\ \E s1 \in RelaxedQ1(initiator), s2 \in RelaxedQ2 :
        /\ q1' = s1
        /\ q2' = s2
        /\ result' = "checked"
    /\ UNCHANGED <<construction, initiator>>

Next == PickStrict \/ PickRelaxed

\* SAFETY: whenever we've picked a pair, they must intersect
Safety == result = "checked" => q1 \cap q2 /= {}

Spec == Init /\ [][Next]_vars

================================================================================
