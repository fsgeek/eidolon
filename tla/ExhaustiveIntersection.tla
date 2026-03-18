------------------------ MODULE ExhaustiveIntersection -------------------------
\* Exhaustive check of Q1 ∩ Q2 ≠ ∅ for crumbling wall quorum construction.
\* Uses ASSUME to check at startup (constant-level evaluation).
\* TLC will report an error if any ASSUME is violated.
\*
\* Crumbling wall: tiers ordered top-to-bottom (Mars=0, Moon=1, LEO=2, Earth=3).
\* A proposer at tier i reads DOWN: needs one node from each tier j >= i.
\* Phase 2: full Earth row (or k-of-Earth for relaxed).
\* Intersection: every tier's Q1 includes an Earth node, Q2 is Earth-only.

EXTENDS Integers, FiniteSets, TLC

Earth == {"e1","e2","e3","e4","e5"}
LEO   == {"l1"}
Moon  == {"u1"}
Mars  == {"m1","m2","m3"}
AllNodes == Earth \union LEO \union Moon \union Mars

\* Tiers ordered top (slow) to bottom (fast), indexed 0..3
Tiers == <<Mars, Moon, LEO, Earth>>
NumTiers == 4

\* Nodes at or below tier i (tiers i..NumTiers-1)
TiersAtOrBelow(i) == UNION {Tiers[j] : j \in (i..NumTiers)}

\* Does set S contain at least one node from each tier j where j >= i?
SpansTiersFrom(S, i) == \A j \in i..NumTiers : S \cap Tiers[j] /= {}

\* ===== CRUMBLING WALL Q1 FAMILIES (per initiating tier) =====
\* Mars proposer (tier 1): needs one from Mars + Moon + LEO + Earth
MarsQ1 == {S \in SUBSET AllNodes : SpansTiersFrom(S, 1)}
\* Moon proposer (tier 2): needs one from Moon + LEO + Earth
MoonQ1 == {S \in SUBSET AllNodes : SpansTiersFrom(S, 2)}
\* LEO proposer (tier 3): needs one from LEO + Earth
LEOQ1  == {S \in SUBSET AllNodes : SpansTiersFrom(S, 3)}
\* Earth proposer (tier 4): needs one from Earth
EarthQ1 == {S \in SUBSET AllNodes : SpansTiersFrom(S, 4)}

\* ===== STRICT Q2: ALL 5 Earth nodes =====
StrictQ2 == {Earth}

\* Verify intersection for each tier's Q1 family against Q2
ASSUME \A q1 \in MarsQ1  : \A q2 \in StrictQ2 : q1 \cap q2 /= {}
ASSUME \A q1 \in MoonQ1  : \A q2 \in StrictQ2 : q1 \cap q2 /= {}
ASSUME \A q1 \in LEOQ1   : \A q2 \in StrictQ2 : q1 \cap q2 /= {}
ASSUME \A q1 \in EarthQ1  : \A q2 \in StrictQ2 : q1 \cap q2 /= {}

\* ===== RELAXED Q2: any 4-of-5 Earth =====
RelaxedQ2 == {S \in SUBSET Earth : Cardinality(S) >= 4}

\* For relaxed Q2, the wall must widen at the base: each tier's Q1
\* needs enough Earth nodes that pigeonhole guarantees intersection.
\* For k-of-|E| Q2: need (|E| - k + 1) Earth nodes in Q1.
\* At k=4, |E|=5: need 2 Earth nodes in every Q1.
MinEarthForRelaxed == 2  \* |E| - k + 1 = 5 - 4 + 1 = 2

\* Relaxed Q1 families: same tier-spanning + minimum Earth count
RMarsQ1  == {S \in SUBSET AllNodes : SpansTiersFrom(S, 1) /\ Cardinality(S \cap Earth) >= MinEarthForRelaxed}
RMoonQ1  == {S \in SUBSET AllNodes : SpansTiersFrom(S, 2) /\ Cardinality(S \cap Earth) >= MinEarthForRelaxed}
RLEOQ1   == {S \in SUBSET AllNodes : SpansTiersFrom(S, 3) /\ Cardinality(S \cap Earth) >= MinEarthForRelaxed}
REarthQ1 == {S \in SUBSET AllNodes : SpansTiersFrom(S, 4) /\ Cardinality(S \cap Earth) >= MinEarthForRelaxed}

\* Verify intersection for each tier's relaxed Q1 against relaxed Q2
ASSUME \A q1 \in RMarsQ1  : \A q2 \in RelaxedQ2 : q1 \cap q2 /= {}
ASSUME \A q1 \in RMoonQ1  : \A q2 \in RelaxedQ2 : q1 \cap q2 /= {}
ASSUME \A q1 \in RLEOQ1   : \A q2 \in RelaxedQ2 : q1 \cap q2 /= {}
ASSUME \A q1 \in REarthQ1  : \A q2 \in RelaxedQ2 : q1 \cap q2 /= {}

\* ===== CRASH FAULT: one Earth node crashed, relaxed Q2 =====
\* With one Earth crashed, 4-of-5 Q2 becomes 4-of-4 (all remaining).
\* Q1 still needs MinEarthForRelaxed Earth nodes from available set.
CrashOK(crashed) ==
    LET Avail == AllNodes \ {crashed}
        AvailEarth == Earth \ {crashed}
        \* Each tier's Q1: spans tiers from initiator down + min Earth nodes
        CMarsQ1  == {S \in SUBSET Avail : SpansTiersFrom(S, 1) /\ Cardinality(S \cap AvailEarth) >= MinEarthForRelaxed}
        CMoonQ1  == {S \in SUBSET Avail : SpansTiersFrom(S, 2) /\ Cardinality(S \cap AvailEarth) >= MinEarthForRelaxed}
        CLEOQ1   == {S \in SUBSET Avail : SpansTiersFrom(S, 3) /\ Cardinality(S \cap AvailEarth) >= MinEarthForRelaxed}
        CEarthQ1 == {S \in SUBSET Avail : SpansTiersFrom(S, 4) /\ Cardinality(S \cap AvailEarth) >= MinEarthForRelaxed}
        CQ2 == {S \in SUBSET AvailEarth : Cardinality(S) >= 4}
    IN  /\ CQ2 /= {}   \* Q2 quorums still exist
        /\ CEarthQ1 /= {} \* Earth can still do Phase 1
        \* Intersection holds for all tier Q1 families
        /\ \A q1 \in CMarsQ1  : \A q2 \in CQ2 : q1 \cap q2 /= {}
        /\ \A q1 \in CMoonQ1  : \A q2 \in CQ2 : q1 \cap q2 /= {}
        /\ \A q1 \in CLEOQ1   : \A q2 \in CQ2 : q1 \cap q2 /= {}
        /\ \A q1 \in CEarthQ1  : \A q2 \in CQ2 : q1 \cap q2 /= {}

ASSUME \A crashed \in Earth : CrashOK(crashed)

\* ===== Reporting =====
ASSUME PrintT(<<"Mars Q1 count (strict)", Cardinality(MarsQ1)>>)
ASSUME PrintT(<<"Moon Q1 count (strict)", Cardinality(MoonQ1)>>)
ASSUME PrintT(<<"LEO Q1 count (strict)", Cardinality(LEOQ1)>>)
ASSUME PrintT(<<"Earth Q1 count (strict)", Cardinality(EarthQ1)>>)
ASSUME PrintT(<<"Mars Q1 count (relaxed)", Cardinality(RMarsQ1)>>)
ASSUME PrintT(<<"Earth Q1 count (relaxed)", Cardinality(REarthQ1)>>)
ASSUME PrintT(<<"Strict Q2 count", Cardinality(StrictQ2)>>)
ASSUME PrintT(<<"Relaxed Q2 count", Cardinality(RelaxedQ2)>>)

\* Minimal spec (TLC needs something to run)
VARIABLE dummy
Init == dummy = 0
Next == UNCHANGED dummy
Spec == Init /\ [][Next]_dummy
===============================================================================
