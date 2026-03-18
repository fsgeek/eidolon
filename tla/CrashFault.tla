------------------------------ MODULE CrashFault --------------------------------
\* Verify that with one Earth node crashed, the relaxed construction still works:
\*  - Can form Q1 quorums (need 7, have 9 available: 4E+1L+1U+3M)
\*  - Can form Q2 quorums (need 4-of-5 Earth, have exactly 4)
\*  - Intersection property still holds for all Q1 x Q2 pairs

EXTENDS Integers, FiniteSets

VARIABLES crashed, q1, q2, phase

vars == <<crashed, q1, q2, phase>>

Earth == {"e1","e2","e3","e4","e5"}
LEO   == {"l1"}
Moon  == {"u1"}
Mars  == {"m1","m2","m3"}
AllNodes == Earth \union LEO \union Moon \union Mars

Tier(n) == CASE n \in Earth -> "Earth"
           []   n \in LEO   -> "LEO"
           []   n \in Moon  -> "Moon"
           []   n \in Mars  -> "Mars"

TiersOf(S) == {Tier(n) : n \in S}
AllTiers == {"Earth","LEO","Moon","Mars"}

Init ==
    /\ crashed \in Earth          \* one Earth node crashes
    /\ q1 = {}
    /\ q2 = {}
    /\ phase = "start"

Available == AllNodes \ {crashed}

\* Q1 (relaxed): all 4 tiers, size >= 7, from available nodes
IsQ1(S) == S \subseteq Available /\ TiersOf(S) = AllTiers /\ Cardinality(S) >= 7

\* Q2 (relaxed): 4-of-5 Earth from available nodes
IsQ2(S) == S \subseteq (Earth \ {crashed}) /\ Cardinality(S) >= 4

\* Check that Q1 quorums exist (liveness)
\* Check that Q2 quorums exist (liveness)
\* Check intersection for all pairs

PickQuorums ==
    /\ phase = "start"
    /\ \E s1 \in SUBSET Available, s2 \in SUBSET (Earth \ {crashed}) :
        /\ IsQ1(s1)
        /\ IsQ2(s2)
        /\ q1' = s1
        /\ q2' = s2
        /\ phase' = "checked"
    /\ UNCHANGED crashed

Done ==
    /\ phase = "checked"
    /\ UNCHANGED vars

Next == PickQuorums \/ Done

\* Safety: intersection must hold
IntersectionSafety == phase = "checked" => q1 \cap q2 /= {}

\* Liveness check encoded as an invariant:
\* If we can't form quorums, TLC will report deadlock (no Next step possible).
\* We CHECK FOR DEADLOCK in the config to detect if quorums can't be formed.

Spec == Init /\ [][Next]_vars

===============================================================================
