--------------------------- MODULE CapabilityBoundary ---------------------------
\* Constant-level exhaustive check of the capability-hazard boundary, plus a
\* numeric agreement check against quorums.py.
\*
\* This module has a one-state behaviour; all the work is done by TLC's
\* evaluator when it checks the invariants.  Each invariant is a closed
\* formula, so "no error" means TLC exhaustively enumerated every subset in
\* every configuration named and found the claim to hold.
\*
\* Tier indexing: 1 = Mars (top of wall) .. 4 = Earth (bottom, fast tier),
\* matching tla/QuorumIntersection.tla.  quorums.py uses 0 = Mars .. 3 = Earth.

EXTENDS Integers, FiniteSets

\* ------------------------------------------------------------ topology(E, M)
NodesOf(E, M) == 1..(E + 2 + M)
EarthOf(E)    == 1..E
LEOOf(E)      == {E + 1}
MoonOf(E)     == {E + 2}
MarsOf(E, M)  == (E + 3)..(E + 2 + M)
TiersOf(E, M) == <<MarsOf(E, M), MoonOf(E), LEOOf(E), EarthOf(E)>>

\* quorums.py: _min_earth_in_q1 = |E| - k + 1
MinEarth(E, k) == E - k + 1

\* quorums.py is_phase1_quorum(respondents, initiator_tier)
P1(S, t, E, M, k) ==
    /\ \A j \in t..4 : S \cap TiersOf(E, M)[j] /= {}
    /\ Cardinality(S \cap EarthOf(E)) >= MinEarth(E, k)

\* quorums.py is_phase2_quorum(respondents)
P2(S, E, k) == Cardinality(S \cap EarthOf(E)) >= k

\* (p1 = TRUE, p2 = FALSE)
IsHazard(S, t, E, M, k) == P1(S, t, E, M, k) /\ ~P2(S, E, k)

HazardExists(E, M, k) ==
    \E S \in SUBSET NodesOf(E, M), t \in 1..4 : IsHazard(S, t, E, M, k)

Ceil2(E) == (E + 1) \div 2

\* --------------------------------------------------------------- the theorem
\* R3: the hazard capability state (1,0) is reachable exactly when k exceeds
\* ceil(|E|/2), i.e. exactly when Phase 1 is cheaper than Phase 2 on Earth.
BoundaryTheorem ==
    \A E \in 2..8 : \A k \in 1..E :
        HazardExists(E, 3, k) <=> (k > Ceil2(E))

\* Equivalent algebraic form: |E| - k + 1 >= k is the safe side.
BoundaryAlgebraic ==
    \A E \in 2..8 : \A k \in 1..E :
        HazardExists(E, 3, k) <=> ~(MinEarth(E, k) >= k)

\* Same claim at |E| = 9 (14 nodes), run separately because it is the
\* expensive one.
BoundaryTheorem9 ==
    \A k \in 1..9 : HazardExists(9, 3, k) <=> (k > Ceil2(9))

\* ------------------------------------------- agreement with quorums.py (|E|=5)
\* Counts produced by exhaustive enumeration over all 2^10 subsets using the
\* repo's CrumblingWallQuorum.  Index [k][t], t = 1 (Mars) .. 4 (Earth).
ExpectedQ1 == << <<  7,   8,  16,  32>>,
                 << 42,  48,  96, 192>>,
                 <<112, 128, 256, 512>>,
                 <<182, 208, 416, 832>>,
                 <<217, 248, 496, 992>> >>

ExpectedHazard == << <<  0,   0,   0,   0>>,
                     <<  0,   0,   0,   0>>,
                     <<  0,   0,   0,   0>>,
                     <<140, 160, 320, 640>>,
                     <<210, 240, 480, 960>> >>

ExpectedBoth == << <<  7,   8,  16,  32>>,
                   << 42,  48,  96, 192>>,
                   <<112, 128, 256, 512>>,
                   << 42,  48,  96, 192>>,
                   <<  7,   8,  16,  32>> >>

Q1Count(t, k)     == Cardinality({S \in SUBSET NodesOf(5, 3) : P1(S, t, 5, 3, k)})
HazardCount(t, k) == Cardinality({S \in SUBSET NodesOf(5, 3) : IsHazard(S, t, 5, 3, k)})
BothCount(t, k)   == Cardinality({S \in SUBSET NodesOf(5, 3) :
                                    P1(S, t, 5, 3, k) /\ P2(S, 5, k)})

AgreesWithPython ==
    \A k \in 1..5 : \A t \in 1..4 :
        /\ Q1Count(t, k)     = ExpectedQ1[k][t]
        /\ HazardCount(t, k) = ExpectedHazard[k][t]
        /\ BothCount(t, k)   = ExpectedBoth[k][t]

\* ------------------------------------------------------ safety is unaffected
\* Cross-intersection still holds for every k: every Phase 1 quorum meets
\* every Phase 2 quorum.  The hazard is a liveness property, not a safety one.
CrossIntersects ==
    \A k \in 1..5 : \A t \in 1..4 :
        \A A \in {S \in SUBSET NodesOf(5, 3) : P1(S, t, 5, 3, k)} :
            \A B \in {S \in SUBSET NodesOf(5, 3) : P2(S, 5, k)} :
                A \cap B /= {}

\* -------------------------------------- flat threshold Flexible Paxos (R1)
\* Baseline: no tier structure, Phase 1 = any q1 nodes, Phase 2 = any q2
\* nodes, safety condition q1 + q2 > n.
FlatHazard(n, q1, q2) ==
    \E S \in SUBSET (1..n) : Cardinality(S) >= q1 /\ ~(Cardinality(S) >= q2)

FlatConfigs == {<<a, b>> \in (1..10) \X (1..10) : a + b > 10}

\* The hazard appears in a flat construction exactly when q1 < q2, i.e.
\* exactly when Phase 1 is cheaper than Phase 2 -- the same criterion as
\* the wall, with Earth playing the role of "the whole system".
FlatTheorem ==
    \A c \in FlatConfigs : FlatHazard(10, c[1], c[2]) <=> (c[1] < c[2])

FlatCounts ==
    /\ Cardinality({c \in FlatConfigs : c[1] >= c[2]}) = 30
    /\ Cardinality({c \in FlatConfigs : c[1] >= c[2]
                                        /\ FlatHazard(10, c[1], c[2])}) = 0
    /\ Cardinality({c \in FlatConfigs : c[1] <  c[2]}) = 25
    /\ Cardinality({c \in FlatConfigs : c[1] <  c[2]
                                        /\ FlatHazard(10, c[1], c[2])}) = 25

\* ------------------------------------------------------------ trivial harness
VARIABLE tick
Init == tick = 0
Next == UNCHANGED tick
Spec == Init /\ [][Next]_tick

================================================================================
