------------------------- MODULE CapabilityReachability -------------------------
\* Capability reachability for the crumbling-wall quorum construction.
\*
\* A proposer's CAPABILITY STATE is the pair (p1, p2) where
\*   p1 = "I can form a Phase 1 quorum from the nodes I can reach"
\*   p2 = "I can form a Phase 2 quorum from the nodes I can reach"
\* Both predicates are monotone in the reachable set, so "formable from R"
\* is equivalent to "R itself satisfies the predicate".
\*
\* (p1=TRUE, p2=FALSE) is the HAZARD state: the proposer can win an
\* election and raise the ballot high-water mark, but can never commit.
\* It preempts the incumbent and then stalls.
\*
\* CLAIM under test (established by exhaustive Python enumeration, R3):
\*   the hazard state is unreachable  <=>  |E| - k + 1 >= k  <=>  k <= ceil(|E|/2)
\*
\* Tier indexing follows the existing tla/ specs: 1 = Mars (top of wall)
\* ... 4 = Earth (bottom, fast tier).  quorums.py uses 0 = Mars ... 3 = Earth;
\* the two differ by a constant offset only.  The paper indexes the other
\* way round (T_0 = Earth ... T_3 = Mars).
\*
\* Nodes are integers so the topology can be resized from the .cfg:
\*   Earth = 1..EarthSize
\*   LEO   = {EarthSize+1}
\*   Moon  = {EarthSize+2}
\*   Mars  = EarthSize+3 .. EarthSize+2+MarsSize

EXTENDS Integers, FiniteSets

CONSTANTS EarthSize,     \* |E|, size of the fast tier
          MarsSize,      \* size of the top tier
          KSet           \* set of phase2_threshold values to explore

Earth    == 1..EarthSize
LEO      == {EarthSize + 1}
Moon     == {EarthSize + 2}
Mars     == (EarthSize + 3)..(EarthSize + 2 + MarsSize)
AllNodes == 1..(EarthSize + 2 + MarsSize)

Tiers    == <<Mars, Moon, LEO, Earth>>
NumTiers == 4

\* quorums.py: self._min_earth_in_q1 = len(fast_tier) - phase2_threshold + 1
MinEarthInQ1(k) == EarthSize - k + 1

\* quorums.py is_phase1_quorum: one respondent from every tier at or below
\* the initiator, plus MinEarthInQ1 fast-tier nodes.
IsPhase1(S, tier, k) ==
    /\ \A j \in tier..NumTiers : S \cap Tiers[j] /= {}
    /\ Cardinality(S \cap Earth) >= MinEarthInQ1(k)

\* quorums.py is_phase2_quorum: at least k fast-tier nodes.
IsPhase2(S, k) == Cardinality(S \cap Earth) >= k

\* ceil(EarthSize / 2)
Bound == (EarthSize + 1) \div 2

VARIABLES k,          \* phase2_threshold, chosen once at init
          initiator,  \* tier of the proposer, 1 = Mars .. 4 = Earth
          reach       \* set of nodes the proposer can currently reach

vars == <<k, initiator, reach>>

TypeOK ==
    /\ k \in KSet
    /\ initiator \in 1..NumTiers
    /\ reach \subseteq AllNodes

Init ==
    /\ k \in KSet
    /\ initiator \in 1..NumTiers
    /\ reach = {}

\* Connectivity heals one node at a time; every subset of AllNodes is
\* therefore a reachable state of this model.
Gain ==
    /\ \E n \in AllNodes \ reach : reach' = reach \cup {n}
    /\ UNCHANGED <<k, initiator>>

Next == Gain

Spec == Init /\ [][Next]_vars

\* ---------------------------------------------------------------- properties

CanPreempt == IsPhase1(reach, initiator, k)
CanCommit  == IsPhase2(reach, k)

\* The hazard capability state (1,0).
Hazard == CanPreempt /\ ~CanCommit

\* Violated exactly when some k in KSet admits the hazard.  Used to make
\* TLC produce an explicit counterexample trace.
NoHazard == ~Hazard

\* The universal half of the boundary claim: whenever the hazard is
\* reachable at all, k must exceed ceil(|E|/2).
HazardImpliesAboveBound == Hazard => (k > Bound)

\* The contrapositive, stated directly.
BelowBoundIsSafe == (k =< Bound) => ~Hazard

================================================================================
