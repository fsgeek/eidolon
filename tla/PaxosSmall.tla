------------------------------ MODULE PaxosSmall --------------------------------
\* Structurally equivalent single-decree Paxos with reduced node count.
\*
\* Full model: 5E+1L+1U+3M=10, Q2=4-of-5 Earth, Q1=all tiers+size>=7
\* Reduced:    3E+1L+1U+1M=6,  Q2=2-of-3 Earth, Q1=all tiers+size>=5
\*
\* The geometric relationship is preserved:
\*   Q2 is any 2-of-3 Earth nodes.
\*   Q1 spans all tiers (needs 1L+1U+1M=3 non-Earth) and size>=5,
\*   so Q1 must contain >= 2 Earth nodes.
\*   Any 2 Earth nodes intersect any 2-of-3 Earth set.
\*   Therefore Q1 ∩ Q2 ≠ ∅.

EXTENDS Integers, FiniteSets

CONSTANTS Values, Proposers, Ballots

VARIABLES maxBal, maxVBal, maxVal, msgs

vars == <<maxBal, maxVBal, maxVal, msgs>>

Earth == {"e1","e2","e3"}
LEO   == {"l1"}
Moon  == {"u1"}
Mars  == {"m1"}
Acceptors == Earth \union LEO \union Moon \union Mars

Tier(n) == CASE n \in Earth -> "Earth"
           []   n \in LEO   -> "LEO"
           []   n \in Moon  -> "Moon"
           []   n \in Mars  -> "Mars"

TiersOf(S) == {Tier(n) : n \in S}
AllTiers == {"Earth","LEO","Moon","Mars"}

\* Q1: all 4 tiers, size >= 5
IsQ1(S) == S \subseteq Acceptors /\ TiersOf(S) = AllTiers /\ Cardinality(S) >= 5

\* Q2: 2-of-3 Earth
IsQ2(S) == S \subseteq Earth /\ Cardinality(S) >= 2

Send(m) == msgs' = msgs \union {m}

Phase1a(p, b) ==
    /\ Send([type |-> "1a", bal |-> b, proposer |-> p])
    /\ UNCHANGED <<maxBal, maxVBal, maxVal>>

Phase1b(a) ==
    /\ \E m \in msgs :
        /\ m.type = "1a"
        /\ m.bal > maxBal[a]
        /\ maxBal' = [maxBal EXCEPT ![a] = m.bal]
        /\ Send([type |-> "1b", bal |-> m.bal, acc |-> a,
                 maxVBal |-> maxVBal[a], maxVal |-> maxVal[a]])
        /\ UNCHANGED <<maxVBal, maxVal>>

Phase2a(p, b) ==
    /\ ~ \E m \in msgs : m.type = "2a" /\ m.bal = b
    /\ \E Q \in SUBSET Acceptors :
        /\ IsQ1(Q)
        /\ \A a \in Q : \E m \in msgs :
            m.type = "1b" /\ m.bal = b /\ m.acc = a
        /\ LET promises == {m \in msgs : m.type = "1b" /\ m.bal = b /\ m.acc \in Q}
               maxAccBal == CHOOSE mb \in {m.maxVBal : m \in promises} :
                   \A m \in promises : m.maxVBal =< mb
           IN IF maxAccBal = -1
              THEN \E v \in Values :
                   Send([type |-> "2a", bal |-> b, val |-> v, proposer |-> p])
              ELSE LET chosen == CHOOSE m \in promises : m.maxVBal = maxAccBal
                   IN Send([type |-> "2a", bal |-> b, val |-> chosen.maxVal, proposer |-> p])
    /\ UNCHANGED <<maxBal, maxVBal, maxVal>>

Phase2b(a) ==
    /\ \E m \in msgs :
        /\ m.type = "2a"
        /\ m.bal >= maxBal[a]
        /\ maxBal'  = [maxBal  EXCEPT ![a] = m.bal]
        /\ maxVBal' = [maxVBal EXCEPT ![a] = m.bal]
        /\ maxVal'  = [maxVal  EXCEPT ![a] = m.val]
        /\ Send([type |-> "2b", bal |-> m.bal, val |-> m.val, acc |-> a])

Init ==
    /\ maxBal  = [a \in Acceptors |-> -1]
    /\ maxVBal = [a \in Acceptors |-> -1]
    /\ maxVal  = [a \in Acceptors |-> "none"]
    /\ msgs = {}

Next ==
    \/ \E p \in Proposers, b \in Ballots : Phase1a(p, b)
    \/ \E a \in Acceptors : Phase1b(a)
    \/ \E p \in Proposers, b \in Ballots : Phase2a(p, b)
    \/ \E a \in Acceptors : Phase2b(a)

Spec == Init /\ [][Next]_vars

Chosen(v) ==
    \E b \in Ballots : \E Q \in SUBSET Acceptors :
        /\ IsQ2(Q)
        /\ \A a \in Q : \E m \in msgs :
            m.type = "2b" /\ m.bal = b /\ m.val = v /\ m.acc = a

Agreement == \A v1, v2 \in Values : (Chosen(v1) /\ Chosen(v2)) => v1 = v2

===============================================================================
