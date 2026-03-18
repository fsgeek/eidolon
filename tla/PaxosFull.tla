------------------------------ MODULE PaxosFull ---------------------------------
\* Single-decree Paxos with exact 10-node topology and relaxed quorum construction.
\* Optimized: tracks Phase2b vote counts per (ballot, value) to check chosen efficiently.

EXTENDS Integers, FiniteSets

CONSTANTS Values, Proposers, Ballots

VARIABLES
    maxBal,      \* maxBal[a]: highest ballot promised
    maxVBal,     \* maxVBal[a]: ballot of highest accepted
    maxVal,      \* maxVal[a]: value of highest accepted
    msgs,        \* set of messages
    p2bEarth     \* p2bEarth[<<b,v>>]: set of Earth nodes that sent 2b for (b,v)

vars == <<maxBal, maxVBal, maxVal, msgs, p2bEarth>>

Earth == {"e1","e2","e3","e4","e5"}
LEO   == {"l1"}
Moon  == {"u1"}
Mars  == {"m1","m2","m3"}
Acceptors == Earth \union LEO \union Moon \union Mars

Tier(n) == CASE n \in Earth -> "Earth"
           []   n \in LEO   -> "LEO"
           []   n \in Moon  -> "Moon"
           []   n \in Mars  -> "Mars"

TiersOf(S) == {Tier(n) : n \in S}
AllTiers == {"Earth","LEO","Moon","Mars"}

\* Q1: all 4 tiers, size >= 7
IsQ1(S) == S \subseteq Acceptors /\ TiersOf(S) = AllTiers /\ Cardinality(S) >= 7

\* Q2: 4-of-5 Earth (checked via p2bEarth count)

Send(m) == msgs' = msgs \union {m}

Phase1a(p, b) ==
    /\ Send([type |-> "1a", bal |-> b, proposer |-> p])
    /\ UNCHANGED <<maxBal, maxVBal, maxVal, p2bEarth>>

Phase1b(a) ==
    /\ \E m \in msgs :
        /\ m.type = "1a"
        /\ m.bal > maxBal[a]
        /\ maxBal' = [maxBal EXCEPT ![a] = m.bal]
        /\ Send([type |-> "1b", bal |-> m.bal, acc |-> a,
                 maxVBal |-> maxVBal[a], maxVal |-> maxVal[a]])
        /\ UNCHANGED <<maxVBal, maxVal, p2bEarth>>

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
    /\ UNCHANGED <<maxBal, maxVBal, maxVal, p2bEarth>>

Phase2b(a) ==
    /\ \E m \in msgs :
        /\ m.type = "2a"
        /\ m.bal >= maxBal[a]
        /\ maxBal'  = [maxBal  EXCEPT ![a] = m.bal]
        /\ maxVBal' = [maxVBal EXCEPT ![a] = m.bal]
        /\ maxVal'  = [maxVal  EXCEPT ![a] = m.val]
        /\ Send([type |-> "2b", bal |-> m.bal, val |-> m.val, acc |-> a])
        /\ IF a \in Earth
           THEN p2bEarth' = [p2bEarth EXCEPT ![<<m.bal, m.val>>] =
                    p2bEarth[<<m.bal, m.val>>] \union {a}]
           ELSE UNCHANGED p2bEarth

Init ==
    /\ maxBal  = [a \in Acceptors |-> -1]
    /\ maxVBal = [a \in Acceptors |-> -1]
    /\ maxVal  = [a \in Acceptors |-> "none"]
    /\ msgs = {}
    /\ p2bEarth = [bv \in Ballots \X Values |-> {}]

Next ==
    \/ \E p \in Proposers, b \in Ballots : Phase1a(p, b)
    \/ \E a \in Acceptors : Phase1b(a)
    \/ \E p \in Proposers, b \in Ballots : Phase2a(p, b)
    \/ \E a \in Acceptors : Phase2b(a)

Spec == Init /\ [][Next]_vars

\* A value is chosen when 4+ Earth nodes have sent Phase2b for some ballot
Chosen(v) == \E b \in Ballots : Cardinality(p2bEarth[<<b,v>>]) >= 4

Agreement == \A v1, v2 \in Values : (Chosen(v1) /\ Chosen(v2)) => v1 = v2

===============================================================================
