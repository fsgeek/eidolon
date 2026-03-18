---------------------------- MODULE PaxosRelaxed --------------------------------
\* Single-decree Paxos with relaxed quorum construction.
\* Multiple proposers, standard promise/accept rules.
\* Quorum system: Phase 2 = any 4-of-5 Earth, Phase 1 = all 4 tiers + size >= 7.
\* Safety: no two different values can both be chosen (agreement).

EXTENDS Integers, FiniteSets

CONSTANTS Values, Proposers, Acceptors, Ballots

VARIABLES
    maxBal,     \* maxBal[a]: highest ballot acceptor a has promised
    maxVBal,    \* maxVBal[a]: ballot of highest accepted proposal
    maxVal,     \* maxVal[a]: value of highest accepted proposal
    msgs        \* set of all messages sent

vars == <<maxBal, maxVBal, maxVal, msgs>>

\* ---- Topology ----
Earth == {"e1","e2","e3","e4","e5"}
LEO   == {"l1"}
Moon  == {"u1"}
Mars  == {"m1","m2","m3"}

Tier(n) == CASE n \in Earth -> "Earth"
           []   n \in LEO   -> "LEO"
           []   n \in Moon  -> "Moon"
           []   n \in Mars  -> "Mars"

TiersOf(S) == {Tier(n) : n \in S}
AllTiers == {"Earth","LEO","Moon","Mars"}

\* Phase 1 quorum: spans all 4 tiers, size >= 7
IsQ1(S) == S \subseteq Acceptors /\ TiersOf(S) = AllTiers /\ Cardinality(S) >= 7

\* Phase 2 quorum: any 4-of-5 Earth nodes
IsQ2(S) == S \subseteq Earth /\ Cardinality(S) >= 4

\* ---- Messages ----
\* Phase1a: proposer sends prepare(bal)
\* Phase1b: acceptor responds with promise(bal, maxVBal, maxVal)
\* Phase2a: proposer sends accept(bal, val)
\* Phase2b: acceptor responds with accepted(bal, val)

Send(m) == msgs' = msgs \union {m}

\* ---- Actions ----

\* Proposer p sends Phase1a for ballot b
Phase1a(p, b) ==
    /\ Send([type |-> "1a", bal |-> b, proposer |-> p])
    /\ UNCHANGED <<maxBal, maxVBal, maxVal>>

\* Acceptor a responds to Phase1a
Phase1b(a) ==
    /\ \E m \in msgs :
        /\ m.type = "1a"
        /\ m.bal > maxBal[a]
        /\ maxBal' = [maxBal EXCEPT ![a] = m.bal]
        /\ Send([type |-> "1b", bal |-> m.bal, acc |-> a,
                 maxVBal |-> maxVBal[a], maxVal |-> maxVal[a]])
        /\ UNCHANGED <<maxVBal, maxVal>>

\* Proposer p completes Phase1 for ballot b, picks value, sends Phase2a
Phase2a(p, b) ==
    /\ ~ \E m \in msgs : m.type = "2a" /\ m.bal = b  \* only once per ballot
    /\ \E Q \in SUBSET Acceptors :
        /\ IsQ1(Q)
        /\ \A a \in Q : \E m \in msgs :
            m.type = "1b" /\ m.bal = b /\ m.acc = a
        \* Pick value: if any acceptor in Q has accepted, use the one with highest ballot
        /\ LET promises == {m \in msgs : m.type = "1b" /\ m.bal = b /\ m.acc \in Q}
               maxAccBal == CHOOSE mb \in {m.maxVBal : m \in promises} :
                   \A m \in promises : m.maxVBal =< mb
           IN IF maxAccBal = -1
              THEN \E v \in Values :
                   Send([type |-> "2a", bal |-> b, val |-> v, proposer |-> p])
              ELSE LET chosen == CHOOSE m \in promises : m.maxVBal = maxAccBal
                   IN Send([type |-> "2a", bal |-> b, val |-> chosen.maxVal, proposer |-> p])
    /\ UNCHANGED <<maxBal, maxVBal, maxVal>>

\* Acceptor a responds to Phase2a
Phase2b(a) ==
    /\ \E m \in msgs :
        /\ m.type = "2a"
        /\ m.bal >= maxBal[a]
        /\ maxBal'  = [maxBal  EXCEPT ![a] = m.bal]
        /\ maxVBal' = [maxVBal EXCEPT ![a] = m.bal]
        /\ maxVal'  = [maxVal  EXCEPT ![a] = m.val]
        /\ Send([type |-> "2b", bal |-> m.bal, val |-> m.val, acc |-> a])

\* ---- Spec ----

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

\* ---- Safety: Agreement ----
\* A value v is "chosen" if there exists a Q2 quorum where all members accepted v at the same ballot.
Chosen(v) ==
    \E b \in Ballots : \E Q \in SUBSET Acceptors :
        /\ IsQ2(Q)
        /\ \A a \in Q : \E m \in msgs :
            m.type = "2b" /\ m.bal = b /\ m.val = v /\ m.acc = a

Agreement == \A v1, v2 \in Values : (Chosen(v1) /\ Chosen(v2)) => v1 = v2

===============================================================================
