# NINeS Problem-Statement Repair

Date: 2026-08-04

## Purpose

Repair one demonstrated accessibility failure: a human networking reader could
not state the paper's problem after reading the abstract and introduction.

## Scope

Make exactly two localized prose replacements in
`docs/paper/nines/main.tex`:

1. Replace the abstract's jargon-heavy opening with a plain-language account
   of the operational shortcut, why it worked in conventional Paxos, and why
   flexible quorum rules allow acquisition and commit capability to separate.
2. Replace the introduction's second paragraph with an explicit statement of
   the problem, the paper's three questions, and a direct causal boundary for
   the GitHub example.

Do not change the GitHub narrative, LogDevice bridge, theorem, experiment,
wall, section order, evidence, terminology after its first definition, claims,
or scope.

## Approved text

Abstract opening:

> Operators often use the number of reachable replicas to decide whether a
> consensus system can make progress. In conventional Paxos configurations,
> this shortcut worked because the same majority rule commonly governed two
> different actions: acquiring authority and committing a value. Flexible
> quorum systems may use different rules for those actions. A reachable set can
> therefore support one while being unable to support the other, even though
> the protocol remains safe.

Introduction replacement:

> The problem we study is that a single health or quorum answer can conceal two
> different consensus capabilities. Given the replicas currently reachable,
> can a proposer acquire authority? Can an authorized proposer commit? Flexible
> quorum systems can answer these questions differently, yet conventional
> health summaries do not identify which capability remains. We ask which
> mixed states a quorum design permits, how an operator can recognize the
> current state from the configured quorums and observed connectivity, and what
> that state does (and does not) say about recovery. The GitHub incident did
> not necessarily involve this specific mechanism; it illustrates the broader
> danger of treating a correct control-plane answer as evidence of every
> capability the service requires.

The remainder of the abstract begins with the existing containment result.

## Verification

- Run the focused claim-language and anonymization tests.
- Rebuild with `latexmk` and require no undefined references, citations,
  multiply defined labels, or overfull boxes.
- Inspect page 1 to ensure the abstract and introduction remain legible.
- Confirm the diff changes only the two approved prose locations and the
  generated PDF.
