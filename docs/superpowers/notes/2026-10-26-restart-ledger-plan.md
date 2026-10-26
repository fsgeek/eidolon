# The restart ledger: Eidolon's lane (plan, not a registration)

**Written:** 2026-10-26 by the Eidolon steward (Claude Opus 5.5), on the first
day of its tenure. This is a plan for Tony and the cloudflare instance to try to
break. Nothing in it is a prediction; each row below gets its own
pre-registration before any run.

## Why this, and why here

The HotOS question is a stopwatch question: *why don't these papers compare
their recovery against a full restart?* The survey answers "they don't"
(0/20 compare recovery to restart). One measured system does not make the
argument, though. The paper needs a small table: several systems, each
measured against the restart baseline on time and on safety, under a crash
and under a false suspicion.

Within the ayllu, tupay twins models and implementations, and cloudflare
develops the authority theory. Nobody owns the table. In the last week the
restart arm has been left out of two registrations that were supposed to
include it:

- The fourth-system rule (cloudflare `measure/FOURTH-SYSTEM-SELECTION.md`)
  says the drawn system is measured for "safety against the idealized restart
  spec, time against that system's empirical restart, crash **and** false
  suspicion". FDBKeeper's registration (F1-F3) tests only the cleanup race. It
  has no restart arm.
- The Caerus isolation round (C1-C3, Q1-Q3) tests whether the repaired
  recovery path works. It has no restart arm either.

Both rounds are worth running as registered. Neither one fills a row of the
table. **Eidolon's job: the table.**

## The method (reused, not reinvented)

I'll use cloudflare's round-1/round-2 measures unchanged, so the rows are
comparable (`cloudflare/docs/hotos/measure/PREREG.md`, `PREREG-round2.md`):

- **T_regain:** time from the fault to the start of the first 1 s window
  whose committed-ack rate is at least 90% of the median pre-fault rate.
- **Safety:** after quiescence, every acknowledged write is present (or
  superseded by a later acked write) on every live node.
- **Stale reads,** where the system has a read path.
- **Arms:** CRASH; FALSE SUSPICION (the old holder is alive and reachable by
  clients, healed after 30 s); RESTART-idealized (everything is restarted from
  durable state at the fault instant), with a **break-even detection delay** D
  reported rather than chosen; and RESTART-empirical, where the system's own
  restart procedure differs from the idealized one.

Each system needs an adapter: a workload that records acknowledgements, a
fault injector, and an export of durable state. Each system gets its own
registration.

## Rows

| System | Recovery arm | Restart arm | Status |
|---|---|---|---|
| HashiCorp Raft | measured (cloudflare r1, r2) | measured: idealized 1.2 s beats recovery 1.95-2.45 s (Q10 falsified); restart pays stale `local` reads | **done** (cloudflare) |
| Caerus | measured: no regain (T_regain = inf, 1 or 2 regions killed) | measured 2026-10-26: idealized 32-36 s; five-region restart plateaus at 0.82 within about 30 s; no state kept (code) | **done at pmr 0**; pmr 30 recovery cell pending C1 (`2026-10-26-caerus-restart-arm-results.md`) |
| FDBKeeper | F1-F3 test the race only | none | owed under the draw rule |
| next draws | - | - | pool continued with the stamped generator |

**The Caerus row** needs one cheap registered measurement. Restart all six
regions at the fault instant with the existing harness (RUNTIME=1200, the
no-CPU-pinning change, prefixed containers), then measure T_regain and confirm
A5 by checking acked writes after restart. The likely verdict is "neither arm
works: recovery never regains, restart regains with nothing". That is still a
row, and a strong one: the system's fault-tolerance claim fails on both axes.

**The FDBKeeper row:** register a restart arm alongside F1-F3 as an additive
registration, as Q1-Q3 were added to C1-C3. Codex's predictions stay as they
are.

*FDBKeeper feasibility (checked 2026-10-26):* the paper's artifact
(`DASE-iDDS/FDBKeeper`, 58c2ff7) is a ClickHouse fork, a multi-hour build.
The evaluation repo has no failure-injection scripts (survey coding 8-616).
The authors have since moved the keeper to a standalone repo,
`DASE-DASLab/fdbkeeper-service` (pushed 2026-03-06): CMake, C++20, `memory`
and `fdb` backends. Building that one is far cheaper, but it is a
**substitution**. Whoever builds first (F1-F3 or this arm) must record which
code was used and diff the cleaner/session paths (KeeperCleaner.cpp:182-288,
KeeperSession.cpp:42-143) against the paper's artifact before any trial. The
restart arm's meaning differs from Raft's here: server state lives in FDB, so
"restart" means restarting the keeper processes over durable FDB, and the
recovery arm under test is FDBKeeper's own lease/cleaner path (client crash
and slow client), not FDB's failover.

**More rows:** continue the fourth-system generator (`random.Random(20261016)`,
pool without 8) for a fifth and sixth system, under the rule cloudflare already
stamped: a documented, bounded build attempt before any redraw. Two more rows
give the paper five systems: a production library, two research prototypes,
and two others. Five is enough for a HotOS table and too few for a census.
That is the right size.

## Two strata, fixed before the next draw (2026-10-26)

**Drawn stratum (what published systems look like).** I'll continue
cloudflare's stamped generator exactly. `r = random.Random(20261016)`;
`r.choice([4,7,8,10,11,14,18,19])` reproduces the fourth system (8, verified
before writing this). Without reseeding, the fifth system is
`r.choice([4,7,10,11,14,18,19])` and the sixth is `r.choice` over the
remainder without the fifth. The unbuildable-redraw rule is cloudflare's,
unchanged. One script, run once, in the commit after this one:
`experiments/ledger_draw.py`.

**Purposive stratum (what people run), labelled purposive in every table.**
Criterion: the consensus store that a default Kubernetes control plane
depends on. That is **etcd**. Chosen for deployment, not for any expected
outcome. Its restart arm is directly comparable to the Raft row. HashiCorp Raft
(cloudflare's row) already belongs to this stratum by the same reasoning
(Consul, Nomad, Vault). Any further purposive pick needs its criterion
written here before the system is named.

Order: the Caerus row is done, then FDBKeeper (Codex builds), then the fifth
and sixth draws. **etcd waits for tupay's D1.** D1's beacon chooses among
etcd-io/raft, hashicorp/raft and tikv/raft-rs, and its method depends on
keeping knowledge sealed. Eidolon does no etcd measurement or code reading
until the beacon has drawn. If D1's target is etcd, Eidolon waits for D1 or
picks a different purposive system under a criterion written beforehand
(`docs/requests/2026-10-26-to-tupay-etcd-ordering.md`). Drawn: libDSE (fifth),
Sonata (sixth) (`results/ledger_draw.txt`).

**Before any public use of the Caerus row:** the author email
(`docs/outbound/2026-10-14-caerus-authors-draft.md`) goes first, and their
answer goes into the row.

## What this plan deliberately leaves out

- **Authority loci, fence taxonomies, the layered statement.** Those belong
  to cloudflare and to the follow-on paper. A row can *mention* which locus
  failed when it is obvious. No row is chosen or scored for authority.
- **Repairing systems.** The Caerus isolation round is the one exception,
  because it gives the frame's "two verdicts": the same system before and
  after a repair, both measured against restart. Its restart arm comes from
  the Caerus row above, which it shares.

## Order of work

1. Caerus restart arm: register, run, score. Cheap, and it reuses the harness.
2. Caerus isolation round (C1-C3 + Q1-Q3). *Revised the same day, after
   reading the full registration:* it requires an independent replay checker
   validated on seeded faulty histories, transaction-identity logging, eight
   controls and debugger samples, which is days of instrumentation. A fresh
   Codex instance implements and runs it (brief:
   `docs/requests/2026-10-26-to-codex-caerus-isolation-implementation.md`),
   and the Eidolon steward audits it before any fault trial. Eidolon holds the
   opposing bet (Q1-Q3), which makes it the right auditor. If no Codex
   instance is available by 2026-11-02, the steward implements it.
3. FDBKeeper: build, the race round (F1-F3), and the restart arm.
4. Draw the fifth system.

## How this plan could be wrong

- If the HotOS paper ends up being only the survey plus the Raft row, the
  table is follow-on-paper work and its urgency drops. That is Tony's and
  cloudflare's framing call. The table is useful either way, so the order of
  work does not depend on it.
- If research prototypes routinely have no durable state (Caerus), every
  restart arm fails safety trivially and the table reads as "prototypes are
  prototypes". The honest answer is to report it and draw a production system
  for balance. Do not drop the rows.
