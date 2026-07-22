# Task 7 regeneration delta report (Steps 3-4)

**Scope:** compare the three sweeps regenerated with repaired code (sitting
uncommitted in the working tree at the start of this session) against the
numbers currently published in `docs/paper/nines/main.tex` (numbers are
identical in `docs/paper/main.tex`). Nothing has been committed. Nothing
outside this file and the scratchpad was written.

**Verdict scheme:** `unchanged` (same to displayed precision) /
`moved-within-CI` (new mean differs from old mean but falls inside the old
mean's 95% CI, or vice versa) / `claim-affected` (new value falls outside
the old CI — the paper's stated number is now contradicted by the
regenerated data).

---

## 0. Headline finding (read this first)

**`results/step9/step9_sweep.csv` + `_ci.csv` no longer contain "Flat
Phase 1" data, and current tooling cannot regenerate that data at all.**

The traceability appendix (`docs/paper/nines/main.tex:603`) cites
`results/step9/` as the artifact for the claim "Flat construction: 0% during
blackout across all 18 sweep points." Step 2's regeneration
(`experiments/step9_sweep.py`, which imports `demo_step_9.py`) has
overwritten that path with **crumbling-wall** behavior (100% during-blackout
success, ~0.183s latency — see §2 below), because `demo_step_9.py`'s global
proposer has hard-coded `initiator_tier=3` since commit `dfc26fc`
(2026-03-18) and there is no flat-quorum code path left in the current
codebase to regenerate from. Confirmed directly: new `results/step9/`
seed=40/186s/300s row now reads `during_success=3,during_total=3,
avg_global_latency=0.182579` — materially the same construction as
`results/step9_crumbling/step9_sweep.csv`'s own seed=40 row
(`during=3/3, avg_latency=0.182681`), not the old flat baseline
(`during=0/0, avg_latency=368.87`, reproduced exactly from commit `aae70f7`
in the prior session — see §1).

This is a structural problem, not a numeric one: it cannot be waved through
as "moved within CI" because there is no CI to compare — the old artifact's
*construction* is gone from this path, and Step 2 as scoped (regenerate
`results/step9/step9_sweep.csv` with current `experiments/step9_sweep.py`)
cannot produce anything else, because current `demo_step_9.py` has no way to
turn tiered Phase 1 back off. Reproducing the flat baseline going forward
requires either (a) checking out `demo_step_9.py`, `datacenter.py`,
`entity.py`, `paxos.py`, `quorums.py` from `aae70f7` into an isolated
snapshot (as the prior Task 7 session did, in
`/tmp/.../scratchpad/prov-aae70f7/`) and running from there, or (b) restoring
a flat-quorum code path behind a flag. Neither happened as part of this
task; see §1 for what *is* known about `aae70f7`'s reproducibility.

**A second, independent provenance problem, predating this session:**
`git show HEAD:results/step9/step9_sweep.csv` (raw) reproduces the flat
`aae70f7` numbers exactly (`during_total=0`, `avg_latency=368.870033` for
seed 40), but `git show HEAD:results/step9/step9_sweep_ci.csv` (aggregate,
same commit) already shows `during_rate_mean=1.0`,
`avg_global_latency_s_mean=0.183072` — i.e. **the checked-in aggregate file
does not match the checked-in raw file it's supposed to summarize**, and
already carried crumbling-wall numbers *before* this session touched
anything. `git log --oneline -- results/step9/step9_sweep_ci.csv` shows its
most recent commit is `dfc26fc`, one later than the raw file's `aae70f7` —
someone regenerated/overwrote the aggregate independently of the raw file at
some point in this repo's history and both got captured in the same bulk
commit. The prior Task 7 session's provenance check (Step 1) verified only
the raw CSV against `aae70f7`; it never checked the aggregate against it,
which is how this went unnoticed. Net effect: **the paper's tab:flat-vs-wall
"Flat Phase 1... 0.0%" claim was already unsupported by the checked-in
aggregate CSV before this task ran**, and after Step 2 it is unsupported by
either file.

---

## 1. Provenance (Step 1, carried forward + extended)

From the prior session's `repairs-task-7-report.md` (verified by extracting
each commit's exact five dependency files into isolated scratch dirs and
re-running the pinned seed/config, not by inference):

| Artifact | Producing commit | Reproduced exactly? |
|---|---|---|
| `results/step9_crumbling/step9_sweep.csv` | `dfc26fc` (2026-03-18) | Yes — every field matches to displayed precision (seed=40, both `blackout_only` and `with_repeater` rows) |
| `results/step9/step9_sweep.csv` (raw, at `aae70f7`) | `aae70f7` (2026-03-14) | Yes — every field matches to displayed precision |

New finding this session (§0): `results/step9/step9_sweep_ci.csv` (aggregate)
does **not** reproduce from `aae70f7` — it was last touched at `dfc26fc` and
already contained crumbling-wall numbers at HEAD, inconsistent with its own
sibling raw file. Not independently re-derived in this session (out of
scope for Steps 3-4), but the mismatch is directly visible by diffing the
two committed files, both shown in §0.

**BLOCKED contract for Step 1 does not trigger** — both named commits are
real, identifiable, and exactly reproducible. The problem is not "we can't
tell what generated it," it's "what's currently on disk at `results/step9/`
no longer represents what the paper's prose and appendix say it represents."

---

## 2. tab:flat-vs-wall (`docs/paper/nines/main.tex:370-378`)

| Row | Metric | Old (paper) | New | Verdict |
|---|---|---|---|---|
| Crumbling wall (Earth-initiated, all 18 pts) | During | 100.0% | 100.0% (all 18/18 `blackout_only` rows, `results/step9/step9_sweep_ci.csv`) | unchanged |
| Crumbling wall | Latency | 0.183s | 0.18257 – 0.18339s across all 18 points | unchanged |
| Flat Phase 1 (all tiers required) | During | 0.0% | **no longer regenerable from `results/step9/`** — see §0 | **claim-affected** (artifact-level, not numeric) |

The crumbling-wall half of this table holds up perfectly under
regeneration — genuinely reassuring, zero-variance across 50 seeds as
claimed, at every one of the 18 (mars_latency × blackout_duration) points,
both `blackout_only` and `with_repeater` scenarios. It is the flat-construction
half whose *artifact* is gone, not its historically-verified numbers (those
still exist, reproduced under `aae70f7` — see §1 — just not at the path the
paper's traceability table names).

---

## 3. tab:pertier (`docs/paper/nines/main.tex:387-399`), full-coverage topology, 186s/900s

Source: `results/tier_liveness/tier_sweep_full_ci.csv`, row key
`(blackout_only, full_coverage, tier, 186.0, 900.0)`.

| Tier | Metric | Old (paper / old CSV) | New | Verdict |
|---|---|---|---|---|
| Earth | During | 100.0% ± 0.0 | 100.0% ± 0.0 | unchanged |
| Earth | Latency | 0.183s | 0.18326s | unchanged |
| Earth | Rec. lag | **62.6s** (CSV: 62.556930 ± 0.008457) | **68.114200 ± 0.008844** | **claim-affected** (+5.56s, ~9%, CIs don't overlap) |
| LEO | During | 100.0% ± 0.0 | 100.0% ± 0.0 | unchanged |
| LEO | Latency | 0.131s | 0.13110s | unchanged |
| LEO | Rec. lag | **61.8s** (CSV: 61.834200 ± 0.005484) | **67.226926 ± 0.006505** | **claim-affected** (+5.39s, CIs don't overlap) |
| Moon | During | 100.0% ± 0.0 | 100.0% ± 0.0 | unchanged |
| Moon | Latency | 5.131s | 5.13141s | unchanged |
| Moon | Rec. lag | **6.7s** (CSV: 6.705218 ± 0.008878) | **27.099389 ± 0.009201** | **claim-affected** (4x, CIs don't overlap) |
| Mars | During | 0.0% ± 0.0 | (undefined — `during_total=0` at this exact param point; see §5) | see §5, not a paper-quoted-number regression |
| Mars | Post | 0.0% ± 0.0 | **100.0% ± 0.0** (n=0 CI seeds shown; raw shows 50/50 seeds all succeed) | **claim-affected, in the good direction** — this is the repair working: first-ever Mars-initiated global success. The paper's own prose ("Mars fails during blackout... Mars also fails before blackout") is now only half true; post-blackout Mars succeeds under full coverage. |

The per-tier success/latency numbers hold up (During and Latency columns
unchanged for Earth/LEO/Moon to displayed precision), but **the entire
"Rec. lag" column has shifted outside its old CI for every tier that has
one**, and the direction and magnitude vary a lot (Earth/LEO +9%, Moon
+300%). All CIs, old and new, are tiny (~0.005-0.01s over 50 seeds), so
these are not noise — something about the repaired temporal-budget/blackout
scheduling (Tasks 1-4) systematically shifted when the first post-blackout
reconciliation attempt lands relative to `blackout_end`. This is consistent
with the paper's own framing of Rec. lag as "scheduling artifacts of the
120s reconciliation interval interacting with blackout boundaries, not
protocol properties" (`main.tex:410`) — the *mechanism* the paper describes
is unchanged, but the specific numbers it plugs into that mechanism
(6.7s/62.6s/61.8s) are now wrong, and the prose sentence that names them
directly (`main.tex:410`, footnote) needs rewriting regardless of which
verdict category the raw numbers get filed under.

Prose at `main.tex:404`: "Mars also fails before blackout... Mars-to-Earth
round-trip time (≈372s per phase) exceeds the 500s per-phase timeout" — the
recon plan's self-review notes flag this "372s/500s" sentence as
deliberately deferred pending Task 7's numbers (spec item 3). New data:
Mars full-coverage `avg_latency_s_mean` at 186s/900s is **752.495681s**
(latency, not the phase timeout itself), and Mars `during_total=0` at that
exact point (no attempts land in the "during" bucket — see §5's discussion
of Mars's own attempt cadence being much slower than 120s). This sentence
needs the human author's attention regardless of this report's verdict;
flagging per the deferred item, not re-deciding it here.

---

## 4. tab:sparse (`docs/paper/nines/main.tex:419-430`), 186s/900s

| Tier | Wall prediction | Old | New | Verdict |
|---|---|---|---|---|
| Earth | works | 100.0% ± 0.0% | 100.0% ± 0.0% | unchanged |
| LEO | works (Wall) / fails (network) | 0.0% ± 0.0% | 0.0% ± 0.0% | unchanged |
| Moon | works | 100.0% ± 0.0% | 100.0% ± 0.0% | unchanged |
| Mars | blocked | 0.0% ± 0.0% | **0.0% ± 0.0%** | unchanged — see §5, this is expected/correct, not a bug |

This table's headline numbers are all unchanged. (Rec. lag for Earth/Moon in
the sparse topology shows the same +5.6s / +20.4s shift documented in §3 for
full coverage — sparse Earth 62.557652→68.123177, sparse Moon
6.711116→27.097140 — same underlying cause, same claim-affected status,
just not tabulated separately since tab:sparse doesn't carry a latency/rec.
lag column.)

---

## 5. Sparse-Mars diagnosis (requested explicitly — is Mars-sparse-zero a bug?)

**Root cause: no bug. Sparse Mars is 0% pre-, during-, *and* post-blackout
because its network topology gives it a direct link to only 2 of the 5
Earth ground stations, while the global proposer's Phase 2 quorum is
strict (requires responses from all 5 Earth nodes) and the network model
does not route — an unlinked pair simply drops packets. This is
independent of blackout state and independent of the Task 1 Mars-LEO
Phase 1 fix.**

Evidence:

1. **The topology, by construction, only gives Mars 2/5 Earth links in the
   base ("sparse") build.** `demo_step_9.py:200-207`:
   ```python
   for earth_loc in ["na-west", "europe"]:
       for i in range(3):
           network.add_link(earth_loc, f"mars-{i}", latency=mars_base_latency_s, jitter=5.0)
   ...
   for i in range(3):
       network.add_link("leo-sat", f"mars-{i}", latency=mars_base_latency_s, jitter=5.0)
   ```
   Only `na-west` and `europe` get direct Mars links in the base topology
   (plus the Task-1 Mars-LEO link, which is unconditional — present in
   *both* sparse and full-coverage). `asia`, `sa-east`, `africa` are absent.
   `experiments/tier_liveness_sweep.py:126-138` (`_add_full_coverage_links`)
   adds exactly those three missing links when `full_coverage=True`:
   ```python
   # Mars missing: asia, sa-east, africa
   for loc in ["asia", "sa-east", "africa"]:
       for i in range(3):
           network.add_link(loc, f"mars-{i}", latency=mars_base_latency_s, jitter=5.0)
   ```
2. **`DatacenterNetwork` does not route.** `datacenter.py:33-35`: "If no
   link exists between two locations, packets are dropped (unreachable)."
   Single-hop only.
3. **Phase 2 for the global wall quorum is strict by default in this
   sweep.** `experiments/tier_liveness_sweep.py`'s `_wire_system_multitier`
   constructs `wall = CrumblingWallQuorum([mars_ids, [moon], [leo],
   earth_ids])` with no `phase2_threshold` argument, so
   `quorums.py:230-238` defaults it to `len(self.fast_tier) == 5` — Phase 2
   needs **all five** Earth nodes to respond. `quorums.py:294-296`:
   `is_phase2_quorum` returns `len(respondents & fast_tier_set) >=
   phase2_threshold`.
4. **Consequence, confirmed by raw CSV + a fresh short run:** In sparse,
   Mars can never physically reach `asia`/`sa-east`/`africa`, so it can
   never assemble a 5-of-5 Earth Phase 2 quorum — pre-blackout, during, or
   post — regardless of the Mars-LEO Phase 1 repair (which only fixes
   *Phase 1* reachability, not Phase 2). Raw CSV
   (`results/tier_liveness/tier_sweep_full.csv`), sparse/186/900/seed=40:
   `pre_success=0,pre_total=1, during_success=0,during_total=1,
   post_success=0,post_total=2` — fails everywhere. Full-coverage, same
   seed/params: `pre_success=1,pre_total=1, ..., post_success=2,post_total=2`
   — succeeds everywhere it's attempted. A fresh short diagnostic run this
   session (`uv run python experiments/tier_liveness_sweep.py
   --mars-latencies-s 186 --blackout-durations-s 300 --seeds 42`, verbose
   output) reproduces the same pattern: sparse `mars 0/1 0/0 0/2`, sparse
   `leo 0/1 0/0 0/4`, full-coverage `mars 1/1 0/0 3/3`, full-coverage
   `leo 8/8 3/3 26/26`.
5. **This is the same structural story the paper already tells for LEO**
   (`main.tex:432-434`, sec:reachability): "strict Phase 2 requires all
   five Earth nodes, and LEO has links to only three ground stations." Mars
   generalizes that exact mechanism with 2/5 instead of 3/5. Sparse-Mars
   staying at 0% pre/during/post is *expected, structurally-predicted
   behavior*, matching the already-published tab:sparse row
   (`0.0% ± 0.0%`) — old and new. Not a repair regression, not a bug, no
   BLOCKED condition here.

What the Task 1 repair *did* change is full-coverage Mars going from 0%
(structurally impossible pre-repair, no Mars-LEO Phase 1 path existed at
all) to 100% post-blackout (§3) — the first time Mars-initiated global
consensus has ever succeeded in this codebase, once it's given all 5 Earth
links to satisfy strict Phase 2 as well as the LEO link to satisfy Phase 1.

---

## 6. tab:relaxed (`docs/paper/nines/main.tex:462-473`)

Source: `results/step10/step10_sweep_ci.csv`.

| Row | Metric | Old (paper) | New | Verdict |
|---|---|---|---|---|
| strict/std/0 | Earth-local | 100.0% | 100.0% | unchanged |
| strict/std/0 | Global during | 100.0% | 100.0% | unchanged |
| strict/std/0 | Global post | 100.0% | 100.0% | unchanged |
| strict/std/0 | **Recovery (s)** | **488.9 ± 2.6** | **93.10974 ± 0.00994** | **claim-affected** (5.25x, CIs don't remotely overlap) |
| strict/std/1 | Earth-local | 100.0% | 100.0% | unchanged |
| strict/std/1 | Global during | "—" (undefined, 0/0 in old raw) | 0.0% ± 0.0 (well-defined) | unchanged in substance (both mean "no successful during-blackout attempt"); formatting difference only |
| strict/std/1 | Global post | 0.0% | 0.0% | unchanged |
| 4-of-5/std/0 | Earth-local | 100.0% | 100.0% | unchanged |
| 4-of-5/std/0 | Global during/post | 100.0%/100.0% | 100.0%/100.0% | unchanged |
| 4-of-5/std/0 | **Recovery (s)** | **489.9 ± 2.5** | **94.26520 ± 0.00994** | **claim-affected** |
| 4-of-5/std/1 | Earth-local | 100.0% | 100.0% | unchanged |
| 4-of-5/std/1 | Global during/post | 100.0%/100.0% | 100.0%/100.0% | unchanged |
| 4-of-5/std/1 | **Recovery (s)** | **489.1 ± 2.1** | **94.26281 ± 0.00970** | **claim-affected** |
| 3-of-5/std/2 | **Earth-local** | **49.7%** | **60.5234% ± 0.0057%** (tiny CI both eras) | **claim-affected** — the specific number the prose at `main.tex:479` quotes ("drops to 49.7%") is now wrong; the qualitative claim (q1=4 unformable with 3 surviving Earth nodes) is presumably still true but the reported magnitude isn't |
| 3-of-5/std/2 | Global during | 98.0% ± 3.9% | 100.0% ± 0.0% | moved-within-CI (100 is inside [94.1, 101.9]) |
| 3-of-5/std/2 | Global post | 100.0% | 100.0% | unchanged |
| 3-of-5/std/2 | **Recovery (s)** | **512.7 ± 2.0** | **94.64042 ± 0.00857** | **claim-affected** |
| 3-of-5/maj/2 | Earth-local | 100.0% | 100.0% | unchanged |
| 3-of-5/maj/2 | **Global during** | **92.0% ± 7.6%** | **100.0% ± 0.0%** | **claim-affected** — 100 falls outside the old CI [84.4, 99.6] |
| 3-of-5/maj/2 | Global post | 100.0% | 100.0% | unchanged |
| 3-of-5/maj/2 | **Recovery (s)** | **513.4 ± 2.2** | **94.63384 ± 0.00918** | **claim-affected** |

The **entire Recovery (s) column** — every row that has a defined value —
dropped by roughly 5x (488-513s old → 93-95s new), with CIs on both sides
too tight (2-3s old, ~0.01s new) for this to be anything but a real,
systematic effect of the repair. Supporting signal: `avg_global_latency_s`
for these same rows dropped from ~368-395s to ~0.18-0.29s (not itself
paper-quoted, but the underlying per-round latency this global proposer
sees) — consistent with the pre-repair code's global proposer round-trips
somehow still being gated by Mars-scale delay even though these rows are
described as "Earth-initiated... both phases... touch only Earth nodes"
(`main.tex:477`). That description is accurate for the *repaired* code (new
avg_latency ~0.18-0.29s is exactly Earth-cross-continental scale); the old
numbers (~370-390s, close to a Mars round trip) suggest the pre-repair
`step10_sweep.py` config was not actually Earth-isolated in practice,
plausibly for the same reasons Tasks 1-4 exist (incomplete blackout
severing / missing routes). This session did not root-cause the Recovery
column shift further — only the Mars-sparse item was in scope for full
root-causing — but the magnitude and consistency across all five defined
rows make it very unlikely to be sampling noise.

---

## 7. Transition-bucket counts (does the reclassification matter?)

`experiments/step9_sweep.py`'s own CSV (`results/step9/step9_sweep.csv`)
carries no `transition` column at all — only `pre`/`during`/`post` — so
step9 offers no transition evidence either way.
`experiments/tier_liveness_sweep.py`'s raw CSV
(`results/tier_liveness/tier_sweep_full.csv`, 7200 rows) does track
`transition_success`/`transition_total`, and the totals are **nonzero and
substantial**:

| Tier | transition_total (sum) | transition_success (sum) |
|---|---|---|
| Mars | 2100 | 592 |
| Moon | 600 | 600 |
| LEO | 1700 | 0 |
| Earth | 0 | 0 |
Overall: **4400 of the swept attempts (across all seeds/params/scenarios)
land in the "transition" bucket**, not pre/during/post — i.e., roughly 15%
of all attempt-rows (4400 of ~28800 total attempt-classifications) straddle
a blackout boundary closely enough that `classify_attempt` (Task 3's 4-way
split) puts them in their own bucket rather than force-assigning them to
pre/during/post. **The reclassification matters**: an older 3-way
classifier that had to pick pre/during/post for every attempt would have
had to arbitrarily assign ~15% of attempts to one of those buckets, which
would directly distort during/post rates for exactly the tiers (Mars, LEO)
whose liveness numbers this paper's central claims depend on.

---

## 8. Wall-clock timing (approximate, from file mtimes — not directly logged)

The three sweeps were already regenerated, uncommitted, before this session
started; no per-sweep timer was captured at generation time. File mtimes on
the six output CSVs give a rough proxy for completion order and inter-sweep
gaps (assuming roughly back-to-back sequential execution, per the brief):

| Sweep | Completed | Gap since previous completion |
|---|---|---|
| step10 | 2026-07-21 18:04:30 PDT | (start time unknown — no baseline) |
| step9 | 2026-07-21 18:52:24 PDT | 47m54s |
| tier_liveness | 2026-07-21 19:45:58 PDT | 53m34s |

This gives step9 ≈ 48 minutes and tier_liveness ≈ 54 minutes wall-clock (if
run started immediately after the prior sweep finished, with no gap for
setup); step10's own duration is unrecoverable from mtimes alone since
there's no earlier sweep's completion time to anchor it. All three are
CPU-only, single-process runs of 50-seed × 9-or-18-config sweeps.

---

## 9. Claim-affected rows (full list, paper line touched)

1. **`main.tex:603` (traceability appendix)** — "Flat construction: 0%...
   `results/step9/` (prior sweep data)" — artifact no longer contains flat
   data; construction is gone from current tooling. (§0, §2)
2. **`main.tex:396`** — tab:pertier Earth Rec. lag: 62.6s → 68.11s.
3. **`main.tex:397`** — tab:pertier LEO Rec. lag: 61.8s → 67.23s.
4. **`main.tex:398`** — tab:pertier Moon Rec. lag: 6.7s → 27.10s.
5. **`main.tex:410`** — prose citing "6.7s for Moon vs. 62.6s for Earth" —
   both numbers stale (items 2, 4 above).
6. **`main.tex:404`** (deferred spec item 3, "≈372s per phase") — flagged
   per existing plan note, not newly re-decided here; new Mars-tier data is
   now available for whoever picks this up (§3).
7. **`main.tex:467`** — tab:relaxed strict/std/0 Recovery: 488.9±2.6 →
   93.11±0.01.
8. **`main.tex:469`** — tab:relaxed 4-of-5/std/0 Recovery: 489.9±2.5 →
   94.27±0.01.
9. **`main.tex:470`** — tab:relaxed 4-of-5/std/1 Recovery: 489.1±2.1 →
   94.26±0.01.
10. **`main.tex:471`** — tab:relaxed 3-of-5/std/2 Recovery: 512.7±2.0 →
    94.64±0.01.
11. **`main.tex:471`** — tab:relaxed 3-of-5/std/2 Earth-local: 49.7% →
    60.52%.
12. **`main.tex:472`** — tab:relaxed 3-of-5/maj/2 Global during: 92.0%±7.6%
    → 100.0%±0.0% (outside old CI).
13. **`main.tex:472`** — tab:relaxed 3-of-5/maj/2 Recovery: 513.4±2.2 →
    94.63±0.01.
14. **`main.tex:479`** — prose "drops to 49.7%" — stale (item 11 above).
15. (Positive/expected, still claim-affected by definition) **`main.tex:399`**
    — tab:pertier Mars Post: 0.0%±0.0 → 100.0%±0.0 under full coverage
    (§3) — the repair delivering its intended effect, but the paper's own
    text ("Mars also fails before blackout... Mars fails during blackout...")
    needs revision to reflect post-blackout Mars success.

Rows NOT affected (moved-within-CI or unchanged), for completeness: tab:flat-vs-wall
crumbling-wall row (unchanged, §2); tab:pertier During/Latency columns for
Earth/LEO/Moon (unchanged, §3); tab:sparse in full (unchanged, §4);
tab:relaxed 3-of-5/std/2 Global during (moved-within-CI, §6); all other
tab:relaxed Earth-local/during/post cells not listed above (unchanged, §6).

---

## 10. Attribution (commit-wise ablation)

Interior boundaries: **T1-topology** = `7f8555f` (Mars-LEO route only),
**T2-controller** = `b46a786` (+ full blackout severing), **T3-regimes** =
`5ca4da0` (+ containment regime classification). **PRE** = the report's own
"old" column (committed CSVs, pre-repair code, effectively `aae70f7`/paper
values). **HEAD** = the report's own "new" column (fully repaired code,
includes T4 time-budget scaling and later fixes on top of T3). All six
ablation runs (T1/T2/T3 × {step10, tier}) returned `ok: true` — no cell
below is "no data."

**Method note on precision:** T1/T2/T3 values below are quoted to the
precision the ablation runs reported (often 6+ significant figures); PRE and
HEAD are quoted to the precision used elsewhere in this report. Where T1,
T2, T3 agree to 4+ significant figures, that is treated as "unchanged
across this range," not coincidence — the CIs on both sides of every jump
below are two-plus orders of magnitude smaller than the jump itself.

### 10a. tab:relaxed — Recovery (s)

| Scenario | PRE | T1 (`7f8555f`) | T2 (`b46a786`) | T3 (`5ca4da0`) | HEAD | Verdict |
|---|---|---|---|---|---|---|
| strict/std/0 | 488.9 ± 2.6 | 62.5644 | 62.5644 | 62.5644 | 93.1097 ± 0.0099 | **Moves at two boundaries.** T1 drops it 488.9→62.56 (−87.2%); flat across T1→T2→T3 (identical to 4+ sig figs — T2's blackout-severing and T3's regime classifier contribute nothing here); then a second, smaller rise 62.56→93.11 (+48.8%) happens strictly between T3 and HEAD. |
| 4-of-5/std/0 | 489.9 ± 2.5 | 63.5131 | 63.5131 | 63.5131 | 94.2652 ± 0.0099 | Same two-boundary pattern: T1 −87.0%, flat T1–T3, post-T3 +48.4%. |
| 4-of-5/std/1 | 489.1 ± 2.1 | 63.5087 | 63.5087 | 63.5087 | 94.2628 ± 0.0097 | Same two-boundary pattern: T1 −87.0%, flat T1–T3, post-T3 +48.4%. |
| 3-of-5/std/2 | 512.7 ± 2.0 | 63.9095 | 63.9095 | 63.9095 | 94.6404 ± 0.0086 | Same two-boundary pattern: T1 −87.5%, flat T1–T3, post-T3 +48.1%. |
| 3-of-5/maj/2 | 513.4 ± 2.2 | 63.9141 | 63.9141 | 63.9141 | 94.6338 ± 0.0092 | Same two-boundary pattern: T1 −87.6%, flat T1–T3, post-T3 +48.1%. |
| strict/std/1 | undefined (0 successes) | undefined (NaN, no recovery observed) | undefined (NaN) | undefined (NaN) | undefined (0.0% during, no recovery) | Never defined at any boundary — no jump to attribute. |

**Causal verdict:** the Recovery(s) column moves at **two separate
repairs, in opposite directions, with near-identical magnitude across all
five populated rows** (drop ≈87%, rise ≈48%): (1) **T1-topology
(`7f8555f`)** is where the Mars-round-trip-scale numbers (~490–513s)
collapse to Earth-scale (~62–64s) — consistent with the report's own §6
hypothesis that pre-repair `step10_sweep.py` wasn't actually Earth-isolated
in practice; whatever bundled with the Mars-LEO route commit fixed that.
(2) A second, independent uplift (~62–64s → ~93–95s) happens **strictly
between T3 and HEAD** — inside the "T4 time-budget scaling + later fixes"
bundle — and **cannot be isolated further**: no ablation point exists
between `5ca4da0` and HEAD, so it is attributed to that whole bundle, not a
named commit.

### 10b. tab:relaxed — Earth-local rate

| Scenario | PRE | T1 | T2 | T3 | HEAD | Verdict |
|---|---|---|---|---|---|---|
| 3-of-5/std/2 | 49.70% | 49.7003% | 49.7003% | 49.7003% | 60.5234% ± 0.0057% | Unchanged across PRE→T1→T2→T3 (identical to 4 decimal places — none of the three named boundaries touch this number). Moves only **post-T3, before HEAD**. **Cause not isolated**: no ablation point exists between `5ca4da0` and HEAD; attributable only to "T4 time-budget scaling / later fixes" as a bundle. |

All other tab:relaxed Earth-local cells are unchanged at every boundary
(PRE=T1=T2=T3=HEAD=100.0%) and are omitted here — nothing to attribute.

### 10c. tab:relaxed — Global during rate

| Scenario | PRE | T1 | T2 | T3 | HEAD | Verdict |
|---|---|---|---|---|---|---|
| 3-of-5/maj/2 | 92.0% ± 7.6% | 100.0% | 100.0% | 100.0% | 100.0% ± 0.0% | **Moves at T1**, and only T1 — 92.0→100.0 is already outside the old CI at the very first boundary, then flat 100.0% through T2, T3, and HEAD. Cause: **T1-topology (`7f8555f`)**, cleanly isolated. |
| 3-of-5/std/2 | 98.0% ± 3.9% | 100.0% | 100.0% (ci95=0.0) | 100.0% | 100.0% ± 0.0% | Same T1-caused move (98.0→100.0), but small enough that 100.0 still falls inside the old 98.0±3.9 CI at every boundary — hence the main report's §6 "moved-within-CI" verdict. Cause is still **T1-topology**; it's just not claim-breaking. |

### 10d. tab:pertier — per-tier Recovery lag (full-coverage, 186s/900s)

| Tier | PRE | T1 | T2 | T3 | HEAD | Verdict |
|---|---|---|---|---|---|---|
| Earth | 62.5569 ± 0.0085 | 62.5636 | 62.5669 | 62.5669 | 68.1142 ± 0.0088 | Flat PRE→T1→T2→T3 (spread of ~0.01s, consistent with seed noise, not a repair effect). Moves only **post-T3** (+5.5473s, +8.9%). **Cause not isolated** beyond the T4+/later bundle — no ablation point between `5ca4da0` and HEAD. |
| LEO | 61.8342 ± 0.0055 | 61.8387 | 61.8370 | 61.8370 | 67.2269 ± 0.0065 | Same pattern: flat through T3, jumps only **post-T3** (+5.3899s, +8.7%). **Cause not isolated** beyond T4+/later. |
| Moon | 6.7052 ± 0.0089 | 6.7045 | 6.7010 | 6.7010 | 27.0994 ± 0.0092 | Same pattern: flat through T3 (~6.70s), jumps **post-T3** to nearly 4x (+20.3984s, +304%) — the largest relative move in the whole report. **Cause not isolated** beyond T4+/later. |

**Bonus, not one of the four requested tables but directly relevant to §9
item 15 (Mars Post 0.0%→100.0%):** the tier ablation data shows Mars
full-coverage Post already at 100.0% at **T1** (`7f8555f`: post=1.0;
`T2`/`T3`: post=1.0 too) — i.e. the Mars-Earth-global-success repair is
cleanly attributable to **T1-topology**, the Mars-LEO Phase-1 route fix,
exactly as the mechanism story in §5 predicts. (Mars `during`/recovery
figures are noisier across T2→T3 — `with_repeater/full_coverage/mars`
recovery drops 1339.6s(T1)→~294–302s(T2/T3) and `during_rate` swings
0.0→0.99→[low-n 0.0] — but `Post`, the number §9 item 15 actually names, is
stable at 100% from T1 onward, so that specific claim's cause is isolated.)

---

## 11. Flat baseline under the repaired harness

**Genuine historical flat data** (`git show aae70f7:results/step9/step9_sweep_ci.csv`,
mars_base_latency_s=186, blackout_duration_s=900) vs. **new flat data under
the repaired harness** (`results/step9_flat/step9_sweep_ci.csv`,
`--global-quorum flat`, 50 seeds):

| Scenario | Metric | OLD genuine flat (`aae70f7`) | NEW flat (repaired harness) | Verdict |
|---|---|---|---|---|
| blackout_only | During | 0.0% ± 0.0% | 0.0% ± 0.0% | unchanged |
| blackout_only | Post | 100.0% ± 0.0% | 100.0% ± 0.0% | unchanged |
| blackout_only | first_success_after_blackout_s | 597.71 ± 1.06 | 756.45 ± 1.22 | **claim-affected** (+158.74s, +26.6%; CIs disjoint: old range [596.65,598.77], new range [755.23,757.67]) |
| blackout_only | avg_global_latency_s | 368.82 ± 0.34 | 368.69 ± 0.26 | unchanged (ranges overlap: old [368.48,369.16], new [368.43,368.95]) |
| with_repeater | During | 100.0% ± 0.0% | 100.0% ± 0.0% | unchanged |
| with_repeater | Post | 100.0% ± 0.0% | 100.0% ± 0.0% | unchanged |
| with_repeater | first_success_after_blackout_s | 264.42 ± 66.2 | 209.95 ± 2.41 | moved-within-CI (new mean 209.95 falls inside old's wide CI [198.22, 330.62]) |
| with_repeater | avg_global_latency_s | 388.12 ± 0.40 | 394.20 ± 0.33 | **claim-affected** (+6.08s, +1.6%; CIs disjoint: old range [387.72,388.52], new range [393.87,394.53]) |

The qualitative story survives intact — during/post success rates identical
in both eras, both scenarios still recover after blackout, `with_repeater`
still recovers dramatically faster than `blackout_only` — but two of the
four quantitative metrics per scenario (first-success timing for
`blackout_only`, and average latency for `with_repeater`) sit outside the
old CI under the repaired harness, so any paper prose quoting exact
flat-mode timing/latency numbers from the `aae70f7` era needs the new
figures.

This comparison is **separate from, and does not resolve,** §0/§2's
artifact-level problem: the currently-committed `results/step9/` path still
cannot regenerate flat data at all (no code path), and the currently
committed `results/step9/step9_sweep_ci.csv` still mismatches its own raw
sibling and mislabels wall-mode data as flat. The port review (five
independently-reverified points, all holding) establishes that
`results/step9_flat/` — a new path, from a new `--global-quorum flat` flag
— is a faithful *construction*-level port of the historical flat quorum
logic; it does not and cannot retroactively fix `results/step9/`.

**What the paper's flat-vs-wall table should now cite:** `results/step9_flat/`
(repaired harness, `--global-quorum flat`) as the **live row** for any Flat
Phase 1 numbers going forward. The `aae70f7` genuine-flat numbers should be
retained only as **provenance history** (to show construction continuity,
per the port review), not re-cited as the current artifact — `results/step9/`
itself remains unable to produce flat data and should not be named in the
traceability appendix without either restoring a flat code path there or
repointing the citation at `results/step9_flat/`.

---

## 12a. Bundle closure by commit elimination (added after ablation synthesis)

The "post-T3 bundle" in §12 collapses to **T4 alone**. Every commit between
`5ca4da0` (T3) and `2547b2e` (the regeneration point) except `fc81aeb` (T4)
is non-behavioral for the sweeps: `2d53ef3` touches only docs/step9-repro.md,
`187c881` only tex/bib/pdf, `8f7df5b` only a sys.path import shim. The flat
port (`5974fdd`) landed after regeneration ran. Therefore all post-T3 numeric
moves are attributable to T4's time-budget scaling, with a legible mechanism:
at 186s Mars latency T4 scales the pre-window 600→930s, shifting blackout
start and end by 330s relative to the 120s reconciliation cadence. Recovery
lag — time from blackout end to the next successful cadence-aligned attempt —
is quantized by that cadence, so lags jump by tens of seconds when the phase
alignment changes (Moon 6.7→27.1, Earth 62.6→68.1, tab:relaxed ~64→~94).
Paper consequence: exact recovery-lag values are cadence-alignment artifacts
to first order; prose should report them as cadence-quantized (bounded by
one reconciliation interval plus round time), not as intrinsic constants.

**Caveat on the §10c T1 attribution (92%→100%):** operationally the jump
occurs at T1, but the claimed mechanism (Mars-LEO routing) is suspicious for
an Earth-initiated global round: Earth-quorum formation never needed Mars or
LEO. Adding links also perturbs the seeded jitter-draw sequence, so PRE's
92% may have been a timing-fragile artifact that T1's RNG realignment
dissolved rather than a routing effect. The robust citable fact is the
repaired-harness value (100%±0, 50 seeds); the old 92% was already labeled
obsolete by the spec. A cheap probe if mechanism matters: rerun PRE
(`9947fe3`) with a disjoint seed set — variance there would confirm fragility.

## 12. One number whose cause could not be isolated

Every `claim-affected` row that moves strictly between `5ca4da0` (T3) and
HEAD — all three tab:pertier Rec. lag cells (Earth, LEO, Moon), the
tab:relaxed 3-of-5/std/2 Earth-local rate, and the second (smaller) half of
every tab:relaxed Recovery(s) jump — shares the same fate: **the cause
cannot be isolated to a specific commit**, because no ablation checkpoint
exists between the T3-regimes commit (`5ca4da0`) and HEAD; the "time-budget
scaling (T4) and later fixes" boundary was never itself ablated as a
separate interior point, only bookended. All of these are flagged
"post-T3, cause bundle T4+/later, not further isolated" rather than
attributed to a named commit.

---

## ESCALATION VERDICT: **BLOCKED**

Multiple rows are `claim-affected` (§9, 15 items), including one
artifact-level break (the Flat Phase 1 baseline is no longer regenerable
from `results/step9/`, §0) and a systematic ~5x shift in every populated
"Recovery (s)" cell in tab:relaxed plus every "Rec. lag" cell in tab:pertier.
Per the Step 4 contract, this is returned to the human author with this
report; no commit was made (per Step 5's own gate and per this task's
explicit instructions). `results/step9_crumbling/` was left untouched, as
directed.

**Attribution status (§10–§12), added this pass:** every claim-affected row
now carries either a named cause or an explicit "cause not isolated" flag —
none are unexamined.

- **Named, single boundary:** tab:relaxed 3-of-5/maj/2 Global-during
  (92.0%→100.0%) and Mars Post (0.0%→100.0%, §9 item 15) both isolate
  cleanly to **T1-topology (`7f8555f`)**, the Mars-LEO Phase-1 route fix.
- **Named, two boundaries (say so, per instruction):** every populated
  tab:relaxed Recovery(s) cell (items 7–10, 13) moves **twice** —
  ~87% drop at **T1-topology**, flat through T2/T3, then a further ~48%
  rise **post-T3** (T4+/later, bundle-only).
- **Cause not isolated (bundle-only, no interior ablation point exists):**
  all three tab:pertier Rec. lag cells (items 2–4), the prose at
  `main.tex:410` restating them (item 5), the tab:relaxed 3-of-5/std/2
  Earth-local rate (item 11) and its prose echo (item 14), and the
  second-half rise of every Recovery(s) cell above — all attributed only
  to "post-`5ca4da0`, T4 time-budget scaling + later fixes" as an
  unresolved bundle (§12).
- **Not attributable via this ablation at all:** the flat-construction
  artifact break (item 1, §0/§11 — a code-path/provenance problem, not a
  numeric drift the T1/T2/T3 commits could move) and the deferred
  `main.tex:404` "≈372s per phase" sentence (item 6, still awaiting the
  human author per the recon plan's own deferral, now with Mars-tier data
  available in §3).

This does not change the BLOCKED verdict — it sharpens what the human
author is blocked on: the T1-topology-caused rows are fully explained and
mechanistically sound (Mars-LEO routing was the fix in both cases); the
post-T3 rows are real, consistent, and large, but still need either a
finer-grained ablation between `5ca4da0` and HEAD or the original author's
knowledge of what T4 specifically changed before the paper's prose can cite
a mechanism for them the way `main.tex:410` currently does for the old
(now-wrong) Rec. lag numbers.
