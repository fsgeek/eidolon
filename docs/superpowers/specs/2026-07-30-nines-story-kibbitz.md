# Story Kibbitz: Notes on the Narrative Revision

**Date:** 2026-07-30
**Author:** Claude (Fable 5, terminal session in this repo; same model family as the desktop reviewer — treat convergences with prior Claude reviews as correlated, per the cross-family audit discipline)
**Responds to:** `2026-07-30-nines-narrative-voice-revision-design.md` and `docs/paper/nines/main.tex` as of commit 60651cb
**Status:** Advisory. Findings are ranked; everything below either changes a revision decision or protects something from regression. No quota was harmed in the making of this document.

Triage frame (per Tony): the gate is story coherence and submission. Items are
sorted into what belongs in the revision now vs. the post-submission
exploration week.

## In scope for the revision

### 1. The title's concept is unfunded (primary finding)

The paper is titled *Legible Consensus*, and:

- The draft never defines legibility. The word appears in the intro only
  inside Contribution 3's label.
- `main.tex` (Liveness: Reading the Wall) says "This instantiates the
  legibility definition of Section 1" — **that definition does not exist**.
  Dangling reference.
- The abstract never uses the word.
- The design doc does not contain "legible" or "legibility" anywhere.

The briefing's thesis had two halves: *allocate the affordable unit of
coincidence to the anchor, and compensate every other tier with legibility.*
The design kept the first half ("choose where coincidence matters most") and
dropped the second — the half the title is named after. Either fund the title
(define legibility in §1; let the compensation idea close the paper) or
retitle. Recommendation: fund it. A reviewer who asks "what does 'legible'
mean here?" must find an answer.

### 2. Valence-after-wall ordering needs one forward reference

Design §5 (wall positive results + residual Moon/Mars (0,1) exposure)
precedes §6 (the reversal: (0,1) is the state that blocked 50/50 seeds). The
reader evaluates the wall's design lesson before learning the residual gap is
the costly one, and §5's win is retroactively hollowed. The abstract's early
disclosure mitigates this. Cheap fix: one clause in §5 naming residual (0,1)
as *the state §6 shows is the expensive one*, so the reversal reads as
designed rather than as bait-and-switch. If the session record shows this
ordering was argued through deliberately, this finding dissolves.

### 3. Small sweep items for the rewrite pass

- **Headline per-tier table:** the Mars "undefined †" during-blackout cell is
  a hole in the central result's table, and reviewers read tables before
  text. Consider reporting the 1800 s point (0% observed directly), demoting
  the cadence explanation.
- **First paragraph:** gloss "acquire proposal authority" with "leader
  election" at first use. It is the PC's native phrase; the §2 parenthetical
  is too late.
- **Abstract:** "spend that coincidence" uses the noun before any coincidence
  has been introduced as one. Small antecedent stumble in the two sentences a
  bidding PC member actually reads.
- **Correlated demand:** "demand for Phase 1 is correlated with its price"
  lost the briefing's operational imagery. One concrete image in the author's
  register (the insurer whose claims all arrive in the hurricane that flooded
  its reserves) would carry this for a networking PC — only if it survives
  the voice model naturally.

### 4. Protect from regression (already right; the rewrite must keep them)

- "On the ease of the proof" paragraph — the daring register done correctly;
  the finding claimed, not apologized for.
- LEO-faster-than-Earth beat: "wall position determines obligations; the
  speed of light determines their cost."
- "The wall is the blueprint; the network is the building site."
- The opposite-remediations paragraph (automated failover restarting the one
  serving incumbent makes fresh service impossible) — the operational money
  shot for this PC; must survive reordering at full prominence.
- The odd-cluster-convention observation (phase symmetry free at odd n, costs
  one node at even n) is a free "retrospective explaining why past designs
  succeeded" — currently two buried sentences in §3; the design's §2 ("Why
  one count once worked") is its natural home.

## Exploration week (post-submission; recorded so they don't silently vanish)

- **Census** (Cassandra witness, Kafka false positive, etcd/ZooKeeper
  predicted-clean): absent from draft and design. The paper currently names
  zero production systems. If cut deliberately, record the decision; if not,
  it is the highest-value half-column this PC could receive, pending the
  Cassandra version pin.
- **Dual-dashboard figure** (one blackout run rendered twice: node-health all
  green vs. capability quadrants): the briefing's centerpiece, in nobody's
  current checklist. The thesis as a picture, and the recorded talk's
  centerpiece. If pre-submission slack exists, it pays better than anything
  else on this list.
- Scarcity lemma enumeration, prevalence half-day, arXiv v2 hygiene items:
  per briefing, unchanged.

## Outcomes (added 2026-07-30, after Codex's design update in commit 880f8ab)

- **#1 legibility:** Addressed beyond what was asked — dedicated definition
  section, intro placement in the success criteria. The *compensation half*
  of the briefing thesis was **rejected with reasons**, and the rejection is
  right: "compensates" smuggles an adequacy claim (that a readout offsets a
  capability) which the registered gradient cannot support — Moon and Mars
  retain (0,1) at every k. "Legibility supplies interpretation where
  construction reaches its limit" keeps the load-bearing role without the
  unfalsifiable rhetoric. Conceded.
- **#2 forward reference:** Adopted, with a correct epistemic guard (no
  intrinsic valence assigned in §5).
- **#3 sweep items:** Odd-cluster promoted to §2; leader-election gloss and
  coincidence-before-spending adopted. On the Mars table cell, **Codex
  corrected my fix**: my suggestion would have mixed a 1800 s Mars cell into
  a 900 s table. The design now requires a single common condition or none.
  My error, their catch — cross-family review ran in both directions.
- **#4 protect list:** Adopted with an improvement — insights preserved at
  full prominence *without freezing wording*, aphorisms still subject to
  voice review.
- **Insurance image:** Rejected (silently) — consistent with this document's
  own caveat that it ships only if it survives the voice model naturally.
- **Census / dashboard figure:** Decisions now recorded in the design
  (post-submission pending version pin; reconsider after page count
  settles). That recording was the actual request.

Verdict: satisfied. The design after 880f8ab is stronger than this kibbitz
asked for in two places (the unmixed-table rule, the de-rhetoricized
legibility role).
