# NINeS Narrative Map

**Design:** `docs/superpowers/specs/2026-07-30-nines-narrative-voice-revision-design.md`
**Manuscript baseline:** `docs/paper/nines/main.tex` before narrative revision
**Repository baseline:** `nines-2027` at `72955ef`; 117 tests passed in 24.82 seconds. Claude's feedback is tracked at `docs/superpowers/specs/2026-07-30-nines-story-kibbitz.md`.

## Voice observations

- Starts from an observation, question, or concrete example before abstraction.
- Explains why one observation led to the next, including false starts and changes of interpretation.
- Uses “we” for choices, measurements, and interpretations; first person does evidentiary work rather than merely adding warmth.
- Places limitations beside the claim they limit. Detection tools remain suggestive until the decision rule is stated; unexpected measurements lead to investigation of the mechanism.
- Prefers a concrete mechanism, historical case, or worked example over a polished slogan.
- Allows uneven sentence length, parenthetical thought, and visible intellectual texture.
- States strong conclusions when the evidence is direct, while marking suspicion, expectation, and generalization as such.
- Keeps proofs compressed, experimental conditions adjacent to measurements, and legal cadence out of academic explanation.

These are revision heuristics, not a stylometric target. No wording from the reference corpus is copied. The published papers are the best guide to evidentiary register; the pre-2025 posts are the best guide to question-led explanation; the declaration is useful for qualification placement and mechanism-by-mechanism rebuttal, but its repetition and legal formulas do not belong in the paper.

## Page budget

The counts below are discovery aids, not targets.

| Checkpoint | Source words | TeX `---` | “rather than” | Last body page | Total PDF pages | Change | Decision |
|---|---:|---:|---:|---:|---:|---:|---|
| Baseline (`72955ef`) | 11,024 | 49 | 22 | 13 | 15 | — | Recover at least one body page by removing repetition and duplicated method |
| Introduction pilot | — | 0 in pilot | 0 in pilot | 12 | 14 | -1 body / -1 total | Page budget recovered without compressing the formal or empirical core; punctuation counts are observations, not quotas |
| Formal-core checkpoint | 10,646 | 34 | 16 | 12 | 14 | -378 words from baseline | Four investigative sections now occupy pages 2--6; proof labels and artifact references remain stable |
| Wall/table audit | 10,535 | 29 | 13 | 12 | 14 | -489 words from baseline | Exact wall readings verified from the registered gradient; central per-tier table rendered on page 8 under one 1800-second condition |
| Behavioral reversal | 10,542 | 21 | 11 | 12 | 14 | -482 words from baseline | Registered method, result, mechanism, and limits now occupy one page immediately after the wall boundary; rendered table fits without overfull text |
| Reading the Wall | 9,657 | 11 | 8 | 11 | 13 | -1,367 words from baseline | Readout, method, baselines, liveness, sparse reachability, relaxation, and structural gradient now form one four-page investigation; duplicate discussion and policy prose removed |
| Integrated ending | 9,516 | 1 | 6 | 11 | 14 | -1,508 words from baseline | Open problem, compact limits, and conclusion finish before references on page 11; traceability is split across two readable appendix tables rather than oversized on one page |

## Paragraph map

“Fixed” means the mathematical or empirical content remains fixed; its location may still change. Figure-internal TikZ commands are grouped with their figure rather than assigned separate prose IDs.

| ID | Current location/opening words | Role | Claim IDs | Action | Target section | Reason |
|---|---|---|---|---|---|---|
| P001 | Abstract | interpretation | C01–C17 | rewrite | Abstract | Disclose the answer in the new investigative order; add exact wall positives and legibility boundary |
| P002 | Intro: “An operator facing…” | question | C01 | rewrite | Introduction | Open with the green-dashboard symptom and two operational questions |
| P003 | Intro: “Planetary distance…” | observation | C01, C16 | merge | Introduction | Mars supplies the magnification after the general symptom, not the premise |
| P004 | Intro: “The approximation has…” | interpretation | C02, C03 | rewrite | Introduction | Separate phase symmetry from majority threshold/count/vantage |
| P005 | Intro: “We characterize…” | definition | C04 | rewrite | Introduction | State the exact answer briefly; leave proof for Capability Gaps |
| P006 | Intro: “The directions are…” | measurement | C09, C10, C15 | merge | Introduction | Retain bounded reversal and authority caveat without exhausting the later experiment |
| P007 | Intro: “We apply…” | interpretation | C06–C08, C14–C16 | rewrite | Introduction | Positive wall result must precede boundary; define legibility here |
| P008 | Intro contributions list | interpretation | C04, C07–C10, C14 | rewrite | Introduction | Shorten to three contributions and remove repeated exposition |
| P009 | Background: Classic Paxos | definition | C02, C03 | moved/revised | Why One Count Once Worked | Same-family symmetry now precedes majority's threshold instance |
| P010 | Background: Flexible Paxos | definition | C02–C04 | moved/revised | Why One Count Once Worked | Cross-phase safety now creates room for predicate separation |
| P011 | Background: crumbling walls | definition | C06 | moved/revised | Putting the Gaps on a Wall | Source construction is attributed before the paper's phase split |
| P012 | Background: wall intersection | proof | C06 | merged | Putting the Gaps on a Wall | Original-wall intersection intuition is compressed and kept distinct |
| P013 | Background: composing ideas | interpretation | C06, C08 | moved/revised | Putting the Gaps on a Wall | Anchor safety and participation policy are stated in separate sentences |
| P014 | Capability: construction-independent scope | transition | C04 | rewrite | Capability Gaps | Let loss of coincidence make the generic question necessary |
| P015 | Formability definition and acquisition gloss | definition | C01, C04 | fixed | Capability Gaps | Core notation and single-decree/Multi-Paxos scope are load-bearing |
| P016 | Four joint states | definition | C04 | fixed | Capability Gaps | Names the gaps without intrinsic valence |
| P017 | Proposition: gap emptiness | proof | C04 | fixed | Capability Gaps | Primary formal result |
| P018 | Proposition proof | proof | C04 | fixed | Capability Gaps | Preserve the short monotonicity proof |
| P019 | Corollary: exact correspondence | proof | C04 | fixed | Capability Gaps | Preserve four-way classification |
| P020 | “On the ease of the proof” | interpretation | C04 | rewritten | Capability Gaps | Short proof is explained through monotonicity and the newly non-vacuous question |
| P021 | Uniform threshold case | proof | C03, C05 | moved/revised | Why One Count Once Worked | Odd/even result is beside coincidence and explicitly post-hoc |
| P022 | Generic auditor | measurement | C04 | fixed | Capability Gaps | Keep complexity, independent check, and proof-primary boundary |
| P023 | System model: epochs and connectivity | definition | C01, C16 | fixed | Putting the Gaps on a Wall | Preserve `N_e`/`C_t` membership boundary |
| P024 | Acceptor universe and 5/1/1/3 topology | definition | C06 | fixed | Putting the Gaps on a Wall | Physical vocabulary for the construction |
| P025 | Three consensus scopes | definition | C06 | fixed | Putting the Gaps on a Wall | Prevents global/local ambiguity |
| P026 | Wall rows and tier indexing | definition | C06 | merge | Putting the Gaps on a Wall | Combine with the table and figure introduction |
| P027 | Per-tier Phase 1 scope table | definition | C06 | fixed | Putting the Gaps on a Wall | Compact structural representation |
| P028 | Wall construction figure | definition | C06, C14 | fixed | Putting the Gaps on a Wall | Main geometry figure |
| P029 | Formal Phase 1 family | definition | C06 | fixed | Putting the Gaps on a Wall | Preserve exact quorum family |
| P030 | Non-anchor witnesses as policy | qualification | C06, C08 | fixed | Putting the Gaps on a Wall | Must remain adjacent to family definition |
| P031 | Phase 2 family | definition | C06 | fixed | Putting the Gaps on a Wall | Defines hot-path anchor and later relaxation |
| P032 | Cross-intersection proposition | proof | C06 | fixed | Putting the Gaps on a Wall | Primary safety result |
| P033 | Cross-intersection proof | proof | C06 | fixed | Putting the Gaps on a Wall | Preserve unchanged |
| P034 | Wall-path proof intuition | interpretation | C06 | merge | Putting the Gaps on a Wall | One sentence is enough after the proof |
| P035 | TLA+ explanation | qualification | C06 | merge | Putting the Gaps on a Wall | Retain accessibility; compress generic TLC tutorial if page pressure remains |
| P036 | Paxos model-check scope and counts | measurement | C06 | fixed | Putting the Gaps on a Wall | Preserve exact topology/count and proof/corroboration distinction |
| P037 | Verification-scope limitation | qualification | C06 | fixed | Putting the Gaps on a Wall | Must stay beside model-check claim |
| P038 | Wall liveness rule | definition | C07, C08, C14 | moved | Where the Wall Works, and Where It Stops | Structural readout now follows the section's exact positive result |
| P039 | Mars-blackout reachability | observation | C07, C08 | moved | Where the Wall Works, and Where It Stops | Concrete input remains beside the structural rule |
| P040 | Blackout liveness figure | definition | C07, C14 | moved/recaptioned | Reading the Wall | Now follows the readout algorithm and is labeled as a structural supplied-connectivity example |
| P041 | “This instantiates the legibility definition…” | interpretation | C14, C15 | rewritten/moved | Reading the Wall | Replaced dangling reference with the exact one-pass check and sole-timely-proposer boundary |
| P042 | Per-tier wall predicates | definition | C07, C08 | move | Where the Wall Works, and Where It Stops | Sets up threshold result |
| P043 | Boundary proposition | proof | C07, C08 | moved/fixed | Where the Wall Works, and Where It Stops | Exact containment threshold remains unchanged |
| P044 | Hitting-set arithmetic | proof | C07 | moved/fixed | Where the Wall Works, and Where It Stops | Mechanism and safety connection remain unchanged |
| P045 | Gradient explanation | interpretation | C07, C08 | rewritten | Where the Wall Works, and Where It Stops | Section opens with named `k=3` closures before residual exposure |
| P046 | Gradient table | measurement | C07, C08 | moved/fixed | Where the Wall Works, and Where It Stops | Registered exhaustive result remains intact; both-reading audit continues in Task 4 |
| P047 | “Read the two rows together” | interpretation | C07, C08 | rewritten | Where the Wall Works, and Where It Stops | `k=3` design value now precedes endpoint and all-tier boundary |
| P048 | Self-reachable sensitivity | qualification | C07, C08 | fixed | Where the Wall Works, and Where It Stops | Required scope for LEO result |
| P049 | Relationship to Flexible Paxos | related work | C04, C07 | moved/revised | Related Work | Prior `(1,0)` examples and the present design-time question now sit together |
| P050 | “Legibility is load-bearing” | interpretation | C08, C14 | rewrite | Where the Wall Works, and Where It Stops | Remove compensation implication; legibility explains residual obligation |
| P051 | Wall-specific readout interface | definition | C14–C16 | rewritten/moved | Reading the Wall | Inputs, outputs, unknown authority, uninferred policy, and edge-demonstration boundary now open the section |
| P052 | Meaning of global | qualification | C06, C15 | moved | Putting the Gaps on a Wall | Learned-value scope now closes the construction before capability enumeration |
| P053 | Related work: Paxos/Flexible Paxos | related work | C02, C04 | merge | Related Work | First-use citations already carry definitions; retain relationship here |
| P054 | Related work: containment/RQS | related work | C04 | fixed | Related Work | Essential novelty boundary |
| P055 | Related work: grid/wall/Satrapy | related work | C06 | fixed | Related Work | Preserve geometry and heterogeneous quorum lineage |
| P056 | Related work: geo-consensus | related work | C06, C16 | fixed | Related Work | Practical comparison |
| P057 | Related work: DTN/CRDT | related work | C16 | fixed | Related Work | Preserve scope distinction |
| P058 | Simulator description | definition | C09–C13 | moved | Reading the Wall | General evaluation method now precedes all supporting measurements |
| P059 | Physical topology | definition | C11, C12 | moved | Reading the Wall | Physical conditions remain adjacent to the experiment inventory |
| P060 | Sparse/full variants | definition | C12 | moved | Reading the Wall | Both network inputs are defined before the full/sparse comparison |
| P061 | Four experiment families | transition | C09–C13 | rewritten | What Happens / Reading the Wall | The design section now names three wall experiment families and points separately to the adjacent behavioral experiment |
| P062 | Experimental-design table | definition | C09–C13 | rewritten/moved | Reading the Wall | Per-tier row now distinguishes its 900- and 1800-second conditions |
| P063 | Fixed parameters and CI method | qualification | C09–C13 | moved | Reading the Wall | Shared conditions remain beside the method with full values in the appendix |
| P064 | Valence question and registration | question | C09, C10 | moved/rewritten | What Happens Inside the Gaps | Now follows the residual wall state and states the registered comparison and falsification boundary |
| P065 | Valence experimental setting | definition | C09, C10 | rewritten/artifact-checked | What Happens Inside the Gaps | Distinguishes swept incumbent budgets from the healthy proposer's fixed eight rounds and defines prevented decision from the harness |
| P066 | Valence result table | measurement | C09, C10 | rewritten/artifact-checked | What Happens Inside the Gaps | Conditions are part of each row; primary registered evidence remains compact |
| P067 | “The first two rows…” | measurement | C09, C10 | rewritten | What Happens Inside the Gaps | The registered prediction fails before the recorded-metric equivalence and accepted-value mechanism are reported |
| P068 | “Failure is cheap, success is expensive” | interpretation | C09, C10 | rewritten | What Happens Inside the Gaps | Replaced the slogan with the observed NACK/backoff versus Phase 2 timeout mechanism |
| P069 | “Two things this is not” | qualification | C09, C10 | merged | What Happens Inside the Gaps | Universal-valence and livelock limits now sit in the paragraphs containing their measurements |
| P070 | Multi-Paxos/single-decree scope | qualification | C09, C10, C15 | rewritten/moved | What Happens Inside the Gaps | Delayed versus immediate exposure now leads directly to the opposite-remediation question |
| P071 | Registered deviations | qualification | C09, C10 | rewritten/moved | What Happens Inside the Gaps | All five deviations remain named; theorem and wall independence are explicit |
| P072 | Geometry comparison setup | definition | C11 | moved | Reading the Wall | Opens the geometry/baseline subsection after method |
| P073 | Flat/majority/wall table | measurement | C11 | recaptioned/artifact-checked | Reading the Wall | Caption now identifies sparse 5/1/1/3 topology, initiator, blackout, timeouts, sweep, and seeds |
| P074 | Flat versus wall result | measurement | C11 | retained | Reading the Wall | Calibration remains one concise observation before majority |
| P075 | Majority baseline interpretation | interpretation | C11 | rewritten | Reading the Wall | States matched blackout survival, cross-tier Phase 1 latency, and initiator sensitivity once |
| P076 | Per-tier central-result setup | definition | C11 | rewritten | Reading the Wall | All four rows now use the verified common 1800-second condition |
| P077 | Per-tier table | measurement | C11 | rewritten/artifact-checked | Reading the Wall | Mixed 900/1800 presentation replaced with four registered common-condition rows |
| P078 | Three-of-four summary | measurement | C11 | rewritten | Reading the Wall | Reports Earth/LEO/Moon 100% and observed Mars 0%; no undefined cell remains |
| P079 | LEO/Moon/Earth latency mechanism | interpretation | C11 | fixed | Reading the Wall | Preserve obligation-versus-physical-cost surprise |
| P080 | Mars cadence and physics explanation | qualification | C11 | rewritten | Reading the Wall | Obsolete undefined-cell explanation removed; 1800-second observation and outside-blackout physics retained |
| P081 | Recovery-lag cadence explanation | qualification | C11 | rewritten | Reading the Wall | Common-condition values now accompany the cadence-bound qualification |
| P082 | Sparse reachability setup | transition | C12 | move | Reading the Wall | Follows full-coverage readout |
| P083 | Sparse table | measurement | C12 | recaptioned/artifact-checked | Reading the Wall | Caption now includes topology, blackout, Mars delay, timeout, and seeds |
| P084 | LEO full-to-sparse drop | measurement | C12 | fixed | Reading the Wall | Core network-versus-wall observation |
| P085 | Two-step operator procedure | interpretation | C12, C14, C15 | rewritten | Reading the Wall | Recast as two supplied inputs; the readout performs no detection |
| P086 | Moon and sparse Mars detail | measurement | C12 | fixed | Reading the Wall | Preserve topology-specific explanation |
| P087 | Strict Phase 2 fragility | observation | C13 | moved | Reading the Wall | Opens coordinated relaxation subsection |
| P088 | Relaxed construction | definition | C13 | fixed | Reading the Wall | Preserve `k=4`/`k=3` crash tolerance |
| P089 | Relaxed Phase 1 family | definition | C13 | fixed | Reading the Wall | Preserve cross-intersection formula |
| P090 | Crash-sweep setup | definition | C13 | recaptioned/artifact-checked | Reading the Wall | Full condition set appears before and in the table caption |
| P091 | Crash-relaxation table | measurement | C13 | moved | Reading the Wall | Primary evidence remains intact |
| P092 | Strict crash-intolerance result | measurement | C13 | merged | Reading the Wall | Folded into one evidence-ordered paragraph |
| P093 | Weakest-link migration | interpretation | C13 | rewritten | Reading the Wall | One punch sentence follows the two-crash comparison and its mechanism |
| P094 | Coordinated relaxation | measurement | C13 | merged | Reading the Wall | Exact 3-of-5 plus local-majority recovery remains beside the table |
| P095 | Tradeoff summary table | definition | C13 | fixed | Reading the Wall | Compact reusable result |
| P096 | “All results are design-level” | qualification | C16 | merged | Limitations | Opens one compact model-scope paragraph instead of repeating the abstract |
| P097 | Anchor concentration | qualification | C06, C13 | rewritten | Limitations | The one-anchor dependency now sits beside the multi-anchor research candidate |
| P098 | Tier bundles properties | qualification | C16 | retained | Limitations | Prevents overgeneralized edge mapping |
| P099 | Abstract network model | qualification | C11, C12 | consolidated | Limitations | Orbital, link, scheduling, and fault omissions are named once |
| P100 | Stylized workload | qualification | C11 | merged | Reading the Wall / Limitations | Cadence stays beside recovery measurements and receives one brief reminder later |
| P101 | Single topology | qualification | C06–C08 | consolidated | Limitations | Structural and measured scopes remain distinct |
| P102 | Crash-stop only | qualification | C06 | consolidated | Limitations | Crash-stop scope and omitted Byzantine behavior share the model-scope paragraph |
| P103 | Per-tier crash tolerance untested | qualification | C13 | retained | Limitations | The Earth-initiator boundary remains explicit |
| P104 | Inter-tier obligation vs replication | interpretation | C06, C13 | rewritten/moved | Reading the Wall | Shortened consequence now closes coordinated relaxation; deployment-overclaim language removed |
| P105 | Capability/authority/contract table | qualification | C15 | merged | Reading the Wall | Replaced prescriptive state table with explicit interface outputs and exclusions |
| P106 | Opposite remediations | interpretation | C09, C10, C15 | move | What Happens Inside the Gaps | Operational surprise belongs immediately after measured reversal |
| P107 | Leadership hierarchy | interpretation | C06, C14 | rewritten/moved | Reading the Wall | Closed-form count is now a compact design-time readout, separate from availability |
| P108 | Quorum-count gradient figure | measurement | C06, C14 | moved/recaptioned | Reading the Wall | Preserves 992/496/248/217 and labels the figure structural rather than stochastic |
| P109 | WPaxos contrast | related work | C06 | merged | Related Work | Removed duplicate post-figure comparison; existing Related Work paragraph carries it |
| P110 | Terrestrial mapping | qualification | C16 | moved/revised | What Remains Unsolved | The edge mapping is a next experiment, explicitly not evidence |
| P111 | Future-work candidates | transition | C17 | rewritten | What Remains Unsolved | Scoped authority, gap-aware behavior, and multi-anchor families are unevaluated candidates |
| P112 | Conclusion: coincidence and correspondence | interpretation | C02–C10 | rewritten | Conclusion | Returns through symmetry, count-readability, and containment without inventory cadence |
| P113 | Conclusion: Mars and mitigation | interpretation | C14–C17 | rewritten | Conclusion | Ends with the two-question diagnostic and performance-preserving open problem |
| P114 | Simulator-parameters table | definition | C09–C13 | fixed | Appendix | Preserve values; update only cross-reference or verified condition drift |
| P115 | Traceability introduction | qualification | C04–C17 | retained | Appendix | Defines executable-artifact scope and environment once |
| P116 | Traceability table | measurement | C01–C17 | rebuilt/artifact-checked | Appendix | Names common conditions, exact wall positives and residuals, experiment bounds, post-hoc status, demonstration limits, and artifact-free future work |

## Claim ledger

| Claim ID | Claim | Kind | Exact scope/evidence | Citation or artifact | Must remain adjacent |
|---|---|---|---|---|---|
| C01 | Acquisition and commit are distinct formability questions over a supplied reachable set | definition | `R_1(C)` and `R_2(C)` over fixed epoch universe `N_e`; actual commit also needs authority and execution | Formal definitions in paper | First-use acquisition gloss; authority caveat |
| C02 | Phase symmetry makes the two formability predicates equal | formal | Same quorum family for Phase 1 and Phase 2; equality for every connectivity state | `lamport1998`; `howard2016` context | Distinguish equality from node health and threshold counting |
| C03 | Majority makes the equal predicate decidable by one reachable-node threshold from a stated vantage | formal | Threshold family; reachable set defined from one proposer/operator vantage | `lamport1998`; uniform corollary | Phase symmetry is the cause of equality; majority is the familiar count-readable instance |
| C04 | Containment gives the exact four-way gap correspondence | theorem + executable audit | Any nonempty finite phase families over fixed `N_e`; pinned domain optional; proof primary; 129,032 exhaustive three-node checks and 16 registered wall cases | `quorum_audit.py`; `experiments/quorum_audit.py`; `results/capability/quorum_audit_registered.json`; `guerraoui2010rqs` for prior containment distinction | Proof, monotonicity, prior-work boundary, deterministic witness scope |
| C05 | Cost-minimal threshold symmetry costs 0 extra participants at odd `n` and 1 at even `n` | post-hoc structural result | 80 valid threshold configurations, `n=3..7`; post-hoc, not preregistered | `results/capability/dual_uniform.csv`; `experiments/capability_dual_sweep.py` | Post-hoc label and safety condition `q_1+q_2 >= n+1` |
| C06 | The wall preserves cross-phase safety with Earth as anchor; other witnesses encode participation policy | proof + model checking | 5/1/1/3 construction; all tiers × strict/`k=4`/`k=3`, 27,921 intersection states complete; reduced Paxos model 67M states complete | `tla/QuorumIntersection.tla`; `tla/ExhaustiveIntersection.tla`; `tla/PaxosSmall.tla`; `peleg1995` | Proof-primary statement; protocol-spec scope; witness-policy qualification |
| C07 | At `k=3`, `(1,0)` is absent everywhere; both gaps are absent for Earth under both readings and for LEO under self-reachable reading | structural enumeration | All `2^10` connectivity states; every tier and `k`; unconstrained and self-reachable columns verified directly in `dual_gradient_map.csv` | `results/capability/dual_gradient_map.csv`; `experiments/capability_dual_sweep.py` | Gap, threshold, tier, and reading in every “closes/prevents” statement |
| C08 | Moon and Mars retain `(0,1)` at every `k`; no tested `k` closes both gaps at every tier | structural enumeration | Registered gradient verified under both readings; residual caused by downward participation obligations, not by legibility | Same as C07 | Positive wall result first; no intrinsic valence or compensation claim |
| C09 | In the registered single-decree experiment, `(1,0)` matched a healthy `(1,1)` contender on recorded metrics and supplied the decided value | preregistered measurement | 5 arms × 4 incumbent maximum-round budgets × 50 seeds; healthy proposer fixed at 8 rounds; treatment and healthy-contender rows both have completion 1.000, median 3.420 s, 2 rounds, and 7 NACKs at every incumbent budget; treatment decides `incumbent-1`; byte-identical rerun | `results/flip/flip_map.csv`; `results/flip/flip_sweep.csv`; `flip.py`; `experiments/flip_sweep.py`; `experiments/flip_verdict.py` | Recorded-metric equivalence only; one topology and flip site; accepted-value mechanism; no universal benignity |
| C10 | In the same experiment, `(0,1)` prevented healthy-proposer decision in 50/50 seeds when both proposers had eight rounds | measurement in preregistered experiment | Early and late `(0,1)` arms both record 0 completions at incumbent budget 8; healthy proposer exhausted its fixed 8 rounds; 48--49 NACKs; bounded retry exhaustion under modeled policy | Same as C09 | Both budgets, policy, topology, one severed link, and Multi-Paxos distinction; no intrinsic-valence or livelock claim |
| C11 | Full-coverage wall per-tier liveness is Earth/LEO/Moon 100% and Mars 0% under a common 1800-second blackout; LEO latency is lower than Earth's | measurement | Verified rows: `blackout_only`, full coverage, 186 s Mars delay, 1800 s blackout, 50 seeds; latencies Earth 0.182859, LEO 0.131199, Moon 5.131478, Mars 753.184341 s; recovery 9.390115, 8.146090, 3.022124, 1351.408681 s | `results/tier_liveness/tier_sweep_full_ci.csv`; `experiments/tier_liveness_sweep.py` | Common condition for all four cells; cadence qualification; obligation/cost distinction |
| C12 | Sparse reachability makes LEO drop from 100% to 0% despite the same wall obligation | measurement | Sparse versus full topology, 186 s Mars delay, 900 s blackout, 50 seeds | Same per-tier artifacts as C11 | Network reachability and wall structure are separate supplied inputs |
| C13 | Relaxing global and Earth-local quorums moves the weakest liveness constraint and can restore two-crash progress | measurement | Earth-initiated, repeater-assisted, 186 s Mars delay, 900 s blackout, 50 seeds; exact `k` and local quorum per table row | `results/step10/step10_sweep_ci.csv`; `experiments/step10_sweep.py` | Exact configuration; no claim for unevaluated per-tier crash behavior |
| C14 | The wall is legible from a supplied connectivity summary in `O(tiers)` time | definition + demonstration | Read phase capabilities and failed obligations from compact wall representation without subset enumeration or protocol execution | `experiments/capability_readout.py`; planetary example JSON | Definition in Introduction; known-input assumption |
| C15 | Legibility does not supply detection, current authority, recovery policy, client contract, or operator action | interface boundary | Runtime authority reported unknown; service policy not inferred | Capability readout artifact and paper table | Adjacent to definition and operational interpretation |
| C16 | The characterization applies structurally to edge-shaped systems; the supplied edge case is not terrestrial evaluation | demonstration / scope | Deterministic edge input only; no terrestrial latency, prevalence, or availability measurement | `examples/capability/edge_remote_01.json`; `experiments/capability_readout.py` | “Not an evaluated result” beside mapping |
| C17 | Extending coincidence farther without restoring inter-tier hot-path latency remains open | future work | No efficacy claim for leases, gap-aware behavior, or multi-anchor families | No artifact; explicitly future work | Avoid impossibility language and new solution claims |

## Section implication audit

| Revised section | Likely reader inference | Causal? | General? | Novel? | Empirical? | Evidence permits it? | Correction |
|---|---|---|---|---|---|---|---|
| Introduction | Green dashboards have caused observed production incidents of this exact form | no | diagnostic is general; prevalence is unknown | two-question framing/characterization are contributions | registered behavior only | resolved in pilot | Dashboard is a hypothetical operator-facing puzzle; no frequency, incident-history, or prevalence claim is made |
| Why One Count Once Worked | Majority voting itself made acquisition and commit identical | structural explanation | threshold families from a stated vantage | odd/even observation is post-hoc | enumeration corroborates corollary | resolved in formal core | Phase symmetry supplies equality; majority supplies the count-readable threshold; odd/even result is explicitly post-hoc |
| Capability Gaps | Containment itself is newly discovered, or the experiment proves the theorem | formal implication only | any finite nonempty phase families | predicate-level four-way correspondence and auditor, bounded against RQS prior art | exhaustive audit corroborates; proof is primary | yes with prior-work boundary | Cite member-indexed containment and state that behavior experiments do not validate the proof |
| Putting the Gaps on a Wall | Every witness is required by Paxos safety, or the geometry is a deployment recommendation | policy choice affects formability | construction generalizes, evaluation topology does not | phase-decomposed wall application | TLA+ corroborates safety only within stated scopes | resolved in formal core | Earth anchor, policy witnesses, and effect of removing a witness are stated separately; analytical-only boundary remains beside the family |
| Where the Wall Works, and Where It Stops | The wall closes all operationally important gaps, or residual `(0,1)` is intrinsically bad | structural reachability | exact only for named `k`, tier, and reading | reusable design technique plus exact boundary | exhaustive connectivity enumeration | resolved and artifact-checked | Opens with `k=3` closures, names both readings and LEO sensitivity, then Moon/Mars residual exposure; bounded experiment bridge denies intrinsic valence |
| What Happens Inside the Gaps | The registered experiment establishes universal valence or validates the wall | no; theorem and wall are established independently | bounded to topology, one flip site, policy, both proposer budgets, and single-decree protocol | behavioral comparison of constructed states | preregistered 5×4×50 simulation | resolved | The section defines the harness outcome, places limits beside each result, and closes by stating that it measures behavior inside constructed states rather than validating the proof or wall enumeration |
| Reading the Wall | The readout detects failures, identifies a live leader, or chooses safe remediation | readout interprets supplied state | structural interface; measurements remain topology-specific | compact typed readout and joined interpretation | existing topology/baseline/liveness/crash experiments | resolved | Configuration and connectivity are named inputs; authority is unknown, policy is uninferred, the edge JSON is a demonstration, and every measurement retains its local conditions |
| What Remains Unsolved | No performance-preserving closure exists | no | open across structured topologies | question, not result | none | resolved | The section asks whether and where coincidence can be extended and labels all three candidate directions unevaluated |
| Related Work, Limitations, Conclusion | Mars experiments empirically establish terrestrial behavior or research chronology followed narrative order | no | structural recurrence only | contributions bounded above | planetary simulator only | resolved | Related Work states no discovery chronology; Limitations denies terrestrial evaluation and prevalence; Conclusion ends with the diagnostic and open question |

## Contextual style review

| Location | Pattern | Rhetorical work | Keep/rewrite/remove | Reason |
|---|---|---|---|---|
| Abstract | Eight results compressed into one polished arc | Answers early | rewrite | Add exact positive wall result and limits; vary cadence without adding length |
| Introduction P004–P008 | Repeated contrast pivots and three parallel contribution blocks | Establishes thesis repeatedly | rewrite/merge | Section order can carry interpretation once |
| Capability P020 | Defensive aphorism around proof simplicity | Explains significance | rewrite | Preserve observation that the question became non-vacuous after phase separation |
| Construction P030/P037 | Em-dashes carry essential policy/scope qualifications | Bounds claims | mostly keep, recast where natural | Qualifications matter more than punctuation count |
| Wall P041 | Refers to a definition absent from Section 1 | Claims legibility | rewrite | Definition must move to Introduction |
| Wall P047/P050 | Impossibility and “load-bearing” slogans precede concrete design value | Announces interpretation | rewrite | Positive named closures should earn the later boundary |
| Valence P068/P069 | Slogan plus “two things this is not” symmetry | Explains reversal and limits | rewrite/merge | Mechanism and limits can be stated directly beside results |
| Baseline P075 | Dense sequence of “not X; it is Y” distinctions | Separates geometry from slack | rewrite | Preserve all comparisons with fewer rhetorical pivots |
| Per-tier P079 | LEO surprise | Genuine juxtaposition | keep insight, revise cadence only if needed | Obligation and physical cost really diverge here |
| Sparse P085 | “Two-step operator procedure” | Makes result usable | rewrite | Readout interprets known inputs; it does not perform detection |
| Crash P092–P094 | Three bold mini-slogans | Reads table | merge/rewrite selectively | Keep weakest-link migration; remove repeated announcements |
| Discussion P105/P106 | Capability table and opposite remediations | Operational interpretation | move | They become stronger beside readout and valence evidence |
| Discussion P110/P111 | Edge prediction followed by three speculative solutions | Opens future work | rewrite | Keep structural mapping and an open door without efficacy claims |
| Conclusion | Compressed restatement of every contribution | Closure | rewrite | Return to two questions, usable wall result, boundary, and open problem |

## Introduction pilot decisions

- Kept the green-dashboard opening as a hypothetical operational puzzle, without implying a documented incident or measured prevalence.
- Established the two-question vocabulary before the formal predicates: can the system acquire proposal authority, and can it complete a commit?
- Explained coincidence through phase symmetry first; majority is the familiar threshold-readable instance. No resource-spending metaphor is used in the pilot.
- Reported the wall's exact positive results before its Moon/Mars boundary, with the tier, threshold, and connectivity reading stated where needed.
- Defined legibility in the Introduction and placed its interface limits beside it: the readout assumes supplied connectivity and does not detect failures, identify current authority, choose recovery policy, infer client contracts, or prescribe operator action.
- Converted the contribution list into connected prose so the investigation, rather than a repeated inventory, carries the section.
- The pilot happens to contain no TeX em dashes, no “rather than,” and no “not X but Y” construction. This is not a constraint on later prose; each construction remains available when it earns its rhetorical work.
- After external review against the voice corpus, restored the short sentence “A node-health dashboard answers neither” and moved the $R_1/R_2$ notation from the two-question paragraph to the Flexible Paxos paragraph. The questions now live in English before they receive symbols.
- Named the two connectivity readings at first use. The unconstrained reading permits any reachable set; the self-reachable reading requires the initiator's colocated acceptor to be reachable. Added one visual clause explaining the wall as tier rows whose Phase~1 paths read downward toward the Earth Phase~2 anchor.
- Recast “the count worked” and “for so long” to avoid an unsupported claim about historical operator practice. The Introduction now asks what makes one count answer both questions in the familiar case.
- Kept the deployment-prevalence qualification beside the opening observation. Kept every required readout exclusion in the Introduction, but divided the list into shorter statements: the readout interprets supplied connectivity; it does not detect connectivity, establish authority, select policy, or ensure that an operator uses it.
- Removed the repeated claim that Mars exposes the distinction “at human scale”; the opening already establishes why the topology makes the separation visible.
- The author reserved concentrated voice review for the integrated manuscript and delegated disposition of the external pilot review. The pilot is therefore the provisional register for continued revision; the final cold-read gate remains authoritative.

## Formal-core checkpoint decisions

- Replaced the generic Background section with the causal sequence “Why One Count Once Worked”: phase symmetry creates equality, majority makes the equal predicate count-readable, and Flexible Paxos permits the questions to separate while preserving cross-phase safety.
- Moved the uniform-threshold odd/even result beside that explanation and removed the claim that it explains the odd-cluster convention. It is labeled as a post-hoc structural observation.
- Preserved Proposition~1, its three-line proof, Corollary~1, the auditor, their labels, and their evidence boundaries. Rewrote the commentary on proof simplicity to explain when the question became non-vacuous.
- Introduced the wall only after the generic characterization. The source geometry, this paper's phase split, the Earth safety anchor, and the non-anchor participation policy now appear in that order.
- Moved the meaning of “global” into the construction section, before liveness and capability claims, so readers know what an Earth-only commit guarantees.
- Opened “Where the Wall Works, and Where It Stops” with the exact `k=3` successes and then the residual Moon/Mars exposure. The section ends by sending the residual state to the registered experiment without assigning it intrinsic valence.
- Moved the Flexible Paxos comparison out of the wall result and into Related Work. The formal investigation now runs without a literature detour.
- Rendered pages 2--6 were inspected after the move. Headings, propositions, equations, figures, and transitions fit without collision or overfull boxes; the long wall-section heading wraps across two lines on page 4 but remains visually balanced.

## Wall and common-condition audit

- Inspected every `k`/tier/state row in `results/capability/dual_gradient_map.csv` under both `reachable_unconstrained` and `reachable_self_reachable`. The prose now states the readings separately instead of inferring either from tier position.
- Recorded the reusable thresholds directly from the gradient: for `k <= 3`, `(1,0)` is absent at every tier; for `k >= 3`, `(0,1)` is absent for Earth under both readings and for LEO only under the self-reachable reading.
- Stated the unconstrained sensitivity explicitly: LEO retains `(0,1)` at every `k` when its colocated acceptor is not required to be reachable. Moon and Mars retain `(0,1)` under both readings at every `k`.
- Described participation witnesses as the source of residual exposure and legibility as the means of reading the obligation. The conclusion's remaining “price of legibility” implication was removed at the same checkpoint.
- Queried `tier_sweep_full_ci.csv` for `blackout_only`, `full_coverage`, 186-second Mars latency, 1800-second blackout, and 50 seeds. Exactly four rows matched the registered values in C11.
- Replaced the mixed 900/1800-second central table with those four rows. The sparse table retains its own explicit 186/900-second condition instead of inheriting the central table's parameters.
- Inspected rendered page 8 at high resolution. The five-column central table and its caption fit without collision; the table appears at the top of the page before its subsection text because of normal two-column float placement.

## Behavioral-reversal audit

- Read all 20 deterministic rows in `results/flip/flip_map.csv` and grouped all 1,000 rows in `results/flip/flip_sweep.csv` by `arm,incumbent_max_rounds`. Each of the five arms has 50 seeds at each of four incumbent budgets.
- Corrected a latent ambiguity in the old prose: `incumbent_max_rounds` is the swept budget; the healthy proposer remains fixed at eight rounds. At incumbent budget eight, both `(0,1)` arms record eight healthy rounds and zero completions in all 50 seeds.
- Verified the `(1,0)` comparison directly. At every incumbent budget, the treatment and healthy-`(1,1)` arms each record completion probability 1.000, median time to first commit 3.420 seconds, two healthy rounds, and seven NACKs. The treatment decides `incumbent-1`, with its own Phase 1 quorum count increasing from one to eight.
- Moved the experiment immediately after the wall boundary. The section now states the registered prediction and falsification boundary, defines the harness outcome, reports the reversal in evidence order, and places the retry-policy, flip-site, single-decree, and livelock limits beside the corresponding measurements.
- Moved the opposite-remediation paragraph out of the later discussion. The operational question now follows the mechanism: the same node-health display can conceal states for which restarting authority and reacquiring authority have opposite consequences.
- The adjacency does not make the experiment evidence for the containment theorem or the wall construction. The theorem is proved independently and the wall gaps are exhaustively enumerated. The experiment measures proposer behavior inside mixed states deliberately constructed from those definitions.

## Reading-the-Wall audit

- Consolidated the readout interface, simulator/topology method, geometry baselines, full-coverage liveness, sparse reachability, crash relaxation, inter-tier witness interpretation, and quorum-count gradient into one section. The exact boundary remains before the behavioral experiment because it is a structural result, not part of the later empirical reading.
- The interface now names configuration and connectivity as supplied inputs. It returns phase capability, witnesses, and typed failed obligations; runtime authority remains unknown and service policy is not inferred. The planetary and edge JSON files exercise the same deterministic interface, and the edge mapping is explicitly not an evaluated result.
- Removed the prescriptive four-state client-contract table. It mixed structural facts with possible policy decisions after the paper had already promised not to choose policy. The interface boundary now carries that distinction directly.
- Moved the 992/496/248/217 quorum-count gradient beside the interface as a design-time reading. Its caption says that the values are exhaustive structural counts without a connectivity distribution, timeout, or stochastic trial. The duplicate WPaxos comparison was removed; Related Work retains the comparison once.
- Inspected the source conditions against the CSV rows and experiment drivers. The condition ledger for every table or figure in the section is:

| Item | Topology / input | Initiator | Mars delay | Blackout | Timeout | Seeds | Quantity |
|---|---|---|---:|---:|---:|---:|---|
| Blackout readout figure | supplied 5/1/1/3 hard-blackout connectivity | all four tiers | n/a | structural state, no duration | n/a | n/a | deterministic formability |
| Quorum-count gradient | fixed strict 5/1/1/3 family | all four tiers | n/a | n/a | n/a | exhaustive, not sampled | structural family count |
| Experimental-design table | method inventory | varies | 186/750/1342 where swept | 300/900/1800 where swept | Appendix A | 50 per point | design, not a result |
| Geometry baseline table | sparse 5/1/1/3, hard blackout | Earth | 186/750/1342 s | 300/900/1800 s | 500 s base, scaled above the registered latency threshold | 50 per point | stochastic sweep with structural 0/1 rates |
| Full-coverage per-tier table | full-coverage 5/1/1/3, hard blackout | Earth/LEO/Moon/Mars | 186 s | 1800 s | 500 s | 50 | stochastic latency/recovery; rates identical across seeds |
| Sparse per-tier table | sparse 5/1/1/3, hard blackout | Earth/LEO/Moon/Mars | 186 s | 900 s | 500 s | 50 | structural 0/1 outcomes in the registered sweep |
| Crash-relaxation table | sparse 5/1/1/3, repeater-assisted | Earth | 186 s | 900 s | 500 s | 50 | stochastic liveness and recovery |
| Relaxation tradeoff table | fixed 5-Earth-node quorum families | Earth/Mars minima | n/a | n/a | n/a | n/a | structural quorum requirements |

- The section reports each interpretive result once: majority matches blackout survival while leaving Earth for Phase 1; LEO is faster than Earth because obligation and physical cost differ; sparse reachability can defeat a satisfiable wall obligation; recovery point values follow cadence while the bound matters to the protocol; and coordinated relaxation moves the weakest link to the Earth-local family before restoring it.
- Rendered evaluation pages were inspected after consolidation. Section headings, the two structural figures, all measurement tables, equations, and the transition into limitations fit without collision or overfull boxes. The two-column floats may precede their source paragraphs on a page, but each caption carries its complete local condition set.

## Final contextual style audit

Discovery after the integrated ending found one TeX `---` sequence and six case-insensitive uses of “rather than.” One of the latter is a source comment and the `---` is a table's missing-value marker, so neither is rendered punctuation. Every rendered match was read in context:

| Location | Match | Decision | Reason |
|---|---|---|---|
| Preamble source comment | “rather than `\\newtheorem`” | keep | Implementation note; absent from the paper readers see |
| Capability auditor | “rather than reusing the containment classifier” | keep | Establishes the independent check's methodological independence |
| Construction policy boundary | “analytical construction rather than a recommended deployment geometry” | keep | Prevents a structural example from becoming deployment advice |
| TLA+ completeness explanation | “it is not a statistical sample” | keep | Necessary epistemic distinction for readers unfamiliar with model checking |
| Protocol verification scope | “single all-tier Phase 1 family rather than the per-tier families” | keep | Names exactly what the specification does and does not cover |
| Behavioral table | `---` | keep | Data marker for an undefined time-to-commit after zero completions, not an em dash |
| Related work hierarchy | “physical latency tiers rather than organizational or logical grouping” | keep | Defines the construction's relationship to prior hierarchy work |
| Related work WAN scope | “scheduled total disconnection rather than heterogeneous latency” | keep | Distinguishes the evaluated phenomenon from the comparison systems |

The repeated analytical-versus-recommended wall qualification in Related Work was removed because the construction section already states it at the point of consequence. Four explanatory em-dash pivots in the formal core were recast as sentences, a colon, or an ordinary clause. Manual review kept two short sentences because they follow evidence and perform real reversals: “The prediction failed. It failed in reverse.” and “The weakest link migrates.” The three uses of the wall as a grammatical actor (“tells,” “gives,” and “turns”) remain local shorthand for the classification or construction, not claims of agency. No empty contribution litany or repeated interpretive announcement remains.

## Ending and traceability audit

- Added `What Remains Unsolved` before the consolidated limitations and conclusion. It begins with the exact `k=3` positive result and residual Moon/Mars exposure, then asks whether coincidence can extend farther without returning inter-tier latency to Phase 2.
- Scoped authority, gap-aware proposer behavior, and multi-anchor families are identified as candidates only. The paper makes no efficacy claim for them and assigns separate safety, performance, and operational work.
- Consolidated simulator, hard-blackout, crash-stop, orbital/link, single-decree, known-connectivity, authority/policy, prevalence, and terrestrial-evaluation limits into four paragraphs. Limits already adjacent to measurements are only recalled briefly.
- Rebuilt the conclusion in investigative order: symmetry and count-readability, containment after predicate separation, the exact wall result and boundary, then the two-question diagnostic. The last sentence preserves the performance constraint and proposes no solution.
- Rechecked traceability against C01--C17. The common 1800-second per-tier condition, sparse and crash conditions, exact `k=3` closures, Moon/Mars residual, eight-round behavioral bound, post-hoc odd/even observation, edge non-evaluation, and artifact-free open problem are explicit. Artifact paths were not changed.
- Recorded the editorial triangulation in `docs/ai-provenance.md`: approved contract and map, pilot gate, same-family review risk, cross-family adjudication, proof/artifact authority, and human authority over voice and claim-affecting prose.
- The initial one-page traceability rebuild exceeded the page by 111 points. It was split into two continued full-width tables; the float warning disappeared, paths and claims remain unchanged, and both pages were inspected. Moving the crash table's source slightly earlier keeps it at the top of page 10, before the open problem, so the conclusion runs without a delayed empirical float and ends above the References heading on page 11.
