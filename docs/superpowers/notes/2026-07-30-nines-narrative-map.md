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
| P040 | Blackout liveness figure | definition | C07, C14 | moved | Where the Wall Works, and Where It Stops | Visualizes successful and failed tier obligations |
| P041 | “This instantiates the legibility definition…” | interpretation | C14, C15 | rewrite | Where the Wall Works, and Where It Stops | Repair dangling Section 1 reference and avoid repeating the full definition |
| P042 | Per-tier wall predicates | definition | C07, C08 | move | Where the Wall Works, and Where It Stops | Sets up threshold result |
| P043 | Boundary proposition | proof | C07, C08 | moved/fixed | Where the Wall Works, and Where It Stops | Exact containment threshold remains unchanged |
| P044 | Hitting-set arithmetic | proof | C07 | moved/fixed | Where the Wall Works, and Where It Stops | Mechanism and safety connection remain unchanged |
| P045 | Gradient explanation | interpretation | C07, C08 | rewritten | Where the Wall Works, and Where It Stops | Section opens with named `k=3` closures before residual exposure |
| P046 | Gradient table | measurement | C07, C08 | moved/fixed | Where the Wall Works, and Where It Stops | Registered exhaustive result remains intact; both-reading audit continues in Task 4 |
| P047 | “Read the two rows together” | interpretation | C07, C08 | rewritten | Where the Wall Works, and Where It Stops | `k=3` design value now precedes endpoint and all-tier boundary |
| P048 | Self-reachable sensitivity | qualification | C07, C08 | fixed | Where the Wall Works, and Where It Stops | Required scope for LEO result |
| P049 | Relationship to Flexible Paxos | related work | C04, C07 | moved/revised | Related Work | Prior `(1,0)` examples and the present design-time question now sit together |
| P050 | “Legibility is load-bearing” | interpretation | C08, C14 | rewrite | Where the Wall Works, and Where It Stops | Remove compensation implication; legibility explains residual obligation |
| P051 | Wall-specific readout interface | definition | C14–C16 | move | Reading the Wall | Put inputs, outputs, and exclusions together |
| P052 | Meaning of global | qualification | C06, C15 | moved | Putting the Gaps on a Wall | Learned-value scope now closes the construction before capability enumeration |
| P053 | Related work: Paxos/Flexible Paxos | related work | C02, C04 | merge | Related Work | First-use citations already carry definitions; retain relationship here |
| P054 | Related work: containment/RQS | related work | C04 | fixed | Related Work | Essential novelty boundary |
| P055 | Related work: grid/wall/Satrapy | related work | C06 | fixed | Related Work | Preserve geometry and heterogeneous quorum lineage |
| P056 | Related work: geo-consensus | related work | C06, C16 | fixed | Related Work | Practical comparison |
| P057 | Related work: DTN/CRDT | related work | C16 | fixed | Related Work | Preserve scope distinction |
| P058 | Simulator description | definition | C09–C13 | move | Reading the Wall | General evaluation method before supporting results |
| P059 | Physical topology | definition | C11, C12 | move | Reading the Wall | Conditions for latency and reachability results |
| P060 | Sparse/full variants | definition | C12 | move | Reading the Wall | Keep immediately before relevant measurements |
| P061 | Four experiment families | transition | C09–C13 | merge | What Happens / Reading the Wall | Split valence method from remaining evaluation rather than previewing distant results |
| P062 | Experimental-design table | definition | C09–C13 | rewrite | What Happens / Reading the Wall | Preserve conditions; consider splitting if adjacency requires it |
| P063 | Fixed parameters and CI method | qualification | C09–C13 | move | Reading the Wall | Keep shared conditions near evaluation and full details in appendix |
| P064 | Valence question and registration | question | C09, C10 | move | What Happens Inside the Gaps | Structural residual state makes this question necessary |
| P065 | Valence experimental setting | definition | C09, C10 | fixed | What Happens Inside the Gaps | Keep topology, retry budgets, seeds, and single-decree scope adjacent |
| P066 | Valence result table | measurement | C09, C10 | fixed | What Happens Inside the Gaps | Primary registered evidence |
| P067 | “The first two rows…” | measurement | C09, C10 | rewrite | What Happens Inside the Gaps | Report result in evidence order and remove repeated interpretation |
| P068 | “Failure is cheap, success is expensive” | interpretation | C09, C10 | rewrite | What Happens Inside the Gaps | Preserve mechanism without freezing slogan |
| P069 | “Two things this is not” | qualification | C09, C10 | merge | What Happens Inside the Gaps | Put each limit beside its corresponding result |
| P070 | Multi-Paxos/single-decree scope | qualification | C09, C10, C15 | fixed | What Happens Inside the Gaps | Preserve delayed versus immediate exposure distinction |
| P071 | Registered deviations | qualification | C09, C10 | fixed | What Happens Inside the Gaps | Epistemic record; compress only without losing all five deviations |
| P072 | Geometry comparison setup | definition | C11 | move | Reading the Wall | Opens baseline subsection |
| P073 | Flat/majority/wall table | measurement | C11 | fixed | Reading the Wall | Preserve full conditions and measured latency |
| P074 | Flat versus wall result | measurement | C11 | merge | Reading the Wall | One concise result before competitive baseline |
| P075 | Majority baseline interpretation | interpretation | C11 | rewrite | Reading the Wall | Preserve matched liveness, doubled Phase 1 latency, and tier-awareness difference |
| P076 | Per-tier central-result setup | definition | C11 | rewrite | Reading the Wall | Change all four rows to common 1800-second condition |
| P077 | Per-tier table | measurement | C11 | rewrite | Reading the Wall | Replace mixed 900/1800 presentation with observed common condition |
| P078 | Three-of-four summary | measurement | C11 | merge | Reading the Wall | Avoid claiming undefined Mars cell once table is corrected |
| P079 | LEO/Moon/Earth latency mechanism | interpretation | C11 | fixed | Reading the Wall | Preserve obligation-versus-physical-cost surprise |
| P080 | Mars cadence and physics explanation | qualification | C11 | rewrite | Reading the Wall | Remove obsolete undefined-cell explanation; retain outside-blackout physics |
| P081 | Recovery-lag cadence explanation | qualification | C11 | rewrite | Reading the Wall | Update common-condition values; preserve bound-versus-point distinction |
| P082 | Sparse reachability setup | transition | C12 | move | Reading the Wall | Follows full-coverage readout |
| P083 | Sparse table | measurement | C12 | fixed | Reading the Wall | Preserve 186/900/50-seed condition explicitly |
| P084 | LEO full-to-sparse drop | measurement | C12 | fixed | Reading the Wall | Core network-versus-wall observation |
| P085 | Two-step operator procedure | interpretation | C12, C14, C15 | rewrite | Reading the Wall | Explain supplied inputs without implying failure detection |
| P086 | Moon and sparse Mars detail | measurement | C12 | fixed | Reading the Wall | Preserve topology-specific explanation |
| P087 | Strict Phase 2 fragility | observation | C13 | move | Reading the Wall | Motivates coordinated relaxation subsection |
| P088 | Relaxed construction | definition | C13 | fixed | Reading the Wall | Preserve `k=4`/`k=3` crash tolerance |
| P089 | Relaxed Phase 1 family | definition | C13 | fixed | Reading the Wall | Preserve cross-intersection formula |
| P090 | Crash-sweep setup | definition | C13 | fixed | Reading the Wall | Keep conditions beside table |
| P091 | Crash-relaxation table | measurement | C13 | fixed | Reading the Wall | Primary evidence |
| P092 | Strict crash-intolerance result | measurement | C13 | merge | Reading the Wall | Fold into table interpretation |
| P093 | Weakest-link migration | interpretation | C13 | rewrite | Reading the Wall | Preserve mechanism; reduce repeated contrast framing |
| P094 | Coordinated relaxation | measurement | C13 | fixed | Reading the Wall | Preserve exact successful configuration |
| P095 | Tradeoff summary table | definition | C13 | fixed | Reading the Wall | Compact reusable result |
| P096 | “All results are design-level” | qualification | C16 | merge | Limitations | Avoid repeating abstract/intro wording |
| P097 | Anchor concentration | qualification | C06, C13 | fixed | Limitations | Cross-cutting construction limit |
| P098 | Tier bundles properties | qualification | C16 | fixed | Limitations | Prevents overgeneralized edge mapping |
| P099 | Abstract network model | qualification | C11, C12 | fixed | Limitations | Preserve omitted physical effects |
| P100 | Stylized workload | qualification | C11 | merge | Reading the Wall / Limitations | Keep cadence qualification beside recovery result; brief reminder later |
| P101 | Single topology | qualification | C06–C08 | fixed | Limitations | Bounds structural evaluation |
| P102 | Crash-stop only | qualification | C06 | fixed | Limitations | Preserve fault-model boundary |
| P103 | Per-tier crash tolerance untested | qualification | C13 | fixed | Limitations | Preserve future experiment boundary |
| P104 | Inter-tier obligation vs replication | interpretation | C06, C13 | merge | Reading the Wall | Place cost consequence with evaluation, not in a second discussion pass |
| P105 | Capability/authority/contract table | qualification | C15 | move | Reading the Wall | Keep structural state separate from permission and policy |
| P106 | Opposite remediations | interpretation | C09, C10, C15 | move | What Happens Inside the Gaps | Operational surprise belongs immediately after measured reversal |
| P107 | Leadership hierarchy | interpretation | C06, C14 | move | Reading the Wall | Introduce gradient figure as a readout of structural cost |
| P108 | Quorum-count gradient figure | measurement | C06, C14 | fixed | Reading the Wall | Preserve 992/496/248/217 gradient |
| P109 | WPaxos contrast | related work | C06 | move | Related Work | Keep prior-work comparison out of result narration |
| P110 | Terrestrial mapping | qualification | C16 | move | What Remains Unsolved | Structural recurrence only; no evaluation |
| P111 | Future-work candidates | transition | C17 | rewrite | What Remains Unsolved | Frame candidates as unevaluated paths toward performance-preserving closure |
| P112 | Conclusion: coincidence and correspondence | interpretation | C02–C10 | rewrite | Conclusion | Return through the investigation rather than compressing every result |
| P113 | Conclusion: Mars and mitigation | interpretation | C14–C17 | rewrite | Conclusion | End with two-question diagnostic and open problem |
| P114 | Simulator-parameters table | definition | C09–C13 | fixed | Appendix | Preserve values; update only cross-reference or verified condition drift |
| P115 | Traceability introduction | qualification | C04–C16 | rewrite | Appendix | Rebuild against final claim ledger |
| P116 | Traceability table | measurement | C04–C16 | rewrite | Appendix | Update common-condition row and final wording without changing artifact identity |

## Claim ledger

| Claim ID | Claim | Kind | Exact scope/evidence | Citation or artifact | Must remain adjacent |
|---|---|---|---|---|---|
| C01 | Acquisition and commit are distinct formability questions over a supplied reachable set | definition | `R_1(C)` and `R_2(C)` over fixed epoch universe `N_e`; actual commit also needs authority and execution | Formal definitions in paper | First-use acquisition gloss; authority caveat |
| C02 | Phase symmetry makes the two formability predicates equal | formal | Same quorum family for Phase 1 and Phase 2; equality for every connectivity state | `lamport1998`; `howard2016` context | Distinguish equality from node health and threshold counting |
| C03 | Majority makes the equal predicate decidable by one reachable-node threshold from a stated vantage | formal | Threshold family; reachable set defined from one proposer/operator vantage | `lamport1998`; uniform corollary | Phase symmetry is the cause of equality; majority is the familiar count-readable instance |
| C04 | Containment gives the exact four-way gap correspondence | theorem + executable audit | Any nonempty finite phase families over fixed `N_e`; pinned domain optional; proof primary; 129,032 exhaustive three-node checks and 16 registered wall cases | `quorum_audit.py`; `experiments/quorum_audit.py`; `results/capability/quorum_audit_registered.json`; `guerraoui2010rqs` for prior containment distinction | Proof, monotonicity, prior-work boundary, deterministic witness scope |
| C05 | Cost-minimal threshold symmetry costs 0 extra participants at odd `n` and 1 at even `n` | post-hoc structural result | 80 valid threshold configurations, `n=3..7`; post-hoc, not preregistered | `results/capability/dual_uniform.csv`; `experiments/capability_dual_sweep.py` | Post-hoc label and safety condition `q_1+q_2 >= n+1` |
| C06 | The wall preserves cross-phase safety with Earth as anchor; other witnesses encode participation policy | proof + model checking | 5/1/1/3 construction; all tiers × strict/`k=4`/`k=3`, 27,921 intersection states complete; reduced Paxos model 67M states complete | `tla/QuorumIntersection.tla`; `tla/ExhaustiveIntersection.tla`; `tla/PaxosSmall.tla`; `peleg1995` | Proof-primary statement; protocol-spec scope; witness-policy qualification |
| C07 | At `k=3`, `(1,0)` is absent everywhere; both gaps are absent for Earth under both readings and for LEO under self-reachable reading | structural enumeration | All `2^10` connectivity states; every tier and `k`; unconstrained and self-reachable readings | `results/capability/dual_gradient_map.csv`; `experiments/capability_dual_sweep.py` | Gap, threshold, tier, and reading in every “closes/prevents” statement |
| C08 | Moon and Mars retain `(0,1)` at every `k`; no tested `k` closes both gaps at every tier | structural enumeration | Same registered exhaustive gradient; caused by downward participation obligations | Same as C07 | Positive wall result must appear first; no intrinsic valence or compensation claim |
| C09 | In the registered single-decree experiment, `(1,0)` matched a healthy contender on recorded metrics and injected an accepted value | preregistered measurement | 5 arms × 4 incumbent retry budgets × 50 seeds; registered topology, policy, and metrics; byte-identical rerun | `results/flip/flip_map.csv`; `results/flip/flip_sweep.csv`; `flip.py`; `experiments/flip_sweep.py`; `experiments/flip_verdict.py` | Recorded-metric and experiment bounds; accepted-value mechanism; no universal benignity |
| C10 | In the same experiment, `(0,1)` prevented healthy-proposer decision in 50/50 seeds at retry budget 8 | preregistered measurement | Bounded retry exhaustion under modeled policy; single-decree Paxos; no unconditional livelock claim | Same as C09 | Retry budget, policy, topology, and Multi-Paxos distinction |
| C11 | Full-coverage wall per-tier liveness is Earth/LEO/Moon 100% and Mars 0% under a common 1800-second blackout; LEO latency is lower than Earth's | measurement | Candidate central-table condition to verify in Task 4: `blackout_only`, full coverage, 186 s Mars delay, 1800 s blackout, 50 seeds; latency is two-phase single-attempt mean | `results/tier_liveness/tier_sweep_full_ci.csv`; `experiments/tier_liveness_sweep.py` | Common condition for all four cells; cadence qualification; obligation/cost distinction |
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
| Where the Wall Works, and Where It Stops | The wall closes all operationally important gaps, or residual `(0,1)` is intrinsically bad | structural reachability | exact only for named `k`, tier, and reading | reusable design technique plus exact boundary | exhaustive connectivity enumeration | ordered correctly; table audit pending | Opens with `k=3` closures, then Moon/Mars residual exposure; bounded experiment bridge denies intrinsic valence |
| What Happens Inside the Gaps | The registered experiment establishes universal valence or validates the wall | no causal validation of theorem/wall | bounded to topology, policy, budget, and single-decree protocol | behavioral comparison of constructed states | preregistered 5×4×50 simulation | no if generalized | Keep each number beside its conditions; distinguish accepted-value consequence from universal harmlessness and retry exhaustion from livelock |
| Reading the Wall | The readout detects failures, identifies a live leader, or chooses safe remediation | readout interprets supplied state | structural interface; measurements remain topology-specific | compact typed readout and joined interpretation | existing topology/baseline/liveness/crash experiments | no without interface boundary | State inputs and exclusions before results; treat edge JSON as demonstration |
| What Remains Unsolved | No performance-preserving closure exists | no | open across structured topologies | question, not result | none | no | Ask whether and where coincidence can be extended; label candidate directions unevaluated |
| Related Work, Limitations, Conclusion | Mars experiments empirically establish terrestrial behavior or research chronology followed narrative order | no | structural recurrence only | contributions bounded above | planetary simulator only | no without scope | Distinguish explanatory order from discovery chronology; end with diagnostic and open question, not deployment prevalence |

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
