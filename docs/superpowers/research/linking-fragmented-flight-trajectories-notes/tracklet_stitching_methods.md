# Track-to-track association and tracklet stitching for aircraft track pieces separated by long gaps

Reading status, so the writer can weight claims. Full text read: Zhang 2008, Oh et al. MCMCDA, Ristani 2016, Luiten HOTA, Killick supplement, Perrichon 2024, OpenSky Report 2025, Dendorfer 2022 and Cetintas 2023 (both via ar5iv extraction, tables partly garbled). Abstract or search summary only: Yeom 2003/2004, Raghu 2018, Blackman 2004, Sun 2017, all over-the-horizon radar papers, all reachability and space-object papers, and the AIS imputation papers. Those are marked "abstract only" or "search summary" where it affects a claim.

## Standard formulations: TSA, tracklet linking, MHT, GNN, min-cost flow, graph matching, MCMC data association

### Takeaway
All of these solve one problem: which fragment continues which, when many targets compete for the same fragments. A persistent address removes that competition, so most of their machinery (mutual-exclusion constraints, hypothesis management, sampling) addresses a difficulty this setting does not have. What transfers is the cost and gate design, and the Markov-chain decomposition of a trajectory into entry, link and exit terms.

### Cited Findings

**Track segment association (TSA), the aerospace formulation**
- Yeom, Kirubarajan and Bar-Shalom (IEEE TAES 40(1), 2004, pp. 293-309) reconnect broken tracks with a discrete optimisation; the motivating case is airborne early warning, with hard manoeuvres, close groups, large errors, long sampling intervals and low detection probability. Abstract only. — [McMaster record](https://experts.mcmaster.ca/scholarly-works/143875)
- Mechanism: forward prediction and backward retrodiction give estimates at a common time for candidate pairs of segments, which are then matched. The test scenario is 120 targets over 120 scans with straight, zigzag, oval and jinking motion. Results are reported as "significant improvement" in mean track life and in position, speed and course RMS error, with no percentage in the abstract. — [2003 IEEE Aerospace Conference record](https://experts.mcmaster.ca/scholarly-works/144032)
- Raghu, Srihari, Tharmarasa and Kirubarajan (IEEE TAES 2018) summarise the standard recipe as predict old segments, retrodict new ones to a common time, then solve a 2-D assignment. They identify two weaknesses: no check of whether a track ended because of a wrong association, and no use of measurements taken during the gap. Their fix handles a **single turn** in the gap, estimating its start, end and rate by maximum likelihood. The abstract gives no numbers. — [Amrita record](https://www.amrita.edu/publication/comprehensive-track-segment-association-for-improved-track-continuity/)

**Min-cost flow (Zhang, Li and Nevatia, CVPR 2008)**
- MAP association with the constraint that one observation belongs to one trajectory. Each trajectory is a Markov chain, P(T) = P_entr · ∏P_link · P_exit, mapped to a flow network whose costs are negative log-probabilities. — [Zhang 2008 PDF](https://www.cvlibs.net/projects/autonomous_vision_survey/literature/Zhang2008CVPR.pdf)
- P_link factorises into size, position, appearance and time-gap terms. The time-gap term is P(Δt) = Z·α^(Δt−1) for 1 ≤ Δt ≤ ξ and 0 otherwise, where α is the detector's miss rate and ξ a hard maximum gap. Parameters are learned from training data (6 of 26 CAVIAR videos). — [Zhang 2008 PDF](https://www.cvlibs.net/projects/autonomous_vision_survey/literature/Zhang2008CVPR.pdf)
- Solved with Goldberg's scaling push-relabel, worst case O(n²m log n), with at most O(log n) runs under Fibonacci search over total flow. The authors observe roughly linear runtime in practice and attribute it to temporally constrained transitions. — [Zhang 2008 PDF](https://www.cvlibs.net/projects/autonomous_vision_survey/literature/Zhang2008CVPR.pdf)
- CAVIAR results (140 ground-truth trajectories, 25,587 test frames), plain flow vs. flow with the explicit occlusion model: MT 104 → 120, ML 7 → 5, fragments **58 → 20**, ID switches **7 → 15**. Comparator Wu et al.: MT 106, ML 9, fragments 35, ID switches 17. — [Zhang 2008 PDF](https://www.cvlibs.net/projects/autonomous_vision_survey/literature/Zhang2008CVPR.pdf)
- The authors state why long gaps are hard: "If we allow association of observations with a large temporal gap between them, the possibility of creating errant association also increases." The paper also notes it cannot recover a trajectory if an object goes undetected for many consecutive frames. — [Zhang 2008 PDF](https://www.cvlibs.net/projects/autonomous_vision_survey/literature/Zhang2008CVPR.pdf)

**MHT and MCMC data association**
- Blackman's tutorial (IEEE AES Magazine 19(1), 2004, pp. 5-18) covers MHT's basis, implementations, and combination with IMM. Abstract only. — [Bohrium record](https://www.bohrium.com/paper-details/multiple-hypothesis-tracking-for-multiple-target-tracking/812365867395317762-4442)
- MCMCDA (Oh, Russell and Sastry) samples partitions of observations into tracks instead of enumerating them. The authors state it, unlike MHT and JPDA, converges to the Bayesian solution given unlimited resources. Runtime is set by sample count, whereas MHT's depends on the problem instance and is not predictable in advance. — [MCMCDA tech report](https://aima.eecs.berkeley.edu/~russell/papers/tr-mcmcda.pdf)
- Comparison with MHT (max 1,000 hypotheses per group, tree depth 5, max Mahalanobis distance 5.9), varying K from 5 to 100 targets: MHT degrades with track count because of pruning. MHT makes no correct associations at false-alarm rate λfV ≥ 80. At high detection probability MHT scores better but "fragments tracks". Without pruning and N-scan-back, MHT grows exponentially "even for a small problem". These authors tuned MHT to match their method at 10 targets, so treat the comparison as favourable to MCMCDA. — [MCMCDA tech report](https://aima.eecs.berkeley.edu/~russell/papers/tr-mcmcda.pdf)

**Tracklet linking outside radar**
- A 2024 review notes that traditional track association algorithms tend to perform poorly with overlapping tracks or data gaps (search summary). — [MDPI JMSE 12(11):1913](https://www.mdpi.com/2077-1312/12/11/1913)
- Compact HF surface-wave radar work attributes fragmentation mainly to incorrect plot-to-track association during tracking and categorises tracklet methods, beginning with statistical similarity between tracklets (search summary). — [Remote Sensing 14:1601](https://mdpi-res.com/d_attachment/remotesensing/remotesensing-14-01601/article_deploy/remotesensing-14-01601.pdf)

### Inferences
- Zhang's table is the cleanest available number on the central trade-off. Allowing longer links cut fragments by 66% (58 → 20) and more than doubled ID switches (7 → 15). It is pedestrian video at 25 fps, so the magnitudes do not transfer; the direction should be assumed to.
- TSA with 2-D assignment is global nearest neighbour applied to track pieces instead of measurements. This is my reading; I did not retrieve a source defining GNN.
- Fit to this setting (my assessment):

| Method | Problem it solves | Needed with a persistent ICAO24? | Fit to batch, per-airframe Spark |
|---|---|---|---|
| 2-D assignment TSA / GNN | Competing candidates for a track end | Only inside one address if pieces overlap in time | Cost expression fits; the assignment does not fit column expressions, and is rarely needed |
| Min-cost flow | Globally optimal multi-target non-overlap | No. One airframe is a chain, and flow on a chain is trivial | Poor, and pointless |
| MHT | Deferred decisions under clutter and online ambiguity | No. Batch has no need to defer, and there is little clutter | Poor |
| MCMCDA | Sampling a posterior over partitions | No | Poor. A stochastic sampler sits badly with OPDI's insistence on reproducible released data |
| Graph matching, hierarchical GNN | Long-range association with appearance | No | Poor |
| Per-boundary score with threshold | Whether adjacent pieces are the same flight | **Yes** | Excellent: `lag` over a window partitioned by `icao24`, pure column expressions |
| Optimal partitioning, DP, Viterbi | Same, plus whole-flight constraints | Only if non-local constraints earn their cost | Needs `applyInPandas` or `mapGroups`, which `CLAUDE.md` calls a deliberate architectural step |

### Gaps
- Yeom's actual percentages and gap lengths are behind IEEE paywalls (DOIs 10.1109/taes.2004.1292161, 10.1109/aero.2003.1235122). I could not obtain them and will not estimate them.
- Graph matching as a distinct family (as opposed to GNN-based hierarchies) was not researched to primary sources.
- Reid (1979) and the Blackman and Popoli text were not retrieved. Kim et al., "MHT Revisited", was searched for and not found.

## Gating and cost measures across long gaps, and how uncertainty growth limits them

### Takeaway
Three gates recur: a hard time bound, a maximum-speed reachability bound, and predicted-state Mahalanobis distance. At gaps of hours the kinematic ones stop discriminating, because the set of reachable positions becomes as large as the region of interest. Their remaining value is rejecting impossible joins, not confirming correct ones.

### Cited Findings
- **Reachability gate.** MCMCDA assumes maximum target speed v̄ and maximum consecutive misses d̄, and builds a neighbourhood tree ‖y_t − y_(t+d)‖ ≤ d·v̄. It sizes d̄ from d̄ ≥ log(1−p̄_dt)/log(1−p_d); for example p_d = 0.7 and p̄_dt = 0.99 give d̄ ≥ 4. — [MCMCDA tech report](https://aima.eecs.berkeley.edu/~russell/papers/tr-mcmcda.pdf)
- **Hard time bound.** Zhang sets P(Δt) = 0 for Δt > ξ, with the prior decaying as α^(Δt−1), so longer gaps are penalised exponentially before position is considered. — [Zhang 2008 PDF](https://www.cvlibs.net/projects/autonomous_vision_survey/literature/Zhang2008CVPR.pdf)
- **Common-time comparison.** TSA compares predicted old and retrodicted new segments at a common time. — [Yeom 2003 record](https://experts.mcmaster.ca/scholarly-works/144032); [Raghu 2018 record](https://www.amrita.edu/publication/comprehensive-track-segment-association-for-improved-track-continuity/)
- **Manoeuvre limits.** Raghu's method covers one turn in a gap. — [Raghu 2018 record](https://www.amrita.edu/publication/comprehensive-track-segment-association-for-improved-track-continuity/)
- **Variance growth.** Under continuous white-noise acceleration, Q = q·[[dt³/3, dt²/2], [dt²/2, dt]], so position variance grows with the cube of the gap. Discrete variants with dt⁴/4 are a different model. — [ossia kalman_pv_filter docs](https://ossia.io/libossia/html/structossia_1_1kalman__pv__filter.html); [arXiv 2506.09441](https://arxiv.org/pdf/2506.09441)
- **Search-space growth.** "The set of possible associations grows exponentially with the increasing gap length"; the paper gives no quantitative curve. — [Dendorfer et al. 2022](https://ar5iv.labs.arxiv.org/html/2210.07681)
- **Non-Gaussian uncertainty.** In orbital tracklet association, the Gaussian assumption often breaks and propagated uncertainty takes a curved "banana" shape (search summary). — [IAC 2018 paper](https://ccd.aiub.unibe.ch/publist/data/2018/artproc/AV_IAC2018.pdf)
- **The closest analogue to a sparse long-gap setting is space surveillance.** Siminski et al. sample hypotheses from one tracklet's admissible region, bounded by physical constraints such as stable orbits, propagate them to the other tracklet's epoch and gate there (search summary). — [Siminski et al., ESDC 2013](https://ccd.aiub.unibe.ch/publist/data/2013/artproc/TS_ESDC2013a.pdf)
- Reachability propagation combined with a particle filter has been used for manoeuvring space objects under sparse measurements (record summary). — [Hall and Singla, Penn State record](https://pure.psu.edu/en/publications/reachability-analysis-based-tracking-applications-to-non-cooperat/)
- Standard association models can fail during manoeuvres; one proposal bounds feasible manoeuvre effort and uses a control-distance metric instead (search summary). — [SDC8 paper 164](https://conference.sdo.esoc.esa.int/proceedings/sdc8/paper/164/SDC8-paper164.pdf)
- **OPDI's own number.** On 2026-06-01, 1,457 of 1,765 sub-minute splits had the same resolved callsign on both halves and a median implied speed of 421 kt across the gap. Current thresholds are 30 min, or 15 min below 5,000 ft. — `opdi/src/opdi/pipeline/segmentation/base.py`, lines 84-92 and 107-110

### Inferences
- A single-turn gap model (Raghu) does not describe hours of oceanic or en-route flight, which contain many heading changes.
- Worked numbers, my arithmetic, independent of any source. Position standard deviation is sqrt(q·T³/3). At an illustrative q = 1 m²/s³ that gives about 44 km at 30 min, 125 km at 1 h and 1,833 km at 6 h. The value of q is arbitrary, but the ratio is not: 6 h has **41.6 times** the standard deviation of 30 min, whatever q is.
- A reachability bound at 500 kt over 6 h is 3,000 NM (about 5,556 km). That passes nearly any pair of positions on a transatlantic or intra-European scale, so at long gaps the gate rarely rejects anything. It remains useful at short gaps and as a sanity check against corrupted positions or duplicate addresses.
- Mahalanobis distance has a perverse property here. As S grows, d² = νᵀS⁻¹ν shrinks for any given miss distance, so wrong pairs look better at longer gaps. A full Gaussian likelihood adds a ln|S| term that offsets this, and a bare chi-square gate does not. I found no paper stating this for long gaps; it follows from the formula.
- Kinematics should be used as a **two-sided** test, which the multi-target literature has less need of. Implied speed above physical maximum rejects the join. Implied speed well below cruise suggests time was spent somewhere, whether holding, on the ground, or on a dog-leg. Implied speed near cruise is compatible with one flight, but is not proof, since a short hop and a turnaround could still fit. OPDI's 421 kt figure shows the signal is informative at short gaps; it does not establish that at hours.
- The stronger cues are not kinematic: altitude at each piece edge (both at cruise vs. both near the ground), proximity of edges to an airport, callsign equality, and total-flight plausibility. These are the analogue of the "appearance" term that dominates in vision. See the warning under the learned-approaches question on ADS-C callsigns.

### Gaps
- I found no source directly measuring Mahalanobis gating failure versus gap duration; searches returned only adjacent work.
- No paper gave calibrated values of q for civil aircraft over hour-long gaps.

## Work specific to aircraft or radar track-segment association over long gaps

### Takeaway
The radar TSA literature exists, but it targets scan-scale breaks in dense multi-target simulation. Over-the-horizon radar work solves a different problem (several simultaneous tracks of one target). I found no paper on linking same-address ADS-B pieces into flights across hours of gap, which is itself a finding.

### Cited Findings
- Yeom's scenario spans 120 scans, and both Yeom and Raghu report simulation results. — [Yeom 2003 record](https://experts.mcmaster.ca/scholarly-works/144032); [Raghu 2018 record](https://www.amrita.edu/publication/comprehensive-track-segment-association-for-improved-track-continuity/)
- **Over-the-horizon radar is not this problem.** Its association task arises from multipath through several ionospheric layers, giving multiple simultaneous tracks per target (search summary). — [Bogner et al. 1998](https://elmi.hbku.edu.qa/en/publications/association-of-tracks-from-over-the-horizon-radar/); [Southcott thesis, Adelaide 1998](https://thesis.library.adelaide.edu.au/dspace/bitstream/2440/19360/2/02whole.pdf)
- Methods in that line include a MAP mode-linking approach under uncertain ionosphere, reported at about 3:1 improvement in ground-coordinate accuracy over conventional mode linking in simulation (search summary). — [Anderson and Krolik 2002, Duke record](https://scholars.duke.edu/individual/pub687223)
- Others use Lagrangian relaxation to avoid exponential hypothesis growth (search summary). — [Rutten record](https://ubmr.hds.hebis.de/EdsRecord/edscal%252Cedscal.16383139)
- **OpenSky Report 2025**, the most directly relevant aviation source, joins ADS-C and ADS-B by the 24-bit transponder code, using callsign only to split ADS-C points into flights. It reports no time, altitude or position gate. Dataset: 720,415 ADS-C messages from 2,606 aircraft, with ADS-C updates typically every 10-15 minutes. Verification is qualitative (selected flights compared in figures). — [OpenSky Report 2025](https://arxiv.org/html/2505.06254v1)
- That paper's conclusion claims accuracy improves "by up to 40%" on transoceanic and African routes, but my full-text extraction found no definition of the metric or reference. A third-party summary also mentioned 16% of trajectories flagged by sinuosity; the same extraction did not find that figure. Treat both as unverified. — [OpenSky Report 2025](https://arxiv.org/html/2505.06254v1)
- Sun, Ellerbroek and Hoekstra (J. Aerospace Information Systems 2017) extract continuous flights from scattered ADS-B points by DBSCAN clustering using the ICAO address with other features, then label phases with fuzzy logic. Search summary only; the full text was unreachable. — [TU Delft record](https://repository.tudelft.nl/record/uuid:a34654cb-a9eb-4d0a-bb75-93971a1750a1)
- A FOSS4G 2026 talk describes splitting ADS-B into candidate trajectories by temporal continuity and inferring arrivals and departures from geometry relative to runway ends plus the vertical profile. Talk abstract, not peer reviewed. — [FOSS4G 2026](https://talks.osgeo.org/foss4g-2026/talk/9MVXWT.ics)
- `traffic`, the reference library, has time-gap splitting; an example in its source calls `split("10 min")`. — `traffic/src/traffic/core/flight.py`, line 2014

### Inferences
- Aviation practice in the sources is overwhelmingly address plus time-gap rule, with little published validation. OPDI comparing against PRISME would have stronger evidence than the works I found.
- OpenSky's report supports the view that the join key is not the hard part. It joins on the address without asking whether the joined pieces were one flight, beyond the callsign split. That is where the unsolved question sits.

### Gaps
- I found no study reporting false-join or fragmentation rates for same-address ADS-B linking at gaps from minutes to hours. This could reflect search limits, but several differently worded queries returned nothing.
- OTHR long-dropout stitching: no source found.

## When identity is known: does it become sequence segmentation, and what does that literature offer?

### Takeaway
Yes. With a persistent address, the association question "same target?" is answered, and the remaining question is "same flight?", which is a segmentation of a time-ordered sequence. If each boundary is scored independently, the optimum is a per-boundary threshold, computable with one window function. Dynamic programming earns its cost only with non-local constraints across a whole flight.

### Cited Findings
- **Change-point optimisation.** PELT is an exact method for minimising penalised segmentation costs, with linear cost in sample size under mild conditions, against quadratic or cubic for alternatives. Published in JASA 107, pp. 1590-1598. — [Killick et al., arXiv 1101.1438](https://arxiv.org/abs/1101.1438v3)
- PELT's linear cost depends on the number of change points growing linearly with n; with slower growth its CPU cost "is no longer linear", though savings stay substantial. — [Killick supplement](https://www.lancaster.ac.uk/~killick/Pub/KillickFearnheadEckley2012SuppMat.pdf)
- **HMM on flights.** Perrichon, Gendre and Klein (J. Open Aviation Science, 2024) segment flights into 6 phases (taxi, take-off, climb, cruise, approach, rollout) with a multivariate HMM, reaching median accuracy of about 97% on nearly 3,000 flights. — [Perrichon et al.](https://www.math.univ-toulouse.fr/~xgendre/rec/joas-7269.pdf)
- In that study, 91% of flights had no invalid phase transitions, compared with 74% for a naive method and 21% for fuzzy logic without preprocessing. About 8% of its reference flights themselves contained an invalid transition. It reports that segmentation is minimally affected by missing values, with an example of 500 missing points. — [Perrichon et al.](https://www.math.univ-toulouse.fr/~xgendre/rec/joas-7269.pdf)
- **HMM and Viterbi with sparse samples.** Newson and Krumm's HMM map matching addresses noisy, sparse data. Its own abstract says the test shows how the algorithm "breaks down as the sampling rate of the GPS is reduced". — [Microsoft Research](https://www.microsoft.com/research/publication/hidden-markov-map-matching-noise-sparseness/)
- **Stay-point analogues.** In human mobility, one trackintel method excludes gaps above a threshold from stay-point generation and assumes users are stationary during missing periods. — [trackintel docs](https://trackintel-myfork.readthedocs.io/en/repair_doc/modules/model.html)
- Sparse-GPS segmentation also uses rule-based breaks on spatial distance or time gap, with adaptive thresholds. — [arXiv 2510.12686](https://arxiv.org/pdf/2510.12686)
- **Identity is not perfectly persistent.** Duplicate addresses exist; Aireon observed recurring values such as 0x000001, 0x123456 and 0xA00000 across several aircraft over five days in 2018. — [Aireon, ICAO NACC 2018](https://www2023.icao.int/NACC/Documents/Meetings/2018/ADSB/App5-Space%20ADS_B_TCAS%20and%20Data%20Observations.pdf)
- ICAO guidance attributes more than half of mismatched-address cases to database defects, with human error and transponder hardware faults also contributing. — [ICAO APAC guideline](https://www.icao.int/sites/default/files/APAC/Documents/edocs/CNS/APX-G-Guideline-on-Addressing-Inconsistencies-of-ICAO-AD-and-ID-Between-Surveillance-Data-and-Flight-Plan.pdf)
- Replacing a failed avionics component with a spare can change the address. — [CAAC paper, ICAO SURICG8](https://www.icao.int/sites/default/files/APAC/Meetings/2023/2023%20SURICG8/5-Presentations/P6_CHN-Aircraft-Address-and-Target-Identification-Quality-Analysis-using-ADS-B-and-FPL_China.pdf)

### Inferences
- **Argument that only adjacent pieces can link (mine, and the core structural point).** Take pieces p1 < p2 < p3 of one airframe, non-overlapping in time. Linking p1 to p3 while excluding p2 would place the aircraft in another flight during its own flight, which is impossible unless p2 is false data. So the graph of admissible links is a path, K pieces have K−1 boundaries, and there are 2^(K−1) segmentations with no competition for successors. Assignment, flow and hypothesis-tree machinery all reduce to nothing.
- **Independent scores:** if each boundary's score depends only on its two neighbouring pieces, the global optimum is simply to threshold each boundary. In Spark this is a `lag` over a window partitioned by the airframe, ordered by time, on a per-piece summary frame, consistent with the codebase's native-column convention.
- **When DP adds something:** only with constraints spanning a whole run of pieces, such as exactly one take-off and one landing per flight, total duration plausible for the type, or route efficiency of the assembled flight. The form is optimal partitioning, F(t) = min_s [F(s) + C(s+1, t) + β], with pieces as data points and β the penalty per flight. A 2-state Viterbi is the special case with only local transitions.
- **PELT itself solves a problem this setting lacks.** K per airframe per day should be small, so plain O(K²) is cheap, and pruning buys nothing. Its value is the conceptual frame and the penalty β as a single auditable fragmentation-versus-merge dial.
- **The DP needs a per-group function**, so it is an architecture decision under `CLAUDE.md`'s no-`applyInPandas` convention. I recommend starting with the threshold and adopting DP only if a benchmark shows that a local rule leaves measurable errors that a global one fixes.
- **Perrichon's missing-value result does not settle long gaps.** Its example concerns missing samples within a flight, not a hole that may hide a landing.
- **The real difficulty is not in this literature.** A hidden landing inside the gap is not association and not ordinary change-point detection, since the change happened where no data exists. The evidence is only at the edges, so the problem is closer to classification of each gap from edge features. Nothing surveyed addresses it directly.
- **Residual association problem.** Time-overlapping pieces under one address, or impossible implied speed, flag a broken identity assumption. These should be rejected or quarantined and not resolved by a tracker. The expected rate on OSN data is unknown.

### Gaps
- I found no literature on "did an unobserved stop occur during the gap", in aviation or analogues (AIS port calls during dark periods may be closest but were not searched specifically).
- I found no Viterbi-based trip segmenter across long GPS gaps; the flight-phase HMM and stay-point lines of work appear separate in what I retrieved.
- How OpenSky flags or resolves duplicate addresses: no source found.
- The rate of duplicate or changed ICAO24s in OPDI's data needs measurement, not literature.

## Learned approaches: training data and proven benefit over rules

### Takeaway
Learned linking is proven within video tracking, where appearance drives the gain. Claims in radar TSA rest on small studies, and benefit over good rules for long-gap motion-only linking is not established. The closest long-gap benchmark deliberately removes the identifier this setting has. Rules should be the default until a labelled benchmark shows a gap to close.

### Cited Findings
- **SUSHI (CVPR 2023)**, a hierarchical GNN, reports IDF1 / HOTA / MOTA of 83.1 / 66.5 / 81.1 on MOT17 private, 79.8 / 64.3 / 74.3 on MOT20 private, and 63.4 / 63.3 / 88.7 on DanceTrack. Horizon: 512 frames. Appearance (ReID embeddings) is reported as the most impactful cue overall; motion has moderate impact. Ablations use the MOT17 train set with 3-fold cross-validation. — [Cetintas et al.](https://ar5iv.labs.arxiv.org/html/2212.03038)
- **Quo Vadis (NeurIPS 2022)** is the most useful honest test of forecasting for gap bridging. Existing trackers "only bridge less than 10% of occlusions longer than three seconds", against about 50% under one second. — [Dendorfer et al.](https://ar5iv.labs.arxiv.org/html/2210.07681)
- In its MOT17 validation table (baseline HOTA 50.71): Kalman filter in bird's-eye view reaches 54.11, Kalman in pixel space 54.08, and the learned MG-GAN 54.52. Long-gap lost-ID reductions are −16.07% (BEV Kalman) and −17.43% (MG-GAN). The best-forecast GAN (20 samples, lowest final displacement error) had the lowest HOTA among those, 53.81; the authors use this to argue forecast accuracy metrics favour recall over precision. — [Dendorfer et al.](https://ar5iv.labs.arxiv.org/html/2210.07681)
- An oracle 3D linear motion model solves 89.3% of gaps in synthetic data, and the authors conclude motion there is mostly linear. Added to ByteTrack on the test set, the method gives +0.09 HOTA on MOT17 (63.14 vs. 63.05) and IDF1 77.71 vs. 77.30. The paper's text and table disagree on which arm the 17.43% belongs to; the table assigns it to MG-GAN. — [Dendorfer et al.](https://ar5iv.labs.arxiv.org/html/2210.07681)
- **Learned radar TSA.** TSA-cTFER, a contrastive transformer (Remote Sensing 2024), reports 94.59% accuracy on interrupted TSA and 94.83% on multi-source TSA. The paper refers to real-world datasets and to data sources updating every 20 s. — [Cao et al.](https://mdpi-res.com/d_attachment/remotesensing/remotesensing-16-03380/article_deploy/remotesensing-16-03380.pdf)
- A 2023 CCC paper claims existing TSA has low accuracy in dense or long-interruption cases and uses ProbSparse self-attention (search summary). — [BIT record](https://pure.bit.edu.cn/en/publications/spatially-dense-multi-target-track-segment-association-algorithm)
- **AIS-ITD-US** is a long-gap association benchmark built from Marine Cadastre AIS, with gap bins up to 120-180 minutes and negatives chosen for parallel, crossing or converging motion. Each sub-trajectory gets a **synthetic ID so the task depends solely on motion**, with vessel identity removed. Page lists no baselines or pair counts. — [IEEE DataPort](https://ieee-dataport.org/documents/ais-itd-us)
- **AISFlow (ICML 2026)** does boundary-conditioned flow matching for long vessel gaps, conditioned on endpoint displacement, speed and course, gap length and heading change. It reports up to 317× lower latency than diffusion baselines; the page gives no accuracy figures or gap durations. — [ICML 2026](https://icml.cc/virtual/2026/73576)
- A KDD 2024 paper uses a diffusion model for long-term vessel gaps, with a transformer over pre- and post-gap segments (search summary). — [ChatPaper summary](https://chatpaper.com/paper/57048)
- For aircraft, BERT-style masking of ADS-B trajectories exists (search summary). — [Sensors 23(22):9120](https://www.mdpi.com/1424-8220/23/22/9120)
- **Project evidence.** PRC 2025's report found "Attempts to complete the trajectory always lead to worse result", and `CLAUDE.md` lists synthetic gap filling as a documented negative result. — `CLAUDE.md`, "Evidence base: PRC challenge findings"
- **Caution on a seemingly strong cue.** In OPDI's ADS-C feed `flight` is an IATA number (`LH133`), not the ICAO callsign ADS-B broadcasts (`DLH133`), and it is empty before about 2026-06. — `CLAUDE.md`, ADS-C section

### Inferences
- **Vision gains do not transfer directly.** SUSHI's main cue is appearance, and its horizon of 512 frames is on the order of tens of seconds of video. Aircraft have no equivalent embedding, and gaps here are hours.
- **Quo Vadis is close to a negative result for complexity.** A learned forecaster beat a Kalman filter by 0.41 HOTA, and the most accurate forecaster was not the best tracker. That supports preferring simple motion models, with the caveat that gaps were seconds long and motion was pedestrian.
- **94.59% should not be read as adequate.** It implies roughly 1 in 20 links wrong, on a task with competing candidates and a different gap regime. For a published dataset, that error rate would need comparing against the rule's error rate on identical labels before any claim of benefit.
- **AIS-ITD-US solves a harder, different problem.** It discards the identifier this setting has. Its value is as a gap-length-stratified evaluation design, not as a method source.
- **Training labels are the actual prize, and OPDI has them.** `flights_tidy()` provides flight-level truth keyed on `AIRCRAFT_ADDRESS` (which is `icao24`), callsign and date (`CLAUDE.md`). So a supervised boundary classifier is feasible, labelled by whether two pieces belong to one Network Manager flight. A logistic regression or gradient-boosted tree over a dozen edge features would be a more defensible first learned step than a sequence model. It can be scored as a Spark expression if kept linear or shallow. Coverage bias applies: PRISME is European, so labels may under-represent wholly non-European flights under worldwide coverage.
- **Imputation for scoring vs. filling.** The PRC negative result concerns completing trajectories for downstream estimation. It does not strictly rule out using a predicted path to score a link and then discarding the path. But that is a new, unproven step, adds a dependency, and I saw no result suggesting it beats edge features.

### Gaps
- I did not obtain TSA-cTFER's interruption lengths, baseline figures, or a clean split between simulated and real results; keyword extraction only.
- No head-to-head of learned vs. rule-based same-identity linking was found in any domain.
- AIS-ITD-US baselines are not on the page; the data sits behind a DataPort subscription.
- The characteristic-aware attention LSTM (PMC12158344) could not be read because of a browser check.

## Documented failure modes and how they are measured

### Takeaway
Fragmentation and false joins are named, separately countable errors, and they trade off. Event counts (MOTA, ID switches, fragments) treat all breaks alike, while identity measures (IDF1) weigh by how much trajectory is affected. For this setting, report merges and fragments separately, by duration, at gap-length strata, since a single combined score hides exactly the trade being decided.

### Cited Findings
- **Definitions.** A fragmentation occurs when the tracker changes identity while the true identity does not change. A merge is the reverse: two true identities merged into one. A mismatch is either. — [Ristani et al. 2016](https://arxiv.org/abs/1609.01775)
- **Formula for MOTA.** MOTA = 1 − (FN + FP + IDSW)/gtDet. — [Luiten et al. HOTA](https://arxiv.org/abs/2009.07736)
- MOTA was introduced with MOTP in the CLEAR evaluations. — [Bernardin and Stiefelhagen 2008](https://zendy.io/pdf-viewer/10.1155/2008/246309)
- **Counts vs. extent.** Ristani's Figure 1: one identity broken into two fragments costs one error, and into eight costs seven under event counts, even when identity 1 still covers the same 67% in both. The identity measure charges 33% to each. IDP = IDTP/(IDTP+IDFP), IDR = IDTP/(IDTP+IDFN), IDF1 = 2·IDTP/(2·IDTP+IDFP+IDFN). — [Ristani et al. 2016](https://arxiv.org/abs/1609.01775)
- **Critique of both.** HOTA's authors argue MOTA is dominated by detection and IDF1 by association; IDF1 is non-monotonic in detection, and its bijective trajectory match ignores association outside matched sections. Their score separates DetA and AssA. — [Luiten et al. HOTA](https://arxiv.org/abs/2009.07736)
- FA-HOTA, in the same paper, adds fragmentation awareness because HOTA by design measures long-range alignment. — [Luiten et al. HOTA](https://link.springer.com/article/10.1007/s11263-020-01375-2)
- HOTA is itself disputed; a 2024 analysis argues it is not a mathematically well-defined metric. — [TGOSPA, arXiv 2412.08321](https://arxiv.org/pdf/2412.08321)
- **Multi-target tracking side.** OSPA(2) was proposed because OSPA misses label switching and fragmentation, while remaining a true metric. — [Beard et al., Curtin](https://espace.curtin.edu.au/handle/20.500.11937/73225)
- **MT, PT, ML, FRMT, IDS** as used by Zhang: mostly tracked > 80%, mostly lost < 20%, FRMT the count of interruptions, IDS the count of swaps. — [Zhang 2008 PDF](https://www.cvlibs.net/projects/autonomous_vision_survey/literature/Zhang2008CVPR.pdf)
- **MCMCDA's measures** suit association directly: NCA (correct associations over test associations), ICAR (incorrect over correct), and Kerr (error in number of tracks). — [MCMCDA tech report](https://aima.eecs.berkeley.edu/~russell/papers/tr-mcmcda.pdf)
- **Radar practice.** Mean track life plus position, speed and course RMS. — [Yeom 2003 record](https://experts.mcmaster.ca/scholarly-works/144032)
- **Measured trade-offs in this note.** Fragments 58 → 20 with ID switches 7 → 15. — [Zhang 2008 PDF](https://www.cvlibs.net/projects/autonomous_vision_survey/literature/Zhang2008CVPR.pdf)
- In Quo Vadis, removing matching thresholds raised lost-ID reduction to −28.02% from −20.53%, at a cost in association precision. — [Dendorfer et al.](https://ar5iv.labs.arxiv.org/html/2210.07681)
- MHT "fragments tracks" at high detection probability. — [MCMCDA tech report](https://aima.eecs.berkeley.edu/~russell/papers/tr-mcmcda.pdf)

### Inferences
- **Errors are asymmetric here, and the literature's scores treat them symmetrically.** A false join produces one flight with a wrong ADES/ADEP pair and a silently dropped landing and take-off. A fragment yields two truthful pieces with a missing endpoint on each. A published dataset should probably weigh false joins more heavily. That is my judgement and a decision for the project, not a literature result.
- **A suggested measurement design**, given PRISME truth:
  - Per boundary: precision and recall of "same flight", stratified by gap length (for example <30 min, 30 min-2 h, 2-6 h).
  - Per airframe-day: IDF1-style score weighted by time or distance, plus merges and fragments counted separately.
  - Downstream: change in ADEP/ADES correctness and take-off and landing event counts.
  - Kerr equivalent: flights produced minus flights in truth.
- **MOTA and HOTA largely do not apply.** Detection is not at issue, since all samples are given and the score would collapse to association. IDF1, merges and fragments are the relevant parts.
- **Any re-linking changes `track_id` or adds a parallel flight identifier.** `CLAUDE.md` documents `track_id` boundaries as silent to joiners, and that the data has no version column. So a linker would best be an additive identifier over existing tracks, leaving `track_id` alone. That design point is my suggestion.

### Gaps
- "Track purity" was requested. I found no primary definition or reported values in what I retrieved.
- No source reports these metrics across gaps of hours in any domain with comparable sampling. Published numbers here come from video at frame scale or radar at scan scale, so none can serve as a prior for OPDI's false-join rate. Only a PRISME benchmark can supply that.
