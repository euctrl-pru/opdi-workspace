# Maritime AIS gap handling and voyage segmentation, as an analogue for linking fragmented ADS-B flights

Evidence grades used below, because they matter for how far to trust a number:
- **[read]**: I read the passage in the primary source (HTML, PDF text, or source code).
- **[excerpt]**: seen only in a search-result excerpt or summary. The full paper was not read, so verify before quoting.

## 1. How do voyage/trip segmentation methods work, and what gap thresholds do they use?

### Takeaway
There is no standard threshold. Published AIS gap cut-offs run from minutes to 48 hours, and most are justified by the study's coverage or transit geometry, not by a validated error rate. The most useful pattern for OPDI is **split conservatively, then rejoin on a physical test**, as in Global Fishing Watch's segmenter, Zhao et al. and AISdb. Pure time-gap splitters such as MovingPandas do not attempt to link.

### Cited Findings

**Threshold inventory**

| Method / study | Time-gap rule | Stop or speed rule | Stated justification | Grade |
|---|---|---|---|---|
| GFW `gpsdio-segment`, current source | `max_hours = 8` (`penalty_hours = 4`) | `max_knots = 25` between points | none in the code | [read] [source](https://raw.githubusercontent.com/GlobalFishingWatch/gpsdio-segment/master/gpsdio_segment/core.py) |
| GFW `gpsdio-segment`, README CLI | `--max-hours` default 24 | `--max-speed` default 30 kn; deprecated `--noise-dist` 0.27 nm | none | [read] [README](https://raw.githubusercontent.com/GlobalFishingWatch/gpsdio-segment/master/README.rst) |
| Hütten, Baltic Sea | >48 h, or distance >750 km | SOG <0.5 kn | 48 h and 750 km are upper bounds for crossing the study region; 0.5 kn is a literature convention plus a minimum in the log-speed distribution | [read] [PDF](https://arxiv.org/pdf/2511.23016) |
| Patroumpas et al., DMA passenger ships | gap *event* at ΔT = 30 min (example); trip break at δ = 3 h (the evaluation section says 1 h, an internal inconsistency) | stop at v <0.5 kn; slow motion at about 1 kn | δ is set longer than ΔT because a vessel can lose contact in open sea while still heading to its destination | [read] [HTML](https://arxiv.org/html/2603.12287) |
| HABIT, DMA and AegeaNET | ΔT = 30 min | stop at <0.5 kn | none stated in what I extracted | [read] [HTML](https://arxiv.org/html/2602.11890) |
| Zhao et al. 2018 (Journal of Navigation), as described by Paulig & Okhrin | Δt >600 s | implied SOG >15 kn splits; sub-tracks are rejoined on implied SOG alone | none in the secondary description | [read, secondary] [Paulig & Okhrin](https://arxiv.org/pdf/2407.04402) |
| Paulig & Okhrin α-method | empirical (1−α) quantile of Δt; 90th, 95th and 99th percentiles of gap are 361 s, 391 s and 6,176 s | length-dependent quantiles of ΔSOG, ROT, distance and reported-minus-implied SOG | data-driven; explicitly rejects AIS transmission intervals as a basis | [read] [PDF](https://arxiv.org/pdf/2407.04402) |
| AISdb | `split_timedelta` commonly 24 h in examples | `speed_threshold` default 50 kn; `distance_threshold` required (examples: 20, 50, 200 km); `minscore` 1e-6 | none | [excerpt] [API](https://aisdb.meridian.cs.dal.ca/doc/api/aisdb.denoising_encoder.html), [tutorial](https://aisviz.gitbook.io/documentation/tutorials/data-cleaning) |
| MovingPandas | `ObservationGapSplitter(gap=...)`, no default; doc example `timedelta(hours=1)` | `StopSplitter(max_diameter, min_duration)`, example 7 and 60 s; `SpeedSplitter(speed, duration)`, example 10 and 5 min | library, so the user chooses | [read] [docs](https://movingpandas.readthedocs.io/en/main/api/trajectorysplitter.html) |
| TempTPI | >15 min | minimum duration and motion criteria | not checked | [excerpt] [arXiv](https://arxiv.org/pdf/2609.09840) |
| STCAD | voyage = interval <2 h | not checked | not checked | [excerpt] [arXiv](https://arxiv.org/pdf/2608.10249) |

**How the main designs work**

- **The GFW segmenter assigns each point to one of several open segments.** It keeps up to 20 open segments and looks back 5 points in each. A candidate is rejected if `hours > 8` or if discrepancy exceeds `hypot(hours, 0.25) * 25 kn`. Otherwise the score is `exp(-alpha^2) / padded_hours`, with `alpha = (25/5) * discrepancy / max_allowed`. — [core.py](https://raw.githubusercontent.com/GlobalFishingWatch/gpsdio-segment/master/gpsdio_segment/core.py)
- **Ambiguity breaks the link instead of forcing a choice.** If several segments score within a factor of 10 of the best, the message closes them and starts a new segment. Transponder class mismatch multiplies the score by 0.1. — [core.py](https://raw.githubusercontent.com/GlobalFishingWatch/gpsdio-segment/master/gpsdio_segment/core.py)
- **The README and the code disagree.** The README says 24 h and 30 kn; the code says 8 h and 25 kn. The README values look stale, so cite the code. — [README](https://raw.githubusercontent.com/GlobalFishingWatch/gpsdio-segment/master/README.rst), [core.py](https://raw.githubusercontent.com/GlobalFishingWatch/gpsdio-segment/master/gpsdio_segment/core.py)
- **GFW's voyage layer is port-based and sits above segments.** A voyage is the transit bracketed by two port visits. — [excerpt] [GFW docs](https://globalfishingwatch.github.io/GFW_technical_documentation/files/anchorages-voyages-port-visits.html)
- **A GFW port visit needs at least two of entry, stop or gap, and exit.** Entry is within 3 km of an anchorage point. A stop starts below 0.2 kn and ends above 0.5 kn. A gap is an AIS gap longer than 4 h. Exit is beyond 4 km. Confidence is 2 for a stop or gap alone, 3 for entry or exit plus a stop or gap, and 4 for all three. — [read] [GFW API caveats](https://globalfishingwatch.org/our-apis/documentation/docs/v3/general-api-doc/data-caveats)
- **TREAD (Pallotta, Vespe & Bryan 2013) does not link across gaps.** A vessel unseen for time τ becomes "lost" and generates an exit event; on reappearing it generates an entry event. Entries, exits and stationary events are clustered by incremental DBSCAN into waypoints, and routes connect waypoints. — [read] [Entropy 15(6)](https://doi.org/10.3390/e15062218)
- **TREAD publishes no universal parameters.** τ, `min_speed`, Eps and N are inputs, and Eps and N are "tuned" per waypoint type and monitored area. I found no numeric value for τ or `min_speed` in the extracted text, though a table or figure may have been lost in extraction. — [read] [Entropy 15(6)](https://doi.org/10.3390/e15062218)
- **TREAD distrusts the reported speed field.** Stationary events use displacement over elapsed time, because "the field, SOG, in the AIS messages is unreliable to be used in detecting stationary events". — [read] [Entropy 15(6)](https://doi.org/10.3390/e15062218)
- **TREAD names three causes of lost tracks.** They are terrestrial coverage gaps, intermittent AIS, and long intervals between satellite overpasses or low detection probability. Intentional switch-off is called "a separate issue". — [read] [Entropy 15(6)](https://doi.org/10.3390/e15062218)
- **Hütten uses a speed-dependent absence threshold.** Where a movement starts or ends in a boundary transit area, a vessel counts as having left only if the time between movements exceeds `t_thr = 6 h * (max(v_exit, v_entry) / 10 kn)^-4`. This falls by more than an order of magnitude when speed doubles. More than three messages between movements means the vessel stayed. — [read] [PDF](https://arxiv.org/pdf/2511.23016)
- **MovingPandas is a set of independent splitters.** The trajectory-splitter page documents no plausibility test or rejoin step. — [read] [docs](https://movingpandas.readthedocs.io/en/main/api/trajectorysplitter.html)
- **AISdb splits, then reconnects.** `encode_greatcircledistance` partitions on speed or distance, then links the most likely sequential segments if `encode_score` reaches `minscore`. Its documentation describes `distance_threshold` inconsistently. — [excerpt] [API](https://aisdb.meridian.cs.dal.ca/doc/api/aisdb.denoising_encoder.html), [source](https://aisdb.meridian.cs.dal.ca/doc/_modules/aisdb/denoising_encoder.html)

### Inferences
- Thresholds track the receiver regime and the question being asked. Studies with dense terrestrial coverage tend to use minutes; satellite or global tools use hours to a day. No threshold is a physical constant, so a value should not be copied from maritime work to aviation.
- Two ideas should be separated, as Patroumpas et al. do: "a gap occurred" (ΔT) and "the trip ended" (δ, with δ > ΔT). OPDI could annotate a gap without committing to a split.
- The GFW segmenter's rule that ambiguity closes segments is a good default for OPDI: an unnecessary split is visible and recoverable, while a wrong merge is silent.
- Its stateful open-segment design does not map onto OPDI's stated convention of native Spark window expressions without `applyInPandas` ([CLAUDE.md](/home/jupyter/work/opdi-workspace/CLAUDE.md)). A pairwise test on fragment endpoints would fit that convention better.

### Gaps
- I found no published Danish Maritime Authority segmentation rule. DMA appears as a data source; the rules attached to it belong to individual papers. The DMA documentation itself was not retrieved. A [HELCOM guidance draft](https://vasab.org/wp-content/uploads/2018/06/2016_HELCOM_AIS-expert-group_guidance-data-handling_draft.pdf) surfaced as an excerpt but was not read.
- GFW's documentation does not state a validated justification for 8 h and 25 kn. The current production segmenter may differ from the public `master` branch.
- The `DiscrepancyCalculator` formula was not visible in the file read, so I cannot say how reported speed or course enter the discrepancy.
- I did not find a review comparing segmentation thresholds against a common benchmark.

## 2. How are gaps assessed for plausibility (implied speed, speed limits, shortest path)?

### Takeaway
The standard test is one-sided: implied speed above a cap means "not the same track", and everything else is merely permitted. Caps of 15 to 50 kn are used with little evidence. Implied speed well *below* expected speed, the hidden-stop signal OPDI cares about, is described mostly in non-peer-reviewed material, and I found no validated threshold for it.

### Cited Findings
- **Upper caps in use:** 15 kn implied (Zhao et al.); 25 kn (GFW code); 30 kn (GFW README, and Chen et al. 2020 per Paulig & Okhrin); 50 kn (AISdb default). — [Paulig & Okhrin](https://arxiv.org/pdf/2407.04402), [core.py](https://raw.githubusercontent.com/GlobalFishingWatch/gpsdio-segment/master/gpsdio_segment/core.py), [AISdb, excerpt](https://aisdb.meridian.cs.dal.ca/doc/_modules/aisdb/denoising_encoder.html)
- **Evidence for the 30 kn cap:** in Paulig & Okhrin's data about 0.3% of messages report more than 30 kn, so the filter keeps 99.7%. — [read] [PDF](https://arxiv.org/pdf/2407.04402)
- **Ship-class-aware limits exist.** Prior work includes ship-type maximum speeds, tactical diameters and acceleration limits of ±1 kn/s; Paulig & Okhrin use ship length as a manoeuvrability proxy. — [read] [PDF](https://arxiv.org/pdf/2407.04402)
- **Reported speed can be compared with implied speed.** Paulig & Okhrin split when reported-minus-implied SOG falls outside its (α/2, 1−α/2) quantile band. — [read] [PDF](https://arxiv.org/pdf/2407.04402)
- **Sentinel speeds must be excluded first.** 102.3 kn means "not available" in AIS, and GFW's code excludes reported speeds in the ranges 51.15–51.25, 62.95–63.05 and 102.25–102.35. — [Paulig & Okhrin](https://arxiv.org/pdf/2407.04402), [core.py](https://raw.githubusercontent.com/GlobalFishingWatch/gpsdio-segment/master/gpsdio_segment/core.py)
- **Acceleration is used too.** Hütten removes messages iteratively on infeasible speed and acceleration, noting that vessels rarely exceed 0.1 g horizontally. — [read] [PDF](https://arxiv.org/pdf/2511.23016)
- **The space-time prism is the formal version of the cap.** With a maximum speed, the reachable area during a gap is a geo-ellipse. The Abnormal Gap Measure is the fraction of cells inside that region where other vessels normally report. Sweeps ran over 2–10 h gaps and 10–50 m/s speeds. — [read] [Sharma, Ghosh & Shekhar](https://arxiv.org/html/2403.06268)
- **That method excludes the difficult cases.** It does not model acceleration, and it excludes sparse regions, device malfunction and weather-related signal loss. — [read] [Sharma et al.](https://arxiv.org/html/2403.06268)
- **Implied-speed reasoning breaks down when identity is doubtful.** One study notes that with many missing points, speed between adjacent points cannot separate a spoofing ship from a regular one. — [excerpt] [JMSE 11(8) 1516](https://mdpi-res.com/d_attachment/jmse/jmse-11-01516/article_deploy/jmse-11-01516.pdf)
- **The low-speed signal appears in non-peer-reviewed material.** One source compares gap displacement with last reported speed times gap duration and reads a short displacement as a possible stop or port call. No error rate is given. — [excerpt, personal developer site](https://moonligh7er.github.io/developer-site/portfolio-pieces/DeepSeer/blog/ais-dark-transits)
- **Route-network shortest paths are used to fill gaps, not to test them.** Hütten reconstructs gap routes as "the most plausible, valid shortest paths" or mooring states. HABIT and GTI build graphs from historical AIS. — [Hütten](https://arxiv.org/pdf/2511.23016), [HABIT](https://arxiv.org/html/2602.11890)

### Inferences
- The speed cap is a falsifier only. It can show that two fragments are not one uninterrupted movement, but it cannot confirm that they are. OPDI needs both a cap and a lower-side test, and the maritime literature offers little validated support for the lower side.
- A per-aircraft-class cap would be the direct analogue of ship-type limits, with wind added.
- For a merge decision, the most transferable test here is Paulig & Okhrin's *reported versus implied* speed comparison, applied across the gap. Reported speed at the gap edges gives the expected value that a lower-side test requires.

### Gaps
- I found no study reporting the precision or recall of implied speed versus reported speed as a same-voyage classifier, and no validated "implied speed below X% of expected means hidden stop" threshold.
- I found no study that compares great-circle and route-network distance as the denominator of a plausibility test.

## 3. How are dark periods and hidden port calls detected, and what are the error rates?

### Takeaway
Welch et al. 2022 is the benchmark: a rule-based filter with a **3.72% false-positive rate and 0.86 precision**, and an F0.5 of 0.739 that implies **recall of about 0.47** (my calculation). Its core principle is that a gap is informative only where and when the receiver would have heard the vessel. I found no published false-positive or false-negative rate for detecting a hidden port call specifically.

### Cited Findings
- **Welch filters, before classification:** gap of at least 12 h; more than 50 nm from shore; predicted satellite reception above 10 positions/day. — [read] [Welch et al. 2022](https://doi.org/10.1126/sciadv.abq2109), [PDF copy](https://www.seafdec.or.th/iuu-events/estimate-iuu-fishing-losses-2026/download/references/Welch%20et%20al%202022%20Hot%20spots%20of%20unseen%20fishing%20vessels.pdf)
- **Why 12 h:** below it, the number of satellites overhead varies substantially, so gaps do not indicate disabling. — [read] [Welch et al.](https://doi.org/10.1126/sciadv.abq2109)
- **Why 50 nm:** to control for interference in crowded near-shore waters and dropped signals at the terrestrial-to-satellite changeover. — [read] [Welch et al.](https://doi.org/10.1126/sciadv.abq2109)
- **Final model:** a ping-rate threshold of 14 positions in the 12 h before the gap. — [read] [Welch et al.](https://doi.org/10.1126/sciadv.abq2109); confirmed as "at least 14 satellite positions" in [GFW API caveats](https://globalfishingwatch.org/our-apis/documentation/docs/v3/general-api-doc/data-caveats)
- **Reported performance:** F0.5 = 0.739, false-positive rate 3.72%, precision 0.86. The model was tested against a labelled set of intentional disabling events from exactEarth, using repeated k-fold cross-validation. — [read] [Welch et al.](https://doi.org/10.1126/sciadv.abq2109)
- **Scale:** 55,368 events, 5,269 MMSI, 101 flag states; more than 40% of vessels in these waters had an event; up to 6% (more than 4.9 M hours) of activity obscured. — [read] [Welch et al.](https://doi.org/10.1126/sciadv.abq2109)
- **GFW's production tiers:** all gaps over 6 h are recorded as "naive gaps", then filtered with the Welch criteria. — [read] [GFW API caveats](https://globalfishingwatch.org/our-apis/documentation/docs/v3/general-api-doc/data-caveats)
- **A gap itself counts as port-call evidence.** In GFW's definition a gap over 4 h in port can stand in for a stop, recorded as starting 4 h after the last message. Stop-or-gap-only visits are confidence 2, may be false, and "require a further inspection". — [read] [GFW API caveats](https://globalfishingwatch.org/our-apis/documentation/docs/v3/general-api-doc/data-caveats)
- **A real false-positive mode was found and fixed.** In October 2024 GFW removed false confidence-4 visits created when a vessel moved for more than 4 h near anchorages without stopping. — [excerpt] [GFW FAQ](https://globalfishingwatch.org/faqs/port-visit-data-updates/)
- **Loitering:** average speed below 2 kn while averaging at least 20 nm from shore; it does not necessarily indicate transshipment. — [read] [GFW API caveats](https://globalfishingwatch.org/our-apis/documentation/docs/v3/general-api-doc/data-caveats)
- **Loitering and encounter durations differ between documents.** GFW documentation attributes an 8 h minimum to Miller et al. 2018 and uses 4 h in one study. Encounters are within 500 m for at least 2 h. — [excerpt] [GFW loitering docs](https://globalfishingwatch.github.io/GFW_technical_documentation/files/loitering.html), [Miller et al.](https://www.frontiersin.org/articles/10.3389/fmars.2018.00240)
- **Receiver-side modelling is an alternative.** Mazzarella et al. 2017 use received signal strength at base stations and characterised coverage patterns to decide whether a shortage of messages is alarming. — [excerpt] [JRC record](https://publications.jrc.ec.europa.eu/repository/handle/JRC102381)
- **Vendors confirm the problem but publish no rates.** Windward describes analysts struggling with false positives when separating "lost" from "dark". — [excerpt, vendor] [Windward](https://windward.ai/blog/mind-the-ais-gap/)
- **Not every study tries.** Hütten does not distinguish deliberate from unintentional gaps, and notes vessels sometimes switch off while moored. — [read] [PDF](https://arxiv.org/pdf/2511.23016)

### Inferences
- **Recall is my calculation, not a reported number.** From F0.5 = 1.25PR/(0.25P+R), with P = 0.86 and F0.5 = 0.739, R is about 0.47. This assumes the reported values are mutually consistent point estimates; supplementary table S2 should hold the actual figure.
- Selecting on F0.5 and applying conservative pre-filters means the Welch dataset is a high-confidence subset of gaps, not a census of all disabling. "Up to 6%" is the authors' wording and should be quoted as such.
- Welch's event-history conditioning (14 pings in 12 h) transfers directly: a fragment-end gap should count as evidence only if the aircraft was being received well just before it.
- GFW's treatment of in-port gaps as stop evidence maps closely to a gap beginning low and near an aerodrome. The October 2024 fix is the matching warning: proximity plus time produces false visits unless a real stop is seen.

### Gaps
- I found no study with per-event precision or recall for hidden port calls inferred from gaps, despite searching for this directly. The closest validation is at aggregate count level (see question 4).
- Welch's supplement (sections S1–S5, table S2) was not available. The construction of the exactEarth labels and the false-negative rate are therefore unverified.
- The paper gives two message counts: 28 billion in Methods and 3.7 billion from fishing vessels in Results. I did not resolve whether these are different populations, though the wording suggests so.

## 4. How has voyage linking been validated when ground truth is limited?

### Takeaway
Four approaches recur: labelled third-party event sets, aggregate comparison with official statistics, synthetic withheld gaps, and indirect internal metrics. Each misses something. **Synthetic gaps test filling, not the linking decision**, because a gap cut from a continuous trip is by construction a true link, so such designs contain no negatives.

### Cited Findings
- **Labelled events:** Welch used exactEarth-labelled intentional events with repeated k-fold CV. — [read] [Welch et al.](https://doi.org/10.1126/sciadv.abq2109)
- **Aggregate counts against official figures (1):** Iphar et al., Ocean Engineering 2024: 171,626 calls at 101 Lesser Antilles ports, median difference of about 11% from official statistics. — [excerpt] [HAL record](https://hal.archives-ouvertes.fr/hal-04400011)
- **Aggregate counts against official figures (2):** Ireland's CSO found AIS calls closely follow official numbers nationally, but Drogheda had significantly fewer AIS-detected visits. — [excerpt] [CSO](https://www.cso.ie/en/releasesandpublications/fp/fp-pvrts/portvisitsusingreal-timeshippingdata/results/)
- **Aggregate counts against official figures (3):** Hütten's results fall within 20% of earlier proprietary-data studies. — [read] [abstract](https://arxiv.org/abs/2511.23016)
- **Bracketing by parameter variation:** Hütten re-runs the analysis with a set that further reduces false positives and a set that reduces false negatives, using the spread as a systematic uncertainty. — [read] [PDF](https://arxiv.org/pdf/2511.23016)
- **Re-running on the rejected data:** Hütten runs the whole pipeline on messages removed by cleaning, to bound what was lost. — [read] [PDF](https://arxiv.org/pdf/2511.23016)
- **Synthetic gaps with DTW scoring:** HABIT inserts one gap at a random location per trip at 60, 120 and 240 min, using 70% of trips to build graphs and 30% to test. Scores are DTW distances against the original path, with imputed points at most 250 m apart. — [read] [HABIT](https://arxiv.org/html/2602.11890)
- **Synthetic scenarios beyond one gap:** DGIVT uses about 1,450 trajectories with single-gap, multiple-gap and reduced-frequency cases. — [excerpt] [AAU](https://projekter.aau.dk/dgivt-depth-map-enhanced-graph-imputation-for-vessel-trajectories-f35050d4.html)
- **Learning coverage as a proxy:** TREAD reports the share of messages mapped to learned routes over 50 days: up to 95% (Strait of Gibraltar), 70% (North Adriatic), 40% (Indian Ocean, satellite). — [read] [Entropy 15(6)](https://doi.org/10.3390/e15062218)
- **Some authors say no quantitative comparison is possible.** Paulig & Okhrin state that "no definitive ground truth" exists and fall back on indirect measures such as maximum jump inside a trajectory. — [read] [PDF](https://arxiv.org/pdf/2407.04402)
- **Their illustrative comparison (one example):** maximum distance inside a trajectory is 53.06 nm for Zhao et al. (0 splits), 37.87 nm for Guo et al. (122 discarded messages), and 1.57 nm for the proposed method (4 splits). — [read] [PDF](https://arxiv.org/pdf/2407.04402)
- **Circular labelling is a live risk.** Sharma et al. label synthetic gaps abnormal if AGM exceeds 0.6, a threshold chosen through trial runs, then score AGM-family methods against those labels. Their accuracy definition is also inconsistent. — [read] [HTML](https://arxiv.org/html/2403.06268)
- **Evaluation protocol can swamp method differences.** On AIS prediction, sharing vessels between train and test lowers one-hour error by 23–25%, and a region-disjoint split raises TrAISformer's one-hour error on the US corpus from 2.2 km to 24.6 km. — [read] [abstract](https://arxiv.org/abs/2609.25827)
- **Independent data is advised.** Emmens et al. 2021 conclude AIS should be corroborated with other sources, because relying on it alone can lead to inconsistencies. — [excerpt] [UvA PDF](https://pure.uva.nl/ws/files/67787170/1_s2.0_S0957417421004164_main.pdf)

### Inferences
- OPDI has an advantage no AIS study above had: flight-level truth from `flights_tidy()` and APDF, joined on `AIRCRAFT_ADDRESS`, callsign and date ([CLAUDE.md](/home/jupyter/work/opdi-workspace/CLAUDE.md)). This allows per-event precision and recall for the European area, which the maritime field mostly cannot report.
- **A withheld-data design needs negatives.** Positive pairs come from cutting a gap in a continuous flight. Negative pairs come from cutting a window around a real turnaround of the same airframe, so the two sides are truly separate flights. I did not find this construction in the AIS literature read here, so it appears to be a contribution OPDI could make, not a borrowed method.
- Aggregate agreement (11%, 20%) can hide offsetting errors, as false links and false splits cancel in counts. It should not be used as the primary validation.
- The region-disjoint result matters directly for worldwide coverage: a learned route prior tuned in Europe should be expected to degrade elsewhere unless measured.

### Gaps
- I found no AIS study that validates the link-or-split decision itself with per-pair precision and recall against port-call logs.
- The Iphar, CSO and Emmens items are based on excerpts, not full text.

## 5. What is known about identity problems (MMSI sharing/spoofing), and is there an aviation analogue?

### Takeaway
Shared identities are rare by signal share but disproportionately damaging: a merged track gets impossible jumps, and outlier filtering can silently discard a second real vessel. Aviation has a direct analogue in duplicate or placeholder ICAO 24-bit addresses, but prevalence evidence there is thin.

### Cited Findings
- **Prevalence:** GFW reports spoofing at less than a quarter of one percent of AIS signals received. No independent verification was found. — [read] [GFW](https://globalfishingwatch.org/?p=387)
- **Separation method:** GFW treats positions needing an unreasonable speed as unrelated and groups possible ones into tracks. It is straightforward when vessels are far apart and hard at close range, where it can yield only short, choppy segments. No numeric parameters are given. — [read] [GFW](https://globalfishingwatch.org/?p=387)
- **Placeholder MMSIs:** country-prefix-plus-zeros values such as 412000000 are common. — [excerpt] [GFW](https://globalfishingwatch.org/?p=387)
- **Production flag:** an MMSI with `overlap_hours_multinames` above 24 in a year is treated as spoofed; this counts hours with overlapping segments of multiple identities. — [excerpt] [GFW vessel info tables](https://globalfishingwatch.github.io/GFW_technical_documentation/files/vessel-info-tables.html)
- **Bad identifiers dominate MMSI counts, not messages.** Removing vessels confined to a 400 m square drops 1% of messages but 37% of observed MMSI numbers, including spurious scrambled ones. — [read] [Hütten](https://arxiv.org/pdf/2511.23016)
- **Bounded impact in one regional study.** Cleaning keeps only the first of vessels sharing an MMSI. Re-analysis of the rejected data gives 7,104 days of valid tracklets against 60,577 days retained, so activity is underestimated by at most 10% (or 3% under equal message frequency). Hütten treats this as negligible, consistent with the Kroodsma et al. supplement. — [read] [Hütten](https://arxiv.org/pdf/2511.23016)
- **Visible harm when unsplit.** Without splitting, trajectories show unrealistic jumps and appear to cross land; sharing, filter interruptions and re-enabling elsewhere are named causes. — [read] [Paulig & Okhrin](https://arxiv.org/pdf/2407.04402)
- **Outlier removal can destroy data.** Apparent outliers may be points of another ship sharing the MMSI. — [excerpt] [JMSE 11(8) 1516](https://mdpi-res.com/d_attachment/jmse/jmse-11-01516/article_deploy/jmse-11-01516.pdf)
- **Learned detection.** The same paper reports 88.4% average accuracy for identifying spoofing ships with an isolation forest. — [excerpt, not verified in full text] [JMSE 11(8) 1516](https://mdpi-res.com/d_attachment/jmse/jmse-11-01516/article_deploy/jmse-11-01516.pdf)
- **Aviation analogue, measured.** Aireon saw 47,684 distinct 24-bit addresses over 28 Aug–1 Sep 2018, with a small subset duplicated: 0x000001 with 32 aircraft IDs, 0x123456 with 23, 0xA00000 with 14. — [excerpt] [ICAO NACC](https://www2023.icao.int/NACC/Documents/Meetings/2018/ADSB/App5-Space%20ADS_B_TCAS%20and%20Data%20Observations.pdf)
- **Aviation causes and effects.** ICAO Asia-Pacific guidance attributes more than half of mismatched-address cases to database errors, and EUROCONTROL warns of garbled replies, track swapping and missed TCAS advisories. — [excerpt] [ICAO APAC guideline](https://www.icao.int/sites/default/files/APAC/Documents/edocs/CNS/APX-G-Guideline-on-Addressing-Inconsistencies-of-ICAO-AD-and-ID-Between-Surveillance-Data-and-Flight-Plan.pdf), [EUROCONTROL](https://www.eurocontrol.int/service/handling-non-equipped-mode-s-and-ads-b-state-aircraft)
- **Aviation identity already causes silent splits in OPDI.** 23.1% of A8's track boundaries traced to a callsign flicker leg. ADS-C's `flight` field is an IATA number (`LH133`), not the ICAO callsign, and would split merged tracks if carried through. — [CLAUDE.md](/home/jupyter/work/opdi-workspace/CLAUDE.md)

### Inferences
- The mapping is close: MMSI to `icao24`, placeholder 412000000 to 0x000001 or 0x123456, and ship name to callsign. GFW's "overlapping segments with multiple identities" translates into a simple OPDI check: the same `icao24` active at two separated positions at once, or with overlapping callsigns.
- A shortlist of known-bad addresses should be excluded from linking before any merge is scored. A merge across a gap on a placeholder address is the worst case, because it joins two unrelated aircraft and passes every speed test if they are close enough.
- Long gaps make identity checks weakest, as the JMSE paper notes for ships, so linking confidence should fall as gap length grows even if kinematics stay plausible.
- `icao24` persists across flights by design, so a clean identity does not imply the same *flight*. MMSI has the same property for voyages, which is why the identity literature does not solve OPDI's question on its own.

### Gaps
- I found no peer-reviewed prevalence study of duplicate 24-bit addresses, and did not retrieve Aireon's or EUROCONTROL's full monitoring reports. The Aireon figures are from one five-day window in 2018.
- I found no study that measures how much MMSI sharing degrades merged-voyage quality as an isolated effect. Hütten's bound is for activity volume.
- I did not research deliberate ICAO-address spoofing.

## 6. What is reported about benefit versus harm of merging across gaps, and about imputation versus linking without filling?

### Takeaway
No study I found measured merged versus unmerged voyages on a downstream metric with ground truth. Evidence is indirect. Linear interpolation is adequate for allocating time over gaps up to about two weeks at ship speeds, route-aware fills beat it on path shape in constrained waterways with inconsistent margins elsewhere, and under-splitting produces visible artefacts. Keeping "linked" separate from "filled" is consistent with this evidence and with OPDI's own negative result.

### Cited Findings
- **Linear versus raster allocation:** Welch's linear interpolation and rasterised probability method performed similarly for disabling events shorter than 2 weeks and deviated beyond that, so only sub-2-week events were mapped. — [read] [Welch et al.](https://doi.org/10.1126/sciadv.abq2109)
- **Route-aware fill versus straight line, measured by DTW:** at 60 min on KIEL, GTI and HABIT deviate less than straight-line interpolation; on the mixed-traffic SAR dataset, some GTI configurations are comparable to or slightly worse. Exact values are in figures, not text. — [read] [HABIT](https://arxiv.org/html/2602.11890)
- **Error versus gap length:** HABIT's median error does not grow proportionally across 1, 2 and 4 h gaps; this is mild on KIEL and more pronounced on SAR, with outliers. — [read] [HABIT](https://arxiv.org/html/2602.11890)
- **Straight lines can be physically wrong at sea.** HABIT notes linear fills give paths a vessel cannot follow. — [excerpt] [HABIT](https://arxiv.org/html/2602.11890)
- **Machine-learned fill, one report:** about 1.0 km versus 1.62 km for linear interpolation (38–39% lower) on a vessel-disjoint split with 30.6% missing, single-seed. The abstract I read does not contain this comparison, so treat it as unverified. — [excerpt] [arXiv 2609.25827](https://arxiv.org/pdf/2609.25827)
- **Graph imputation limits:** DGIVT beats linear interpolation and GTI on single and multiple gaps but underperforms on point count and shape. — [excerpt] [AAU](https://projekter.aau.dk/dgivt-depth-map-enhanced-graph-imputation-for-vessel-trajectories-f35050d4.html)
- **Kinematic baseline:** BAARTR, which enforces boundary velocities, reports the lowest median RMSE against linear, spline, Hermite, PCHIP and Akima. — [excerpt] [JMSE 14 116](https://mdpi-res.com/d_attachment/jmse/jmse-14-00116/article_deploy/jmse-14-00116.pdf)
- **Harm of not splitting:** see the 53.06 nm versus 1.57 nm maximum-jump comparison and land-crossing artefacts. — [read] [Paulig & Okhrin](https://arxiv.org/pdf/2407.04402)
- **Harm of splitting too much:** rejecting singular anomalous points may accidentally separate one trajectory, per Paulig & Okhrin citing Zhao et al.; both methods include a rejoin step. — [read] [PDF](https://arxiv.org/pdf/2407.04402)
- **Practical reconstruction with a coverage-limited dataset works at aggregate level.** Hütten's journey model yields within 20% of proprietary-data results. — [read] [abstract](https://arxiv.org/abs/2511.23016)
- **Aviation negative result on filling (OPDI's evidence base).** PRC 2025: "Attempts to complete the trajectory always lead to worse result". The 2024 winner masks bad values to NULL, preserving the distinction between no data and interpolated data. That target was fuel and mass estimation, not flight identity. — [CLAUDE.md](/home/jupyter/work/opdi-workspace/CLAUDE.md)

### Inferences

**Lessons that transfer**
1. **Separate three decisions:** annotate a gap, link fragments under one identity, and fill positions. Maritime work often blurs them; the evidence for each differs. OPDI can link without adding rows, which respects its NULL-mask design and the PRC result.
2. **Make gaps informative only where reception was good.** Welch's 12 h-before ping rule translates to pre-gap message rate per fragment, and its reception map to OSN coverage by altitude and region.
3. **Treat ambiguity as non-link.** This is GFW's design, and it keeps errors visible.
4. **Use displacement-derived speed**, not only reported speed (TREAD), and exclude sentinels first.
5. **Publish link confidence**, as GFW's port-visit levels do, so consumers can choose. OPDI's `osn_tracks` has no version column, and any linking is another silent `track_id` boundary unless expressed as a new field or version ([CLAUDE.md](/home/jupyter/work/opdi-workspace/CLAUDE.md)).
6. **Use a speed-dependent gap threshold.** Hütten's `v^-4` idea captures that the same silence means different things at different speeds.

#### What does not transfer because ships are slow and aircraft are fast, follow 3D profiles and have short port stays
The aircraft figures here are illustrative assumptions for arithmetic, not sourced measurements: cruise near 450 kn, and a turnaround of about 45 min. This whole block is my reasoning, with sources only where linked.

- **Absolute thresholds do not transfer.** GFW's bound allows 25 kn × 8 h = 200 nm of movement. A jet covers that in under 30 min. The maritime values, from minutes to 48 h and 15 to 50 kn, have no direct aviation meaning.
- **The time ratio is inverted, which weakens the main signal.** If a ship's port stay is long relative to the gap, a hidden stop lowers implied speed sharply. Over an illustrative 3 h gap, a 45 min aircraft stop lowers implied speed by only about 25%, before descent and climb costs, which may be close to what wind and routing can produce. So a lower-side implied-speed test must be calibrated for aviation, not borrowed.
- **The stop is short compared with GFW's evidence windows.** GFW's >4 h port gap, and the older documented [3 h minimum visit](https://globalfishingwatch.github.io/GFW_technical_documentation/files/anchorages-voyages-port-visits.html) (excerpt), would miss an ordinary airline turnaround. A long-stop rule does not catch the critical case: a short sector and a quick turn.
- **Aviation has evidence ships lack: the vertical profile.** A ship's state at each gap edge is 2D and speed. An aircraft has altitude and vertical rate, and a hidden landing needs descent and climb. Cruise-to-cruise gaps and descent-into-gap or climb-out-of-gap patterns have no maritime equivalent, so this should be a primary feature in OPDI even though the AIS literature offers no thresholds for it.
- **Coverage correlates with the event of interest differently.** Welch excludes the near-shore zone because reception there is ambiguous. Aircraft coverage is presumably weakest at low altitude near airfields, and OPDI's gap rules already carry a low-altitude variant (`track_gap_low_altitude_meters`, [CLAUDE.md](/home/jupyter/work/opdi-workspace/CLAUDE.md)). OPDI cannot simply exclude that zone, because it is where the decision matters. A cruise-level gap in poor coverage, by contrast, resembles satellite-AIS periodicity.
- **Route networks are likely weaker constraints.** Ships are confined by land, depth and traffic schemes, which is why route fills help in straits. Aircraft have far less horizontal confinement outside terminal areas, and TREAD's fall from 95% to 40% of traffic learned in unconstrained, low-rate areas points the same way. I would expect less benefit for aviation, but that is untested.
- **Speed caps do little for short aircraft gaps.** At sea a cap can exclude many candidates. For aircraft, the cap is wide relative to geography, so it rejects few pairs except genuine identity errors. Its role is more identity check than voyage test.
- **Physical limits differ.** Aircraft ground speed varies with wind and strongly with flight phase. A fixed cap should depend on phase and type, and ship acceleration limits (0.1 g, ±1 kn/s) do not apply.
- **The ADS-C parallel is real but limited.** OPDI's ADS-C feed (median cadence 277–1,139 s, 94–99% above FL300, `on_ground` never true) resembles satellite AIS, so Welch's reception-aware approach fits it ([CLAUDE.md](/home/jupyter/work/opdi-workspace/CLAUDE.md)). Its low-altitude sparsity (1–67 rows/day below 10,000 ft) means it rarely resolves a hidden landing directly.
- **Intent differs.** Maritime dark-vessel work targets deliberate concealment. I assume ADS-B gaps in OPDI are mostly coverage-driven, so disabling drivers (EEZ boundaries, transshipment, piracy) mostly do not apply, although the statistical logic does.

### Gaps
- I found no controlled measure of merge benefit or harm on downstream voyage metrics, in any domain read here.
- HABIT's actual linear-interpolation DTW values exist only in figures, which I could not read, so I cannot quantify its margin.
- BAARTR, DGIVT and arXiv 2609.25827's interpolation figures are excerpt-only, and their numbers should be checked before citation.
- Aircraft turnaround and cruise values above are unsourced assumptions. Other notes in this research effort may provide measured values.
- I did not reach Journal of Navigation, Ocean Engineering or IEEE T-ITS full texts directly, so review-level coverage is incomplete. Zhao et al. 2018 is described only through Paulig & Okhrin, and Mazzarella et al. 2017 only through its abstract.
