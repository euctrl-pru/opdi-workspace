# Aviation and ADS-B practice for splitting state vectors into flights and linking across reception gaps

Notes current to 2026-11-03. Evidence labels used below:
- **[source code]**: read directly by me. `traffic` is at commit `aee9ba0` (2026-07-20). readsb is the `dev` branch, downloaded 2026-11-03.
- **[paper text]**: read from the primary document, directly or through a fetch of the page.
- **[search extract]**: the primary page blocked direct fetching, or was not opened, so the claim rests on a search-index extract of it. Verify before quoting.

## How do `traffic`, `pyopensky`, OpenSky's flights tables, Junzi Sun's work and Schäfer et al. decide where a flight ends?

### Takeaway
Open-source tooling splits on a time gap of 10 minutes within an `(icao24, callsign)` group, or on a 30-minute density scale in Sun's clustering. Only OpenSky's published 2021 method has an explicit rejoin rule: pieces merge if a constant extrapolation fits within 0.5 degrees. None of these sources reports a measured error rate for the split decision.

### Cited Findings
**`traffic` (Olive)**
- `Flight.split()` defaults to `value=10`, unit minutes. The docstring says it "stops a flight and yields a new one after a gap of 10 minutes without data." **[source code]** — [flight.py L1478-1533](https://github.com/xoolive/traffic/blob/master/src/traffic/core/flight.py)
- `_split()` recurses on the single largest timestamp difference while that difference exceeds the threshold. It uses time only, with no altitude, position or speed test. **[source code]** — [flight.py L96-116](https://github.com/xoolive/traffic/blob/master/src/traffic/core/flight.py)
- `Traffic.iterate()` groups by `on=["icao24", "callsign"]`, then applies `Flight.split(by or "10 minutes")`. If a `flight_id` column exists, it uses that instead and does no splitting. **[source code]** — [traffic.py L640-694](https://github.com/xoolive/traffic/blob/master/src/traffic/core/traffic.py)
- `split()` accepts a `condition(f1, f2)` callback, and a split happens only if it returns true. The docstring example `no_split_below_5000ft` suppresses low-altitude splits caused by a filtered query. This is the library's only hook for conditional linking, and it ships no transoceanic default. **[source code]** — [flight.py L1495-1513](https://github.com/xoolive/traffic/blob/master/src/traffic/core/flight.py)

**`pyopensky`**
- `history()` with a flight-table join matches state vectors to `flights_data4` on callsign and on time bounds around `firstseen`/`lastseen`, with an optional buffer. So it inherits OpenSky's segmentation and adds no rule of its own. **[source code, lines seen by grep, not read in full]** — [trino.py L569, L684-694](https://github.com/open-aviation/pyopensky/blob/master/src/pyopensky/trino.py)

**OpenSky flights tables**
- OpenSky's current docs say flights are "reconstructed from the state vectors" but do not document the segmentation rules. `firstseen` is the "estimated start of the flight; may be null if departure was not observed." `lastseen` determines the `day` partition. "a callsign is not a unique flight identifier"; the stored callsign is the most frequently observed value. — [OpenSky Trino docs](https://openskynetwork.github.io/opensky-api/trino.html)
- The only published description I found is Strohmeier, Olive, Lübbe, Schäfer and Lenders (2021, ESSD 13, 357-366) **[paper text]**:
  - A flight is "the continuous time between the first received ADS-B contact of one specific aircraft and the last."
  - "Such a flight's length must be at least 15 min."
  - "If an aircraft leaves OpenSky's coverage range for more than a threshold T=10 min", the track is "considered finished at the point of last contact."
  - Exception: if time, distance and reported velocity fit a constant extrapolation, "minus a small threshold of 0.5° longitude and/or latitude", the segments are merged. Otherwise the aircraft is assumed to have landed outside coverage.
  - "If the last position seen is above 2500 m, no candidate is defined and the value is set to 'NULL'"; "If there is no airport within 10 km, the value is set to NULL."
  - The paper calls airport assignment "necessarily an extrapolation" and reports no quantified airport accuracy, failure rate, or share of missing origin/destination.
  - Source: [ESSD 2021](https://essd.copernicus.org/articles/13/357/2021/)
- The same dataset's records state: "no crosschecking with external sources of data has been conducted." **[search extract]** — [Zenodo](https://zenodo.org/records/7923702)
- A reviewer in the open-review discussion of the ESSD preprint flagged that an accuracy assessment "is currently missing." **[search extract]** — [ESSD RC1](https://essd.copernicus.org/preprints/essd-2020-223/essd-2020-223-RC1.pdf)

**Junzi Sun**
- Sun, Ellerbroek and Hoekstra (2017, JAIS, DOI 10.2514/1.I010520) use DBSCAN clustering to extract continuous flights, then fuzzy logic for phases. **[search extract of abstract]** — [TU Delft record](https://research.tudelft.nl/en/publications/flight-extraction-and-phase-identification-for-large-automatic-de/)
- The companion code scales time to 0-1000 and altitude to 0-100, sets `dt = mms.scale_ * 0.5 * 60 * 60  # time interval of 30 mins`, and calls `DBSCAN(eps=dt, min_samples=100)`. So the effective separation is a 30-minute time-scale distance, with `MIN_DATA_SIZA = 100` samples minimum. **[source code]** — [flightextract.py](https://github.com/junzis/flight-data-processor/blob/master/flightextract.py)
- Sun et al. (2023, SIDs) "initially segment the trajectory data into multiple flights" with `traffic`, then keep flights having data below 6,000 ft so that they run "from the beginning of the climb to the end of the descent." The paper states no linking rule beyond that. **[paper text]** — [SIDs 2023 paper 73](https://www.sesarju.eu/sites/default/files/documents/sid/2023/Papers/SIDs_2023_paper_73%20final.pdf)

**Other segmentation examples**
- A 2026 arXiv paper on controller awareness (V2TATC) splits tracks where the callsign changes or the gap exceeds two minutes. **[search extract]** — [arXiv 2608.28981](https://arxiv.org/pdf/2608.28981)

### Inferences
- A transatlantic flight with a multi-hour hole becomes two pieces under `traffic` defaults. Under the 2021 OpenSky rule it probably becomes two as well. 0.5 degrees of latitude is about 55 km, which is under 2% of a 4,000 km gap, and constant-velocity extrapolation over hours is unlikely to land that close. This is my arithmetic, not a measured result, and how the tolerance is applied is not specified beyond the quote.
- OpenSky's definition is keyed on "one specific aircraft" and it stores the modal callsign, so a callsign change does not appear to be a boundary there. `traffic` does treat it as one. A flight list and a `traffic` iteration can therefore disagree for the same state vectors.

### Gaps
- Whether `flights_data4`/`flights_data5` production still runs the 2021 rule is undocumented. I found no changelog.
- I did not read Schäfer et al. (2014), "Bringing up OpenSky", or the body of the 2017 JAIS paper. TU Delft PDF hosts refused connections, so the DBSCAN evidence is code plus abstract.
- I found no flight-segmentation logic documented for `pyModeS` (a decoder) or OpenAP (a performance model). This is absence of evidence from limited searching, not confirmation that none exists.

## How do Flightradar24, FlightAware, ADS-B Exchange/tar1090/readsb and Eurocontrol treat oceanic gaps, and by which identity do they link?

### Takeaway
readsb has the most useful public rule. It works per airframe and never starts a new leg on a time gap alone: it needs a major descent, then ground contact or a major climb. Commercial trackers carry flights through oceans with estimates and satellite data, using route knowledge, but do not publish their identity-matching algorithm.

### Cited Findings
**readsb / tar1090 (`mark_legs`, `globe_index.c` L1152-1600) [source code]** — [globe_index.c](https://github.com/wiedehopf/readsb/blob/dev/globe_index.c)
- Trace identity is the ICAO hex alone. The function does not read callsign.
- The `flags & 2` bit marks "start of a new leg (tries to detect a separation point between landing and takeoff that separates fligths)." — [README-json.md](https://github.com/wiedehopf/readsb/blob/dev/README-json.md)
- Adaptive altitude threshold: mean altitude / 3, clipped to 200-2,500 (units are feet, inferred from the `_alt_factor` scaling). A "major climb" or "major descent" is a high-to-low swing beyond that threshold.
- **Ground leg (`leg_now`):** a major descent exists, the aircraft is on the ground, and either the reception gap is over 25 min or it is over 45 min since last airborne.
- **Float leg, for gaps:** major climb later than major descent by over 12 min, and a reception gap over 5 min since the descent, where the sample after the gap is on the ground, has invalid baro altitude, or is below 20,000 ft. This is the closest public implementation of hidden-turnaround detection I found.
- **Float leg, for ground:** climb over 1 min after descent, and over 1 min on ground.
- **Leg boundary placement:** a reception gap over 5 min inside the descent-to-climb window; otherwise the midpoint of the ground period; otherwise the descent-to-climb midpoint.
- **Disabled:** a time/distance rule (`if (0 && elapsed > 30 min ... distance < 10 km per 30 min`) with the comment "disable .... let's see if we really need it."
- No accuracy figure is published. `--leg-focus=<hex>` prints the decisions for one aircraft. — [readsb man page](https://dyn.manpages.debian.org/unstable/readsb/readsb.1.en.html)

**Flightradar24**
- Estimates run up to 240 minutes after leaving coverage when the route is known, and about 10 minutes "for flights without a known destination." Web users can choose 5-240 minutes; apps cap at 120. **[search extract; direct fetch returned 403]** — [FR24 blog](https://www.flightradar24.com/blog/inside-flightradar24/exploring-estimated-coverage-in-flightradar24/)
- "Aireon positions will be displayed in 15-minute increments when the aircraft is outside terrestrial coverage areas." Terrestrial is primary, and Aireon is used only outside terrestrial or other satellite coverage. Reported February 2026. — [Runway Girl Network](https://runwaygirlnetwork.com/2026/02/flightradar24-closes-last-blind-spots-with-aireon-space-based-ads-b/)

**FlightAware**
- When the next report arrives, estimated positions are removed and a connecting great-circle segment is drawn. "the white line reflects a lack of data and you can't draw any conclusions from it." No thresholds are given. (2014 post.) — [FlightAware announcement](https://flightaware.com/news/article/Improvements-to-FlightAware-Maps--Understanding-Map-Behavior/196)
- A position-only flight has no filed plan and "may enter and leave areas of coverage." Some airlines send satellite position reports, providing "transoceanic coverage." The page does not say how positions match plans. — [FlightAware FAQ](https://www.flightaware.com/about/faq/)

**Eurocontrol / Network Manager**
- CPRs are "named 'correlated' because the 4D position ... is augmented with flight information such as CALLSIGN, ADEP, ADES and ICAO 24-bit address." **[search extract]** — [ansperformance.eu](https://ansperformance.eu/acronym/cpr)

### Inferences
- FR24's split between "known route" (240 min) and "unknown destination" (10 min) implies that schedule or route data sustains identity across gaps. FR24 does not state this as a linking rule.
- For a transatlantic flight, readsb's logic yields no leg boundary when both gap edges are at cruise, because no major descent occurred. It would miss a real hidden turnaround only if the descent or climb went unobserved on both sides, which is the hard case for any method.
- Network Manager identity is correlation against the flight plan, not inference from surveillance data, which is why the PRU ground truth does not face this problem in the same form.

### Gaps
- I found no public statement of how FR24 or FlightAware assign flight identity through a gap, and nothing authoritative on `fa_flight_id` regeneration.
- ADS-B Exchange documentation specific to oceanic linking was not found. I assume, unverified, that it behaves like readsb, whose trace format it is commonly associated with.
- I found no ETFMS or PRU document on oceanic CPR gaps or on the matching rules.

## What do PRC Data Challenge winners and other trajectory papers do with gaps, and are gap thresholds published with justification?

### Takeaway
PRC winners did not face the identity problem, because organisers supplied `flight_id`. Thresholds in use span 2 minutes to 6 hours and I found no study that justifies a value by measured linking accuracy. GAIA (2024) is the most complete published rule set.

### Cited Findings
**PRC winners**
- 2024 winner (`team_likable_jelly`) deduplicates on the organiser-supplied `flight_id` and `timestamp`, and its `FilterIsolated` masks values with no neighbour within 20 s (`df[v].mask(dt > 20)`). That is point cleaning, not splitting. **[source code]** Local files: `prc-data-challenges/2024/team_likable_jelly/filter_trajs.py` L21, `filterclassic.py` L63-75. Repository: [team_likable_jelly](https://github.com/PRC-Data-Challenge-2024/team_likable_jelly)
- 2025 winner (`resourceful-quiver`) tried adding origin/destination endpoints and completing vertical profiles, saw "significant higher RMSE", and removed both. Its conclusion: "Attempts to complete the trajectory always lead to worse result, though this maybe due to the biased heuristic we provided." **[paper text]** Local file: `prc-data-challenges/2025/resourceful-quiver/PRC_2025_report.pdf`, §3.2 and §7. Repository: [resourceful-quiver](https://github.com/PRC-Data-Challenge-2025/resourceful-quiver)
- The same repository has a 5-minute `assign_clusters()` in `preprocessor.py` L131-135, and a `fill_gap` trigger of `dt > 1000` s in `fill_gap.py` L127. A grep of `.py` files found no caller of `assign_clusters` outside its definition, and I did not scan notebooks. **[source code]**
- The 2025 dataset pairs ACARS with OpenSky ADS-B and is "augmented with flight list information from EUROCONTROL". The abstract notes "coverage gaps" but not how identities were matched. — [Sun, Spinielli, Strohmeier 2026, JOAS](https://journals.open.tudelft.nl/joas/article/view/8750)

**GAIA: Teoh et al. (2024, ACP), Supplement S1.2 [paper text]** — [supplement](https://acp.copernicus.org/articles/24/725/2024/acp-24-725-2024-supplement.pdf); [main paper](https://acp.copernicus.org/articles/24/725/2024/)
- Rationale: "multiple unique flights can share the same identifier and/or can be airborne at the same time in rare instances."
- Step 1: group by ICAO address. Sub-group when there is more than one unique call sign, aircraft type, origin-destination pair and/or tail number.
- Rejected outright: fewer than 3 waypoints, or recorded length of 5% or less of the OD distance.
- Split at flagged waypoints when any of these is violated:
  - Segment length must not exceed the OD great-circle distance, or 5,000 km without airport data.
  - `dt` must not exceed great-circle travel time at 180 m/s (jets) or 70 m/s (turboprop/piston), or 6 h without airport data.
  - Ground speed must be 100-350 m/s above 10,000 ft, or 20-300 m/s below it.
  - No waypoints below 10,000 ft between the start and end of cruise, unless it is a diversion. Flights without a cruise phase must be 2 h or shorter.
- **Diversion exception:** the flagged waypoints have `dt` below a "minimum aircraft turnaround time ... set at 10 minutes", segment length over 1 km, and under 2 h from the lowest flagged waypoint to the end.
- Gap filling: great-circle interpolation to 40-60 s. Step climb at the segment start/end if `dt` is 0.5 h or less, or at midpoint if over 0.5 h. Linear interpolation if absolute ROCD exceeds 500 ft/min. If `dt` exceeds 1 h below 50% of service ceiling, assume cruise at about 80% of ceiling.
- Measured context: about 90% of waypoints have `dt` < 300 s in terrestrial coverage, "but dt can be up to 40000 s (~11 h) when satellite data is not available."
- Rejection rates were about 6.6% (2019), 3.3% (2020), 4.5% (2021). OD metadata was available for 79% of flights (92% of jets). Waypoints spanned origin to destination for 67.4% of flights (77.6% of jets).
- The supplement shows one SIA26 example segmented into three flights. It reports no split or link accuracy.

**Threshold comparison**

| Threshold | Used by | Purpose | Stated justification |
|---|---|---|---|
| 20 s | team_likable_jelly | Isolated-point removal | None for flight identity |
| 2 min | V2TATC, 2026 | Track window split | Not seen |
| 5 min | readsb; apparently uncalled in resourceful-quiver | Candidate leg boundary, only with descent/climb | Comments only |
| 10 min | `traffic`; OpenSky 2021 T | Hard split (OpenSky can rejoin) | Not given |
| 10 min | GAIA | Minimum turnaround, for the diversion test | Stated assumption |
| 25 / 45 min | readsb | Ground leg | Comments only |
| 30 min | Sun DBSCAN | Density scale | Not in code; paper not read |
| 15 / 30 min | OPDI gap family (below 5,000 ft / general) | Local reference | Published contract; see `opdi/src/opdi/pipeline/segmentation/base.py` L84-90 |
| Great-circle time at 180 m/s, or 6 h | GAIA | Upper bound on `dt` | Physical plausibility |

### Inferences
- Rules cluster into three designs: time-only (`traffic`), time with a physics rejoin (OpenSky 2021, GAIA), and vertical-profile evidence (readsb, GAIA's condition 6).
- The PRC negative result concerns synthetic geometry and its authors allow that their heuristic may be at fault. It does not bear on assigning one identity across a gap while leaving the gap empty.
- **Local discrepancy to check:** `CLAUDE.md` lists "gap segmentation at 5 min" as a PRC finding worth porting. I could not find a 5-minute flight split in either winner's active path. The nearest is the apparently uncalled `assign_clusters`, and I did not read every file or notebook.

### Gaps
- I found no published ROC or precision/recall study of gap thresholds against ground truth such as flight plans or APDF.
- The 2024 challenge description paper and the other team reports were not reviewed for segmentation detail. The ansperformance.eu data page returned 403.

## What published work uses airframe rotation, ICAO24+callsign, turnaround detection or ADEP/ADES continuity, and how are hidden turnarounds detected?

### Takeaway
`(icao24, callsign)` is the default key, with OD and schedule metadata added by GAIA. I found no published method that uses airframe rotation sequences to join legs across gaps, and no validated hidden-turnaround detector. Practical detection rests on vertical-profile and timing heuristics.

### Cited Findings
- GAIA uses callsign, type, OD pair and tail number as separating attributes within an ICAO group, then plausibility rules. This is the closest published use of OD continuity. **[paper text]** — [supplement](https://acp.copernicus.org/articles/24/725/2024/acp-24-725-2024-supplement.pdf)
- GAIA's "below 10,000 ft during cruise" check with a 10-minute turnaround floor is an explicit published hidden-turnaround test. Below the floor the event is treated as a diversion candidate, and otherwise as a new flight. **[paper text]** — [supplement](https://acp.copernicus.org/articles/24/725/2024/acp-24-725-2024-supplement.pdf)
- readsb's float-leg rule infers a landing from descent, a gap over 5 min, then a climb more than 12 min later, with the post-gap sample below 20,000 ft or on the ground. **[source code]** — [globe_index.c](https://github.com/wiedehopf/readsb/blob/dev/globe_index.c)
- The OpenSky 2021 rule treats failed extrapolation as evidence the aircraft landed outside coverage. **[paper text]** — [ESSD 2021](https://essd.copernicus.org/articles/13/357/2021/)
- OpenSky Report 2025 splits ADS-C "into individual flights" by callsign, then queries ADS-B "with the same transponder code." No time thresholds are stated. **[paper text]** — [arXiv 2505.06254](https://arxiv.org/html/2505.06254v1)
- Lange, Sieling and Gonzalez Parra (about 2019, J. Air Transport Management) measure turnaround times from ADS-B. The summary page gives no definition or threshold. — [uni.lu summary](https://www.uni.lu/fdef-fr/?p=329)
- A delay-propagation study sets a 30-minute minimum service time in simulation; this is a model assumption, not a detector. **[search extract]** — [arXiv 1301.1136](https://arxiv.org/pdf/1301.1136)

### Inferences
- Signals available for a hidden turnaround, combining these sources: altitude at both gap edges; a descent before and climb after the gap; implied ground speed across the gap; callsign, OD and schedule change; and landing-candidate airport near the gap edges.
- A sound physical test, which I did not find published, would be to ask whether the gap is long enough for descent, minimum turnaround and climb at the observed geometry. GAIA's 10-minute floor is the only published turnaround number I found applied this way.
- A same-callsign return on a later day is handled by GAIA's `dt` limit, not by the callsign. So callsign alone cannot be the join key.

### Gaps
- No evaluated rotation-based linker was found in two targeted searches. The literature may exist under other terms, such as tail assignment; this is not proof of absence.
- I found no failure rate for the `(icao24, callsign)` key on long-haul flights. A study apparently defining "1 flight = unique pair (icao24, callsign)" appeared in search extracts, but I could not attribute it to a specific document, so it is not cited.

## What schedule and flight-plan reference data exists, and what is the evidence on its accuracy?

### Takeaway
Reference data exists but its open accuracy evidence is thin. The hardest public numbers are GAIA's coverage figures from commercial data, and these measure availability, not correctness.

### Cited Findings
- OpenSky's `estdepartureairport`/`estarrivalairport` are inferred and can stay null; "an airport can remain unknown." — [OpenSky Trino docs](https://openskynetwork.github.io/opensky-api/trino.html)
- The R package `openSkies` `getRouteMetadata` returns origin and destination airports, operator IATA code and flight number through OpenSky's live API, which it says can return malformed responses. **[search extract]** — [getRouteMetadata](https://search.r-project.org/CRAN/refmans/openSkies/html/getRouteMetadata.html)
- OpenSky COVID-era records say callsign matching "comes from public open API" and warn some information could be erroneous. **[search extract]** — [Zenodo](https://zenodo.org/records/7923702)
- OpenFlights route data has not been updated since June 2014. **[search extract]** — [OpenFlights FAQ](https://nginx.openflights.org/faq)
- GAIA, using Spire schedule data, has OD codes for 79% of flights. Cirium fleet data covers 59% (79% of jets). **[paper text]** — [ACP 2024](https://acp.copernicus.org/articles/24/725/2024/)
- FAA material says it disabled flight-ID matching because mismatches were frequent (US, general-aviation centred). **[search extract]** — [SKYbrary](https://skybrary.aero/articles/identification)
- FAA's PAPR tool reports the share and maximum consecutive reports where broadcast tail differs from the one derived from the ICAO address, a per-flight check. **[search extract]** — [PAPR guide](https://adsbperformance.faa.gov/paprusersguide.pdf)
- A document of unclear provenance gives about 2-3% of European flights with a callsign not matching the plan. **Treat as unverified.** — [FlightID.pdf](https://apex4.revion.com/ords/r/jolt2/142/files/static/v76/FlightID.pdf)
- Local ground truth: `flights_tidy()` joins on `AIRCRAFT_ADDRESS` + callsign + date, as documented in the workspace `CLAUDE.md`.
- Local caution on ADS-C: the `flight` field is the IATA number (`LH133`), not the ICAO callsign (`DLH133`), per the census in the workspace `CLAUDE.md`. A callsign join across these sources would fail without mapping.

### Inferences
- A schedule's OD pair can separate same-identifier flights, but its error rate would become the linker's error rate, and no open source measures it.
- Network Manager plans and APDF are the only references here suited to validating a linker, and they are limited to their European scope.

### Gaps
- I found no accuracy evaluation of OpenSky's route database, adsbdb, hexdb or VRS standing data. Direct searches returned nothing relevant.
- I found no published OAG/Cirium match rate for ADS-B callsigns, and no public ICAO-to-IATA mapping accuracy study.

## Does ADS-B over satellite (Aireon, Spire) or NAT track data feature in linking work?

### Takeaway
Yes for satellite data, which mostly removes the gap rather than bridging it. Fusion papers merge first and segment afterwards. I found no linking work that uses NAT track messages.

### Cited Findings
- Sun, Tassanbi, Obojski and Plantholt (2023, SIDs) merge Spire space-based ADS-B with OpenSky, segment with `traffic`, select by transponder code from a mid-Atlantic band at about 30,000 ft, require data below 6,000 ft, and resample to 15 s. They obtain about 200 complete east-coast-to-Western-Europe flights on 10 March 2021. No link accuracy is given. The paper prints "latitudes between -45 degrees and -25 degrees", which from context looks like an error for longitudes. **[paper text]** — [SIDs 2023](https://www.sesarju.eu/sites/default/files/documents/sid/2023/Papers/SIDs_2023_paper_73%20final.pdf)
- Sun, Olive, Strohmeier and Lenders (arXiv, 2025) use over 720,000 ADS-C messages from 2,606 aircraft in 2024, at "typically every 10 to 15 minutes". They claim accuracy improves "by up to 40% for transoceanic flights and routes over Africa", and the body does not explain the calculation. "the Alphasat satellite only covers approximately half of this oceanic area," and western tracks may be underrepresented. **[paper text]** — [arXiv 2505.06254](https://arxiv.org/html/2505.06254v1)
- The workspace's own census measured median ADS-C cadence of 277-1,139 s inside `EUROPE_BBOX`, broadly consistent with the above, per the workspace `CLAUDE.md`.
- GAIA notes satellite data reduces `dt`, which reaches about 11 h without it, and improves interpolated lateral and vertical accuracy. It gives no satellite share. **[paper text]** — [supplement](https://acp.copernicus.org/articles/24/725/2024/acp-24-725-2024-supplement.pdf)
- FR24 uses Aireon at 15-minute display increments. — [Runway Girl Network](https://runwaygirlnetwork.com/2026/02/flightradar24-closes-last-blind-spots-with-aireon-space-based-ads-b/)
- The 2025 OpenSky report says space-based ADS-B's commercial nature creates barriers to open science, which motivates its ADS-C use. **[search extract]** — [arXiv 2505.06254](https://arxiv.org/html/2505.06254v1)

### Inferences
- Even at a 10-15 minute cadence, a `traffic`-style 10-minute rule would fragment a fused track. Any fusion needs a longer or source-aware threshold, or rules like GAIA's.
- The SIDs selection (full profile on both ends) yields clean linked flights but is a survivorship filter. It says nothing about the flights it drops.

### Gaps
- I found no paper using NAT track messages or oceanic clearances to link segments, and no Aireon-based open linking study.
- I found no published comparison of link error with and without satellite data.
