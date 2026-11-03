# Kinematic and operational evidence for linking track pieces across a coverage gap

**How far to trust these notes.** Several primary documents could not be read directly. The ansperformance.eu chapters, the Flightradar24 blog and MITRE returned HTTP 403. The ICAO NACC host did not resolve, the Sandia posters returned 404 or a connection reset, NPR timed out, and the NASA and EUROCONTROL Ed. 10 PDFs would not decode. Figures from those sources come from search-result summaries of the page, and each is tagged **[snippet]**. Confirm them against the primary source before they go into a published threshold. Items tagged **[local]** come from this repository's own measurements. Everything in the Inferences sections is my own arithmetic or reasoning, and its assumptions are stated.

## Implied ground speed: cruise ranges, jet-stream effect, route extension

### Takeaway
An upper bound near 750 kt is defensible, because the fastest ADS-B ground speeds reported are about 705-725 kt. The lower bound is the weak side. A global floor must allow for headwinds and route extension, so it ends up near 300-320 kt for jets, and that blinds it to hidden stops on longer gaps. A floor relative to each flight's own ground speed at the gap edges looks far more discriminating, but it is unvalidated.

### Cited Findings
**Cruise speed by category (weak sourcing; no BADA table retrieved)**
- A320: about 450 kt TAS at M0.79, per a blog citing the EUROCONTROL performance database; other pages give M0.78-0.80 — [Jettly](https://jettly.com/post/a320-cruise-speed) [snippet].
- B737-800: about M0.785 as a baseline; B777: about M0.84 as a baseline, M0.89 maximum operating — [FlyAway Simulation, 737/A340](https://flyawaysimulation.com/ask/answers/boeing-737-800-airbus-a340-300-cruise-speeds/); [FlyAway Simulation, 777](https://flyawaysimulation.com/ask/answers/boeing-777-top-speed/) [snippet; simulator-community source, low authority].
- ATR 72-600: maximum cruise 510 km/h, about 275 kt. This is a maximum, not a typical value — [search result, source not pinned down; Aircraft Commerce ATR guide is the closest](https://www.aircraft-commerce.com/wp-content/uploads/aircraft-commerce-docs1/Aircraft%20guides/ATR%2042%2072/ISSUE49_ATR42%2072FUEL.pdf) [snippet].

**Wind effects**
- Typical transatlantic jet-stream wind is about 110 mph (about 96 kt), shortening eastbound and lengthening westbound flights by about 1 h each — [EarthDate](https://earthdate.org/node/457) [snippet].
- BA112 JFK-LHR, February 2020: 4 h 56 min on a B747, with peak ground speed of about 825 mph (about 717 kt) and a jet of about 265 mph (about 230 kt) along the route — [AVweb](https://avweb.com/aviation-news/jet-stream-helps-747-set-transatlantic-record). Other reports give a 200 mph (174 kt) tailwind and "over 800 mph" (695+ kt) from Flightradar24 — [NPR](https://www.npr.org/2020/02/09/804266975/british-airways-sets-speed-record-crossing-the-atlantic-in-under-5-hours) [snippet]. The wind figure conflicts across sources.
- January 2025: QR704 reached 833 mph (about 724 kt) and BA274 reached 814 mph (about 707 kt), per Flightradar24 — [Business Insider via itiger](https://www.itiger.com/news/2505047525) [snippet].
- February 2024: a United 787 JFK-Lisbon was reported at 1,344 km/h (about 726 kt). Only one source was found, and it is probably ground speed — [Gulf News](https://gulfnews.com/amp/story/world%2Famericas%2Flook-planes-are-now-flying-faster-than-the-speed-of-sound-1.500024428) [snippet].
- All of these are Flightradar24 instantaneous ADS-B readings, not official records or sustained averages.

**Route extension: Europe (KEA, en-route, achieved trajectory)**
- Union-wide KEA was 2.96% in 2024 and 2.99% in 2023, against a 2.40% target. Persistent deterioration is attributed to the war in Ukraine — [EUROCONTROL PRB monitoring, SES RP3 2024](https://www.eurocontrol.int/performance/prb-monitoring/rp4/test/rp3/2024/ses-rp3/environment.html) [snippet].
- Worse national values: Poland 4.49% in 2024 ([PRB Poland 2024](https://www.eurocontrol.int/performance/prb-monitoring/rp4/test/rp3/2024/poland/environment.html)); Switzerland 4.43% / 4.10% / 4.14% in 2023 / 2024 / 2025 ([PRB Switzerland 2025](https://www.eurocontrol.int/performance/prb-monitoring/rp4/test/2025/switzerland/environment.html)) [snippet].
- 2022 was the worst level since 2016 — [PRB SES RP3 2022](https://www.eurocontrol.int/performance/prb-monitoring/rp4/test/rp3/2022/ses-rp3/environment.html) [snippet].

**Route extension: North Atlantic (not the same measure)**
- Flown tracks have air distances "several hundred kilometres" longer than fuel-optimal routes. Potential savings are 0.7-7.8% westbound and 0.7-16.4% eastbound, depending on airspeed and which daily track is compared — [Wells et al.](https://nottingham-repository.worktribe.com/output/5276253) [snippet]. This is **air distance against the optimum**, not ground distance against the great circle.
- The headline figures (16.4%, about 125 NM) are disputed as resting on flawed assumptions — [OPSGROUP](https://ops.group/blog/nat-tracks-nil-an-experiment/?print=pdf) [snippet].
- Extra distance over direct routes is "significantly higher" in US-Europe oceanic airspace, attributed to limited surveillance and VHF (July 2012 traffic; no percentage extracted) — [NASA NTRS](https://ntrs.nasa.gov/api/citations/20190027238/downloads/20190027238.pdf). Strategic planning could improve efficiency by 3-5% — [NASA NTRS](https://ntrs.nasa.gov/api/citations/20180004216/downloads/20180004216.pdf) [snippet].
- In one simulation, about 76% of flights cut cruise time by more than 30 min on wind-optimal tracks compared with the great circle, which shows why flights deliberately leave the great circle — [Comparison of wind-optimal planning for NAT](https://openpolar.no/Record/ftenac:oai:HAL:hal-01974474v1) [snippet; attribution to this exact record is unconfirmed; see also [HAL](https://hal-enac.archives-ouvertes.fr/hal-01168653v1)].
- About half of NAT flights use the OTS and most others fly random routes — [SKYbrary](https://skybrary.aero/articles/north-atlantic-operations-organised-track-system) [snippet]. Up to 12 tracks are published daily, and since March 2022 random routes have been allowed at FL330 and below — [AIN](https://oldain.ainonline.com/aviation-news/air-transport/2022-02-04/operators-may-soon-fly-outside-north-atlantic-tracks-fl330) [snippet].

### Inferences
- **Jet TAS band.** Using ISA speed of sound of about 573 kt above the tropopause, M0.78-0.85 gives about 447-487 kt TAS. With the wind evidence above, plausible sustained ground speed is about 300-650 kt.
- **Sustained speed versus peak.** BA112's average was about 606 kt over the whole flight, taking JFK-LHR as about 2,990 NM (my figure, not sourced). A 725 kt peak is not a gap-average speed. An upper bound of 750 kt therefore leaves margin for timestamp and position error, and exceeding it points to an identity error rather than wind.
- **Great-circle understatement.** True distance = great-circle distance x (1 + extension), so implied speed falls by the same factor. At 480 kt, 3% gives 466 kt, 5% gives 457 kt and 16.4% (the extreme, disputed figure) gives 412 kt. A 3-10% allowance looks reasonable on the NAT, but no source directly measures NAT ground track against the great circle.
- **Working global floor.** 480 kt TAS - 150 kt headwind = 330 kt, less 5% extension = about 315 kt. The 150 kt is my assumption: well above the 96 kt typical wind, below the 230 kt peak jet, because westbound planning avoids the core. Turboprops need their own floor (275 kt max less wind gives about 175-225 kt), so a global jet floor would reject legitimate turboprop links unless the aircraft type or edge speed is used.

### Gaps
- No distribution of actual NAT ground speed by direction and season was found. This can be measured directly from ADS-C or OSN segments.
- No measured route extension for Europe-Asia flights avoiding Russian airspace was found, and these could be well above the KEA figures.
- BADA values were not retrieved.

## Minimum turnaround times and the margin of an implied-speed test

### Takeaway
Realistic hidden stops cost about 45 min (low-cost narrow-body) to about 155 min (average wide-body). A global speed floor can detect the cheapest stop only up to about 690 NM. Two tests avoid much of that limit: a time-slack test that shows a stop cannot fit, and a geometric test of whether any airport is reachable inside the gap.

### Cited Findings
- **EUROCONTROL/CODA 2022 means:** heavy 118.9 min scheduled and 134.3 min actual; medium 50.4 min scheduled and 58.6 min actual — [EUROCONTROL Standard Inputs v9.0.3](https://ansperformance.eu/economics/cba/standard-inputs/v9.0.3/chapters/turnaround_time.html) [snippet; page returned 403].
- **Low/base/high values:** heavy 67/106/168 min, medium 31/52/93 min. The table is headed 2022 but the accompanying text refers to 2019, and its rows are not clearly labelled — [same source](https://ansperformance.eu/economics/cba/standard-inputs/v9.0.3/chapters/turnaround_time.html) [snippet; **verify before use**].
- **Truncation:** cut-offs of 180 min (heavy) and 150 min (medium) remove overnight stops and maintenance from the figures, and the data blends traditional, low-cost and charter segments — [same source](https://ansperformance.eu/economics/cba/standard-inputs/v9.0.3/chapters/turnaround_time.html) [snippet].
- **Low-cost short turns:** 25-30 min scheduled for low-cost narrow-body short-haul against 45-60 min legacy narrow-body and 2-3 h wide-body long-haul, on one page; 25-35 min low-cost and 115-150 min long-haul wide-body on another. Neither cites CODA, and I could not tie each figure to its page — [AeroCorner](https://aerocorner.com/blog/airline-aircraft-turnaround/); [Mental Momentum research page](https://research.mental-momentum.ai/r/what-happens-behind-gate-when-you-board-o55f9q) [snippet; low authority].
- **US majors, March 2023 (narrow-body focus):** actual means of 59-77 min across six majors. Southwest 52 planned, 59 actual; Delta highest at 77; JetBlue 63 planned, 76 actual. No minimums or wide-body minutes are published in the text — [OAG](https://www.oag.com/blog/science-aircraft-turnarounds).
- **Business-model differences:** ADS-B-derived ground times still differ clearly by airline business model; numbers are behind a paywall — [Lange, Sieling & Gonzalez Parra 2019, JATM 77:39-45](https://ideas.repec.org/a/eee/jaitra/v77y2019icp39-45.html).

### Inferences
Assumption: a stop costs turnaround + about 20 min of penalty relative to staying in cruise (descent, approach, taxi, climb). V = 480 kt.

**Formula.** v_implied = D / (D/V + T_stop), so v_implied / V = T_fly / (T_fly + T_stop). The test detects a stop only if v_implied falls below the floor. With floor ratio f = v_floor / V, it detects only while T_fly < T_stop x f / (1 - f).

**Implied speed with a hidden stop (kt), by distance flown (V = 480):**

| Distance (NM) | T_stop 45 min: gap / implied | T_stop 154 min: gap / implied |
|---|---|---|
| 240 | 75 min / 192 | 184 min / 78 |
| 480 | 105 min / 274 | 214 min / 135 |
| 960 | 165 min / 349 | 274 min / 210 |
| 1,920 | 285 min / 404 | 394 min / 292 |
| 2,880 | 405 min / 427 | 514 min / 336 |

**Largest distance at which a stop is still detected:**

| Stop scenario | T_stop (min) | Global floor 315 kt (f = 0.656) | Per-flight floor (f = 0.85) |
|---|---|---|---|
| Low-cost 25 min turn | 45 | 687 NM (gap 131 min) | 2,040 NM |
| Medium "low" 31 min | 51 | 779 NM (gap 148 min) | 2,312 NM |
| Medium actual mean 58.6 min | 79 | 1,206 NM (gap 230 min) | 3,581 NM |
| Heavy "low" 67 min | 87 | 1,329 NM (gap 253 min) | 3,944 NM |
| Heavy actual mean 134.3 min | 154 | 2,352 NM (gap 448 min) | 6,981 NM |

The f = 0.85 column assumes the reference speed is that flight's own observed ground speed at the gap edges, and that legitimate flights stay above 85% of it. That 0.85 is **unvalidated** and should be calibrated with synthetic gaps, since winds change along the route.

**Short gap versus long gap.**
- Short gap, both edges at cruise: a stop needs about 20 min descent + at least 25 min ground + about 20 min climb, so about 65 min minimum. Below about 60 min the hidden landing is close to physically excluded at any distance. Descent is roughly 110 NM from FL370 on a 3:1 profile (rule of thumb, not sourced here).
- Long gap: the speed signal fades in proportion. A 45 min stop on a 2,880 NM gap only reduces the implied speed by 11% (480 to 427 kt), which is no larger than the disputed route-extension figure and smaller than a wind change of about 55 kt.

**A stronger test than the floor: time slack.** slack = T_gap - D / V_fast. If slack < T_turn_min + a minimum penalty (say 25 + 10 = 35 min), a stop cannot fit.

| Gap / distance | V_fast 650 kt: slack | Result | V_fast 572 kt (edge 520 kt + 10%): slack | Result |
|---|---|---|---|---|
| 60 min / 450 NM | 19 min | Stop excluded | 13 min | Stop excluded |
| 120 min / 1,000 NM | 28 min | Stop excluded | 15 min | Stop excluded |
| 180 min / 1,400 NM | 51 min | Not excluded | 33 min | Stop excluded |
| 360 min / 2,800 NM | 102 min | Not excluded | 66 min | Not excluded |

**Airport reachability.** A stop is possible only if an airport lies inside the ellipse with foci at the gap edges and major axis V_fast x (T_gap - T_stop). Over mid-ocean this leaves only a few candidates, such as Iceland, the Azores, Shannon and Gander, so it could settle many NAT cases that speed alone cannot.

**When the speed test is blind or wrong.**
- **On-route technical stop:** long distance with a short stop gives only a small speed change. This is the main false-join risk, and it plausibly keeps the same callsign.
- **Short-sector chain with same callsign:** low speed makes it detectable, if the gap is short relative to the stop.
- **Out-and-back beyond coverage:** D is near zero, so implied speed is very low. It is correctly rejected as a single continuous transit, but it looks the same as an air return.
- **Fuel tankering** does not itself alter the kinematics. It only makes a longer multi-sector chain possible with no fuel stop, so it weakens any assumption that a long chain requires refuelling. I found no source quantifying this.
- **Long hidden ground time (hours):** speed falls so far that this is the easy case.

### Gaps
- No primary published minimum-turnaround distribution by type was found. Stated values are means, scheduled figures, or low-authority pages. Non-passenger technical stops could be shorter than 25 min, and no source was found.
- A measured minimum can be computed from OPDI's own consecutive same-`icao24` flights, or from APDF block times (`BLOCK_TIME_UTC`, per CLAUDE.md).

## Diversions, holding and returns to origin

### Takeaway
Diversions affect about 0.25-0.32% of flights, too few to dominate a linker's errors overall, though they may be enriched among ambiguous candidates (unmeasured). No reliable air-return rate was found. Each of the three mainly lowers implied speed, so a floor test mostly turns them into missed links rather than false joins.

### Cited Findings
- **Europe:** in 2024, over 30,000 of 9.4 million flights (0.32%) with a destination in the Network Manager area landed somewhere other than initially planned — [EUROCONTROL Standard Inputs, cost of diversion](https://ansperformance.eu/economics/cba/standard-inputs/latest/chapters/cost_of_diversion.html) [snippet; page returned 403].
- **US (BTS via an aggregator; BTS itself was not read):** 0.167% in 2020, 0.250% in 2024, 0.273% in 2025 (highest since 2003), 0.314% for Jan-Jul 2026. BTS does not record cause. By airport in 2025: Reagan National 0.528%, LaGuardia 0.479%, Salt Lake City 0.068%, Aspen 4.6% — [AirAdvisor](https://airadvisor.com/en/statistics/flight-diversions). July 2026: 3,615 diversions, 0.518% of scheduled flights — [same source](https://airadvisor.com/en/statistics/flight-diversions) [snippet]. The European and US figures use different definitions and are not comparable.
- **Medical diversions:** a meta-analysis gave 11.1 per 100,000 flights (95% CI 5.9-17.6; I² = 97%) — [search result; likely this record](https://em-consulte.com/article/1474214/the-global-incidence-of-in-flight-medical-emergenc) [snippet; link attribution unconfirmed]. About 1 medical event per 212 flights in a 77,790-event cohort — [Duke Health](https://corporate.dukehealth.org/news/new-study-reveals-frequency-flight-medical-emergencies) [snippet]. Emirates: more than 60 medical diversions across more than 194,000 flights in 2016, about 0.3 per 1,000 — [The Mercury via PressReader](https://www.pressreader.com/south-africa/the-mercury-south-africa/20170208/282123521249757) [snippet].
- **Holding:** additional ASMA time is the standard European inbound-queuing measure, in minutes per IFR arrival — [ANS Performance](https://ansperformance.eu/definition/additional-asma-time/) [snippet]. No per-airport minutes were retrieved.

### Inferences
| Event | What the speed test sees | Likely error |
|---|---|---|
| Diversion, then onward leg to the planned destination | Two legs with a long ground stop, same callsign possible | **False join** if the gap is long and the speed floor is permissive; otherwise rejected correctly |
| Diversion with no onward leg | Track ends at the alternate, no candidate on the far side | None for linking, but a benchmark sees ADES ≠ planned destination and can score it wrongly |
| Air return inside a gap | One true flight, D near zero, very low implied speed | **Missed link**, unless the rule treats it as two pieces of one flight; hard to distinguish from out-and-back |
| Holding in a gap | Adds minutes, implied speed falls | **Missed link**, only near the floor |
| Weather or ATC reroute in a gap | Distance understated | **Missed link**, only near the floor |

- Holding happens mostly near terminals, usually within good coverage, so it should be rare in an oceanic gap but more plausible at the edges of coverage in Africa or Asia. A 30 min hold on a 60 min gap would halve implied speed, so short gaps are the most exposed. Its frequency in gaps is unmeasured.
- The per-flight rate may understate the problem. Among the candidates that actually reach a linker, disruption could be enriched, because a diversion produces exactly the "same airframe, same day, long gap" pattern. Measure this rather than assuming 0.3%.

### Gaps
- No air-turnback rate per departure was found from any regulator or airline.
- No diversion rate specific to long-haul or oceanic flights was found.
- No holding statistics by airport in minutes were retrieved.

## Altitude and phase evidence at gap edges; endurance limits

### Takeaway
Both edges at cruise is sound evidence only for short gaps, about 60 min or less, and it does not exclude a landing beyond that. Edges at low altitude are weak evidence of a landing, because low altitude is also where reception fails. Endurance gives a coarse cap of about 19-20 h for a whole flight.

### Cited Findings
- **Gap segmentation in OPDI already uses altitude:** on 2026-06-01, of 44,406 A8 track boundaries, 69.7% (30,961) came from gaps over 30 min and 2.3% (1,005) from gaps over 15 min at low altitude. Only 5.0% (2,199) were genuine callsign changes, and 23.1% (10,241) were callsign flicker — [local] `opdi-portal/papers/track-construction-v3/data/flicker_2026.csv`.
- **Altitude alone misses turnarounds:** "below 5,000 ft" is satisfied everywhere at a high-elevation airport, so the turnaround becomes invisible and two flights merge. OPDI's A5-family arm therefore tests height above field elevation inside a detection zone, plus a speed limit — [local] `opdi-portal/papers/track-construction-v1/index.qmd` (around lines 1058-1064; read from a grep excerpt, not in full).
- **The `traffic` library's reference rule:** a single gap threshold, default ten minutes, unless a predicate says the gap is not a real break — [local] `opdi-portal/papers/track-construction-v1/index.qmd` (around lines 913-915).
- **Academic segmentation:** Sun, Ellerbroek & Hoekstra (2017) cluster with DBSCAN on ICAO address and time, then assign phases with fuzzy logic. Exact parameters were not confirmed — [TU Delft](https://research.tudelft.nl/en/publications/flight-extraction-and-phase-identification-for-large-automatic-de/) [snippet].
- **Tracklet-end fitting:** Sandia's stitching work fits linear regressions to the ends of tracklets to score candidate links — [Purdue/Sandia poster](https://datamine.purdue.edu/posters/Symposium2023_Poster_Sandia_Flight_PDF.pdf) [snippet; fetch failed].
- **Longest flights:** Singapore-New York is about 16,700 km (about 9,020 NM) in just under 19 h. Perth-London is 14,498 km (about 7,830 NM) in about 17 h — [search results via Time Out](https://www.timeout.com/news/the-worlds-longest-flight-is-set-to-launch-in-2026-and-link-these-major-cities-111824); [Flying](https://www.flyingmag.com/?p=133068) [snippet; exact source for each figure unconfirmed].
- **Project Sunrise:** Sydney-London is about 17,000 km, projected at 19-22 h. It is not yet operating; the launch has slipped to 2027, with reports of H1 2027 and of October 2027 — [Fox Business](https://www.foxbusiness.com/lifestyle/qantas-sydney-london-worlds-longest-nonstop-flight); [Flying](https://www.flyingmag.com/?p=133068) [snippet; projections].

### Inferences
- **Both edges at cruise.** Sound below about 60 min (see the 65 min minimum above). Beyond that it only says the aircraft was cruising at both moments, and a cruise-to-cruise gap of several hours can contain a stop.
- **Stronger combinations.** Descending at edge A plus climbing at edge B strongly suggests a landing between them. Level or similar FL, heading along the connecting line, and similar ground speed all support continuity. None of these has a measured threshold in the sources found; tracklet-end fitting is the only precedent.
- **Reception bias at low altitude.** Radio horizon is about 1.23 x sqrt(h ft) NM (standard approximation, not sourced here): about 230 NM at FL350 and about 39 NM at 1,000 ft. Tracks thus lose low-altitude samples far earlier, so a low-altitude edge might reflect the receiver rather than a landing. Altitude should be read together with distance to the nearest airport and to the nearest receiver.
- **Step climbs and early descents** will put cruise-level differences into genuine continuous flights, so an FL-difference test needs tolerance. I have no measured threshold.
- **Endurance cap.** Total elapsed time from first piece's start to last piece's end should be at most about 20 h today (about 19 h scheduled plus margin), and about 22-23 h once Project Sunrise flies. This is only a sanity bound. It removes few real candidates, since airframes turn around well inside it. Per-type range limits would be much tighter.

### Gaps
- I found no peer-reviewed paper that specifically tests altitude or vertical rate at gap edges as a linking feature. The closest precedents are tracklet-end regression and OPDI's own rules.
- Narrow-body endurance figures were not retrieved. From memory, and unverified, the A321XLR is about 4,700 NM. Use type-level range tables before setting a per-type cap.

## Callsign behaviour and ICAO24 errors

### Takeaway
A matching callsign is weak evidence. It can be the same on separate legs, and in OPDI's data a garbled callsign produces far more track boundaries than a real change does. A mismatched `icao24` is strong negative evidence, apart from a small population of bad addresses.

### Cited Findings
**Callsign**
- Multiple legs may share a callsign on the same day, "even for Flights between the same Airports, but not at the same time" — [ACI data dictionary](https://aci-standards.atlassian.net/wiki/spaces/ACIDD/pages/17695395/Callsign) [snippet].
- Commenters report that using one flight number in both directions is common because of number shortages, while others say commercial systems typically prevent duplicate same-day use, so the evidence conflicts — [One Mile at a Time forum](https://onemileatatime.com/community/american/two-american-planes-with-same-callsign-end-up-on-same-frequency-heading-straight-for-one-another/) [snippet; anecdotal].
- If a flight with that number is already airborne, a letter suffix is added to the second flight's callsign, so late running can make the callsign differ from schedule — [FlightAware forum](https://discussions.flightaware.com/t/differing-flight-numbers-letter-suffix/11869) [snippet; anecdotal].
- Normal airline practice uses a different callsign per leg on one aircraft — [Bellingcat](https://bellingcat.com/resources/how-tos/2019/10/15/a-beginners-guide-to-flight-tracking) [snippet].
- Charter and business aircraft can change Flight ID between mission and deadhead legs — [Air Care Alliance white paper](https://www.aircarealliance.org/wp-content/uploads/2020/09/042018-CSMM-and-changing-flight-ID-1-1.pdf) [snippet].
- **OPDI, 2026-06-01:** 12,440 boundaries attributed to callsign change, of which 10,241 traced to a flicker (X→Y→X) and only 2,199 were genuine — [local] `opdi-portal/papers/track-construction-v3/data/flicker_2026.csv`. The 2,199 is not a rate of mid-air changes, because it can include changes during a turnaround.
- 42.4% of legacy-rule tracks carried no callsign at all, and the failure mode was missing callsigns rather than mid-flight changes — [local] `opdi-portal/papers/track-construction-v1/index.qmd` (lines 880-885).
- The ADS-C `flight` field is the **IATA** number (`LH133`), while ADS-B broadcasts the ICAO callsign (`DLH133`), so a direct string match across the two fails — [local] workspace `CLAUDE.md`.

**ICAO24**
- Over five days in August-September 2018, Aireon saw 47,684 distinct addresses and a small subset was duplicated. One address (`0x000001`) was tied to 32 flight IDs — [ICAO NACC / Aireon](https://www2023.icao.int/NACC/Documents/Meetings/2018/ADSB/App5-Space%20ADS_B_TCAS%20and%20Data%20Observations.pdf) [snippet; host unreachable]. The count of 47,684 looks low for all global traffic, so check the scope in the original.
- Sydney, early ADS-B: 4.1% of 779 aircraft had an error in Flight ID or address — [early implementation experiences](https://aea.net/training/courses/adsbforum/pdf/ADS-B%20Early%20Implementation%20Experiences.pdf) [snippet; old and small sample].
- The FAA called invalid 24-bit addresses one of its largest non-performing-emitter (NPE) problems — [Aviation Today](https://www.aviationtoday.com/2017/07/27/18-percent-ads-b-aircraft-npe/) [snippet]. Duplicate addresses can drop targets on ATC displays — [AOPA](https://aopa.org/news-and-media/all-news/2017/march/pilot/adsb-npe) [snippet].
- Incorrect or duplicate addresses can seriously degrade ACAS II — [ICAO ADS-B TF paper](https://www.icao.int/sites/default/files/Meetings/AMC/MA/2005/ADSB_ADSB_TF3/ip13.pdf) [snippet].
- Military addresses reportedly are not fixed, based on a blog citing a 2011 USAF document I could not verify — [GerryAirways](https://www.gerryairways.com/?p=1865) [snippet; low confidence].

### Inferences
- **Callsign as a feature.** Same callsign with same `icao24` does not establish continuity. Same-number return legs and technical stops would pass. Different callsign across the gap is a fairly strong negative, but only after normalising garbled and blank values, which are common. Using the last stable (debounced) value at each edge is the natural adaptation of A9.
- **ICAO24 as a key.** One flight should keep one address, so a mismatched address across a gap is a near-certain non-link, apart from bit errors. Airframe swaps are a schedule-level matter: they change `icao24`, so they cannot cause false joins under an `icao24` key.
- **Duplicates produce impossible speeds.** An address shared by two aircraft can yield impossible implied speed, which the 750 kt bound catches, or plausible speed by coincidence, which it does not. Exclude placeholder addresses such as `000001`.

### Gaps
- **No measured rate of mid-air callsign change on commercial flights was found**, oceanic or otherwise.
- **No measured share of same-day same-callsign same-airframe legs was found.** OPDI can compute this directly, and it should, because it sets the false-join base rate for any callsign-based rule.
- No peer-reviewed rate of duplicate or invalid ICAO addresses was found.

## Reception reality: size and location of coverage gaps

### Takeaway
I found no published figures for gap length by region on the OpenSky network, and no source addresses Siberia. Regional gap lengths will have to be measured from OPDI's own data. The estimates below are first-order geometry only.

### Cited Findings
- ADS-B coverage is limited to regions with ground receivers, leaving gaps over oceans, the global South and sparsely populated areas. OpenSky has over 7,000 registered receivers — [Sun, Olive, Strohmeier & Lenders, OpenSky Report 2025](https://arxiv.org/abs/2505.06254) [snippet].
- The same report does **not** quantify gap length or uncovered area; Fig. 7 is qualitative — [arXiv HTML](https://arxiv.org/html/2505.06254v1).
- ADS-C updates arrive every 10-15 min against 0.5-2 s for ADS-B. The study covers over 720,000 ADS-C messages from about 2,600 aircraft in 2024. Alphasat covers about half the North Atlantic, better in the east, and the data is primarily EMEA — [arXiv HTML](https://arxiv.org/html/2505.06254v1).
- Reconstruction accuracy is claimed to improve "by up to 40% for transoceanic flights and routes over Africa", with no baseline, metric or sample stated — [arXiv HTML](https://arxiv.org/html/2505.06254v1). Treat with caution.
- Many African carriers lack ADS-C equipment, leaving gaps mainly on domestic flights; more receivers are recommended there. Pacific coverage would need additional ADS-C receivers — [arXiv](https://arxiv.org/abs/2505.06254) [snippet].
- Before Aireon, ground-based ADS-B covered about 30% of the globe, according to the company itself — [AVweb](https://avweb.com/news/aireons-space-based-ads-b-goes-live/) [snippet; vendor figure].
- **OpenSky's ADS-C feed in OPDI's own census (eleven days, 2023-09 to 2026-10, inside the European box):** 140-5,420 rows/day from 74-994 airframes, median cadence 277-1,139 s, 94-99% above FL300, 66-84% west of 10W, and 1-67 rows/day below 10,000 ft — [local] workspace `CLAUDE.md`; `benchmarks/adsc_census.py`.
- **NAT volume, for scale:** 580,487 Europe-North America flights in 2023, second-hand — [PilotGPT citing ForeFlight](https://www.pilotgpt.com/blog/north-atlantic-track) [snippet; unverified]. Shanwick about 1,600/day in 2019 and about 800/day later; Gander up to 1,500/day at the summer peak, on different bases — search summary [snippet; source not pinned down, likely [UKFSC](https://www.ukfsc.co.uk/wp-content/uploads/public_pdfs/19-1.pdf) or [ATT](https://att.mydigitalpublication.co.uk/2022/page-10)]. Eastbound peak 0400-0500 UTC, westbound 1100-1400 UTC — [UKFSC slides](https://www.ukfsc.co.uk/wp-content/uploads/public_pdfs/19-1.pdf) [snippet; older data].

### Inferences
- **NAT estimate (geometry only).** Ireland (10W) to Newfoundland (53W) at about 52N spans about 1,590 NM. Taking away about 230 NM radio horizon on each side leaves about 1,130 NM, or about 2.4 h at 480 kt. Receivers in Iceland, Greenland or the Azores, or high-latitude tracks, would reduce it, while poor receiver density or sensitivity would raise it. **Working range: about 1.5-3 h (roughly 700-1,400 NM).** At those distances a global floor misses low-cost-size stops but still catches average wide-body stops, and wide-bodies dominate the NAT (my assumption). Airport reachability is also tight there.
- **Where edges occur.** NAT and other oceanic edges should be at cruise altitude, consistent with a radio horizon limited by geometry. African and Siberian edges are more likely to occur at any phase, because receivers are sparse near airports as well. Fewer cruise-to-cruise cases and more hidden-landing ambiguity should be expected, so a rule validated over the NAT should not be assumed to carry over to Africa.
- **Pacific:** transpacific flights could have gaps of several hours; no measurement was found. Africa and Siberia cannot be estimated from receiver geometry without the receiver map.

### Gaps
- No regional gap-length distribution exists in the sources I found. **Recommended measurement:** for each same-`icao24` consecutive piece pair in worldwide OSN data, map gap length, edge altitude and edge location by region and month. That gives gap population and base rate at once. `opdi-world` is the natural source.
- Spire and Aireon coverage comparisons were not retrieved, and neither is open.

## Validation design: reference data, error rates, synthetic gaps

### Takeaway
I found no peer-reviewed study that validates a gap-linking rule with this design, though the elements exist in Sandia's and MITRE's work. The critical addition is a **negative control**: synthetic gaps that hide a real landing and turnaround. Otherwise a false-join rate cannot be measured at all.

### Cited Findings
- **Synthetic gaps:** Sandia/Purdue students built ground truth by removing data from complete flights, then scored stitched results; nonsensical paths were eliminated; classifying flights by ICAO code reduced the matching space by 66% — [Sandia poster 2022](https://datamine.purdue.edu/corporate/sandia/TDM_Symposium2022_Poster_Sandia_Flight.pdf); [Sandia poster 2023](https://datamine.purdue.edu/posters/Symposium2023_Poster_Sandia_Flight_PDF.pdf) [snippet; both fetches failed; student posters, not peer reviewed].
- **Threaded Track (MITRE):** early versions linked radar tracks using callsign, aircraft type, airports and Mode A, with the callsign requirement limiting false positives. A later version associated by temporal and spatial proximity. "Segment fusion across gaps" is listed as a missing feature — [MITRE PR 17-3649](https://www.mitre.org/sites/default/files/publications/pr-17-3649-geospatial-data-threaded_track.pdf) [snippet; 403].
- **OpenSky ADS-C/ADS-B merge:** ADS-C is split into flights by callsign, then ADS-B is queried by the same transponder code, with results filtered and resampled. No time or distance thresholds and no validation are reported — [arXiv HTML](https://arxiv.org/html/2505.06254v1). This is a matching recipe, not a validated linker.
- **Great-circle fallback:** earlier work relied on simple great-circle approximations between coverage zones, introducing potentially significant error — [OpenSky ICNS 2025](https://www.lenders.ch/publications/conferences/icns25.pdf) [snippet].
- **OPDI's existing reference set:** `flights_tidy()` gives ADEP, ADES, `AOBT_3` and `AIRCRAFT_ADDRESS` (equal to `icao24`), joined on address + callsign + date. APDF gives block and movement times by `SRC_PHASE`, is monthly, and is **European** — [local] workspace `CLAUDE.md`. `track-construction-v1` already scores arms on clean/fragmented/merged rates, and is the natural template — [local] `opdi-portal/papers/track-construction-v1/index.qmd`.

### Inferences
**Proposed design (mine; no study found that does all of it):**

1. **Positive control (missed links).** Take continuous flights with reference ADEP/ADES, delete the mid-section between two cruise points, and check whether the pieces are linked. Stratify by gap length (15, 30, 60, 120, 180, 360 min), direction and aircraft class.
2. **Negative control (false joins).** Take consecutive flights of the same `icao24` confirmed by block times. Delete the end of flight 1 and the start of flight 2 so that the landing and turnaround fall in the gap, and check that the pieces are not linked. Include the hard subset where callsign is the same on both sides, and the short-turn subset. Vary how much is removed, so that both low-altitude and cruise edges appear.
3. **Real-gap check.** On real NAT, Africa or other gaps, compare the pieces' detected ADEP (first piece) and ADES (last piece) to the flight plan. Agreement supports a link. A flight plan with ADES at an intermediate airport, or a matching second flight plan, identifies a false join.
4. **Metrics.** False-join rate = wrongly linked / negative pairs; missed-link rate = unlinked / positive pairs. Report both with confidence intervals, for each stratum. By the rule of three, zero false joins among n negatives gives a 95% upper bound of about 3/n, so a claim of under 0.1% needs at least 3,000 negatives **in that stratum**.
5. **Report precision at realistic prevalence.** A balanced control set overstates precision. Estimate the true share of hidden stops among real candidates from step 3, and report expected false-joined flights per day. OPDI's 2026-06-01 run gives a scale: 30,961 boundaries from gaps over 30 min, so each 1% of false joins would be about 310 corrupted pairs a day, if every one of them were offered as a candidate.
6. **Downstream check.** Confirm that linked tracks leave `take-off`/`landing` counts consistent, so that no linked track contains an interior landing, and that the flight-list ADEP/ADES improves against reference data.

**Known threats to validity.**
- European synthetic gaps come from dense short-haul traffic with different wind exposure from the NAT jet core, and APDF is European. So a rule tuned on them may not transfer. Long-haul flights over Europe give only partial cover. Report them as a separate stratum.
- Deleting data is cleaner than real loss, which depends on altitude and geometry. Mimicking real edges, taking edge location and altitude from the census of real gaps, would reduce this.
- Track identity boundaries matter: per the workspace notes, `track_id` differs across legacy, A8 and A9 and any join between them returns silently empty, so validation must fix one segmentation arm and state it.
- Synthetic gaps inherit the data's own errors, since a "continuous" flight may already hide undetected issues.
- ERA5 wind enrichment is a documented negative result in the PRC challenge (fuel RMSE), but that result concerned fuel prediction, not linking. Edge-speed references avoid the question and should be tried first.

**Threshold map: value, basis, and what could go wrong**

| Criterion | Starting value | Basis | Failure mode |
|---|---|---|---|
| Same `icao24` | Required | One flight, one address | Missed link on bit errors; false join on duplicates. Exclude placeholders |
| Upper implied speed | About 750 kt | Reported peaks 705-726 kt | Short gaps with timestamp error can exceed it, so require a minimum D and T |
| Global lower implied speed | About 315 kt jets; about 175-225 kt turboprops | TAS, wind and 5% extension (assumed) | Blind to 45 min stops beyond about 690 NM; misses air returns and holds; needs aircraft class |
| Per-flight lower ratio | v_implied ≥ 0.85 x edge ground speed | Detection table; **unvalidated** | Wind changes along the route; distrust in long gaps; calibrate by direction and season |
| Time slack | Reject the stop hypothesis if slack < about 35 min | 25 min low-cost turn + 10 min penalty | Sub-25 min stops; a too-high V_fast leaves it indecisive |
| Cruise-to-cruise short gap | Both at or above about FL250-FL300 and gap < about 60 min implies link | Descent, ground and climb need about 65 min (assumed) | Military and training flights, touch-and-go |
| Edge phase | Descent at A + climb at B rejects a link | Descent and climb signature | Step changes or ATC-directed level changes |
| Airport reachability | No airport in the ellipse implies no stop | Geometry | Needs a complete airport list; remote or military strips |
| Callsign | Must agree after debouncing and blank handling | A9 evidence; same-callsign legs exist | Not sufficient alone; ADS-C uses IATA numbers |
| Total elapsed time | At most about 20 h (about 23 h after Sunrise) | Longest current flights | Coarse only; per-type limits are sharper |
| Long-gap policy | Above about 3-6 h with no slack or airport evidence: do not link, or mark as low confidence | A 45 min stop moves implied speed by only 11-16% beyond about 1,900 NM | Lower coverage of the longest gaps, which is the cost of a low false-join rate |

The 3-6 h boundary and FL250-FL300 are judgment calls and should be set from the negative-control results rather than from this table.

### Gaps
- No peer-reviewed false-join or missed-link benchmark for gap linking was found. The Sandia posters are the nearest match, and I could not read their numbers.
- No study of negative controls with hidden turnarounds was found.
- Whether reference data can identify technical stops, so that those cases can be labelled, is untested here.
