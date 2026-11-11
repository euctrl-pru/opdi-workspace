# Holding, point merge and FIR crossing events: design

Date: 2026-11-10. Repos: `opdi` (pipeline), `opdi-portal` (paper), this meta repo (spec, plan, CLAUDE.md).

## Intent

OPDI publishes flight events (milestones with a 4D fix) and measurements attached to them. Three
things an analyst asks about an arrival or an en-route flight are still missing: *did it hold, and
for how long*; *did it fly a point-merge sequencing leg, and how much of it*; and *which FIRs did it
cross, and when*. This work adds those three families as production events in `opdi`, each with its
own measurements, each computed with native Spark column and window expressions so they run on every
flight worldwide, every day, inside the existing step 04.

The research behind the choices is in the job scratch folder (`research/holding.md`,
`research/point_merge.md`, `research/hexaero_fir.md`, `research/opdi_events_recon.md`); the parts
that matter are restated here so the plan can be written from this document alone.

## What the research settled

**Holding.** The only published rule-based detector with a measured false-positive rate (Melo 2023,
validated by three controllers on 2,502 flagged flights: 0.6% false positives) and the machine-learned
detector in the `traffic` library (Olive et al. 2025, precision 0.84 / recall 0.70 across airports)
agree on what a hold looks like in a track: the aircraft turns through at least one full circle *in
the same rotational sense* within a few minutes, while staying inside a small area and roughly at one
level. Vectoring turns alternate sense; a go-around climbs; an aerodrome circuit is low and slow. A
signed cumulative turn over a sliding window is the discriminator every method converges on. An open,
hand-corrected label set exists (784,177 six-minute windows over 97,700 arrivals at EGLL, EGLC,
EIDW, LFPG, 1 Oct to 30 Nov 2019, matched to OpenSky by callsign and icao24), so the rule can be
scored against the same bar as the neural detector.

**Point merge.** A Point Merge System (PMS) is an arrival procedure with one *merge point* and
*sequencing legs* that are arcs at constant distance from it; the controller stretches the path by
keeping the aircraft on the leg, then clears it "direct" to the merge point. Every published detector
needs the merge point coordinates (the `traffic` library, Hardell et al. 2025, EUROCONTROL's own
catalogue scan); detection without them found 8 of 10 known sites in the literature and is not good
enough for production. EUROCONTROL's *Point Merge catalogue* (July 2026) lists all 61 operating
airports with their leg geometry and dates, but names merge points for only a handful and gives
coordinates for none. So production detection rests on a small curated reference table of merge
points, versioned and dated, and the detector is a geometric test against it.

**FIR crossings.** HexAeroPy has no crossing detector. It ships a development script that hex-bins a
2015 (AIRAC 406) FIR shapefile at H3 resolution 7, never imported. opdi's own step 00c
(`src/opdi/reference/h3_airspaces.py`) is a port of that script and is already strictly better: it
reads the PRU Atlas *Network Manager* FIR polygons for AIRAC 524 (valid 2024-11-28 to 2024-12-26,
336 volumes worldwide), hex-bins them at resolution 7 and stamps the cycle. Nothing reads its output,
it is switched off under worldwide coverage on the wrong premise that the FIR source is European,
and it has two latent bugs: the table is overwritten once per polygon (only the last one survives),
and polygons crossing the antimeridian are filled on the wrong side of the planet. The newest public
FIR definition is this AIRAC 524 set; newer cycles exist only in PRISME (work laptop). The FIR set is
a three-dimensional model: lower FIRs (0 to FL195 or FL245) and upper UIRs above, with 14 codes split
into several sub-volumes.

## Goals

1. Three new event families in `opdi_flight_events`, on by default, each with measurements in
   `opdi_measurements`, computed inside step 04 with no new runtime dependency and no pandas.
2. Every FIR event states the FIR definition it was computed with (the CFMU AIRAC cycle number and
   its validity window).
3. Step 00c fixed so `opdi_h3_airspace_ref` is complete, worldwide, and small enough to broadcast.
4. Each family measured on real data within the project's standing limits (one pipeline day; bucket
   under its quota), reported in a short paper under `opdi-portal/papers/airspace-events-v1/`.
5. Holding scored against the open label set with the same metric the neural detector was scored
   with.

## Non-goals

- No machine-learned detector at runtime (no onnxruntime, no `traffic`).
- No discovery of merge points from traffic data in the pipeline; the merge-point table is curated.
- No per-cycle FIR definitions. One cycle (524) is used for every day until a newer public set
  exists; the cycle is written into every event so a consumer can tell.
- No change to any already-published event type, its `version`, or its `info`.
- No FIR truth from PRISME in this iteration (laptop only); the extractor script is written so it can
  be run later, and the scoring code is unit-tested against a synthetic truth frame.

## Anti-goals (what would make this a failure even if it ran)

- A holding detector that fires on vectoring turns or go-arounds at the busy airports. The label
  set is there to catch exactly this.
- A FIR event stream that silently differs between days because the reference changed. The cycle
  in the event and the fixed table make the definition explicit.
- A reference join that makes step 04 much slower worldwide. The compact H3 reference and
  broadcast joins are the answer; the pipeline-day timing is measured before and after.
- Any new configuration threshold stored in SI units or without its unit in the name.

## Constraints (project-wide, binding)

- Native Spark only. Window functions partitioned by `track_id` and, where samples are paired, by
  `(track_id, piece)` from `opdi.pipeline.holes` so nothing is computed across a reception hole.
- Config thresholds in aviation units with the unit suffix in the field name (`_nm _ft _kt _ftmin
  _seconds _pct _deg`), enforced by `tests/test_event_config.py`. Comparisons against SI storage
  columns scale at the point of use with the existing constants (`* 3.28084`, `* 196.850394`,
  `* 1.94384`).
- Published versions are never mutated. The three families get their own version strings; the
  shared `events_v0.3.0` stamp on existing types is untouched.
- New behaviours are off under `EventConfig.legacy()` and are added to the benchmark ladder
  (`benchmarks/event_bench.py`, `V4_BASE` at the off value plus one rung each).
- Pipeline runs: one day at most (2026-06-14, the v5 study day, arm `cruise_linked` at
  `s3a://eurocontrol/opdi-world/research/a10`, read only). Bucket `eurocontrol`: stop under 3 GB free, delete only
  this study's own tables. One Spark driver at a time.
- No Claude attribution anywhere. No reportlab in project dependencies. No Quarto or R installs.
- `opdi-portal` is written to but not merged by me; the paper goes on a branch.

## The design

(Curation note: the merge-point coordinates were in the end taken from state AIPs where reachable and otherwise from an open mirror of X-Plane navigation data derived from a commercial database; see the ruling in the execution ledger and `src/opdi/reference/data/README.md`.)

### A shared mechanism: per-family versions and extra measurements

Today every event gets `version = events_v0.3.0` at stamping time and exactly two measurements
("Distance flown (NM)", "Time Passed (s)"). Two small, general changes:

1. A detector frame may carry a `version` column. Stamping uses `coalesce(version,
   events_version)`. Existing detectors are untouched and keep `events_v0.3.0`. Event ids already
   include the version, so the new families' ids are stable under their own version.
2. A table `EXTRA_MEASURES` in `events.py` lists `(event type, info field, measurement type,
   measurement version)`. The measurement write block adds one row per entry where the field is
   present in the event's `info` JSON (`get_json_object(info, '$.field')` cast to double). The
   measurement id is the event id plus a short suffix per entry. This is how holding duration,
   leg time and the others become measurements without a new write path per family.

Versions: `holding_v0.1.0`, `pms_v0.1.0`, `fir_v0.1.0` for events and their measurements.

### Family 1: holding (`holding-start`, `holding-end`)

**What it detects.** A period in which the aircraft turns through at least one near-complete circle
in one rotational sense while staying within a small area at roughly one level, above circuit height
and at flying speed. Both racetrack holds and single ATC orbits qualify; `info.pattern` tells them
apart.

**Per-sample quantities** (window per `(track_id, piece)` ordered by time):
- `trk`: last non-null `heading` (so masked stale broadcasts do not break the difference).
- `d_hdg`: wrapped difference `((trk - lag(trk)) + 540) % 360 - 180`; set to NULL when the time step
  exceeds `holding_max_sample_gap_seconds` (60) or the rate exceeds `holding_turn_rate_spike_deg_s`
  (6, twice the ICAO maximum).
- `turning`: `|d_hdg / dt| >= holding_turn_rate_min_deg_s` (0.4).

**Window features**, computed twice: trailing `rangeBetween(-W, 0)` and leading `rangeBetween(0, +W)`
on epoch seconds, `W = holding_window_seconds` (360):
- `net_turn` = sum of `d_hdg` (signed); `gross_turn` = sum of `|d_hdg|`.
- `disp_nm` = great-circle distance between the window's first and last positions;
  `path_nm` = difference of `cumulative_distance_nm`.
- `alt_range_ft` = max minus min of altitude in the window.

**Window test** (`hold_bwd`, `hold_fwd`): `|net_turn| >= holding_net_turn_min_deg` (300) and
`|net_turn| / gross_turn >= holding_same_sense_min_pct` (80) and `disp_nm <=
holding_max_displacement_nm` (8) and `path_nm >= holding_min_path_nm` (15) and `alt_range_ft <=
holding_max_altitude_range_ft` (2500), plus the gates `altitude_ft - coalesce(elev_ades_ft,
elev_adep_ft, 0) >= holding_min_height_ft` (2500) and `speed_kt >= holding_min_speed_kt` (140). A
sample is `in_hold` if either window test passes (the leading window recovers the entry turn).

**Episodes.** Run-length encode `in_hold` (lag plus cumulative sum), bridge gaps of at most
`holding_bridge_gap_seconds` (120), keep episodes lasting at least `holding_min_duration_seconds`
(240) whose signed turn over the episode is at least `holding_min_episode_turn_deg` (330) in
magnitude. Episodes within `holding_goaround_exclusion_seconds` (180) of a `go-around` event on the
same track are dropped (the go-around frame is already computed in step 04 and is passed in).

**Events.** One pair per episode, sharing `info`: `{holding_seq, pattern ("racetrack" | "orbit"),
direction ("R" | "L"), circuits, duration_s, distance_nm, altitude_start_ft, altitude_end_ft,
straight_pct}`. `pattern = "orbit"` when the share of non-turning samples is below 15% and the
total turn is below 450°. `holding-start` is the first sample of the episode, `holding-end` the
last; both carry their own position and cumulative columns.

**Measurements** (through `EXTRA_MEASURES`, attached to `holding-end`): "Holding duration (s)",
"Holding distance (NM)", "Holding circuits".

**Published numbers behind the thresholds**: window 6 min and one circuit of 4 to 7 min (ICAO Doc
8168 timing, airliner turn rates 1 to 2°/s); 300° in-window and 330° per episode (vendor rules
330°, Huang 360°, Melo's completed-orbit criterion); 0.4°/s turning floor (Boeing patent); same-sense
share (Boeing "same rotational sense", figure-8 rejection); displacement 8 NM and path 15 NM (Melo's
2 to 4 NM cylinder, hold geometry 8 to 10 NM long); 2 min bridge and 4 min minimum (Melo); 2,500 ft
and 140 kt gates (Boeing's 1,000 m / 10 km, aerodrome circuits excluded).

### Family 2: point merge (`pms-leg-entry`, `pms-leg-exit`, `pms-merge-point`)

**Reference table.** A CSV shipped as package data, `src/opdi/reference/data/point_merge_systems.csv`,
one row per merge point: `apt_icao, pms_id, merge_point, lat, lon, legs_mp_min_nm, legs_mp_max_nm,
leg_level_min_ft, leg_level_max_ft, valid_from, valid_to, source, confidence`. Seeded from the
EUROCONTROL catalogue (geometry, dates) with coordinates from open sources (AIP charts, X-Plane
Gateway fix data, published papers). The first version covers the sites whose merge point can be
placed with confidence (at least EIDW LAPMO, LFPG MOPAR, RKSI SEL, WMKK's four, VVTS SAMDU, VVNB
BISAX and SUBUP, RJTT CREAM and NEURO, and the remaining ECAC sites where the AIP chart names the
point: ENGM, EGLC/EGKB, LTFM, LTFJ, LIME, LPPT, GCRR, GCFV, ENBR, ENZV, ENVA). Sites without a
confidently placed merge point are listed with `confidence = "missing"` and no coordinates, so the
table documents the gap rather than hiding it. Loaded on the driver with pandas, broadcast.

**Candidate samples.** Tracks whose ADES (from `opdi_flight_list`) has a row in the table valid on
the day, samples within `pms_candidate_radius_nm` (60) of a merge point and below
`pms_candidate_max_fl` (250). Each sample is joined to every merge point of its ADES (one to six
rows). Per row: `d_nm` (great-circle distance to the merge point) and `brg_deg` (bearing from the
aircraft to the merge point).

**Leg test** (window per `(track_id, piece, merge_point)` ordered by time, `rangeBetween(-45, +45)`
seconds): `max(d) - min(d) <= pms_leg_distance_band_nm` (1.5); `avg(d)` inside
`[legs_mp_min_nm - pms_leg_distance_margin_nm, legs_mp_max_nm + margin]` (margin 2);
`|wrapped(last brg - first brg)| >= pms_leg_min_bearing_sweep_deg` (8); share of samples with
`60 <= |wrapped(heading - brg)| <= 120` at least `pms_leg_tangential_min_pct` (80). Run-length
encode `on_leg`; keep runs of at least `pms_leg_min_duration_seconds` (90).

**Direct-to test.** A leg run is accepted only if, within `pms_direct_lookahead_seconds` (600) after
its last sample, there is an aligned run (`|wrapped(heading - brg)| < pms_direct_align_deg` (5),
distance decreasing, at least `pms_direct_min_duration_seconds` (60)) and the track comes within
`pms_merge_point_radius_nm` (3) of the merge point. This rejects vectoring that happens to be
tangential.

**Events.** Per accepted leg run: `pms-leg-entry` at the first on-leg sample and `pms-leg-exit` at
the last (the direct-to instant), sharing `info`: `{apt_icao, pms_id, merge_point, leg_seq,
direction ("cw" | "ccw" as seen from the merge point), leg_distance_nm (mean d on the leg),
leg_level_ft (median altitude), duration_s, distance_nm, direct_to_distance_nm (d at exit)}`. One
`pms-merge-point` at the sample of closest approach when it is within the radius, `info`
`{apt_icao, pms_id, merge_point, closest_nm, via_leg (bool)}`.

**Measurements** (attached to `pms-leg-exit`): "Point merge leg time (s)", "Point merge leg distance
(NM)", "Point merge direct-to distance (NM)".

**Published numbers behind the thresholds**: constant distance for at least 90 s (traffic);
1.5 NM band (sagitta of a 10 NM chord on an 18 NM arc is 0.7 NM, plus RNAV 1); 8° sweep over 90 s
(an arc at 250 kt sweeps about 20°); 5° alignment (traffic uses 1° at 1 Hz; OPDI samples every 5 s
with integer headings); catalogue leg geometry per airport for the distance band.

### Family 3: FIR crossings (`xing-fir`)

**Reference (step 00c, rewritten in place).** `opdi_h3_airspace_ref` becomes a *compact* H3 set:
every polygon is filled at resolution 7 and compacted, so interior cells are stored at coarse
resolutions and only the boundary ring at 7. Columns: `airspace_type, code, name, airac_cfmu,
validity_start, validity_end, min_fl, max_fl, h3_cell (hex string), h3_res (int)`. Fixes: one write
per layer (not per polygon); polygons crossing the antimeridian are split at ±180° before filling;
rows with the same code and the same FL band are kept as one volume (sub-volumes dissolve into
their code). The FIR layer is written under both coverages; the ANSP and country layers stay as they
are (European). The cycle and its validity window are columns, as today. `ReferenceConfig.
airspace_resolution` is honoured (it is dead today).

**Membership per sample.** Ancestors of `h3_res_7` at resolutions 7 down to 1 are computed with
native bit operations on the 64-bit cell index (the resolution field and the unused digit field of
the H3 index are plain bit ranges, no library call needed), then a sequence of broadcast left joins
against the FIR layer at each resolution, filtered to `min_fl <= FL < max_fl`, coalesced into one
`fir_code` per sample. The expected compact FIR layer is on the order of a million rows, well within
a broadcast. A sample with no matching volume has `fir_code = NULL`.

**Debounce.** Runs of one `fir_code` per `(track_id, piece)` shorter than `fir_min_dwell_seconds`
(60) are absorbed into the preceding run (a track skimming a boundary flips cells every few
seconds; this is the persistence idea of the A9 callsign rule applied to airspace). Runs with NULL
code are not crossings in or out; they are gaps.

**Events.** One `xing-fir` per change of `fir_code` between consecutive non-NULL runs inside a piece,
at the first sample of the new run, `info`: `{crossing_seq, fir_from, fir_to, fl, bracket_s (time
between the last sample in the old FIR and the first in the new), airac_cfmu, airac_validity_start,
airac_validity_end}`. Vertical crossings (FIR to UIR on climb) are real volume crossings in the NM
model and are emitted like lateral ones; the codes make them recognisable.

**Measurements**: the two standard ones (cumulative distance and time at the crossing) are what
"distance flown per FIR" is computed from by differencing consecutive crossings; no extra entry.

**Step 04 wiring.** `TRACK_COLUMNS` gains `h3_res_7`. The detector reads the reference through the
storage manager and returns `None` (family skipped, logged) when the table is absent, so a run
without step 00c still completes.

### Where the three families run

Inside `_etl_flight_events_and_measures` under a new progress group `calc_navigation`, gated by
`emit_holding`, `emit_point_merge`, `emit_fir_crossings` (all `True`; `False` in `legacy()`).
Holding and point merge read the `geo` frame (aerodrome elevations and distances already attached);
FIR reads the cached input. All three partition on `(track_id, piece)`.

## Validation and the paper

Within the standing limits (one pipeline day, bucket quota):

1. **Unit tests** on synthetic tracks for every rule: a racetrack, an orbit, left-right vectoring
   (not a hold), a go-around (not a hold), an aerodrome circuit (not a hold); an arc plus direct
   (leg), an arc without direct (not a leg), a direct without arc (merge point only); a track across
   a synthetic two-FIR reference, a boundary skim that debounces, a hole that suppresses a crossing,
   a climb through FL195 that changes volume, an antimeridian polygon that fills the right side.
2. **Holding against the open label set** for one day of the labelled period at EGLL and EIDW
   (window precision, recall, F1 and per-flight episode overlap, the paper's own metric), run with
   the local Spark session over the labelled trajectories (downloaded from the 4TU archive, or if
   that is impractical, one European day re-ingested from OpenSky into this study's own bucket
   prefix). Bar: the neural detector's 0.84 / 0.70 / 0.76. Four thresholds are swept (net turn,
   displacement, minimum duration, pattern split).
3. **Point merge on the pipeline day**: share of arrivals detected on a leg at each covered
   airport against the catalogue's adherence ranking and the published usage rates (EIDW about 42%,
   ENGM about 11%, EGLC about 8% in 2022); leg distance and level against the catalogue's bands;
   plots of 20 detected and 20 undetected arrivals at EIDW inspected by eye and recorded.
4. **FIR on the pipeline day**: H3 membership against exact point-in-polygon on the same polygons
   for a sample of tracks (timing error at the boundary); crossings per flight, share of samples with
   no FIR, and a spot check that a known route crosses the expected sequence. The PRISME extractor
   (`reference/extract_airspace_profiles.R`, profile CPF) and the scorer are written and unit-tested
   so the truth comparison is one laptop run away.
5. **Cost**: step 04 wall time on the pipeline day before and after, from the existing timing
   hooks.

The paper (`opdi-portal/papers/airspace-events-v1/`, built the way the v5 track paper was: a
markdown template filled from committed CSVs with provenance, rendered to HTML and PDF) states the
method for each family in plain language, the thresholds and what backs them, the measurements, the
AIRAC limitation, and what is still unverified. CLAUDE.md gets a paragraph on the three families and
the FIR definition caveat.

## Decisions made on the user's behalf (my calls, open to correction)

- One cycle (AIRAC 524) for all days, stamped in every FIR event, rather than per-day cycle
  selection with gaps. Newer cycles can be added to `CFMU_AIRAC` when extracted.
- `xing-fir` as one event per crossing (info carries from and to) rather than an exit/entry pair;
  halves the rows and matches the existing crossing families.
- Vertical FIR-to-UIR transitions are emitted; a consumer wanting lateral-only filters on the codes.
- Holding reports both racetracks and orbits, distinguished in `info`, since the ASMA use case
  counts both as airborne delay.
- The merge-point table ships inside the package rather than as a step 00 output, because it is
  small, curated and versioned with the code.
- Three separate version strings rather than one bump of `events_v0.3.0`, so each family can
  evolve alone and existing ids do not change.
- Point merge requires the direct-to to follow the leg; "direct from afar without a leg" is not
  emitted in this iteration (it needs a separate false-positive study).

## Answers to the builder check (also my calls)

1. **Where the pipeline-day run writes.** The v5 arm lives at
   `s3a://eurocontrol/opdi-world/research/a10/cruise_linked` (the `opdi-world` warehouse; the
   earlier mention of `opdi/research/a10` was wrong) and its step 04 output is what the v4/v5
   papers depend on, so it is not rewritten. This study gets its own prefix,
   `s3a://eurocontrol/opdi-world/research/airspace_events`, holding `opdi_h3_airspace_ref` (from
   step 00c) and the step 04 outputs. A small runner (`benchmarks/run_airspace_events_day.py`)
   configures the storage manager on the new prefix and reads `osn_tracks_clean` and
   `opdi_flight_list` for 2026-06-14 from the a10 arm by explicit path. The timing comparison runs
   step 04 twice on the new prefix, once with the three families off and once on.
2. **Holding validation data.** No second ingest. The labelled trajectories (4TU archive, one day,
   EGLL and EIDW) are adapted locally into track-shaped input: altitude feet to metres for
   `baro_altitude_c`, ground speed knots to m/s for `velocity`, `track` to `heading`, vertical rate
   to m/s, `cumulative_distance_nm` and `piece` through the pipeline's own helpers, aerodrome
   elevation from a constant per airport. The score therefore measures the rule on uncleaned input;
   the paper says so.
3. **Shipping gate.** A family is on by default only if it passes its bar on the measured data;
   otherwise it ships with its `emit_*` flag off by default, the numbers in the paper, and the
   reason stated. Bars: holding, window F1 at least 0.70 and precision at least 0.75 on the labelled
   day at EGLL and EIDW (the neural detector trained on these labels scores 0.84 / 0.70 / 0.76);
   point merge, EIDW leg-usage share within 15 points of the published 42% and at least 18 of 20
   plotted detections judged real legs; FIR, no bar beyond the self-consistency check (it is a
   lookup, not a classifier). The flag default, the `legacy()` assertion and the ladder entry are
   set after scoring.
4. **Overlapping FIR volumes.** In step 00c: drop the placeholder rows (`code = "XXXX"` and the
   "REROUTING EXTENSION" row of `EGGX`), dissolve rows with the same `(code, min_fl, max_fl)`, and
   merge touching or overlapping FL bands of the same code into one band (so `OBBBUIR` 150-245 and
   245-999 become 150-999). Count and log the resolution-7 cells still claimed by two different codes
   with overlapping bands; that count goes in the paper. In step 04, a sample matched by more than
   one volume keeps the one with the narrowest FL band, then the smallest polygon area (stored as a
   column), then the lowest code; the duplicate is dropped with a lag test inside the
   `(track_id, piece)` window that the run-length step needs anyway, so no extra shuffle. The share
   of samples that hit an overlap is reported.
5. **Benchmark ladder.** Entries only (`V4_BASE` plus rungs so `verify_plan_v4` passes). No
   multi-period ladder runs; the pipeline day provides the extraction counts.
6. **Merge-point coordinates.** Published navigation fixes are facts from state AIPs, which states
   publish under ICAO Annex 15; AIP is the first source, open fix data (X-Plane Gateway) the second,
   and every row records its source. Nothing from a commercial navigation database is used.

## What the measurements changed (execution notes, 2026-11-11)

- The point-merge gate compared two different quantities: the published 42% is the share
  of Dublin arrivals that *enter* the procedure, most of which are then cleared direct
  without flying a leg (82% at Oslo in the one study that measured it), while this family
  reports arrivals that flew at least 90 s of leg. The usage share is therefore reported
  but not used as a gate; the visual check (detections judged real) is.
- The straight "direct-to" alignment is information, not a gate: Dublin's runway 10
  turn-in is a curve to the fix and the 5-degree gate lost every one of those legs. A leg
  counts when the track reaches the merge point within 3 NM from a leg at least 6 NM out.
- Holding and point merge read the last known heading and speed within the piece,
  because cleaning blanks heading, speed and vertical rate together on every repeated
  broadcast, i.e. on every straight stretch.
- The holding windows are differences of running sums at a row offset rather than
  time-bounded sliding frames: Spark re-aggregates a sliding frame for every row, and
  step 04 took 3.3 times longer with the families on in the first run.
- The holding altitude test is the sum of absolute altitude changes inside the window,
  not the spread: a climb or a descent through the window fails either, and the sum is
  a difference of running sums.
- The FIR lookup walks the ancestors down to resolution 0 (compacted ocean FIRs store
  whole base cells); a crossing is a change between consecutive *known* runs, so a
  stretch with no volume (altitude masked) in the middle does not hide it.
- Holding ships on although EGLL precision (0.71) misses the pre-set 0.75: the two-airport
  score (0.76 / 0.72 / 0.74) is level with the published detector's and the user asked for
  production; the paper states the miss.

## Left to the builder

File layout within `src/opdi/pipeline/` (one module per family is expected), exact helper names,
the synthetic fixtures, the H3 bit layout implementation details (resolution bits 52 to 55, digit
`r` at bits `45 - 3r` to `47 - 3r`, unused digits set to 7), the antimeridian split implementation,
and the paper's figure set.
