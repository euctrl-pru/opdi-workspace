# OPDI_WORKSPACE

Meta-repo for the **Open Performance Data Initiative** (OPDI), EUROCONTROL PRU. Eight sub-repos as git submodules.

```bash
git clone --recurse-submodules <url> OPDI_WORKSPACE
git submodule update --init --recursive   # if already cloned
```

## The repos

| Path | Remote | Branch | Role |
|---|---|---|---|
| `opdi/` | `euctrl-pru/opdi` | `main` | **The deliverable.** PySpark pipeline, runs on the OpenSky (OSN) server. |
| `opdi-portal/` | `euctrl-pru/opdi-portal` | `feature/new-topics` | Quarto website. Concepts, methodology, roadmap, papers. |
| `eurocontrol/` | `eurocontrol/eurocontrol` | `main` | **R** package over the PRISME Oracle warehouse. Ground truth. |
| `traffic/` | `xoolive/traffic` | `master` | Reference algorithms. Read-only — never edit. |
| `prc-data-challenges/2024/prc_data_challenge` | `euctrl-pru/…` | `main` | 2024 ATOW challenge description. |
| `prc-data-challenges/2024/team_likable_jelly` | `PRC-Data-Challenge-2024/…` | `main` | **2024 winner** (Alligier & Gianazza, ENAC). |
| `prc-data-challenges/2025/prc_data_challenge_website_2025` | `euctrl-pru/…` | `master` | 2025 fuel-flow challenge description. |
| `prc-data-challenges/2025/resourceful-quiver` | `PRC-Data-Challenge-2025/…` | `main` | **2025 winner** (TU Delft). |

Only `opdi/`, `opdi-portal/` and this meta-repo are written to. The rest are reference material.

## What OPDI does

Ingests OSN ADS-B state vectors → splits them into tracks → derives a flight list (ADEP/ADES) → extracts **flight events** (milestones) and **measurements** → publishes parquet at `eurocontrol.int/performance/data/download/OPDI`.

The data model (see `opdi-portal/content/concepts.qmd`):
- **EVENT** — a milestone with a 4D fix (lon, lat, altitude, timestamp), a `type`, a `source`, an algorithm `version`, and free-form `info`. Things that are not instants (level segments, holdings) are modelled as an **event pair**.
- **MEASUREMENT** — a metric attached to an event by `event_id`.

Published event types (v0.0.2): `take-off`, `landing`, `top-of-climb`, `top-of-descent`, `level-start`, `level-end`, `first-/last-xing-fl{50,70,100,245}`, `entry-/exit-{runway,taxiway,apron,hangar,threshold,parking_position,deicing_pad}`, `first_seen`, `last_seen`.
Beside it, `events_v0.2.0` retires `take-off`/`ATOT`/`ALDT`/`AOBT`/`AIBT`/`entry-runway`/`exit-runway` for the A-CDM runway family `line-up`, `take-off-roll`, `airborne`, `landing` (T16 — same name, now a threshold crossing, not the old descent-to-ground event), `touchdown`, `runway-vacated`, `off-block`, `on-block`, `go-around`, `runway-crossing-entry`/`runway-crossing-vacated`, plus a second top-of-climb/top-of-descent pair (`top-of-climb-cco`/`top-of-descent-cdo`) published alongside the original; still pending its benchmark campaign — see `opdi-portal/papers/flight-events-v4/`.

## Pipeline layout (`opdi/`)

A plain step registry in `src/opdi/runner.py`, run via `opdi run`, `python opdi.py`, or `run_pipeline()`. One (step, day) unit is also a command (`python -m opdi.orchestration {reference|step|day|check-space|prune|plan}`), which is all the Airflow 2.7.2 DAGs in `opdi/airflow/dags/opdi_pipeline.py` call (`opdi_daily`, `opdi_backfill`; the DAG file never imports `opdi`, see `opdi/airflow/README.md`):

| Step | Module | Does |
|---|---|---|
| 00 | `reference/` | H3 airport zones, HexAero airport layouts, airspaces, OurAirports, aircraft DB |
| 01 | `ingestion/osn_statevectors.py` | Ingest OSN state vectors |
| 02 | `pipeline/tracks.py` | Track splitting, H3 indexing, altitude cleaning |
| 03 | `pipeline/flights.py` | Flight list — ADEP/ADES detection |
| 04 | `pipeline/events.py` | Event + measurement extraction |
| 04b | `pipeline/flight_list_milestones.py` | Milestone columns on the flight list |
| 09 | `output/published.py` | Publish the day: one flight-list file, one merged flight-events file, monthly index, catalogue |

Steps 05–08 (monthly v0.0.2 exporters, monitoring) were removed on 2026-11-11: the v0.0.2 monthly release is no longer produced.

**Environments** (`src/opdi/config/`, a package since 2026-11-11; every name still imports from `opdi.config`): `opensky` (default; plain parquet over S3A, optionally on Kubernetes) and `local` (Spark-native Iceberg, used by the tests). The Azure `dev` / `live` environments were removed. `utils/storage.py` `StorageManager` is the switch (`use_s3 = not enable_iceberg`).

**Release v2.0.0 and where data goes.** One warehouse setting (argument, else `OPDI_WAREHOUSE`, else `s3a://eurocontrol/opdi`), for both coverages; every table lives under the release root `<warehouse>/v2.0.0/` (`ProjectConfig.release`, `.release_root`); progress state under `<release root>/_state/`; published files under `<release root>/published/` (`flight_list/flight_list_YYYYMMDD.parquet`, `flight_events/flight_events_YYYYMMDD.parquet`, `index/flight_index_YYYYMM.parquet`, `catalogue.parquet`). A day's files hold the flights whose flight-list `DOF` is that day and all their events. The published columns are declared once in `src/opdi/output/schema.py`; the portal data dictionary and the Trino DDL (`src/opdi/sql/trino/`, rendered by `python -m opdi.sql.render`) are generated from it — change a column there, never in the generated files. `opdi.published` is a Spark-free reader (pandas/DuckDB) with views giving the classic separate `events` / `measurements` / `flight_list_v0` shapes. Study scripts that point `warehouse_path` at a complete prefix must set `cfg.project.release = ""`. Measured on 2026-06-14 worldwide: 175,615 flights, 6.77 M events, 7 MB + 411 MB published per day, about 1.3 GB of working tables per day (`prune --drop-published-working`, Airflow Variable `opdi_keep_working`).

**The package keeps two behaviours only:** today's default (v2.0.0) and the presets reproducing v0.0.2 (`segmentation.method="legacy"`, `decimation="modulo"`, `DetectionConfig.legacy()`, `EventConfig.legacy()`). Every other variant (the segmentation rules `recommended`, `debounced`, ...; detection mode `nearest`; `trend_ooa`) lives in `opdi/benchmarks/study_variants/` and is registered by the study scripts (`register_study_arms()`); selecting one without registering raises a message naming the new home. `tests/test_golden_outputs.py` (reference files in `tests/golden/`, regenerate with `tools/make_golden.py`) compares every working and published table of a simulated day for both behaviours: a change that alters it changes published data. Tests of benchmark scripts are under `tests/benchmarks/`. Version strings are in `src/opdi/versions.py`. Code is ruff (E,F,W,I,N,D,B,UP, google docstrings) and black (line length 100) clean; `tests/test_docs_plain_language.py` rejects development history and internal wording in docstrings, comments and `readme.md` — docstrings are written for researchers and students who use the data, and link the portal methods pages (`opdi.PORTAL_URL`).

### Conventions that matter

- **Everything is native Spark** — column expressions and window functions partitioned by `track_id`. No `traffic`, no `applyInPandas`, no `pandas_udf` anywhere in the current codebase. Introducing one is a deliberate architectural step, not a default.
- **Units: storage is SI, everything human-facing is aviation.** The OSN schema is SI — altitudes in **metres**, velocity and `vert_rate` in **m/s** — because it mirrors OpenSky's own schema. Everything OPDI *publishes* is aviation: `events.py` emits `altitude_ft`, `roc_ft_min`, `speed_kt`, `FL`, `cumulative_distance_nm`, converting at point of use (`* 3.28084` → ft, `* 196.850394` → ft/min, `* 1.94384` → kt).
  **New config thresholds go in aviation units, with the unit in the field name** (`baro_altitude_d1_max_ft_s`, not a converted SI constant). Where a threshold must meet SI data, scale the *comparison*, not the stored value — `cleaning/native.py:AVIATION_UNIT_FACTOR` is the pattern, and it works there because the output is a NULL mask, which carries no unit. Reuse the constants above rather than introducing new ones, so the two can never drift. Getting units wrong is the most likely source of a silent bug: a threshold 3.28× too large simply never fires.
- **Do not convert the storage layer to aviation units.** `track_gap_low_altitude_meters` feeds the gap family of segmentation rules, whose thresholds are a published contract; changing it breaks `track_id` continuity with data published under those thresholds. The `osn_tracks` DDL comments are also a published contract.
- **Track identity is a versioned choice, not a frozen rule.** The default
  segmentation is A10 `cruise_linked` (since 2026-11-09, branch
  `track-construction-v5`), selected through `src/opdi/pipeline/segmentation/`.
  A10 is A9 `debounced` plus one change: a silence over 30 min (a **hole**)
  does not end a track when the samples either side of it (its **edges**) fit
  one continuous flight -- both edges at or above 18,000 ft, or the old edge
  still climbing, or the new edge descending (above 5,000 ft, on >= 3 vertical
  rates); straight-line speed across the hole 400-750 kt, hole at most 600 min,
  callsigns not both present and different, no descent-then-climb, no
  placeholder `icao24`. **Descent was off in v4** on 3 false joins in 119
  checkable holes; v5 found all three were NM *estimated* arrival times at
  Seoul minutes before ADS-B still showed the aircraft airborne, and the study
  truth no longer counts events at aerodromes outside the European box (NM
  times there are estimates) -- 0 false joins in 117 since. Edge values are read past blank samples within
  5 min. Measured over two days against Network Manager: flights crossing the
  edge of the European box in one track 25% -> ~80%, false joins ~0.05% of
  checkable linked holes; see `opdi-portal/papers/track-construction-v5/`
  (v4 is the proposal and is frozen). A10 also forgets a callsign not heard
  for longer than 600 min (`methods.callsign_memory_minutes`), so two day-runs
  judge a flicker after a hole alike; A9 by name keeps its unbounded memory.
  The `method` value `standard` is a **pointer to the current production arm**
  and resolves to `cruise_linked`; it resolved to `debounced` between the A9
  promotion and 2026-11-09, and to A8 `recommended` between 2026-08-27 and
  the A9 promotion. **Three `track_id` boundaries now exist, and all are silent
  to a joiner (empty join, not an error):** (1) 2026-08-27, legacy -> A8, when
  the `_{year}_{month}` suffix vanished; (2) A8 -> A9, the debounce; (3)
  2026-11-09, A9 -> A10, one id where there were two across a linked hole.
  Prior rules stay reachable under their own names (`debounced`,
  `recommended`, `legacy`). Nothing in the published data says which rule
  produced a row -- `osn_tracks` has no version column.
  **Consequences of A10 inside the pipeline:** a day's run reads 11 h back
  instead of 3 (`tracks.required_lookback_hours`); a track can hold a hole, so
  detectors that pair or interpolate samples (level segments, flight-level and
  ring crossings) work per **piece** -- the samples between holes --
  via `pipeline/holes.py` and `EventConfig.track_hole_seconds` (1800, equal to
  the segmentation gap on purpose, so a track without a hole gives the events it
  always gave); per-track facts (first/last seen, tops, runway and block
  milestones, the flight list) stay per track. Cumulative distance is
  `distance_v0.0.3`: it reads from the last *known* position (the released
  `distance_v0.0.2` zeroed two segments around every position cleaning had
  blanked, ~14% of all distance on 2026-06-14, unrelated to holes) and adds the
  straight line across a hole; `EventConfig.legacy()` keeps v0.0.2 -- and a
  level-off event's `info.distance_nm` moves with it on every track with a
  blanked position (types and times do not). The flight
  list names the aerodrome actually ranked first (it used `first()` over an
  unordered group, wrong ~1% of the time once a task holds 128+ groups), exact
  ties go to the identifier, and a day run always rebuilds its endpoint
  candidates. **The track-construction-v3 paper still describes A9 as
  available-but-not-default and needs a follow-up render.**
- **Three navigation event families (2026-11), each with its own version.**
  Step 04 emits `holding-start`/`holding-end` (`holding_v0.1.0`),
  `pms-leg-entry`/`pms-leg-exit`/`pms-merge-point` (`pms_v0.1.0`) and
  `xing-fir` (`fir_v0.1.0`), from `src/opdi/pipeline/{holding,point_merge,fir}.py`,
  under the `navigation` progress group and the flags `emit_holding`,
  `emit_point_merge`, `emit_fir_crossings` (all on; off under `legacy()`; V4
  ladder rungs V10-V12). A detector may return a `version` column and step 04
  stamps `coalesce(version, events_version)`, so `events_v0.3.0` and the ids of
  the published types are untouched. Measurements beyond the two every event
  gets come from `events.EXTRA_MEASURES` (an `info` field read as a measurement
  row; holding duration/distance/circuits, point-merge leg time/distance/
  direct-to distance). **Holding** is a signed cumulative turn over a 6 min
  window (at least 300 degrees one way, within 8 NM, 15 NM flown, under
  2,500 ft of altitude change), placed trailing, leading and centred,
  computed as differences of running sums at a row offset because Spark
  re-aggregates a sliding frame per row (the time-bounded version made step
  04 3.3x slower). Scored against the hand-corrected 2019 labels (one day,
  EGLL and EIDW): precision 0.76, recall 0.72, F1 0.74, level with the
  published neural detector (0.84/0.70/0.76); EGLL alone 0.71/0.71/0.71.
  **Point merge** needs merge-point coordinates: the curated table
  `src/opdi/reference/data/point_merge_systems.csv` (61 catalogue airports,
  54 placed, AIP where reachable else AIRAC 2406 navdata; see its README). A
  leg is 90 s at constant distance in the published band moving across the
  bearing, kept when the track then reaches the merge point within 3 NM;
  the straight-direct alignment is `info.direct_aligned`, not a gate (Dublin
  runway 10 turns in on a curve). On 2026-06-14 Dublin had 52 of 418
  arrivals on a leg; 19 of 20 plotted detections were real. **Arrivals per
  airport must be counted with an inbound `xing-40nm`, not from the flight
  list's `ades`, which also names a trend-guessed destination for tracks that
  never land** (22,926 "LFPG arrivals" on one day otherwise). **FIR**: step
  00c (`reference/h3_airspaces.py`) now builds a compact worldwide H3 layer
  from the PRU Atlas NM FIR polygons of **CFMU AIRAC 524 (valid 2024-11-28 to
  2024-12-26, the newest public cycle)** through h3's integer API (about 1.1 M
  cells, 5 min), splitting antimeridian polygons, dropping `XXXX` and the
  `EGGX` rerouting row and merging same-code bands; the earlier port
  overwrote the table once per polygon; **a warehouse that still holds that
  old table (columns `h3_res_7*`, no `h3_cell`) must have it dropped before
  step 00c runs**, which refuses to write over it, and step 04 skips the FIR
  family with a message until then. The detector finds a sample's volume
  from its res-7 cell's ancestors (bit arithmetic, broadcast joins per
  resolution, narrowest FL band wins an overlap), absorbs runs under 60 s,
  and emits one `xing-fir` per change with `fir_from`, `fir_to`, `fl` and
  **the AIRAC cycle in every event**: the same definition is used for every
  day, which is the limitation to know about. Vertical FIR-to-UIR crossings
  are emitted too. Against exact point-in-polygon: 99.5% of samples agree,
  crossing timing p90 10 s. Cleaning blanks heading, speed and vertical rate
  together on a repeated broadcast, so holding and point merge read the last
  known value within the piece. Step 04 on a worldwide day: 2,157 s with the
  families off, 4,431 s on. Not yet verified: `xing-fir` against NM airspace
  profiles (extractor `reference/extract_airspace_profiles.R`, laptop only;
  scorer `benchmarks/fir_truth_score.py`), point merge at the Norwegian
  sites (zero legs found) and at unplaced merge points. Paper:
  `opdi-portal/papers/airspace-events-v1/`; spec and plan under
  `docs/superpowers/{specs,plans}/2026-11-10-airspace-events*`.
- **ADS-C is a second source, off by default.** `AdscConfig.enabled`
  (`src/opdi/config.py`) unions OpenSky's ADS-C topic
  (`s3a://readsb-aircraft-states/topics/readsb.aircraft.adsc.avro`, hourly
  `hour=<unix>` partitions, snappy parquet despite the `.avro` name) into the
  state-vector frame **before** segmentation, via step 01b and
  `TrackProcessor._merge_adsc`. Enabling it is a fourth `track_id` boundary
  after legacy -> A8 -> A9 and is silent to a joiner, so the study arms live at
  `s3a://eurocontrol/opdi/research/adsc/{base,merged}` and nothing published
  moves. **What the feed is**, measured over eleven days 2023-09 to 2026-10
  (`benchmarks/adsc_census.py`, staged at `benchmarks/out/adsc_census/`):
  140-5,420 rows/day inside `EUROPE_BBOX` from 74-994 airframes, median cadence
  277-1,139 s, 94-99% above FL300, 66-84% west of 10W, `on_ground` **never**
  true, 1-67 rows/day below 10,000 ft, and 21 candidate extra fields (`ias`,
  `tas`, `mach`, `oat`, `wd`/`ws`, `nav_altitude_mcp`, `registration`,
  `ac_type`, `alt_geom`, ...) at **0.0% on every day**. The feed's schema grew:
  `track`/`gs`/`baro_rate` empty before ~2025-09, `flight`/`squawk`/`on_ground`
  before ~2026-06, so a merged arm over 2023-2024 writes NULL `velocity` and
  `vert_rate`. **Three things are easy to get wrong.** The `flight` field is the
  **IATA** number (`LH133`), not the ICAO callsign ADS-B broadcasts
  (`DLH133`), so it is dropped -- carried through, the A9 callsign-change break
  would split every merged track at its first ADS-C sample. The 5 s thinning
  rules are not applied (`modulo` would delete a feed whose instants are
  arbitrary millisecond doubles). And `CleaningConfig.sparse_sources` exempts
  ADS-C from the isolated-point and stale-broadcast stages, without which step
  02a NULLs every ADS-C value (20 s rule, ~600 s cadence) and the fusion
  measures as a no-op. The `source` column exists **only** in a run with the
  flag on -- a column present in new partitions of a production table and
  absent from old ones breaks `spark.read.parquet` schema inference silently.
  **The two feeds do not share a clock:** median +1.41 s along-track against
  interpolated ADS-B (8.7 m cross-track vs 415 m along-track, a 48x ratio, so
  it is timing and not position; 764 pairs over 293 airframes, 2026-06-05..07).
  ADS-B's own position age is a median 0.34 s, correlating at 0.21, so it does
  not explain the centre -- but it reaches 152.3 s, so the extreme offsets are
  at least partly a stale reference. The offset varies by airframe: per-airframe
  medians span -184.6 s to +58.7 s, and sigma within one airframe spans 0 to
  44.76 s. So `time_offset_seconds` ships at 0.0: a constant would damage the
  aircraft already synchronised. `benchmarks/adsc_time_sync.py` is the
  instrument. The derivative-spike filter is deliberately untouched
  (`latlon_d1_max_deg_s` is ~1,100 m/s of apparent speed, three orders of
  magnitude above this error). The study's tables at
  `research/adsc/{base,merged}` no longer exist and `regenerate_adsc_v1.py`
  reports its pipeline jobs stale, because the fingerprints now cover
  `config.py`, `coverage.py` and the runner; only re-running the arms clears
  that, and it needs room in the 200 GB bucket. A worldwide day (2026-06-05)
  was measured on only the airframes with ADS-C, because a full day does not
  fit the bucket: +2,425 tracks and +2,812 track-hours, about 1.4% and 1.2% of
  a full worldwide day. But 2,440 of the 2,509 ADS-C-only tracks belong to
  airframes with an ADS-B track that day, so a track count is not a count of
  newly covered aircraft. Against Network Manager, ADS-C does not move far-end
  ADEP/ADES on flights landing that day (395 of 546, 581 of 788), and it closes
  only 30 of 1,680 cruise holes over 30 min: the reports exist but bunch, and one
  remaining gap over 30 min still splits the track. See
  `docs/superpowers/plans/2026-10-16-adsc-fusion.md` and
  `opdi-portal/papers/adsc-fusion-v1/`.
- **Geographic coverage is one `CoverageConfig` (`src/opdi/coverage.py`), not six copies.** It used to be a bounding box written out six times across five files — four of them textually identical `LAT_MIN`/`LAT_MAX`/`LON_MIN`/`LON_MAX` class constants with no link between them, so widening coverage meant finding all six and missing one produced no error (ingest keeps rows detection zones don't cover — a silent wrong answer, not a crash). **Worldwide is the default** (since 2026-10-20, branch `worldwide-default`): `OPDIConfig.for_environment(env)` and every entry point (`run_period.py`, `opdi run`, `opdi.py`) run worldwide unless told otherwise — `worldwide=False` in code, `--europe` on the command line. The European box (`opdi.coverage.EUROPE_BBOX`) is still a **published contract**: it reaches every released `track_id` and every detection study tuned inside it, and it must stay byte-identical — but a caller that wants it now has to ask. Benchmark scripts that call `for_environment()` without `worldwide=` therefore run worldwide too. `bbox=None` means worldwide and is not a box of ±180/±90, because a longitude range cannot express "everywhere" once the coordinate wraps at the antimeridian; it compiles to `lit(True)` instead. Worldwide writes to its own warehouse (`opdi-world`) rather than the European one, and `OPDI_WAREHOUSE` is **deliberately ignored** under worldwide — that variable is how production points at the European prefix and is normally exported, so honouring it there too would silently send planet-wide rows into `opdi-prod`. A `coverage_marker` table stamps each warehouse with the coverage that built it and refuses to mix: pointing a worldwide run at a warehouse already marked European fails at step 00 naming both coverages. Out-of-area (`OOA`) labelling is inert worldwide (there is no edge to have crossed), and step 00c (airspaces) does not run worldwide (its ANSP/FIR sources are European; there is no global FIR source). See `opdi/readme.md`'s "Worldwide coverage" section and `opdi-benchmarker/README.md`, which follows the same switch for its source and cache but not its reference prefix (APDF is European regardless of pipeline coverage).
- **Never mutate a published `version` string.** New algorithms get a new `version`; existing event types keep theirs so released data stays reproducible.
- Executors run `docker/Dockerfile` → `quintengs/opdi-spark`. Any new runtime dependency must be added there or executors will fail at import.

## Ground truth (`eurocontrol/`)

R only, and **only runnable on the work laptop** — it needs PRISME Oracle access via ROracle plus `<SCHEMA>_USR` / `_PWD` / `_DBNAME` env vars.

Extract → `arrow::write_parquet()` → commit to `opdi/reference/` under **git-lfs** → pull on OSN. Never query the DB from the pipeline or from a paper render.

**APDF is in long/movement form** — there is no literal AOBT/ATOT column. Discriminate on `SRC_PHASE`:

| Milestone | Column | Filter |
|---|---|---|
| AOBT | `BLOCK_TIME_UTC` | `SRC_PHASE == 'DEP'` |
| ATOT | `MVT_TIME_UTC` | `SRC_PHASE == 'DEP'` |
| ALDT | `MVT_TIME_UTC` | `SRC_PHASE == 'ARR'` |
| AIBT | `BLOCK_TIME_UTC` | `SRC_PHASE == 'ARR'` |

Also: `AP_C_RWY` (runway), `AP_C_STND` (stand), `C40_/C100_CROSS_{TIME,LAT,LON,FL}` + `_BEARING` (ASMA rings).

`flights_tidy()` gives flight-level truth: `ADEP`, `ADES`, `AOBT_3`, `FLT_TOW`, and `AIRCRAFT_ADDRESS` — which **is `icao24`**, the join key to ADS-B. Join on `AIRCRAFT_ADDRESS` + callsign + date.

⚠️ `apdf_tidy()` covers **one month at a time** and filters on `SRC_DATE_FROM` as well as `MVT_TIME_UTC` — a wide window silently drops rows. Loop monthly.

## Working practices

- **Benchmark every new milestone** against `eurocontrol` ground truth. Reproducible, committed as parquet under `opdi/reference/`, documented in a Quarto paper under `opdi-portal/papers/`.
- **Papers render by running their analysis.** A paper's `.qmd` invokes its
  regeneration entrypoint (`benchmarks/regenerate_v6.py` for the V6 study)
  before drawing anything, so the figures are what the checked-out code
  produces rather than what someone once copied into `data/`. The entrypoint is
  idempotent: it re-runs a job only when the *source files that job depends on*
  have changed since its output was written, so a render with everything
  current is a fast no-op. Rendering a stale paper therefore needs cluster
  credentials, and that is intended — the numbers come from Spark over S3
  against Network Manager reference data and cannot be recomputed without it.
  `OPDI_RENDER=check` fails fast on staleness; `OPDI_RENDER=allow-stale`
  renders a draft anyway, and the paper's provenance table then shows which
  figures are unverified.
  **This reverses the earlier "papers must render offline" rule** (decided
  2026-08-10). Papers V1–V5 and the decimation study pre-date the change and
  still read committed caches; they have no regeneration entrypoint, and their
  numbers are traceable only to the version that published them.
- **Every committed figure carries provenance.** `benchmarks/provenance.py`
  stamps each output with the script, argv, git SHA, dirty flag and a
  fingerprint over the source files it depends on, written to
  `data/_manifest.json` beside the CSVs. An output with no manifest entry is
  reported in the paper as unverified rather than shown as fact. This exists
  because three staged CSVs were once found to derive from tables written days
  earlier by different parameters, and nothing in the file or its timestamp
  said so.
- Respect documented **negative results** from the PRC challenges — see below.
- When touching a submodule: commit inside it first, then commit the updated pointer in the meta-repo.

## Evidence base: PRC challenge findings

The two winning solutions are the best available evidence on ADS-B cleaning. Key results:

**Worth porting** — dedup on `(track_id, timestamp)`; lat/lon range checks; **stale-broadcast removal** (ADS-B updates position and velocity in separate message types, so identical consecutive values mean *repeated*, not *measured*); **derivative spike filtering with a ≥2-vote kill rule** (a point dies only if it participates in ≥2 flagged derivative windows, which targets the middle of a spike while sparing legitimate step changes); isolated-point removal at 20 s. **Unverified:** "gap segmentation at 5 min" was listed here as a finding, but the research report behind `track-construction-v4` (`docs/superpowers/research/linking-fragmented-flight-trajectories.md`) could not find it in either winner's active code, so do not cite it as evidence until someone does.

**Documented negative results — do not implement:**
- Synthetic gap filling: *"Attempts to complete the trajectory always lead to worse result"* (2025 report §7).
- GPS-jamming removal: leaving jamming untouched scored better (2025 report §7).
- ERA5 wind enrichment: removing wind *improved* RMSE 201.04 → 199.91 (2025 Table 4).

**Design note:** 2024 masks bad values to NULL and keeps the row; 2025 drops rows then resamples to 1 s. The 2024 approach is the Spark-natural one — no row explosion, pure column expressions, and it preserves the distinction between "no data" and "interpolated data".

## Known inconsistencies

- **Take-off and landing merge order.** The default publishes the runway-pass time first (`acdm_first`) while the flight-events-v4 study found more times within 10 s with the climb-and-descent time first (30.9% / 41.1% against 24.7% / 25.5%); the portal page `methods/takeoff-landing` states both. Not changed: it would alter published times.
- **Portal (`feature/new-topics`) has never been rendered** since the 2026-11 restructure (no Quarto/R on the pod). `tools/check_site.py` checks links, the nine-heading methods template, that every accuracy number on a methods page is a row of `assets/validation/validation_measures.csv`, and wording; `PUBLISHING_CHECKLIST.md` lists what the owner must supply. The community page is excluded from rendering until its members agree.
- Event `info` is order-dependent in two places (found 2026-11-10 by the v5
  consistency check: 4,126 of 15,032 identical tracks differed in `info`
  only): the HexAero layout detector names one of several *overlapping*
  taxiway/stand polygons by row order (`osm_id`/`osm_ref` differ between two
  runs of the same code), and two FL crossings in the same second can swap
  `crossing_seq`. Types and times are deterministic.
- `opdi_h3_airspace_ref` (step 00c, both coverages) is read by the `xing-fir` detector (`pipeline/fir.py`).
- ~~`opdi` has **no tests**~~ — **out of date.** `opdi` has a real suite (260
  tests as of 2026-08-23) under `tests/`, run with
  `.venv310/bin/python -m pytest tests/`. It uses a local Spark session via the
  `spark` fixture in `conftest.py` — no cluster, no credentials — so the pure
  column expressions and config presets are testable on a laptop. Anything
  needing S3 or the cluster is not, and stays in `benchmarks/`.
- `opdi-portal/content/roadmap.qmd:38-43` past-releases table stops at OPDI v0.0.2 (release 3), 2024-12-01. `methodology.qmd:8` separately claims "Latest available methodology versions: v0.0.2 (May/June 2024)". Both are stale; confirm the real current release month against published data before rewriting either.
- **Resolved:** the six-copy bounding-box duplication noted above is fixed — coverage now lives in one `CoverageConfig` (`src/opdi/coverage.py`), consumed by ingestion, detection and all three H3 reference generators (zones, layouts, runway grid). Left in this list as history, not as an open item.
