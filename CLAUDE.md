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

No Airflow. A plain step registry in `src/opdi/runner.py`, run via `opdi run`, `python opdi.py`, or `run_pipeline()`:

| Step | Module | Does |
|---|---|---|
| 00 | `reference/` | H3 airport zones, HexAero airport layouts, airspaces, OurAirports, aircraft DB |
| 01 | `ingestion/osn_statevectors.py` | Ingest OSN state vectors |
| 02 | `pipeline/tracks.py` | Track splitting, H3 indexing, altitude cleaning |
| 03 | `pipeline/flights.py` | Flight list — ADEP/ADES detection |
| 04 | `pipeline/events.py` | Event + measurement extraction |
| 05–08 | `output/`, `monitoring/` | Parquet export, cleanup, stats |

**Environments** (`src/opdi/config.py`): `dev` / `live` use Iceberg-on-Hive over Azure ADLS with Kerberos; `local` uses Spark-native Iceberg; **`opensky` uses neither Hive nor Iceberg** — plain parquet over S3A at `s3a://eurocontrol/opdi`, optionally on Kubernetes. `utils/storage.py` `StorageManager` is the switch (`use_s3 = not enable_iceberg`).

### Conventions that matter

- **Everything is native Spark** — column expressions and window functions partitioned by `track_id`. No `traffic`, no `applyInPandas`, no `pandas_udf` anywhere in the current codebase. Introducing one is a deliberate architectural step, not a default.
- **Units: storage is SI, everything human-facing is aviation.** The OSN schema is SI — altitudes in **metres**, velocity and `vert_rate` in **m/s** — because it mirrors OpenSky's own schema. Everything OPDI *publishes* is aviation: `events.py` emits `altitude_ft`, `roc_ft_min`, `speed_kt`, `FL`, `cumulative_distance_nm`, converting at point of use (`* 3.28084` → ft, `* 196.850394` → ft/min, `* 1.94384` → kt).
  **New config thresholds go in aviation units, with the unit in the field name** (`baro_altitude_d1_max_ft_s`, not a converted SI constant). Where a threshold must meet SI data, scale the *comparison*, not the stored value — `cleaning/native.py:AVIATION_UNIT_FACTOR` is the pattern, and it works there because the output is a NULL mask, which carries no unit. Reuse the constants above rather than introducing new ones, so the two can never drift. Getting units wrong is the most likely source of a silent bug: a threshold 3.28× too large simply never fires.
- **Do not convert the storage layer to aviation units.** `track_gap_low_altitude_meters` feeds the gap family of segmentation rules, whose thresholds are a published contract; changing it breaks `track_id` continuity with data published under those thresholds. The `osn_tracks` DDL comments are also a published contract.
- **Track identity is a versioned choice, not a frozen rule.** The default
  segmentation is now A9 `debounced` (`callsign_min_persistence_seconds = 30`),
  selected through `src/opdi/pipeline/segmentation/`. A9 is A8 plus one guard:
  it treats a callsign change as real only if the new value persists for 30 s
  before the next transition, so a callsign flickering to a garbled value and
  back (X→Y→X) no longer splits one flight into several. Measured: 23.1% of A8's
  track boundaries traced to a flicker leg; A9 gives ~19% fewer tracks,
  merge-neutral against A8. The `method` value `standard` is a **pointer to the
  current production arm** and now resolves to `debounced`; it resolved to A8
  `recommended` between 2026-08-27 and this change. **Two `track_id` boundaries
  now exist, and both are silent to a joiner (empty join, not an error):**
  (1) at 2026-08-27, legacy → A8, when the `_{year}_{month}` suffix vanished and
  ids became `{hash}_{offset}`; (2) at this change, A8 → A9, when the debounce
  altered which samples share an id. Prior rules stay reachable for
  reproduction under their own names: `recommended` is A8 (it ignores the
  persistence field), `legacy` is the pre-2026-08-27 rule. A9 at
  `callsign_min_persistence_seconds = 0` is textually identical to A8, so
  zeroing it is an alternative way to reproduce A8 by config. Nothing in the
  published data says which rule produced a row — `osn_tracks` has no version
  column — so a consumer comparing across either boundary must be told. See
  `opdi-portal/papers/track-construction-v1/` (legacy → A8) and
  `track-construction-v3/` (the A9 investigation; commit 8bd2f30). **The
  track-construction-v3 paper still describes A9 as available-but-not-default
  and needs a follow-up render to reflect the promotion.**
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

**Worth porting** — dedup on `(track_id, timestamp)`; lat/lon range checks; **stale-broadcast removal** (ADS-B updates position and velocity in separate message types, so identical consecutive values mean *repeated*, not *measured*); **derivative spike filtering with a ≥2-vote kill rule** (a point dies only if it participates in ≥2 flagged derivative windows, which targets the middle of a spike while sparing legitimate step changes); isolated-point removal at 20 s; gap segmentation at 5 min.

**Documented negative results — do not implement:**
- Synthetic gap filling: *"Attempts to complete the trajectory always lead to worse result"* (2025 report §7).
- GPS-jamming removal: leaving jamming untouched scored better (2025 report §7).
- ERA5 wind enrichment: removing wind *improved* RMSE 201.04 → 199.91 (2025 Table 4).

**Design note:** 2024 masks bad values to NULL and keeps the row; 2025 drops rows then resamples to 1 s. The 2024 approach is the Spark-natural one — no row explosion, pure column expressions, and it preserves the distinction between "no data" and "interpolated data".

## Known inconsistencies

- `opdi/sql/create_tables_*.sql` creates `opdi_flight_list_v2`, but all Python uses `opdi_flight_list`. The SQL is stale.
- `opdi/docs/pipeline_overview.rst` misdescribes the phase algorithm (says "vertical-rate thresholds"; it is fuzzy logic), the measurements (says "between consecutive events"; they are cumulative from track start), and the track grouping key.
- `opdi_h3_airspace_ref` is generated by step 00c and **read by nothing** — no airspace/FIR events exist.
- ~~`opdi` has **no tests**~~ — **out of date.** `opdi` has a real suite (260
  tests as of 2026-08-23) under `tests/`, run with
  `.venv310/bin/python -m pytest tests/`. It uses a local Spark session via the
  `spark` fixture in `conftest.py` — no cluster, no credentials — so the pure
  column expressions and config presets are testable on a laptop. Anything
  needing S3 or the cluster is not, and stays in `benchmarks/`.
- `opdi-portal/content/roadmap.qmd:38-43` past-releases table stops at OPDI v0.0.2 (release 3), 2024-12-01. `methodology.qmd:8` separately claims "Latest available methodology versions: v0.0.2 (May/June 2024)". Both are stale; confirm the real current release month against published data before rewriting either.
- **Resolved:** the six-copy bounding-box duplication noted above is fixed — coverage now lives in one `CoverageConfig` (`src/opdi/coverage.py`), consumed by ingestion, detection and all three H3 reference generators (zones, layouts, runway grid). Left in this list as history, not as an open item.
