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
Beside it, `events_v0.2.0` retired `take-off`/`ATOT`/`ALDT`/`AOBT`/`AIBT`/`entry-runway`/`exit-runway` for an A-CDM runway family, and published a second top-of-climb/top-of-descent pair alongside the original.

**`events_v0.3.0` (current default) publishes one event type per question.** `ATOT` and `ALDT` are single types again, each a coalesce of the A-CDM detector (preferred — the interpolated 15 ft crossing) over the legacy one (the fallback, and in practice most rows: A-CDM needs surface reception and reaches 4-7% of the network against ~90%). **`info.method` names the arm on every such row** — `"acdm"` or `"legacy"` — and it matters: the merged column mixes two estimators with different biases in a ratio that varies by aerodrome, so any aggregate over it carries a bias invisible in the column itself. `AOBT`/`AIBT` take back their names from `off-block`/`on-block`; `airborne`/`touchdown`/`top-of-climb-cco`/`top-of-descent-cdo` cease to exist, the PRU arm taking the plain `top-of-climb`/`top-of-descent`. Retained from v0.2.0: `line-up`, `take-off-roll`, `landing` (T16 — a threshold crossing, **not** the old descent-to-ground event of the same name), `runway-vacated`, `go-around`, `runway-crossing-entry`/`runway-crossing-vacated`. Rings are `xing-{40,50,60,100,110,120}nm`. `ATOT`/`ALDT` also carry `info.runway` and `info.runway_bearing_deg` (the centreline's *true* bearing — a property of the pavement, not a heading). Reconstruct v0.2.0 with `event_bench.V020_BASE`, v0.1.0 with `V4_BASE`, v0.0.2 with `EventConfig.legacy()`.

**Step 04b** folds these onto `opdi_flight_list` as `ATOT`/`ALDT`/`AOBT`/`AIBT`, `RWY_{DEP,ARR}`, `RWY_{DEP,ARR}_BEARING_DEG`, `STND_{DEP,ARR}` and `C{40,50,60,100,110,120}_{ARR,DEP}` — 22 columns, so a flight's milestones can be read without joining the event table. It rewrites **every** partition each run, not just the window: Spark samples one file for a partitioned parquet schema, so partitions that disagree hide the new columns silently.

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
- **Track identity is a versioned choice, not a frozen rule.** As of 2026-08-27
  the default segmentation is A8 `recommended` (group on `icao24`, break on a
  genuine non-blank callsign change with the lookback bounded to the gap
  threshold), selected through `src/opdi/pipeline/segmentation/`. **`track_id`
  changes shape from the next production run forward** — A8 carries no
  `_{year}_{month}` suffix, so identifiers become `{hash}_{offset}` and any
  consumer parsing the suffix breaks. Past months will not reproduce. Every
  dataset published before this date used the legacy rule, which stays
  reachable as the `legacy` arm. See `opdi-portal/papers/track-construction-v1/`.
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
