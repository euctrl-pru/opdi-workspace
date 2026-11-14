# OPDI industrialisation: design

Date: 2026-11-11. Repos: `opdi` (pipeline), `opdi-portal` (website), this meta repo (spec, plan, CLAUDE.md).

## Intent

OPDI moves from a research pipeline to a published product for the external research community. A
researcher should be able to download one day of data, understand every column, know how each value was
computed, how accurate it is and where it fails, without reading the code or the study papers. The
maintainers should be able to produce any period with one scheduled job and read the code without its
development history.

The person using the data mostly opens the **flight list** (one row per flight, with the main milestones
already on it); some go on to visualise or analyse **events**. Delivery is built around that.

## Goals

1. **Published data layout.** Two files per day in the production warehouse:
   - `flight_list` — one row per flight.
   - `flight_events` — one row per event, with that event's measurements as columns and the flight's
     own columns repeated on every row (the "merged table"). This is the default way events and
     measurements are delivered.
   A day's files hold the flights that *started* that day. Ready-made views give back the classic
   separate shapes (`flight_list`, `flight_events`, `measurements`) for anyone built on those.
2. **Production environment** on OpenSky: warehouse `s3a://eurocontrol/opdi`, worldwide coverage, one
   setting to point it elsewhere (config field, `OPDI_WAREHOUSE`, and a DAG parameter).
3. **Airflow DAG** (Airflow 2.7.2, existing instance) that processes a period day by day: a scheduled
   daily run, and backfills of any range (first: today back to 2025-01-01, newest first; later e.g. to
   2019) by parameters only.
4. **Code quality in `opdi`**: PEP 8 clean under the project's linters, docstrings written for OPDI
   users in plain language with no development history, idle and non-default code removed, structure
   tidied, short comments where the logic is not obvious. Nothing that works today breaks.
5. **Portal**: the whole pipeline documented for data users, each algorithm explained; a column-level
   data dictionary; a validation note and a limitations section per algorithm in one fixed format with an
   error-distribution figure and measures; the PRC Data Challenge section updated to the 2026 edition;
   all portal branches merged into `feature/new-topics`; the community page hidden; everything ready to
   render and publish. Not merged to `main`.

## Non-goals

- Running the 2025-2026 backfill (space has to be made first; the DAG and a one-day production run are
  delivered and verified).
- Rendering the site with Quarto (not installed on this pod, and not to be installed); every page is
  written so that `quarto render` on the owner's machine needs no cluster. Python-drawn figures are
  committed as images.
- New algorithms or threshold changes. Outputs of the default pipeline stay byte-for-byte what `main`
  produces today, except the new published files.
- Rewriting the study papers. They stay as the detailed record and are linked as "full study".

## Anti-goals

- A clean-up that changes a published or default result. Guard: the existing test suite, plus a
  before/after comparison of step 02-04b outputs on one day.
- Documentation that states an accuracy nobody measured. Every figure on the portal traces to a
  committed CSV; anything unmeasured says "not validated".
- Internal vocabulary reaching users: study arm names (A8, A9, A10), "rung", "ladder", paper version
  numbers, task numbers, incident anecdotes, dates of internal decisions.
- A DAG that hides state on a worker's disk, or that can write a half-published day.

## Decisions taken with the owner

- Merged table shape: measurements as columns on the event row; flight columns repeated; flights by start day.
- Warehouse `eurocontrol/opdi`, worldwide by default, easily changed; space is made before the run.
- First backfill 2026 back to 2025-01-01, newest first; daily job processes the next day; easy to extend to 2019.
- Study scripts stay in the `opdi` repository, tidied and separate from the package.
- Airflow 2.7.2 exists on the OpenSky side.
- Validation notes: one identical structure, with error distribution figures and measures.
- The package keeps today's default plus the v0.0.2 reproduction; intermediate variants go to the study scripts.
- **The new release is called v2.0.0.**
- Community page hidden for now.

Approved by the owner on 2026-11-11 with the v2.0.0 naming and the indexing additions.

## My calls (open to correction)

- **What "previous status quo" means (confirmed by the owner).** Only the v0.0.2 line was ever published. The package keeps
  today's default behaviour and the presets that reproduce v0.0.2 (`EventConfig.legacy()`,
  `DetectionConfig.legacy()`, `segmentation.method="legacy"`, `decimation="modulo"`, flight list from raw
  tracks). The intermediate variants that were defaults only during development (earlier track rules,
  intermediate event configurations, the six study rules, the `nearest` flight-list mode, the
  out-of-area option, the pandas stage stub) leave the package. Where a paper's regeneration script
  needs one, it moves to `benchmarks/` as study code that imports the package; the package never imports
  from `benchmarks/`.
- **ADS-C fusion** stays in the package behind its existing off-by-default flag: it is a documented
  optional source with a published study, not an idle arm.
- **Internal tables are dropped per day once published** unless the DAG is told to keep them
  (`keep_internal`), except the short window of tracks the next day's lookback needs. Bucket free space
  is checked before each day; under the floor the day fails early.
- **Versioning.** The release is `v2.0.0`: it names the published path and a `release` column on
  every published row. The per-algorithm version stays beside it as detail (`version` on the flight
  list and on each event; measurement versions documented per column), so a later release that changes
  one family is traceable. Internal version strings that were never published are simplified to match
  the release where that removes confusion; the v0.0.2 strings are untouched.
- **File format and naming.** Parquet, zstd, one file per day:
  `<warehouse>/published/v2.0.0/flight_list/flight_list_YYYYMMDD.parquet` and
  `.../flight_events/flight_events_YYYYMMDD.parquet`. Written to a temporary key and renamed, so a
  reader never sees a partial file. The dataset catalogue records each published day.
- **Views** are shipped as SQL for DuckDB (reads parquet over HTTPS/S3 directly) plus a small Python
  helper `opdi.published` with `read_flight_list`, `read_flight_events`, and `as_events()`,
  `as_measurements()` that return the classic shapes. Documented on the portal with copy-paste snippets
  for Python, R and DuckDB.
- **Portal working branch** is `feature/new-topics` itself (the owner's branch): fast-forwarded to the
  line that already contains it plus the newer papers, then merged with `development-2026` and
  `origin/main`'s configuration values. Pushed; no merge to `main`.
- **Code docstrings link to the portal** by stable anchors (`https://www.opdi.aero/methods/<page>#<id>`),
  one constant base URL in the package.

## Design

### 1. Published layout and the merged table (`opdi`)

New module `opdi/output/published.py`, step `05` (replacing the old local exporters, steps 05 and 06,
which cap at four months, write on the driver and overwrite versions).

- `build_flight_list(day)`: the day's `opdi_flight_list` rows, columns in lower snake case, typed, in
  a fixed documented order.
- `build_flight_events(day)`: `opdi_flight_events` for the day, left-joined to `opdi_measurements`
  pivoted to columns (one column per measurement type, snake case with unit suffix:
  `distance_flown_nm`, `time_passed_s`, `holding_duration_s`, `holding_distance_nm`,
  `holding_circuits`, `pms_leg_time_s`, `pms_leg_distance_nm`, `pms_direct_to_distance_nm`), joined to
  the flight list on the flight id for the flight columns (prefixed `flight_`? no: same names as in the
  flight list, since event columns do not collide once `id` is `event_id` and the flight's is
  `flight_id`). Column list is a single declared schema (`PUBLISHED_FLIGHT_LIST`,
  `PUBLISHED_FLIGHT_EVENTS`) with name, type, unit and description; the portal's data dictionary is
  generated from the same declaration so the two cannot drift.
- `publish_day(day)`: writes both files (single file each, temporary key then rename), then the
  manifest row. Idempotent; re-running replaces the day.
- **Indexing (owner's request).** Parquet has no index; the published files carry the equivalents
  readers use: (a) rows sorted (flight list by `adep`, `first_seen`; events by `flight_id`,
  `event_time`) with row groups of about 100,000 rows so min/max statistics prune; (b) bloom filters
  on `flight_id`, `icao24`, `flt_id`, `adep`, `ades` and event `type`; (c) a monthly flight index
  `.../index/flight_index_YYYYMM.parquet` (`flight_id, icao24, flt_id, adep, ades, first_seen,
  last_seen, dof, file`) rebuilt when a day of that month is published; (d) a dataset catalogue
  `.../catalogue.parquet`, one row per published day (row counts, time span, versions, file sizes,
  pipeline commit), which replaces the per-day manifest; (e) an `h3_res_5` column on every event for
  regional filtering.
- `opdi/published.py` (reader side, no Spark): pandas/pyarrow/duckdb helpers and the view SQL.
- The internal `opdi_flight_events` and `opdi_measurements` tables remain the pipeline's working
  tables (step 04 is unchanged); they are not published.

### 2. Production environment and configuration

- `OPDIConfig.for_environment("opensky")` gains an explicit, documented warehouse rule: the warehouse
  is `OPDI_WAREHOUSE` if set, else the default `s3a://eurocontrol/opdi`; coverage (worldwide by
  default) no longer rewrites the path. The coverage marker keeps refusing to mix coverages in one
  warehouse. `published_prefix` defaults to `<warehouse>/published/v2.0.0`.
- The hidden local progress logs (`OPDI_live/logs/*.parquet`) move into the warehouse (`_state/`), so
  any worker sees the same state.

### 3. Airflow DAG

`opdi/airflow/dags/opdi_pipeline.py` (plus `README.md`), Airflow 2.7.2, standard operators only.

- One DAG `opdi_daily`, `schedule="@daily"`, `catchup=False`, processing the logical date's *next
  completed* day (yesterday in UTC, i.e. "the next day" after the last processed one).
- One DAG `opdi_backfill`, manually triggered with params `start_date`, `end_date`, `order`
  (`newest_first` default), `warehouse`, `keep_internal`, `steps`, `max_active_days` (1 by default; the
  pipeline's lookback makes days sequential for step 02). It expands to one task group per day.
- Per day, tasks in order: `check_space` → `ingest` (01) → `tracks` (02) → `clean` (02a) →
  `flight_list` (03) → `events` (04) → `flight_list_milestones` (04b) → `publish` (05) →
  `prune_internal`. A `reference` task (step 00) runs once per DAG run and skips what exists.
- Each task calls `python -m opdi.periodrun --start D --days 1 --steps X ...` semantics through a
  thin `opdi.orchestration.run_unit(step, day, warehouse)` function (a PythonOperator if the Airflow
  workers have the package, otherwise the documented BashOperator form; both provided, chosen by one
  variable). Retries with backoff on the storage errors seen in practice; the S3 client timeouts
  raised.
- Unit tests load the DAG file with Airflow's `DagBag` when Airflow is importable, and otherwise test
  the pure functions that build the day list and task order.

### 4. Code clean-up (`opdi`)

Order matters; each stage ends with the full test suite and, for stages that touch pipeline code, the
one-day before/after comparison.

1. Tooling: `ruff` (E, F, W, I, plus D for docstrings with the Google convention, N) and `black`,
   line length 100 as configured; fix the two real bugs first (undefined name on one ingest path,
   missing import).
2. Remove idle code and move study variants (see "My calls"); delete root-level leftovers
   (`task6_*.py`, build logs, `publish_layouts.py`, stale `sql/`, the unused monitoring/export
   steps or rewrite them against the published layout), the stale `docs/industrialization-plan.md`.
3. Rewrite docstrings and comments: what the thing does, inputs, outputs, units, and the reason for a
   non-obvious choice in one sentence; no history. Config field docstrings state meaning, unit,
   default and effect.
4. Structure: split `config.py` (2,167 lines) into a `config/` package by concern, split
   `events.py` into the orchestrating processor and the detector modules it already mostly has, one
   place for shared geometry (haversine, bearing, wrap) instead of copies, a single `versions.py`
   for every version string. Public import paths keep working through re-exports.
5. `benchmarks/`: a README explaining it is study code, a `study_variants/` module for the moved
   arms, tests for study code under `tests/benchmarks/`, package tests under `tests/`.
6. Regenerate Sphinx API docs skeleton or drop it in favour of the portal (my call: keep a minimal
   API reference, generated, linked from the portal's Resources page).

### 5. Portal

Site map after the work (sidebar):

- Home
- **Data**: Overview and download (daily files, how to read them: Python, R, DuckDB; views); Data
  dictionary (flight list columns; flight events columns; event types; measurement columns; `info`
  fields per event type; version strings); Data preview; Releases and change log; Known limitations
  (index of every algorithm's limitations).
- **Methods**: Pipeline overview (ingest → tracks → cleaning → flight list → events → milestones →
  publishing, with a diagram); one page per algorithm family, all with the same template:
  1. What it is  2. How it is computed  3. Reference data  4. Validation method  5. Results (table of
  measures)  6. Error distribution (figure)  7. Headline accuracy  8. Limitations  9. Full study (link).
  Families: track identity; trajectory cleaning; departure and destination airports; take-off and
  landing times; off-block and in-block times; runway milestones and go-arounds; airport surface
  events; vertical events (level segments, top of climb/descent, flight-level crossings, level-offs);
  distance rings; holding; point merge; FIR crossings; measurements (distance flown, time passed);
  first and last seen. Plus a **Validation summary** table: every event type, flight-list column and
  measurement with its headline accuracy, reference, and links to the note and its limitations.
- **PRC Data Challenges** (updated: 2026 edition on taxi-out time with its sourced facts; totals
  recomputed or left out where unconfirmed; wording of the 2025 edition corrected to what the
  challenge states).
- **OPDI Sensors** (unchanged content).
- Resources (all method studies listed, software, API reference); Roadmap (rewritten to what is
  current); About.
- Community: page kept in the repository, removed from the sidebar, the home page card and
  cross-links.

Figures: `opdi-portal/tools/draw_validation_figures.py` (matplotlib, run from a scratch environment)
reads the committed study CSVs under `papers/*/data` and writes `assets/validation/*.svg`; a
`validation_measures.csv` holds every number the notes quote, with its source file, and a check
script fails if a note quotes a number not in it.

The home page, `R/portal_config.R` (version, labels, counts merged from `origin/main`'s newer values,
"July" spelling) and every "v0.0.2" statement are brought up to date; the old monthly and ten-day
download pages are replaced by the daily layout, with the v0.0.2 archive linked as the previous release.

## Constraints

- Native Spark only in the pipeline; no new runtime dependency on executors. Airflow is an optional
  extra (`opdi[airflow]`), never imported by the package core.
- Config thresholds keep aviation units with the unit in the field name.
- Published version strings are never changed; the v0.0.2 reproduction keeps working (tests pin it).
- The pod has a 16 GB memory limit: one heavy Spark job at a time; long runs detached.
- Bucket: 200 GB quota; this work writes only a one-day verification under a scratch prefix and
  deletes it, unless the owner has made space.
- No Claude attribution in commits or pull requests. No Quarto or R installs. No reportlab in project
  dependencies. `opdi-portal` is not merged to `main`.

## Order of work

1. Published layout + reader + views (small, defines the schemas the portal documents).
2. Production configuration and state in the warehouse; Airflow DAGs.
3. Code clean-up in stages (lint → removal → docstrings → structure), verified each stage.
4. Portal: branch merge, data dictionary generated from the schemas, methods pages and validation
   notes with figures, PRC update, hide community, stale content.
5. One-day production verification through the DAG's unit functions; final review; pull requests.

## Left to the builder

Exact module names inside the `config/` package, the figure style, the wording of each page, the
retry counts, and which study scripts need which moved variant (found by running the study tests).

## Answers to the builder check

Defaults the plan is built on; the first four await the owner's confirmation and each changes one task.

1. **The v0.0.2 monthly release path stays.** The `dev`/`live` environments (Azure, Iceberg, Hive) and
   the old export steps keep working with unchanged behaviour; they are tidied cosmetically and
   documented as the v0.0.2 release path. The new publishing step gets its own step id (`09`).
2. **Download location is one setting.** `PUBLISHED_BASE_URL` in `opdi.published` and one value in the
   portal's `R/portal_config.R`; default placeholder
   `https://www.eurocontrol.int/performance/data/download/OPDI/v2.0.0/`, with the OpenSky S3 path
   documented as the alternative.
3. **Production root is a clean prefix.** Nothing existing is deleted or moved. With the warehouse
   `s3a://eurocontrol/opdi`, the release root is `<warehouse>/v2.0.0/` (internal tables, `_state/`,
   `published/`, its own coverage marker). `opdi-prod` and `opdi-world` are left for the owner.
   `OPDI_WAREHOUSE` selects the warehouse for every coverage; the environment variable a pod may still
   export is reported at start-up so a wrong target is visible before anything is written.
4. **Airflow tasks are shell commands.** BashOperator running `python -m opdi.orchestration ...` behind a
   configurable command prefix (virtualenv, container, or a hop to the pod that hosts the Spark
   driver); credentials from the environment or an Airflow Connection. Only core Airflow 2.7.2 operators.
5. **Daily lag** is two days by default (`lag_days`), because a day needs the following day's data.
6. **Classic views** reproduce the v0.0.2 file schemas (upper-case flight-list names, `id`,
   `event_id`, measurement `type` strings); measurement ids are rebuilt as the event id plus the
   documented suffix and measurement versions come from the declared schema.
7. **Portal "current release"** is one switch in `portal_config.R` (`current_release`): v2.0.0 is
   presented as the new release, v0.0.2 stays downloadable, and the switch flips when the backfill is done.
8. **2026 challenge facts** come from the sourced research notes; unconfirmed totals are left out.

## Owner's answers to the four open points (2026-11-11)

1. The v0.0.2 monthly release is **no longer produced**. The old export steps (05, 06), the monitoring
   steps tied to them, and the Azure `dev`/`live` environments leave the package. The v0.0.2
   *algorithms* stay reproducible through the presets. Supersedes answer 1 above.
2. Published data is accessed through **Trino** (the OpenSky Trino instance, and a Trino file browser
   whose URL is to be defined). The virtual views are Trino definitions: external tables over the
   published folders and views for the classic shapes. The Python/DuckDB reader remains as the route
   for downloaded files; its base URL is a placeholder setting. Supersedes answer 2 above.
3. `eurocontrol/opdi`, `opdi-prod` and `opdi-world` may be deleted by the owner once nothing needed
   remains there; the work ends with the exact commands and a list of what was checked.
4. The Airflow workers' environment is unknown; the configurable command prefix stays and the
   requirements are documented.
