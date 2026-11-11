# OPDI v2.0.0 Publishing and Orchestration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Publish one flight-list file and one merged flight-events file per day under a single configurable release root, with indexes, reader views, and Airflow 2.7.2 DAGs for the daily run and backfills.

**Architecture:** A declared schema (`opdi/output/schema.py`) is the single source for the published columns. A Spark publishing step (`opdi/output/published.py`, step `09`) builds both daily tables from the pipeline's working tables and writes them atomically with sorting, bloom filters, a monthly flight index and a catalogue. A Spark-free reader (`opdi/published.py`) returns the tables and the classic v0.0.2 shapes. `opdi/orchestration.py` exposes one (step, day) unit as a CLI; the DAG file only chains those commands.

**Tech Stack:** PySpark 4, pyarrow, duckdb (reader extra), Airflow 2.7.2 core operators, pytest.

**Spec:** `docs/superpowers/specs/2026-11-11-industrialisation-design.md` (meta repo, branch `industrialisation`).

**Worktree:** `/home/jupyter/work/opdi-workspace/opdi/.claude/worktrees/industrialisation` (branch `industrialisation`). Tests: `.venv310/bin/python -m pytest`. The pod has a 16 GB memory limit: run one Spark test session at a time; long runs detached (`setsid nohup script`).

## Global Constraints

- Release name `v2.0.0`. Release root `<warehouse>/v2.0.0/`; published area `<release root>/published/`; file names `flight_list/flight_list_YYYYMMDD.parquet`, `flight_events/flight_events_YYYYMMDD.parquet`, `index/flight_index_YYYYMM.parquet`, `catalogue.parquet`.
- Default warehouse `s3a://eurocontrol/opdi`, worldwide; one setting changes it (`OPDI_WAREHOUSE`, config field, DAG param). Nothing existing in the bucket is deleted or moved.
- A day's files hold the flights that started that day (`dof`).
- Published columns: lower snake case, unit suffix on numeric measures (`_nm`, `_s`, `_ft`), `release` = `"v2.0.0"` on every row, per-algorithm `version` kept.
- Step 04 and the working tables are unchanged. The old export steps 05/06 are removed in plan B; the publishing step is `09`.
- Published data is served through Trino: table and view definitions ship in `src/opdi/sql/trino/`.
- Native Spark in the pipeline; no new executor dependency. Airflow is optional (`opdi[airflow]`) and never imported by the package core. Only Airflow 2.7.2 core operators.
- No Claude attribution in commits. Docstrings in plain language for OPDI users, no development history.

## Review Focus

1. A day with zero flights, or a flight with zero events: both files are still written (empty, with schema) and the catalogue row says 0.
2. An event type with no measurement of a given kind: the measurement column is NULL, never 0.
3. Re-publishing a day replaces its two files, its catalogue row and its index rows exactly once (no duplicates).
4. A flight whose events run past midnight: all its events are in the start day's file.
5. A reader with only pyarrow (no duckdb): `read_flight_list` and `read_flight_events` still work; the classic views raise a clear message naming the missing extra.

---

### Task 1: The declared schema

**Files:** Create `src/opdi/output/schema.py`, `tests/test_published_schema.py`.

**Interfaces — Produces:**
- `@dataclass(frozen=True) class Column: name: str; dtype: str; unit: str; description: str; source: str; group: str` (`dtype` a Spark SQL type string; `source` the working-table column or expression name; `group` one of `identity, aircraft, airports, times, milestones, runway, rings, quality, event, position, measurement, release`).
- `RELEASE = "v2.0.0"`.
- `FLIGHT_LIST: tuple[Column, ...]` — every `opdi_flight_list` column in lower snake case in this order: `flight_id` (from `ID`), `icao24`, `flt_id`, `dof`, `adep`, `ades`, `adep_p`, `ades_p`, `adep_source`, `ades_source`, `registration`, `model`, `typecode`, `icao_aircraft_class`, `icao_operator`, `first_seen`, `last_seen`, `atot`, `aldt`, `aobt`, `aibt`, `rwy_dep`, `rwy_dep_bearing_deg`, `rwy_arr`, `rwy_arr_bearing_deg`, `stnd_dep`, `stnd_arr`, `c40_arr` … `c120_arr`, `c40_dep` … `c120_dep` (radii 40, 50, 60, 100, 110, 120), `track_duration_min`, `superseded_dep`, `superseded_arr`, `movement_dep`, `movement_arr`, `version`, `release`.
- `EVENT_COLUMNS` (`event_id`, `flight_id`, `type`, `event_time`, `longitude`, `latitude`, `altitude_ft`, `h3_res_5`, `source`, `version`, `info`), `MEASUREMENT_COLUMNS` (`distance_flown_nm` ← "Distance flown (NM)", `time_passed_s` ← "Time Passed (s)", `holding_duration_s`, `holding_distance_nm`, `holding_circuits`, `pms_leg_time_s`, `pms_leg_distance_nm`, `pms_direct_to_distance_nm`, each with `source` = the measurement `type` string), and `FLIGHT_EVENTS = EVENT_COLUMNS + MEASUREMENT_COLUMNS + (the flight-list columns except flight_id, version, release, each unchanged in name) + (flight_version, release)`.
- `MEASUREMENT_SUFFIX: dict[str, str]` measurement type → id suffix (`_d`, `_t`, and `events.measurement_suffix(type)` for the others) and `MEASUREMENT_VERSION: dict[str, str]`.
- `as_markdown(columns) -> str` (table: name, type, unit, description) and `as_csv(columns) -> str`, used by the portal.

- [ ] Step 1: tests — names are unique and snake case; every `FLIGHT_LIST.source` exists in `flights` DDL column list plus `flight_list_milestones.ADDED_COLUMNS`; every measurement `source` is in `events.EXTRA_MEASURES` types or the two standard types; every description is a non-empty sentence without the words `ladder`, `rung`, `study`, `A8`, `A9`, `A10`; `as_markdown(FLIGHT_LIST)` has `len(FLIGHT_LIST) + 2` lines.
- [ ] Step 2: run, expect import failure. Step 3: implement. Step 4: pass. Step 5: commit `output: declared schema of the published tables`.

### Task 2: Building and publishing a day

**Files:** Create `src/opdi/output/published.py`, `tests/test_published_build.py`; modify `src/opdi/runner.py` (register step `09`), `src/opdi/periodrun.py` (`run_day_step` branch for `09`, add to optional day steps).

**Interfaces — Consumes** Task 1. **Produces:**
- `build_flight_list(flight_list: DataFrame) -> DataFrame` (rename/cast/order per `FLIGHT_LIST`, add `release`).
- `build_flight_events(events: DataFrame, measurements: DataFrame, flight_list: DataFrame) -> DataFrame` (pivot measurements by `type` to the declared columns with `first(value)`; left join to events on `milestone_id == id`; left join flights on `flight_id`; `h3_res_5` via the native parent expression of the event position's res-7 cell is not available on events, so compute with the same UDF `tracks._geo_to_h3_cell(lat, lon, 5)`; column order per `FLIGHT_EVENTS`).
- `PublishedLayout(root: str)` with `.flight_list(day)`, `.flight_events(day)`, `.flight_index(month)`, `.catalogue` paths.
- `publish_day(spark, storage, day: date, layout: PublishedLayout, commit: str) -> dict` — reads the day's working tables (`dof == day`), builds both frames, writes each as ONE parquet file: `sortWithinPartitions` after `coalesce(1)`, flight list by (`adep`, `first_seen`), events by (`flight_id`, `event_time`); options `compression=zstd`, `parquet.block.size` such that row groups are about 100,000 rows (`spark.sql.files.maxRecordsPerFile` unset; `parquet.page.row.count.limit` default), `parquet.bloom.filter.enabled#<col>=true` for `flight_id, icao24, flt_id, adep, ades` (+ `type` for events); written to `<final>.tmp/` then the single part file is renamed to the final key and the tmp dir removed (Hadoop FileSystem API via `spark._jvm`); then `update_index(...)` and `update_catalogue(...)`; returns counts.
- `update_index(spark, layout, month)`: rebuilds the month's index from that month's published flight-list files (columns `flight_id, icao24, flt_id, adep, ades, first_seen, last_seen, dof, file`).
- `update_catalogue(spark, layout, day, counts, commit)`: one row per day (`day, flights, events, first_event, last_event, flight_list_bytes, flight_events_bytes, release, flight_list_version, event_versions (array), commit, published_at`), replacing an existing row for that day.

- [ ] Step 1: tests with the local `spark` fixture and `tmp_path` as layout root, on hand-built working tables (2 flights, 5 events incl. one after midnight, measurements for some): columns equal the declared names in order; the post-midnight event is in the start day's frame; an event without a holding measurement has NULL `holding_duration_s`; `publish_day` leaves exactly one `.parquet` at each final path and no `.tmp`; publishing twice leaves one catalogue row and no duplicate index rows; a day with no flights writes two empty files with the schema and a catalogue row with zeros; pyarrow metadata of the written events file shows sorted `flight_id` across row groups and a bloom filter on `flight_id`.
- [ ] Steps 2-4: red, implement, green. Step 5: register step `09` ("Publish the day's files") in `runner.STEPS` and `periodrun.run_day_step`; test `test_periodrun.py` addition: `"09"` is an optional day step. Commit `output: publish one flight list and one merged events file per day`.

### Task 3: Release root and warehouse setting

**Files:** Modify `src/opdi/config.py` (`OPDIConfig.for_environment`, `_worldwide_warehouse`), `src/opdi/periodrun.py` (`DEFAULT_WAREHOUSE`, `_resolve_warehouse`), `src/opdi/utils/storage.py` (state location); tests `tests/test_config_release.py`, updates in `tests/test_periodrun.py`, `tests/test_coverage*.py`.

**Interfaces — Produces:** `ProjectConfig.release: str = "v2.0.0"`; `ProjectConfig.release_root` property = `f"{warehouse_path}/{release}"` under `opensky`, `warehouse_path` elsewhere; `StorageManager` resolves every table under `release_root`; `PublishedLayout(f"{release_root}/published")`; `OPDIConfig.for_environment("opensky", warehouse: Optional[str] = None)` → warehouse = argument, else `OPDI_WAREHOUSE`, else `s3a://eurocontrol/opdi`, for every coverage (the `-world` suffix rule and `OPDI_WAREHOUSE_WORLD` go); a start-up line prints the resolved warehouse, release root and coverage; ETL progress logs are read/written under `<release_root>/_state/` in S3 mode (local directory otherwise).

- [ ] Step 1: tests — default opensky root is `s3a://eurocontrol/opdi/v2.0.0`; `OPDI_WAREHOUSE=s3a://x/y` gives `s3a://x/y/v2.0.0` under worldwide and Europe alike; `dev`/`live`/`local` paths unchanged; the coverage marker still refuses a mismatch; a second `StorageManager` on the same root sees a progress mark written by the first (tmp dir standing in for S3).
- [ ] Steps 2-5: red, implement, green, update the tests that pinned `opdi-world`/`opdi-prod`, commit `config: one warehouse setting and a release root`.

### Task 4: Reader and views

**Files:** Create `src/opdi/published.py`, `src/opdi/sql/views.sql` (DuckDB), `src/opdi/sql/trino/{tables.sql,views.sql}`, `src/opdi/sql/render.py`, `tests/test_published_reader.py`, `tests/test_trino_sql.py`; `pyproject.toml` extra `reader = ["pyarrow", "duckdb", "pandas"]`.

**Interfaces — Produces:** `PUBLISHED_BASE_URL = "https://www.eurocontrol.int/performance/data/download/OPDI/v2.0.0/"` (overridable by `OPDI_PUBLISHED_URL` or the `base` argument); `file_url(table, day, base=None) -> str`; `read_flight_list(start, end=None, base=None, columns=None) -> pandas.DataFrame`; `read_flight_events(start, end=None, base=None, columns=None, types=None)`; `as_events(df) -> DataFrame` (classic `id, flight_id, type, event_time, longitude, latitude, altitude, source, version, info`); `as_measurements(df) -> DataFrame` (classic long `id, event_id, type, value, version`, ids = event id + `MEASUREMENT_SUFFIX`, NULL values dropped); `as_flight_list_v0(df)` (upper-case v0.0.2 names); `duckdb_views(base=None, start=None, end=None) -> str` returning the SQL of `views.sql` with the file list substituted (`opdi_flight_list`, `opdi_flight_events_merged`, `opdi_flight_events`, `opdi_measurements`). No Spark import in this module.
- Trino: `sql.render.trino_tables(catalog, schema, location) -> str` generates `CREATE TABLE IF NOT EXISTS <catalog>.<schema>.flight_list (...) WITH (external_location = '<location>/flight_list', format = 'PARQUET')` and the same for `flight_events` and `flight_index`, with every column, type and `COMMENT` taken from Task 1's schema; `sql.render.trino_views(catalog, schema) -> str` generates `CREATE OR REPLACE VIEW` for `events` (classic ten columns), `measurements` (classic long shape by `UNNEST` of the measurement columns, ids rebuilt with the declared suffixes, NULLs dropped) and `flight_list_v0` (upper-case names). The checked-in `.sql` files are the rendered output for `catalog=minio, schema=opdi, location=s3a://eurocontrol/opdi/v2.0.0/published`; `tests/test_trino_sql.py` asserts they equal a fresh render, that every schema column appears with its comment, and (with `sqlglot` if importable, else skipped) that the statements parse in the Trino dialect.

- [ ] Step 1: tests against files written by Task 2's `publish_day` into `tmp_path` (base = `file://` path): round trip row counts; `as_measurements` rows equal the non-NULL measurement cells and ids end with the declared suffix; `as_events` has exactly the ten classic columns; `duckdb_views` executes in duckdb and `select count(*) from opdi_measurements` equals the pandas result; with `duckdb` import blocked, readers work and `duckdb_views` raises `ImportError` naming `opdi[reader]`.
- [ ] Steps 2-5: red, implement, green, commit `published: reader and classic views`.

### Task 5: Orchestration unit and Airflow DAGs

**Files:** Create `src/opdi/orchestration.py`, `airflow/dags/opdi_pipeline.py`, `airflow/README.md`, `tests/test_orchestration.py`, `tests/test_airflow_dag.py`; `pyproject.toml` extra `airflow = ["apache-airflow==2.7.2"]` (not installed here).

**Interfaces — Produces:**
- `orchestration.DAY_STEPS = ("01", "02", "02a", "03", "04", "04b", "09")`; `days(start: date, end: date, order="newest_first") -> list[date]`; `run_unit(step: str, day: Optional[date], warehouse: Optional[str], worldwide=True) -> int` (one step for one day through `periodrun.run_day_step` with its session handling, or step `00` when `day is None`); `check_space(min_free_gb: float) -> None` (raises under the floor); `prune_internal(day, keep_days: int)` (drops `osn_statevectors_v2`, `osn_tracks`, `osn_tracks_clean` partitions older than the lookback window needs, never anything under `published/`); CLI `python -m opdi.orchestration {reference|step|day|check-space|prune} --day YYYY-MM-DD --step 02 --warehouse ...` with exit code 0/1.
- DAG file: constants read from Airflow Variables with defaults (`opdi_command_prefix` = `""`, `opdi_warehouse` = `""`, `opdi_min_free_gb` = `5`, `opdi_lag_days` = `2`, `opdi_keep_internal` = `false`); `opdi_daily` (`schedule="0 6 * * *"`, `catchup=False`, `max_active_runs=1`, day = `data_interval_end - lag_days`); `opdi_backfill` (`schedule=None`, params `start_date`, `end_date`, `order`, `warehouse`, `keep_internal`): tasks `reference`, `plan_days` (pushes the ordered day list) and `process_day`, a BashOperator mapped over that list with `max_active_tis_per_dag=1`, each running `python -m opdi.orchestration day --day D` (all day steps in order, resumable through the warehouse state, publishing last). The daily DAG keeps one task per step for visibility. Task order per day: `check_space >> ingest >> tracks >> clean >> flight_list >> events >> milestones >> publish >> prune`; `reference` first. `retries=2`, `retry_delay=10 min`, `execution_timeout=6 h` per task. Doc strings (`doc_md`) explain each DAG and how to backfill to 2019.

- [ ] Step 1: tests — `days()` ordering and inclusivity; `run_unit` rejects an unknown step; CLI parses and returns 1 on a raised step (monkeypatched); `prune_internal` never targets a path containing `/published/`; DAG test: `pytest.importorskip("airflow")`, else a static test parses `opdi_pipeline.py` with `ast` and asserts the two `dag_id`s, the daily task ids in order and that only `BashOperator`/`PythonOperator`/`EmptyOperator` from `airflow.operators` are imported.
- [ ] Steps 2-5: red, implement, green, write `airflow/README.md` (what the workers need, variables, daily run, backfill 2026→2025, extending to 2019, changing the warehouse, recovering a failed day), commit `airflow: daily and backfill DAGs over the per-day unit`.

### Task 6: One-day verification on the cluster

- [ ] Step 1: with `OPDI_WAREHOUSE=s3a://eurocontrol/opdi/research/industrialisation_check` (scratch; delete after), run `python -m opdi.orchestration step --step 09 --day 2026-06-14` reading the working tables of the existing study prefix through a one-off copy of that day's `opdi_flight_list`, `opdi_flight_events`, `opdi_measurements` (about 1.5 GB). Detached, one Spark job at a time.
- [ ] Step 2: read both files back with `opdi.published` from the pod; record row counts, file sizes, row-group count and a `flight_id` point query's row groups read; compare events count with the working table.
- [ ] Step 3: delete the scratch prefix; write the measured sizes into `airflow/README.md` ("what a day costs"). Commit.
