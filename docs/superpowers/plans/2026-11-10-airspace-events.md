# Holding, Point Merge and FIR Crossing Events Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Three new flight-event families (`holding-start`/`holding-end`, `pms-leg-entry`/`pms-leg-exit`/`pms-merge-point`, `xing-fir`) with their measurements, computed natively in step 04, measured on real data and written up.

**Architecture:** One module per family under `src/opdi/pipeline/` (`holding.py`, `point_merge.py`, `fir.py`), each a pure function from the step 04 input frame to the nine-column milestone frame plus a `version` column; step 04 stamps `coalesce(version, events_version)` and turns listed `info` fields into measurement rows. Step 00c is rewritten to produce a compact, worldwide H3 FIR reference. Validation is offline scripts under `benchmarks/` writing provenance-stamped CSVs that a markdown paper is built from.

**Tech Stack:** PySpark column/window expressions, h3 v4 (driver side only, step 00c), shapely (step 00c), pandas (driver-side reference loading and offline scoring), pytest with the local `spark` fixture.

**Spec:** `docs/superpowers/plans/../specs/2026-11-10-airspace-events-design.md` in this meta repo (`/home/jupyter/work/opdi-workspace/.claude/worktrees/airspace-events/docs/superpowers/specs/2026-11-10-airspace-events-design.md`).

**Repos and worktrees:** code in `/home/jupyter/work/opdi-workspace/opdi/.claude/worktrees/airspace-events` (branch `airspace-events-v1`, from `main` = v5 tip d24bb36); paper in `/home/jupyter/work/opdi-workspace/opdi-portal/.claude/worktrees/airspace-events` (branch `airspace-events-v1`); spec, plan and CLAUDE.md in `/home/jupyter/work/opdi-workspace/.claude/worktrees/airspace-events` (branch `airspace-events-v1`). Tests run with `.venv310/bin/python -m pytest` from the opdi worktree (the venv is a symlink target shared with the main checkout: use `/home/jupyter/work/opdi-workspace/opdi/.venv310/bin/python`). Research notes: `/home/jupyter/.claude/jobs/616d5918/tmp/research/`.

## Global Constraints

- Native Spark only: column expressions and window functions partitioned by `track_id` and, where samples are paired, by `("track_id", PIECE)` from `opdi.pipeline.holes`. No `applyInPandas`, no `pandas_udf`, no new runtime dependency.
- Every numeric `EventConfig` field ends in `_nm _ft _kt _ftmin _seconds _pct _deg` (`tests/test_event_config.py::test_every_measurement_carries_its_unit`); bools and strings are exempt. Comparisons against SI columns scale at the point of use with `* 3.28084` (m to ft), `* 196.850394` (m/s to ft/min), `* 1.94384` (m/s to kt).
- Published versions are never mutated: `events_v0.3.0` and the ids of existing types stay as they are. New families carry `holding_v0.1.0`, `pms_v0.1.0`, `fir_v0.1.0`.
- New behaviour is off under `EventConfig.legacy()`; `V4_BASE` in `benchmarks/event_bench.py` gets each new flag at its off value plus one rung each; `tests/test_event_bench_guard.py` pins `len(V4_BASE)` (10 today, 13 after).
- Cluster work: one day (2026-06-14). Inputs read from `s3a://eurocontrol/opdi-world` (full day, read only) or the v5 arm `s3a://eurocontrol/opdi-world/research/a10/cruise_linked` (read only). All writes under `s3a://eurocontrol/opdi-world/research/airspace_events/`. Bucket `eurocontrol`: stop under 3 GB free (8.85 GB free at plan time), delete only tables under this prefix. One Spark driver at a time.
- No Claude attribution in commits or PR bodies. No reportlab in project dependencies (the scratch venv at `/home/jupyter/.claude/jobs/616d5918/tmp/pdfenv/venv` has it for `--pdf`). No Quarto or R installs.
- Commit inside each submodule worktree first; the meta repo gets the plan, spec and CLAUDE.md only (pointers are updated at the end, not per task).
- Plain-language writing in the paper and docstrings; thresholds named with the number that backs them.

## Review Focus

1. A track whose `heading` is NULL for a stretch (masked stale broadcasts) must not produce a spurious 180° turn or a NULL-poisoned window: the holding turn series carries the last known heading forward and a gap over 60 s breaks the series (Task 2 test `test_a_heading_gap_does_not_create_a_turn`).
2. A hole (silence over `track_hole_seconds`) inside a hold, a leg or a FIR transit: nothing pairs across it. Holding and point-merge windows are per piece; a FIR change across a hole is not a crossing (Task 2 `test_a_hold_split_by_a_hole_gives_two_episodes`, Task 4 `test_a_leg_is_not_paired_across_a_hole`, Task 6 `test_no_crossing_across_a_hole`).
3. A flight at FL350 over France is in `LFFFUIR`, at FL100 in `LFBBFIR`; a climb through FL195 is a `xing-fir` from the lower to the upper volume (Task 6 `test_a_climb_through_the_band_boundary_is_a_crossing`).
4. A cell claimed by two FIR volumes with overlapping bands (placeholder or sub-volume) must not duplicate the sample or emit a flicker (Task 5 `test_placeholder_rows_are_dropped` and `test_same_code_bands_are_merged`; Task 6 `test_a_double_match_keeps_the_narrower_band_once`).
5. A run without step 00c output or without the point-merge table for the flight's destination must complete and emit nothing for that family, not fail (Task 6 `test_missing_reference_table_skips_the_family`, Task 4 `test_a_destination_without_a_pms_yields_nothing`).

---

### Task 1: Per-family versions and extra measurements in step 04

**Files:**
- Modify: `src/opdi/pipeline/events.py:1642-1651` (stamping), `:1707-1748` (measurement write), `:106-110` (`TRACK_COLUMNS`)
- Test: `tests/test_events_processor.py`

**Interfaces:**
- Produces: `EXTRA_MEASURES: list[tuple[str, str, str, str]]` in `events.py` = `(event_type, info_field, measurement_type, measurement_version)`; `extra_measurement_rows(df_events, measurement_id) -> Optional[DataFrame]` with the `opdi_measurements` columns `id, milestone_id, type, value, version, dof`; the stamping rule `version = coalesce(col("version"), lit(events_version))` when a `version` column is present on the union.
- Produces: `TRACK_COLUMNS` contains `"h3_res_7"`.

- [ ] **Step 1: Write the failing tests** in `tests/test_events_processor.py`, using the existing `_run` helper (lines 373-404) with a stub detector frame injected through a monkeypatched `calculate_firstseen_lastseen_events` that returns one `first_seen` row with `version = "fam_v9.9.9"` and `info = '{"duration_s": 42.0}'`, and with `EXTRA_MEASURES` monkeypatched to `[("first_seen", "duration_s", "Test duration (s)", "fam_v9.9.9")]`:

```python
def test_a_detector_version_survives_stamping(spark, monkeypatch): ...
    # the written opdi_flight_events row of type first_seen has version "fam_v9.9.9";
    # a level-start row in the same run still has version EventConfig().events_version
def test_an_info_field_listed_in_extra_measures_becomes_a_measurement(spark, monkeypatch): ...
    # opdi_measurements has a row type "Test duration (s)", value 42.0, version "fam_v9.9.9",
    # milestone_id == the first_seen event id, id == milestone_id + "_m0"
def test_an_event_without_the_listed_field_gets_no_extra_row(spark, monkeypatch): ...
def test_track_columns_include_h3_res_7(): assert "h3_res_7" in TRACK_COLUMNS
```

- [ ] **Step 2: Run them**: `.venv310/bin/python -m pytest tests/test_events_processor.py -k "version_survives or extra_measures or without_the_listed or h3_res_7" -v`. Expected: 4 FAIL (`EXTRA_MEASURES` undefined, version overwritten, column missing).

- [ ] **Step 3: Implement.** In the stamping block: `if "version" not in df_events.columns: add lit(None).cast("string")` before the union is finished (make `add()` align frames with `unionByName(..., allowMissingColumns=True)` and give the union a NULL `version` when absent), then `withColumn("version", F.coalesce(col("version"), lit(self.events.events_version)))`. Add `EXTRA_MEASURES` (empty list for now; Tasks 2 and 4 fill it) and `extra_measurement_rows(df_events, measurement_id)` that, for entry `i`, filters `type == event_type` and `get_json_object(info, "$." + field).isNotNull()`, builds `value = get_json_object(...).cast("double")`, `id = measurement_id(f"_m{i}_")`, and unions all entries; the write block unions its result onto `df_measurements` when not None. Add `"h3_res_7"` to `TRACK_COLUMNS` (after `h3_res_12`).

- [ ] **Step 4: Run** the four tests and the whole file. Expected: PASS; `tests/test_events_processor.py` all green.

- [ ] **Step 5: Commit**: `git add src/opdi/pipeline/events.py tests/test_events_processor.py && git commit -m "events: per-family version stamping and info-backed extra measurements"`

### Task 2: Holding detector

**Files:**
- Create: `src/opdi/pipeline/holding.py`, `tests/test_holding.py`, `tests/navigation_fixtures.py`
- Modify: `src/opdi/config.py` (`EventConfig` fields), `src/opdi/pipeline/events.py` (`EXTRA_MEASURES`)

**Interfaces:**
- Produces: `calculate_holding_events(geo: DataFrame, config: EventConfig, go_arounds: Optional[DataFrame] = None) -> Optional[DataFrame]` returning the nine milestone columns plus `version = "holding_v0.1.0"` (`HOLDING_VERSION`). `geo` is the step 04 frame after `add_distance_measure`, `add_time_measure`, `add_piece` and `_with_aerodrome_geometry` (columns used: `track_id, piece, event_time, lat, lon, baro_altitude_c, velocity, heading, cumulative_distance_nm, cumulative_time_s, elev_ades_ft, elev_adep_ft`; the elevation columns are optional and treated as 0 when absent).
- Produces: `tests/navigation_fixtures.py::fly(spark, legs, *, speed_kt=250.0, alt_ft=10000.0, lat0=51.0, lon0=0.0, hdg0=90.0, t0=0.0, step_s=5.0, track_id="trk-1", callsign="TEST123") -> DataFrame` integrating `legs = [(duration_s, turn_rate_deg_s, vert_rate_ftmin), ...]` on a flat-earth step, returning `make_track` output with `baro_altitude_c`, `cumulative_distance_nm`, `cumulative_time_s`, `piece` (through `add_distance_measure`, `add_time_measure`, `add_piece`) and `elev_ades_ft = 0.0`, `elev_adep_ft = 0.0`. Also `racetrack(n_circuits, leg_s=60.0, turn_rate=1.5)` = `[(leg_s, 0, 0), (180/turn_rate, turn_rate, 0)] * (2 * n_circuits)`.
- Config fields (all in `EventConfig`, after `track_hole_seconds`): `emit_holding: bool = True`, `holding_window_seconds: float = 360.0`, `holding_max_sample_gap_seconds: float = 60.0`, `holding_turn_rate_spike_deg_s` is not a valid suffix: name it `holding_turn_rate_spike_deg: float = 30.0` (degrees per 5 s sample, i.e. 6°/s) and `holding_turn_rate_min_deg: float = 2.0` (0.4°/s per 5 s), both documented as "per sample"; instead compute the rate per second and compare to `holding_turn_rate_spike_deg / 5` ... **Decision:** keep rates per second and name the fields with `_deg` plus a `_seconds` normaliser: `holding_turn_rate_spike_deg: float = 6.0` and `holding_turn_rate_min_deg: float = 0.4`, docstrings saying "degrees per second". `holding_net_turn_min_deg: float = 300.0`, `holding_same_sense_min_pct: float = 80.0`, `holding_max_displacement_nm: float = 8.0`, `holding_min_path_nm: float = 15.0`, `holding_max_altitude_range_ft: float = 2500.0`, `holding_min_height_ft: float = 2500.0`, `holding_min_speed_kt: float = 140.0`, `holding_bridge_gap_seconds: float = 120.0`, `holding_min_duration_seconds: float = 240.0`, `holding_min_episode_turn_deg: float = 330.0`, `holding_orbit_max_straight_pct: float = 15.0`, `holding_orbit_max_turn_deg: float = 450.0`, `holding_goaround_exclusion_seconds: float = 180.0`.

- [ ] **Step 1: Write `tests/navigation_fixtures.py`** with `fly` and `racetrack` as specified (flat earth: `dlat = v*dt*cos(hdg)/60 NM per degree`, `dlon = v*dt*sin(hdg)/(60*cos(lat))`; speed stored as `velocity = speed_kt / 1.94384`, altitude as `baro_altitude = alt_ft / 3.28084`, `baro_altitude_c = baro_altitude`).

- [ ] **Step 2: Write the failing tests** in `tests/test_holding.py`:

```python
def test_two_racetrack_circuits_give_one_holding_pair(spark):
    sdf = fly(spark, [(300, 0, 0)] + racetrack(2) + [(300, 0, 0)])
    rows = calculate_holding_events(sdf, EventConfig()).collect()
    types = sorted(r.type for r in rows); assert types == ["holding-end", "holding-start"]
    info = json.loads(rows[0].info)
    assert info["pattern"] == "racetrack" and info["direction"] == "R"
    assert 1.5 <= info["circuits"] <= 2.5 and 400 <= info["duration_s"] <= 700
    assert all(r.version == "holding_v0.1.0" for r in rows)
    start = next(r for r in rows if r.type == "holding-start"); end = ...
    assert 240 <= end.cumulative_time_s - start.cumulative_time_s <= 700
def test_a_single_left_orbit_is_an_orbit(spark):
    sdf = fly(spark, [(300, 0, 0), (240, -1.5, 0), (300, 0, 0)])  # 360° left in 4 min
    info = json.loads(calculate_holding_events(sdf, EventConfig()).first().info)
    assert info["pattern"] == "orbit" and info["direction"] == "L"
def test_alternating_vectors_are_not_a_hold(spark):
    sdf = fly(spark, [(60, 0, 0), (80, 1.5, 0), (60, 0, 0), (80, -1.5, 0)] * 3)
    assert calculate_holding_events(sdf, EventConfig()).count() == 0
def test_a_low_slow_circuit_is_not_a_hold(spark):  # 1,000 ft, 100 kt
    sdf = fly(spark, racetrack(2), speed_kt=100.0, alt_ft=1000.0)
    assert calculate_holding_events(sdf, EventConfig()).count() == 0
def test_a_hold_near_a_go_around_is_dropped(spark):
    sdf = fly(spark, racetrack(2)); ga = spark.createDataFrame([("trk-1", "go-around", <start + 60 s>)], "track_id string, type string, event_time timestamp")
    assert calculate_holding_events(sdf, EventConfig(), go_arounds=ga).count() == 0
def test_a_heading_gap_does_not_create_a_turn(spark):
    # straight flight whose heading is NULL for 90 s, then a 200° turn: no episode
def test_a_hold_split_by_a_hole_gives_two_episodes(spark):
    # racetrack(2), 40 min silence, racetrack(2): two holding-start rows, each piece its own
def test_off_flag_returns_none(spark):
    assert calculate_holding_events(fly(spark, racetrack(2)), EventConfig(emit_holding=False)) is None
```

- [ ] **Step 3: Run**: `pytest tests/test_holding.py -v`. Expected: FAIL (module not found).

- [ ] **Step 4: Add the config fields** to `EventConfig` with docstrings carrying the backing numbers from the spec, `emit_holding=False` in `legacy()`, and assertions in `test_every_new_behaviour_is_off_under_legacy` / `test_the_shipped_new_behaviours_are_on` (`tests/test_event_config.py:56-105`). Run `pytest tests/test_event_config.py -v`; expected PASS.

- [ ] **Step 5: Implement `src/opdi/pipeline/holding.py`** per the spec's "Family 1" section: `_turn_series(df, config)` (last-known heading, wrapped delta, gap and spike nulling, `turning`), `_window_features(df, config, leading: bool)` (sum of `d_hdg`, sum of `|d_hdg|`, first/last lat-lon haversine in NM via the same formula `add_distance_measure` uses, path from `cumulative_distance_nm`, altitude range in ft), `_episodes(df, config)` (run-length encoding with lag/cumsum inside `Window.partitionBy("track_id", PIECE).orderBy("event_time")`, bridging, duration and turn filters, pattern/direction/circuits), then the two event rows per episode sharing the `info` JSON `{holding_seq, pattern, direction, circuits, duration_s, distance_nm, altitude_start_ft, altitude_end_ft, straight_pct}`; `HOLDING_VERSION = "holding_v0.1.0"`. Go-around exclusion: left anti-join of episodes against `go_arounds.filter(type == "go-around")` on `track_id` with `abs(unix(start) - unix(ga_time)) <= exclusion` or `abs(unix(end) - ...)`. Add to `EXTRA_MEASURES`: `("holding-end", "duration_s", "Holding duration (s)", HOLDING_VERSION)`, `("holding-end", "distance_nm", "Holding distance (NM)", HOLDING_VERSION)`, `("holding-end", "circuits", "Holding circuits", HOLDING_VERSION)`.

- [ ] **Step 6: Run** `pytest tests/test_holding.py tests/test_event_config.py -v`. Expected: all PASS.

- [ ] **Step 7: Commit**: `git add src/opdi/pipeline/holding.py src/opdi/config.py src/opdi/pipeline/events.py tests/ && git commit -m "events: holding-start/holding-end from signed cumulative turn"`

### Task 3: Point merge reference table

**Files:**
- Create: `src/opdi/reference/data/point_merge_systems.csv`, `src/opdi/reference/point_merge.py`, `tests/test_point_merge_ref.py`
- Modify: `pyproject.toml` or `setup.cfg` package data so the CSV ships in the wheel (check `[tool.setuptools.package-data]`; add `"opdi.reference": ["data/*.csv"]` if absent).

**Interfaces:**
- Produces: `load_point_merge_systems(spark, path: Optional[str] = None) -> DataFrame` with columns `apt_icao, pms_id, merge_point, lat, lon, legs_mp_min_nm, legs_mp_max_nm, leg_level_min_ft, leg_level_max_ft, valid_from (date), valid_to (date, NULL = open), source, confidence`, filtered to rows with coordinates (`confidence != "missing"`); and `point_merge_systems_for_day(spark, day: date) -> DataFrame` (valid rows on `day`). `PMS_CSV = files("opdi.reference") / "data" / "point_merge_systems.csv"`.

- [ ] **Step 1: Write the CSV** from `/home/jupyter/.claude/jobs/616d5918/tmp/research/point_merge_systems.csv` (the geocoded table produced by the research thread; copy it verbatim, then review every `high`/`medium` row's distance check in `point_merge_systems_notes.md`; a row the notes flag as failing the 7 to 45 NM check becomes `missing`). Add a `README.md` beside it stating the sources, the licence note from the spec, and the date of curation.

- [ ] **Step 2: Write the failing tests**:

```python
def test_every_catalogue_airport_has_a_row(): # 61 distinct apt_icao in the raw CSV
def test_placed_rows_have_coordinates_and_geometry(spark): # lat/lon/legs_mp_* non-null for confidence in (high, medium)
def test_rows_for_a_day_respect_validity(spark): # EDDP-like row with valid_to 2020-01-30 absent on 2026-06-14
def test_eidw_lapmo_is_about_12_nm_from_the_airport(spark): # haversine to 53.4213,-6.2701 within 9..15 NM
```

- [ ] **Step 3: Run**: `pytest tests/test_point_merge_ref.py -v`. Expected: FAIL (module missing).

- [ ] **Step 4: Implement `src/opdi/reference/point_merge.py`** (pandas read of the package CSV, `spark.createDataFrame`, explicit schema, date parsing, filters).

- [ ] **Step 5: Run** the tests. Expected: PASS.

- [ ] **Step 6: Commit**: `git add src/opdi/reference/ tests/test_point_merge_ref.py pyproject.toml && git commit -m "reference: curated point merge systems table"`

### Task 4: Point merge detector

**Files:**
- Create: `src/opdi/pipeline/point_merge.py`, `tests/test_point_merge.py`
- Modify: `src/opdi/config.py` (`EventConfig` fields), `src/opdi/pipeline/events.py` (`EXTRA_MEASURES`)

**Interfaces:**
- Consumes: `fly` (Task 2), `load_point_merge_systems` (Task 3).
- Produces: `calculate_point_merge_events(geo: DataFrame, pms_ref: DataFrame, config: EventConfig) -> Optional[DataFrame]`; `geo` must carry `ades` (from `_with_aerodrome_geometry`), `piece`, and the standard columns; `pms_ref` is the Task 3 frame. `PMS_VERSION = "pms_v0.1.0"`.
- Config fields: `emit_point_merge: bool = True`, `pms_candidate_radius_nm = 60.0`, `pms_candidate_max_fl` is not a valid suffix: use `pms_candidate_max_altitude_ft = 25000.0`, `pms_leg_window_seconds = 45.0`, `pms_leg_distance_band_nm = 1.5`, `pms_leg_distance_margin_nm = 2.0`, `pms_leg_min_bearing_sweep_deg = 8.0`, `pms_leg_tangential_min_pct = 80.0`, `pms_leg_min_duration_seconds = 90.0`, `pms_direct_lookahead_seconds = 600.0`, `pms_direct_align_deg = 5.0`, `pms_direct_min_duration_seconds = 60.0`, `pms_merge_point_radius_nm = 3.0`.

- [ ] **Step 1: Write the failing tests** with a synthetic merge point at (52.0, 0.0), ref row `apt_icao="XXXX"`, `legs_mp_min_nm=15`, `legs_mp_max_nm=17`, and tracks built with `fly` plus `.withColumn("ades", lit("XXXX"))`. An arc leg: start 16 NM due north of the point heading 090 (tangential, clockwise seen from the point), turn rate `-(speed_kt/16)/3600*180/π ... ` simplify: a constant turn rate `r = -(v_kt / 16.0) * (180/π) / 3600` deg/s keeps 16 NM distance; fly 180 s of it, then turn to the bearing to the point and fly direct 240 s (passes within 3 NM).

```python
def test_an_arc_followed_by_a_direct_gives_a_leg_pair_and_a_merge_point(spark): ...
    # types == {pms-leg-entry, pms-leg-exit, pms-merge-point}; info.leg_distance_nm within 15..17;
    # info.direction == "cw"; info.direct_to_distance_nm within 14..18; duration_s within 150..200;
    # version pms_v0.1.0 on all rows
def test_an_arc_without_a_direct_is_not_a_leg(spark): # arc then fly away tangentially: no rows
def test_a_direct_without_an_arc_gives_only_the_merge_point(spark): # one pms-merge-point, via_leg False
def test_a_destination_without_a_pms_yields_nothing(spark): # ades "YYYY"
def test_a_leg_is_not_paired_across_a_hole(spark): # arc 100 s, 40 min silence, arc 100 s, direct: zero leg pairs (each piece < 90 s? make each 80 s)
def test_off_flag_returns_none(spark)
```

- [ ] **Step 2: Run**: `pytest tests/test_point_merge.py -v`. Expected: FAIL.

- [ ] **Step 3: Add the config fields** (docstrings with the backing numbers), `emit_point_merge=False` in `legacy()`, assertions in the two config tests. Run `pytest tests/test_event_config.py`; PASS.

- [ ] **Step 4: Implement `src/opdi/pipeline/point_merge.py`** per the spec's "Family 2": candidate join (`geo.join(broadcast(pms_ref), geo.ades == pms_ref.apt_icao)` then radius/altitude filter), `d_nm` and `brg_deg` as column expressions (haversine; initial bearing), window `Window.partitionBy("track_id", PIECE, "merge_point").orderBy(epoch).rangeBetween(-W, W)` for the leg test, run-length encoding of `on_leg`, the direct-to lookahead with `rangeBetween(0, lookahead)` over the same partition (`min(d_nm)`, first aligned-run start), the three event types and `info` as in the spec; `EXTRA_MEASURES` entries `("pms-leg-exit", "duration_s", "Point merge leg time (s)", PMS_VERSION)`, `("pms-leg-exit", "distance_nm", "Point merge leg distance (NM)", PMS_VERSION)`, `("pms-leg-exit", "direct_to_distance_nm", "Point merge direct-to distance (NM)", PMS_VERSION)`.

- [ ] **Step 5: Run** `pytest tests/test_point_merge.py tests/test_event_config.py -v`. Expected: PASS.

- [ ] **Step 6: Commit**: `git commit -am "events: point merge leg and merge point detection against the curated table"`

### Task 5: Step 00c rewritten: compact, worldwide, correct

**Files:**
- Modify: `src/opdi/reference/h3_airspaces.py` (whole module), `src/opdi/runner.py:330-346` (`reference_substeps_for_coverage`), `src/opdi/config.py` (`ReferenceConfig.airspace_resolution` honoured), `sql/create_tables_*.sql` DDL for `opdi_h3_airspace_ref`
- Test: `tests/test_h3_airspaces.py` (new), `tests/test_periodrun.py:785` (flip the worldwide-off expectation)

**Interfaces:**
- Produces: table `opdi_h3_airspace_ref` with columns `airspace_type, code, name, airac_cfmu (string), validity_start (timestamp), validity_end (timestamp), min_fl (int), max_fl (int), area_km2 (double), h3_cell (string), h3_res (int)`; `AIRSPACE_SCHEMA` updated. Functions: `split_antimeridian(geom) -> shapely geometry` (a polygon whose ring has a longitude jump over 180° is shifted to [0, 360), cut at 180 and the eastern part shifted back), `dissolve_volumes(df: pd.DataFrame) -> pd.DataFrame` (drop `code == "XXXX"` and rows whose name contains "REROUTING EXTENSION"; union geometries of equal `(code, min_fl, max_fl)`; merge overlapping or touching bands of the same code into one row with the union geometry), `compact_cells(geometry_wkt, res) -> list[tuple[str, int]]` (polyfill at `res`, `compact_h3_set`, pair each cell with its resolution), `overlap_report(df_cells: pd.DataFrame) -> pd.DataFrame` (resolution-`res` cells claimed by two codes with overlapping bands, before compaction; printed and returned). `AirspaceH3Generator.process_fir()` writes the layer in one `mode="overwrite"` call per layer with `partition_by=["airspace_type"]` (so layers do not clobber each other); `process_all()` keeps ANSP and countries.

- [ ] **Step 1: Write the failing tests** (pure functions, no Spark except one small end-to-end on a two-polygon frame with a `StubStorage` capturing writes):

```python
def test_an_antimeridian_polygon_fills_the_right_side(): # ring 170..-170 lon: cells' centres all have |lon| >= 170
def test_placeholder_rows_are_dropped(): # XXXX and "REROUTING EXTENSION" gone
def test_same_code_bands_are_merged(): # OBBBUIR 150-245 + 245-999 -> one row 150-999
def test_sub_volumes_with_one_code_dissolve(): # two CZQXFIR 0-999 rows -> one row
def test_compact_cells_carry_their_resolution(): # a 2°x2° box at res 7: some cells res < 7, all parents of res-7 fill
def test_overlap_report_names_the_pairs(): # two different codes, same box, bands 0-999 and 0-195 -> one pair row
def test_the_layer_is_written_once(spark): # stub storage sees exactly one write_table call for FIR with both polygons' rows
def test_worldwide_runs_00c(): # reference_substeps_for_coverage(worldwide cfg)["00c"] is True
```

- [ ] **Step 2: Run**: `pytest tests/test_h3_airspaces.py -v`. Expected: FAIL.

- [ ] **Step 3: Implement** the functions above and rewrite `process_fir/process_ansp/process_countries` to build one pandas frame per layer (rows `(…, h3_cell, h3_res)`), print the overlap report for FIR, and write once. Replace the `h3_res_7*` columns and the DDL. Flip `reference_substeps_for_coverage` so 00c runs worldwide (update its docstring: the FIR layer is worldwide; ANSP is European and simply absent elsewhere) and the test at `tests/test_periodrun.py:785`. Use `config.reference.airspace_resolution` as `res`.

- [ ] **Step 4: Run** `pytest tests/test_h3_airspaces.py tests/test_periodrun.py tests/test_h3_zones_v4.py -v`. Expected: PASS.

- [ ] **Step 5: Commit**: `git commit -am "reference: step 00c writes a compact worldwide FIR layer, antimeridian split, placeholders dropped"`

### Task 6: FIR crossing detector

**Files:**
- Create: `src/opdi/pipeline/fir.py`, `tests/test_fir.py`
- Modify: `src/opdi/config.py` (`emit_fir_crossings: bool = True`, `fir_min_dwell_seconds: float = 60.0`)

**Interfaces:**
- Consumes: Task 5 table columns; Task 2 `fly`.
- Produces: `h3_parent_expr(cell_col, res: int) -> Column` (native: `conv(cell, 16, 10).cast("long")`, clear bits 52-55 and set them to `res`, OR the mask `(1 << (3 * (15 - res))) - 1`, back to lower-case hex with `lower(conv(x, 10, 16))`); `attach_fir(df, fir_ref: DataFrame, config) -> DataFrame` adding `fir_code, fir_band_width, fir_area_km2, airac_cfmu, validity_start, validity_end` (sequential broadcast left joins for `res` 7 down to 1 on `h3_parent_expr("h3_res_7", r) == h3_cell` and `min_fl <= FL < max_fl` with `FL = baro_altitude_c * 3.28084 / 100`; duplicates resolved by the lag test inside `Window.partitionBy("track_id", PIECE).orderBy("event_time", "fir_band_width", "fir_area_km2", "fir_code")`); `calculate_fir_events(sdf: DataFrame, fir_ref: Optional[DataFrame], config: EventConfig) -> Optional[DataFrame]` (None when `fir_ref` is None or the flag is off), events `xing-fir` with `version = FIR_VERSION = "fir_v0.1.0"` and `info` `{crossing_seq, fir_from, fir_to, fl, bracket_s, airac_cfmu, airac_validity_start, airac_validity_end}`; `load_fir_reference(storage) -> Optional[DataFrame]` (reads `opdi_h3_airspace_ref` filtered to `airspace_type == "FIR"`, None if the table is absent).

- [ ] **Step 1: Write the failing tests** with a synthetic reference built in the test from two adjacent boxes filled with `h3.polygon_to_cells` at res 7 and compacted (one helper `_ref(spark, volumes)` where `volumes = [(code, min_fl, max_fl, area, lon_min, lon_max)]`, lat 50..52):

```python
def test_h3_parent_expr_matches_the_library(spark): # for 20 random res-7 cells and r in 1..7, equals h3.cell_to_parent
def test_a_straight_flight_across_the_boundary_gives_one_crossing(spark): # from "AAAA" to "BBBB": one xing-fir, fir_from AAAA, fir_to BBBB, crossing_seq 1, airac_cfmu "524"
def test_a_boundary_skim_shorter_than_the_dwell_is_not_a_crossing(spark): # 30 s excursion into BBBB and back: no rows
def test_no_crossing_across_a_hole(spark): # last sample in AAAA, 40 min silence, first in BBBB: no rows
def test_a_climb_through_the_band_boundary_is_a_crossing(spark): # AAAAFIR 0-195 and AAAAUIR 195-999 same box: climb 150 -> 250: one crossing AAAAFIR -> AAAAUIR
def test_a_double_match_keeps_the_narrower_band_once(spark): # PLAC 0-999 area 1e6 and AAAA 0-195 area 1e4 same box: fir_code AAAA, row count unchanged
def test_missing_reference_table_skips_the_family(spark): # calculate_fir_events(sdf, None, cfg) is None
def test_off_flag_returns_none(spark)
```

- [ ] **Step 2: Run**: `pytest tests/test_fir.py -v`. Expected: FAIL.

- [ ] **Step 3: Add config fields** and legacy/config test assertions; run `pytest tests/test_event_config.py`; PASS.

- [ ] **Step 4: Implement `src/opdi/pipeline/fir.py`** per the spec's "Family 3" (debounce: run-length encode `fir_code` per `(track_id, piece)`, run duration from first/last epoch, runs shorter than the dwell take the code of the previous long run via `last(code_if_long, ignorenulls=True)`, re-encode, then crossings where the previous run's code is non-NULL and differs; event at the first sample of the new run; `bracket_s` = that sample's time minus the previous run's last time).

- [ ] **Step 5: Run** `pytest tests/test_fir.py tests/test_event_config.py -v`. Expected: PASS.

- [ ] **Step 6: Commit**: `git commit -am "events: xing-fir from a compact H3 FIR reference, debounced per piece"`

### Task 7: Wire the three families into step 04

**Files:**
- Modify: `src/opdi/pipeline/events.py` (`_etl_flight_events_and_measures` after the `calc_seen` block, `process_month` flags and `_mark_processed`, `full_rebuild`), `benchmarks/event_bench.py` (`V4_BASE`, `LADDER_V4`), `tests/test_event_bench_guard.py:146`
- Test: `tests/test_events_processor.py`

**Interfaces:**
- Consumes: Tasks 2, 4, 6 functions and Task 3 loader.
- Produces: progress key `"navigation"` (`calc_navigation`), gated per family by `emit_holding`, `emit_point_merge`, `emit_fir_crossings`; `full_rebuild` includes it; `V4_BASE` gains `emit_holding=False, emit_point_merge=False, emit_fir_crossings=False`; rungs `("V10_holding", {"emit_holding": True})`, `("V11_point_merge", {"emit_point_merge": True})`, `("V12_fir", {"emit_fir_crossings": True})`; guard `len(V4_BASE) == 13`.

- [ ] **Step 1: Write the failing tests** in `tests/test_events_processor.py` using `_run` with a stub storage that holds `opdi_h3_airspace_ref` (two-box synthetic from Task 6's helper, imported) and a flight list with `ades = "XXXX"`, and a track that holds twice then crosses the FIR boundary:

```python
def test_a_full_run_emits_the_three_families(spark): # written events contain holding-start, holding-end, xing-fir with their own versions; measurements contain "Holding duration (s)"
def test_the_navigation_group_is_tracked_in_the_progress_log(spark, tmp_path): # after process_day, _load_processed("navigation") has the day
def test_families_off_emit_nothing(spark): # EventConfig(emit_holding=False, emit_point_merge=False, emit_fir_crossings=False): none of the new types
def test_the_ladder_ends_at_the_shipped_configuration(): # event_bench.verify_plan_v4 passes with the new rungs (existing guard test extended)
```

- [ ] **Step 2: Run**: `pytest tests/test_events_processor.py tests/test_event_bench_guard.py -v`. Expected: the new tests FAIL.

- [ ] **Step 3: Implement**: in the ETL, `if calc_navigation:` compute `pms_ref = point_merge_systems_for_day(spark, day or month)` and `fir_ref = load_fir_reference(self.storage)` once, then `add(calculate_holding_events(geo, self.events, go_arounds=<the go-around frame computed above, or None>))`, `add(calculate_point_merge_events(geo, pms_ref, self.events))`, `add(calculate_fir_events(sdf_input, fir_ref, self.events))`, each inside its flag; print one line per family with its row count only when `log_counts` is on (follow the existing print style). Add the progress key and the ladder entries.

- [ ] **Step 4: Run** `pytest tests/test_events_processor.py tests/test_event_bench_guard.py tests/test_event_config.py -v`, then the full suite `.venv310/bin/python -m pytest tests/ -x -q 2>&1 | tail -5` (baseline has 9 known failures from before this branch; record which).

- [ ] **Step 5: Commit**: `git commit -am "events: holding, point merge and FIR families run in step 04 under the navigation group"`

### Task 8: Holding scored against the open label set

**Files:**
- Create: `benchmarks/holding_labels_score.py`, `tests/test_holding_labels_score.py`
- Data: `/home/jupyter/.claude/jobs/616d5918/tmp/research/holding_data.md` has the download plan. Facts that bind this task: the 4TU files are one per airport and cannot be sliced by day (EGLL 447 MB, EIDW 403 MB, URLs in the notes; `hp_labels.parquet.gz` is already at `$CLAUDE_JOB_DIR/tmp/research/holding_data/`). The day is **2019-10-01** (EGLL 890 positive windows over 253 holding flights of its arrivals; EIDW 436 over 66). Trajectories are resampled to 1 s with aviation units (`altitude` ft, `groundspeed` kt, `track`, `vertical_rate` ft/min); the adapter thins them to one sample per 5 s to match OPDI's cadence. Label `flight_id` does not match the trajectory `flight_id`: join on callsign plus time overlap (every label window lies inside its trajectory). The metric (`06_metrics.py` in the same folder) is time-weighted per flight: episodes from consecutive positive windows, intersection and union of truth and detected episode durations per flight, precision = mean(I)/mean(d_c), recall = mean(I)/mean(d_t), IoU per flight; flights with neither are excluded. Trajectories are CC BY-NC-SA: commit scores only, never extracts. Keep downloads under `$CLAUDE_JOB_DIR/tmp/research/holding_data/`.

**Interfaces:**
- Produces: `as_track_frame(spark, traj: pd.DataFrame, elevation_ft: float, step_s: float = 5.0) -> DataFrame` (traffic-format columns to the step 04 input shape with `track_id = flight_id`, thinned to one sample per `step_s`, `baro_altitude_c` in m, `velocity` m/s, `heading = track`, `vert_rate` m/s, cumulative columns, `piece`, `elev_ades_ft`); `match_labels(labels: pd.DataFrame, traj_flights: pd.DataFrame) -> pd.DataFrame` (label `flight_id` to trajectory `flight_id` by callsign prefix and time overlap, one to one); `windows_from_episodes(labels: pd.DataFrame, episodes: pd.DataFrame) -> pd.DataFrame` adding `hp_c` = window overlaps a detected episode; `score(windows) -> dict(precision, recall, f1, iou, n_flights, n_windows, n_positive)` reimplementing `06_metrics.py` (time-weighted per flight, as the notes describe); `sweep(grid) -> pd.DataFrame` over `holding_net_turn_min_deg ∈ {270, 300, 330}`, `holding_max_displacement_nm ∈ {6, 8, 10}`, `holding_min_duration_seconds ∈ {180, 240, 360}`; `main(argv)` writes `holding_score_<day>.csv`, `holding_sweep_<day>.csv`, `holding_episodes_<day>.csv` with `benchmarks/provenance.py` manifests into `--results-dir`.

- [ ] **Step 1: Write the failing tests** (`as_track_frame` unit conversions and 5 s thinning on ten 1 s rows; `match_labels` on two trajectories sharing a callsign at different times; `windows_from_episodes` on a 3-window label frame and one episode covering windows 2 and 3; `score` on one flight with a 600 s truth episode and a 480 s detected one fully inside it: precision 1.0, recall 0.8, iou 0.8).
- [ ] **Step 2: Run** `pytest tests/test_holding_labels_score.py -v`; expected FAIL.
- [ ] **Step 3: Implement** the script (local `SparkSession.builder.master("local[4]")`, memory 8g).
- [ ] **Step 4: Run** the tests (PASS), then download the EGLL and EIDW files (curl, md5 checked against `traffic/src/traffic/data/datasets/__init__.py`), then the script on 2019-10-01 for EGLL and EIDW with `--results-dir /home/jupyter/.claude/jobs/616d5918/tmp/research/holding_results` first, read the numbers, then with `--results-dir <portal paper>/data/holding`. Expected: a `holding_score` CSV with precision, recall and F1 per airport for the shipped thresholds; the sweep CSV with 27 rows per airport.
- [ ] **Step 5: Record** in the ledger whether the gate (F1 ≥ 0.70 and precision ≥ 0.75 at both airports) passed. If the sweep's best point beats the shipped thresholds by more than 0.03 F1 at both airports, change the defaults to that point in `config.py` (docstring says why), re-run the unit tests, and re-run the score once.
- [ ] **Step 6: Commit**: `git add benchmarks/holding_labels_score.py tests/test_holding_labels_score.py && git commit -m "bench: holding rule scored against the Olive 2019 label set"` (CSV commits happen in the portal worktree in Task 12).

### Task 9: Pipeline-day runner and the offline checks

**Files:**
- Create: `benchmarks/run_airspace_events_day.py`, `benchmarks/fir_consistency.py`, `benchmarks/point_merge_report.py`, `benchmarks/fir_truth_score.py`, `reference/extract_airspace_profiles.R`, `tests/test_run_airspace_events_day.py`, `tests/test_fir_truth_score.py`, `tests/test_point_merge_report.py`

**Interfaces:**
- `run_airspace_events_day.py`: constants `PREFIX = "s3a://eurocontrol/opdi-world/research/airspace_events"`, `INPUT_WAREHOUSE = "s3a://eurocontrol/opdi-world"`, `ARMS = ("off", "on")`; reuses the `redirect_arm`/`guard_write_path` pattern of `benchmarks/run_track_link_day.py` with `PASSTHROUGH` = reference tables plus `osn_tracks_clean`, `opdi_flight_list` (read from `INPUT_WAREHOUSE`; fallback flag `--input a10` resolves them to the v5 arm), `DERIVED` = `opdi_h3_airspace_ref` (written once under `PREFIX/ref`), `opdi_flight_events`, `opdi_measurements` (under `PREFIX/<arm>`); `build_config(arm)` sets the three `emit_*` flags to `arm == "on"`; steps `00c` (only with `--steps 00c`) and `04`; `--timings-csv`; the free-space guard. Tests: `arm_path`, `guard_write_path` refusing `opdi-world/osn_tracks_clean`, `build_config("off")` flags False and `("on")` True.
- `fir_consistency.py`: reads `PREFIX/on/opdi_flight_events` and a sample of `osn_tracks_clean` (2,000 tracks with a crossing), computes exact shapely point-in-polygon FIR membership from the pruatlas 524 WKT on the driver for those tracks' samples, and writes `fir_consistency_<day>.csv` (share of samples agreeing, boundary timing error quantiles in seconds, crossings per flight distribution, share of samples with no FIR, overlap share) with provenance.
- `point_merge_report.py`: per covered airport on the day: arrivals (flight list `ades`), arrivals with a leg pair, share, median `leg_distance_nm`, `leg_level_ft`, `duration_s`; writes `point_merge_usage_<day>.csv`; `--plot EIDW --n 20` writes PNGs of 20 detected and 20 undetected arrivals (matplotlib, driver-side pandas of those tracks) to the results dir for visual inspection. Test: the aggregation on a hand-made events+flight-list frame.
- `fir_truth_score.py`: `score_crossings(events: pd.DataFrame, truth: pd.DataFrame, tolerance_s=120) -> dict` aligning `xing-fir` rows to truth `(flight, fir_from, fir_to, entry_time)` sequences by icao24 + callsign + nearest time; test on a hand-made pair. `reference/extract_airspace_profiles.R` mirrors `extract_apdf_full.R` using `airspace_profiles_tidy(conn, wef, til, airspace = "FIR", profile = "CPF")` joined to `flights_tidy()` for `AIRCRAFT_ADDRESS`; not run here.

- [ ] **Step 1: Write the failing tests** for the runner constants/config, the report aggregation and the truth scorer.
- [ ] **Step 2: Run** them; expected FAIL.
- [ ] **Step 3: Implement** the four scripts and the R extractor.
- [ ] **Step 4: Run** the tests; expected PASS.
- [ ] **Step 5: Commit**: `git commit -am "bench: airspace events pipeline-day runner, FIR consistency, point merge report, FIR truth scorer"`

### Task 10: Run the day on the cluster

**Files:** results under `<portal worktree>/papers/airspace-events-v1/data/{pipeline_day,fir,point_merge}`; logs under `$CLAUDE_JOB_DIR/tmp/airspace/`.

- [ ] **Step 1:** Check inputs exist: `opdi-world/osn_tracks_clean/dof=2026-06-14` and `opdi-world/opdi_flight_list/dof=2026-06-14` (list with the boto3 snippet in `$CLAUDE_JOB_DIR/tmp/sz_a10.py`). If the flight list is absent, use `--input a10`.
- [ ] **Step 2:** `python benchmarks/run_airspace_events_day.py --day 2026-06-14 --steps 00c --executors 8 2>&1 | tee $CLAUDE_JOB_DIR/tmp/airspace/s00c.log`. Expected: `opdi_h3_airspace_ref` written under `PREFIX/ref`, the overlap report printed, row count on the order of 10^6.
- [ ] **Step 3:** `... --arm off --steps 04 --timings-csv <data>/pipeline_day/timings.csv`, then `--arm on`. Expected: both finish; free space checked before each.
- [ ] **Step 4:** `python benchmarks/fir_consistency.py --day 2026-06-14 --results-dir <data>/fir` and `python benchmarks/point_merge_report.py --day 2026-06-14 --results-dir <data>/point_merge --plot EIDW --n 20`. Read the 40 PNGs (the `Read` tool shows images) and record in `<data>/point_merge/eidw_visual_check.csv` one row per plot: `track_id, detected, judged_real_leg (yes/no/unsure)`.
- [ ] **Step 5:** Extraction counts per new type and measurement type from `PREFIX/on` (`benchmarks/events_compare.py` style query, written to `<data>/pipeline_day/counts.csv` with provenance), and step 04 timings off vs on.
- [ ] **Step 6:** Free space: delete nothing unless under 3 GB; if needed, delete `PREFIX/off/opdi_measurements` first (the counts are already extracted).
- [ ] **Step 7:** Ledger: the measured numbers for the three gates.

### Task 11: Shipping gates

**Files:** `src/opdi/config.py`, `tests/test_event_config.py`, `benchmarks/event_bench.py`

- [ ] **Step 1:** Apply the spec's gates (holding: F1 ≥ 0.70 and precision ≥ 0.75 at EGLL and EIDW; point merge: EIDW share within 15 points of 42% and ≥ 18/20 plotted detections real; FIR: consistency share ≥ 99% of samples agreeing outside a 10 s boundary band). For any family that fails, set its `emit_*` default to `False`, say why in the docstring, move the flag out of `V4_BASE` and its rung (a family off by default is not on the ladder), adjust `test_the_shipped_new_behaviours_are_on`.
- [ ] **Step 2:** Run `pytest tests/test_event_config.py tests/test_event_bench_guard.py -v`; PASS. Full suite once more; record the count.
- [ ] **Step 3:** Commit: `git commit -am "events: shipping defaults for the three families set from the measured gates"`

### Task 12: Paper, regeneration entry point, CLAUDE.md

**Files:**
- Create (portal): `papers/airspace-events-v1/report_template.md`, `index.qmd`, `index.html`, `airspace-events-v1.pdf`, `data/...` (CSVs from Tasks 8 and 10 with `_manifest.json`); modify `papers/index.qmd` (card).
- Create (opdi): `benchmarks/build_airspace_v1_paper.py`, `benchmarks/regenerate_airspace_v1.py`, `tests/test_build_airspace_v1_paper.py`
- Modify (opdi): `benchmarks/build_track_v4_paper.py` `Paper.__init__` gains `pdf_name: Optional[str] = None, builder: Optional[str] = None` (defaults keep today's names).
- Modify (meta): `CLAUDE.md` (a paragraph after the track-identity one: the three families, their versions, the FIR AIRAC 524 caveat, the gates and what shipped on/off, the overlap rule).

- [ ] **Step 1:** Write `report_template.md` in plain language: what each family detects and why the rule looks like that (one paragraph each with the backing numbers), the thresholds table, the measurements table, the AIRAC limitation (one cycle, 524, two years old, newer cycles only on the laptop), the results (holding P/R/F1 vs the 0.84/0.70/0.76 bar, point merge usage shares vs published, FIR consistency and crossings per flight), what shipped on and off, cost (step 04 timings), and what is still unverified (no FIR truth yet; point merge table coverage). Placeholders are filled by `build_context(data)`; every number comes from a CSV (`literal_numbers` check).
- [ ] **Step 2:** Write the builder test (context keys present, `--check` passes after a build) and run it; FAIL then PASS.
- [ ] **Step 3:** Build: `OPDI_PAPER_DIR_AIRSPACE=<portal paper dir> python benchmarks/build_airspace_v1_paper.py --pdf` using the scratch venv's reportlab (`/home/jupyter/.claude/jobs/616d5918/tmp/pdfenv/venv/bin/python` with `PYTHONPATH` to the opdi `src` and `benchmarks`). Expected: `index.qmd`, `index.html`, PDF written; `--check` returns 0.
- [ ] **Step 4:** `regenerate_airspace_v1.py --check` reports every job fresh (jobs: `holding_labels`, `pipeline_day`, `fir`, `point_merge`; same `Job`/manifest pattern as `regenerate_track_v5.py`).
- [ ] **Step 5:** Commit portal (`papers/airspace-events-v1`, `papers/index.qmd`), commit opdi (builders, tests), update CLAUDE.md and the submodule pointers in the meta worktree, commit meta.
- [ ] **Step 6:** Push the three branches (`git push -u origin airspace-events-v1` in each worktree), open draft PRs for opdi and the meta repo (not the portal), plain bodies, no attribution.

## Self-review notes

- Spec coverage: shared mechanism (Task 1), families (2, 4, 6), reference table (3), step 00c (5), wiring and ladder (7), validation items 1-5 (2/4/5/6 unit tests, 8, 9, 10), gates (11), paper and CLAUDE.md (12). The PRISME extractor and truth scorer (spec non-goal 5) are in Task 9.
- Naming: `calculate_holding_events(geo, config, go_arounds=None)`, `calculate_point_merge_events(geo, pms_ref, config)`, `calculate_fir_events(sdf, fir_ref, config)`, `load_fir_reference(storage)`, `point_merge_systems_for_day(spark, day)`, `EXTRA_MEASURES`, `HOLDING_VERSION`, `PMS_VERSION`, `FIR_VERSION`, `PIECE` are used consistently across Tasks 1-7 and 9.
- Config field names all end in an allowed suffix (`_deg` for the two turn-rate fields, documented as per second; `_ft` for the candidate ceiling instead of a flight-level name).
- Review Focus items map to named tests in Tasks 2, 4, 5 and 6.
