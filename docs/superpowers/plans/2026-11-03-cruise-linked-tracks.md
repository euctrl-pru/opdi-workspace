# Cruise-linked tracks Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a research-only segmentation arm that keeps one `track_id` across an en-route reception hole when the edges look like one flight, measure its false joins, and publish `track-construction-v4`.

**Architecture:** One new `BreakRule` (A10 `cruise_linked`) whose link decision is a pure function of named edge features, so the study scores exactly what ships. A read-only re-segmentation grid picks the parameters, then one sampled day runs through the real pipeline to measure the flight list and detector damage.

**Tech Stack:** PySpark column expressions and window functions, pytest with the local `spark` fixture, Quarto (R chunks) for the paper.

**Spec:** `docs/superpowers/specs/2026-11-03-cruise-linked-tracks-design.md` (meta-repo). Read it first. Its evidence is `docs/superpowers/research/linking-fragmented-flight-trajectories.md`.

## Global Constraints

- Branches: `track-construction-v4` in `opdi` (from `main`) and in `opdi-portal` (from `feature/new-topics`). Work in worktrees. Never push to `main`.
- Python: `opdi/.venv310/bin/python -m pytest <file> -p no:cacheprovider`. A worktree needs a symlinked `.env`. Export `OPDI_PORTAL` or `OPDI_PAPER_DIR` when the sibling walk fails inside a worktree.
- Baseline: 9 tests already fail in a nested worktree (`test_check_report_numbers`, `test_events_labelling`, `test_flight_detection`, `test_flight_list_v7_redirect`, 2 in `test_h3_airport_layouts`, 2 in `test_regenerate_track_v2`, `test_regenerate_v62`). A new failure is yours.
- `debounced`, `recommended`, `legacy`, `standard` keep an unchanged partition. `standard` still resolves to `debounced`.
- Aviation units with the unit in the field name; scale the comparison and reuse `FT_PER_M`, `KT_PER_MPS`, `196.850394`.
- Nothing is written outside `s3a://eurocontrol/opdi-world/research/a10/`. `opdi-prod` and `opdi-world` production tables are read-only. Nothing existing is deleted.
- Worldwide only: `OPDIConfig.for_environment("opensky", worldwide=True)`. `OPDI_WAREHOUSE` is ignored under worldwide.
- Stop any cluster run if the bucket has under 3 GB free of its 200 GB. Stage 2 is one day, 2026-06-14, on a 10% airframe sample.
- `*.csv` is gitignored: stage study CSVs into the paper's `data/`, which is tracked, with a provenance entry.
- A link is decided in one place. Benchmarks call `linkable`; they never reimplement it.
- Commit trailer: `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>` and `Claude-Session: https://claude.ai/code/session_01F8JMNXryGq6PJRccES7ESa`.

## Review Focus

1. A track with three pieces across two holes: it must become one track only if both holes pass. Pinned in Task 1.
2. NULL position or altitude at a hole's edge, including a hole's edge with fewer than 5 samples: not linkable, and never a NULL `_brk` that reads as continuous. Pinned in Task 1.
3. A hole across the antimeridian: distance comes from the short way round. Pinned in Task 1.
4. A re-run of a day after changing A10's defaults: stale local ETL logs must not skip a table build. Per-arm, per-prefix log directory, pinned in Task 5.
5. A truth flight with no track at all under one arm: it counts as a miss in that arm and never leaves the denominator. Pinned in Task 3.

---

### Task 1: The rule

**Files:**
- Modify: `src/opdi/pipeline/segmentation/base.py`, `src/opdi/pipeline/segmentation/methods.py`, `src/opdi/config.py` (`SegmentationConfig`)
- Create: `tests/test_segmentation_cruise_linked.py`

**Interfaces:**
- Produces, in `methods.py`:
  - `stable_callsign(p: SegmentationParams) -> Column`, extracted unchanged from `debounced`, and `debounced_callsign_change(p) -> Column`.
  - `link_feature_columns(p, window: WindowSpec | None = None) -> dict[str, Column]`, with keys `gap_min`, `gc_nm`, `alt_before_ft`, `alt_after_ft`, `vr_before_fpm`, `vr_after_fpm`, `spd_before_kt`, `cs_before`, `cs_after`, `icao24`. `window` defaults to `segment_window()`; Task 2 passes one per control.
  - `linkable(f: Mapping[str, Column], p) -> Column`: never NULL, per the spec table.
  - `cruise_linked() -> BreakRule`, registered as `ARMS["cruise_linked"]`.
- Produces on `SegmentationParams` and `SegmentationConfig`, with the spec's defaults: `link_max_gap_minutes=360.0`, `link_cruise_floor_ft=25000.0`, `link_edge_vert_rate_fpm=500.0`, `link_callsign_lookahead_minutes=10.0`, `link_min_implied_speed_kt=315.0`, `link_max_implied_speed_kt=750.0`, `link_min_speed_ratio=0.0`, `link_min_stop_minutes=0.0`, `link_vfast_kt=650.0`, and `LINK_EDGE_SAMPLES = 5`, `PLACEHOLDER_ICAO24 = ("000000", "000001", "ffffff")` as constants.

- [ ] **Step 1: Write the failing tests**

Build tracks with `conftest.make_track`, one airframe, at the spec's edges. Default fixture: 10 samples at 11,000 m before and after a hole, level, callsign `DLH1`, positions 1,530 NM apart across a 191-minute hole.

```python
def test_a_transatlantic_hole_is_one_track(spark):            # 1 track under A10, 2 under debounced
def test_the_second_hole_is_judged_on_its_own(spark):         # 3 pieces, second hole too slow: 2 tracks
def test_a_low_edge_splits(spark):                            # new edge at 3,000 ft: 2 tracks
def test_descent_then_climb_splits(spark):                    # edges at FL300, vert_rate -1500 and +1500 ft/min
def test_null_vert_rate_is_neutral(spark):                    # vert_rate NULL on all edge samples: 1 track
def test_different_callsigns_split(spark):                    # DLH1 then BAW2: 2 tracks
def test_blank_then_different_callsign_splits(spark):         # DLH1, hole, 2 min blank, BAW2: 2 tracks
def test_blank_throughout_is_neutral(spark):                  # both sides blank: 1 track
def test_too_slow_splits(spark):                              # implied speed 200 kt: 2 tracks
def test_impossible_speed_splits(spark):                      # implied speed 900 kt: 2 tracks
def test_a_hole_past_the_maximum_splits(spark):               # 361 min at 480 kt: 2 tracks
def test_time_budget_when_enabled(spark):                     # link_min_stop_minutes=35: 191 min, 1,530 NM splits; 1,950 NM links
def test_speed_ratio_when_enabled(spark):                     # ratio 0.85, edge 480 kt, implied 367 kt: splits; off: links
def test_placeholder_address_splits(spark):                   # icao24 000001: 2 tracks
def test_null_edge_position_fails_closed(spark):              # lat NULL at the old edge: 2 tracks, and _brk is 1, not NULL
def test_too_few_edge_samples_still_decides(spark):           # 1 sample before the hole: decided from that one
def test_the_antimeridian_is_crossed_the_short_way(spark):    # lon 178 to -178 on the equator at FL350 (240 NM), 31 min hole, 465 kt: 1 track
def test_short_gaps_behave_as_debounced(spark):               # same partition as debounced on a 5-sample flicker and a 20 min gap
def test_debounced_partition_is_unchanged(spark):             # stable_callsign extraction: existing debounce tests pass unmodified
def test_defaults_are_on_both_config_classes():               # field-by-field equality, as test_segmentation_base does
```

- [ ] **Step 2: Run to verify they fail**

Run: `.venv310/bin/python -m pytest tests/test_segmentation_cruise_linked.py -q -p no:cacheprovider`
Expected: FAIL on import of `cruise_linked`.

- [ ] **Step 3: Implement**

Extract `stable_callsign` and `debounced_callsign_change` from `debounced`, keeping the comments, and call them from `debounced`. Add the parameters. In `cruise_linked`:

```python
break = callsign_change | low_altitude_gap | (general_gap & ~linkable(link_feature_columns(p), p))
```

Edge frames: `w.rowsBetween(-LINK_EDGE_SAMPLES, -1)` before and `w.rowsBetween(0, LINK_EDGE_SAMPLES - 1)` after. `cs_after` looks ahead by time, with a `rangeBetween` on `unix_timestamp(_ts)` of `link_callsign_lookahead_minutes`. Import `haversine_nm` from `opdi.pipeline.flights`. Wrap `linkable` in `coalesce(..., lit(False))`, with `vr_*` NULL handled inside as neutral.

- [ ] **Step 4: Run to verify they pass, along with the existing arms**

Run: `.venv310/bin/python -m pytest tests/test_segmentation_cruise_linked.py tests/test_segmentation_debounce.py tests/test_segmentation_base.py tests/test_segmentation_methods.py tests/test_segmentation_default.py -q -p no:cacheprovider`
Expected: all PASS. If `test_segmentation_methods.py:434`'s per-arm loop needs a fixture for the new arm, extend it rather than exempting the arm.

- [ ] **Step 5: Add the midnight guard, test first**

In `tests/test_track_claim.py`: `test_cruise_linked_reads_back_past_its_longest_gap`, asserting `required_lookback_hours("cruise_linked", p)` is 7 at the default and `required_lookback_hours("debounced", p)` is 3; `test_too_short_an_explicit_lookback_raises`; and `test_a_piece_after_midnight_is_claimed_once`, which segments a frame with old piece 20:50-20:55 and new piece 00:10-00:20 under both day windows and `keep_tracks_starting_in`, and asserts each sample lands in exactly one claimed track.

Then add `required_lookback_hours(method: str, p: SegmentationParams) -> int` in `tracks.py`, using `ceil(link_max_gap_minutes / 60) + 1` for `cruise_linked`, and use it in `process_day`.

Run: `.venv310/bin/python -m pytest tests/test_track_claim.py -q -p no:cacheprovider`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/opdi tests/test_segmentation_cruise_linked.py tests/test_track_claim.py
git commit -m "feat(segmentation): A10 cruise_linked keeps one track across an en-route hole"
```

### Task 2: Control generators and hole labels

**Files:**
- Create: `benchmarks/track_link_controls.py`, `tests/test_track_link_controls.py`

**Interfaces:**
- Consumes: `link_feature_columns`, `linkable` (Task 1); `track_truth.load_flight_intervals`.
- Produces:
  - `real_hole_candidates(tracks: DataFrame, p) -> DataFrame`: one row per gap over `gap_minutes`, with the Task 1 features plus `t_old_edge`, `t_new_edge`.
  - `label_real_holes(holes, truth) -> DataFrame`: adds `label` in `true_link`, `false_join`, `unknown`, per the spec. Matches on `icao24` and time only.
  - `negative_controls(tracks, truth, floor_ft, depths_min=(0, 30, 60, 120)) -> DataFrame` and `positive_controls(tracks, truth, floor_ft, gaps_min=(60, 120, 180, 360)) -> DataFrame`: one row per control, with features and `control_id`, `depth_min`, `gap_stratum`, `same_callsign`, `expected` (`split` or `link`).
  - `rule_of_three_upper(n_events: int, n: int) -> float`: Clopper-Pearson upper bound at 95%, which is about `3/n` at zero events; `None` when `n == 0`.

- [ ] **Step 1: Write the failing tests**

```python
def test_a_hole_inside_one_flight_is_a_true_link(spark):
def test_a_hole_holding_a_landing_and_a_takeoff_is_a_false_join(spark):
def test_a_hole_with_no_truth_is_unknown(spark):
def test_labels_never_use_callsign(spark):                # different callsigns, same interval: still true_link
def test_base_cut_uses_the_last_and_first_cruise_samples(spark):
def test_a_deep_cut_moves_into_cruise_on_each_side(spark):
def test_a_deep_cut_outside_cruise_is_dropped(spark):     # 120 min on a 90 min cruise: no control at that depth
def test_controls_of_30_min_or_less_are_excluded(spark):
def test_a_continuing_flight_control_can_pass_the_speed_test(spark):
    # A-B-C, deep cut: implied speed near 384 kt, so the control is a real test
def test_positive_control_has_a_hole_of_the_requested_length(spark):
def test_rule_of_three():                                  # (0, 3000) is about 0.001; (0, 60) is about 0.05; (0, 0) is None
```

- [ ] **Step 2: Run to verify they fail**

Run: `.venv310/bin/python -m pytest tests/test_track_link_controls.py -q -p no:cacheprovider`
Expected: FAIL on import.

- [ ] **Step 3: Implement the functions above in `benchmarks/track_link_controls.py`**

Controls select only the samples around each cut, then call `link_feature_columns` with a window partitioned on `control_id`, so a control is scored by the same expressions as a real hole. Exact-CI via `scipy` only if already in `.venv310`; otherwise the bisection on the binomial tail is a few lines.

- [ ] **Step 4: Run to verify they pass**

Run: `.venv310/bin/python -m pytest tests/test_track_link_controls.py -q -p no:cacheprovider`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add benchmarks/track_link_controls.py tests/test_track_link_controls.py
git commit -m "feat(bench): real-hole labels and negative and positive controls for track linking"
```

### Task 3: The Stage 1 grid job and the gates

**Files:**
- Create: `benchmarks/track_link_grid.py`, `tests/test_track_link_grid.py`

**Interfaces:**
- Consumes: Task 2 functions; `linkable`; `SegmentationParams`.
- Produces: `grid_points() -> list[SegmentationParams]` (54 points per the spec), `pick_recommended(rows: list[dict]) -> dict | None`, and CSVs `link_grid_{day}.csv`, `link_controls_{day}.csv`, `link_recommended.csv` with provenance.

- [ ] **Step 1: Write the failing tests**

```python
def test_the_grid_has_54_points():
def test_a_flight_with_no_track_stays_in_the_denominator():
def test_the_most_flights_in_one_track_wins_among_passing_points():
def test_a_point_over_the_negative_gate_is_rejected():      # upper bound 1.2% overall
def test_a_same_callsign_stratum_over_5_percent_is_rejected():
def test_a_same_callsign_stratum_under_60_is_insufficient_not_failed():
def test_real_hole_false_joins_over_1_percent_are_rejected():
def test_no_passing_point_returns_none_and_names_the_lowest_bound():
```

- [ ] **Step 2: Run to verify they fail**

Run: `.venv310/bin/python -m pytest tests/test_track_link_grid.py -q -p no:cacheprovider`
Expected: FAIL on import.

- [ ] **Step 3: Implement**

Read `opdi-world/osn_tracks` for `--day`, and call `real_hole_candidates` and the controls once, caching the feature frames. For each `grid_points()` entry, apply `linkable` to the cached features and aggregate. That avoids re-segmenting 54 times, which is sound because links between holes are independent per boundary. Compute `tracks_per_flight` and `share_one_track` by truth flight from the link decisions; compute the two V3 two-callsign counts for the recommended point only, with `track_fragmentation.measure`. Write nothing to S3.

- [ ] **Step 4: Run to verify they pass**

Run: `.venv310/bin/python -m pytest tests/test_track_link_grid.py -q -p no:cacheprovider`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add benchmarks/track_link_grid.py tests/test_track_link_grid.py
git commit -m "feat(bench): the Stage 1 grid and the acceptance gates"
```

### Task 4: Run Stage 1 and set the defaults

**Files:**
- Modify: `src/opdi/pipeline/segmentation/base.py`, `src/opdi/config.py` (only if a point passes), `tests/test_segmentation_cruise_linked.py`
- Stage: `opdi-portal/papers/track-construction-v4/data/`

- [ ] **Step 1: Check the bucket and run both days**

Run: `.venv310/bin/python -u benchmarks/track_link_grid.py --days 2026-06-14 2025-06-14 --results-dir $OPDI_PAPER_DIR/data`, in the background, with `PYSPARK_DRIVER_PYTHON` set and `PYSPARK_PYTHON` unset.
Expected: exit 0, bucket usage unchanged, and `link_recommended.csv` holding one row or an explicit "none".

- [ ] **Step 2: Sanity-check before believing it**

Confirm: the real-hole `unknown` share is reported; controls per stratum are as expected; the most permissive point does not win trivially because the gates bind. If the deep-cut false-join rates are zero everywhere, inspect 20 controls by hand before accepting that. Report the result to the human before Step 3: it decides whether the study continues as planned.

- [ ] **Step 3: If a point passes, update the defaults to it, and the tests that pin them; commit**

```bash
git add src/opdi tests/test_segmentation_cruise_linked.py
git commit -m "feat(segmentation): A10 defaults from the Stage 1 grid"
```

If none passes, change no defaults, and carry the lowest-bound point into Task 5 as an explicit parameter, labelled not recommended.

### Task 5: The Stage 2 runner and one day

**Files:**
- Create: `benchmarks/run_track_link_day.py`, `tests/test_run_track_link_day.py`

**Interfaces:**
- Produces: `arm_path(arm, table) -> str`, `guard_write_path(path) -> None` (raises; an allow-list on `^s3a://eurocontrol/opdi-world/research/a10/`), `sample_airframes(df) -> DataFrame`, `log_dir_name(arm) -> str`.
- Pattern: `benchmarks/run_adsc_day.py`'s `redirect_arm`, with its three disjoint table sets, raising on an unclassified table. Reference tables pass through to `opdi-world`. Arms: `debounced`, `cruise_linked`. `storage.check_coverage()` before any step.

- [ ] **Step 1: Write the failing tests**

```python
def test_the_arms_differ_only_in_method():            # read from what run_arm builds
def test_inputs_are_shared_between_arms():
def test_a_write_outside_a10_is_refused():
def test_an_unclassified_table_raises():
def test_the_sample_is_stable_and_about_a_tenth(spark):
def test_each_arm_and_prefix_has_its_own_logs():
def test_a_low_bucket_stops_the_run():                 # under 3 GB free: raises before Spark starts
```

- [ ] **Step 2: Run to verify they fail, implement, run to verify they pass**

Run: `.venv310/bin/python -m pytest tests/test_run_track_link_day.py -q -p no:cacheprovider`
Expected: FAIL, then PASS. The sample filter applies in `write_table` for `osn_statevectors_v2` only, as `run_adsc_day.py` does. Bucket usage comes from listing `eurocontrol`, against a 200 GB quota.

- [ ] **Step 3: Commit, then run**

```bash
git add benchmarks/run_track_link_day.py tests/test_run_track_link_day.py
git commit -m "feat(bench): one sampled day through the real pipeline for both arms"
```

Run in the background: `run_track_link_day.py --day 2026-06-14 --arm debounced --steps 01 02 02a 03 04 04b`, then `--arm cruise_linked --steps 02 02a 03 04 04b`.
Expected: exit 0, about 5 GB written, the lookback printed as at least the required value. Check free space between arms; apply the spec's deletion order if needed.

### Task 6: Score Stage 2

**Files:**
- Create: `benchmarks/track_link_pipeline_score.py`, `tests/test_track_link_pipeline_score.py`

**Interfaces:**
- Consumes: `adsc_flight_list_truth.load_truth`, `label_far_end`, `match_one_record`, `rows_per_flight`, `both_ends_table`, `pair_table`; `track_truth.load_flight_intervals`.
- Produces: `link_flight_list.csv`, `link_both_ends.csv`, `link_events.csv`, `link_level_across_hole.csv`, `link_distance_across_hole.csv`, `link_partition_check.csv`, `link_movements.csv`, all restricted to flights inside the day.

- [ ] **Step 1: Write the failing tests**

```python
def test_flights_inside_the_day_only():
def test_a_level_segment_spanning_a_hole_is_counted(spark):
def test_distance_across_a_hole_is_compared_to_the_great_circle(spark):
def test_a_debounced_boundary_missing_from_a10_must_be_a_linked_hole(spark):
def test_a10_adding_a_boundary_fails_the_check(spark):
def test_movements_within_a_tenth_of_a_percent_pass():
```

- [ ] **Step 2: Run to verify they fail, implement, run to verify they pass**

Run: `.venv310/bin/python -m pytest tests/test_track_link_pipeline_score.py -q -p no:cacheprovider`
Expected: FAIL, then PASS.

- [ ] **Step 3: Commit, run, and report**

```bash
git add benchmarks/track_link_pipeline_score.py tests/test_track_link_pipeline_score.py
git commit -m "feat(bench): score the flight list and detector damage for both arms"
```

Run: `.venv310/bin/python -u benchmarks/track_link_pipeline_score.py --day 2026-06-14 --results-dir $OPDI_PAPER_DIR/data`
Expected: all CSVs staged. Apply the spec's withdrawal conditions, and report the outcome to the human before writing the paper's conclusion.

### Task 7: The paper

**Files:**
- Create: `benchmarks/regenerate_track_v4.py`, `tests/test_regenerate_track_v4.py`, `opdi-portal/papers/track-construction-v4/index.qmd`, `opdi-portal/papers/track-construction-v4/track-construction-v4.pdf`
- Modify: `opdi-portal/papers/index.qmd`, meta-repo `CLAUDE.md`

**Interfaces:**
- Consumes: every CSV from Tasks 4 and 6. Pattern: `regenerate_track_v3.py`, one `Job` per output family, `code_paths` naming `segmentation/methods.py`, `segmentation/base.py`, `tracks.py`, `config.py` and the job's script.

- [ ] **Step 1: Write the failing tests, then the entrypoint**

Copy the checks in `tests/test_regenerate_adsc_v1.py`: every job declares outputs and existing dependencies, names are unique, no two jobs stage one file, `--check` needs no credentials, and the arms are named as a prerequisite, not a job.

Run: `.venv310/bin/python -m pytest tests/test_regenerate_track_v4.py -q -p no:cacheprovider`
Expected: FAIL, then PASS.

- [ ] **Step 2: Write `index.qmd` on v3's structure**

Sections per the spec's "The paper". Every number is read from a CSV by an accessor that stops on a missing row. Carry the research report's evidence caveats; do not state a `[snippet]` or `[note-derived]` figure as a finding. Include the required limitations and the `track_id` boundary warning verbatim in meaning. Do not recommend promotion: that decision is outside this paper.

- [ ] **Step 3: Render and check**

Run: `quarto render index.qmd --to html` and the PDF build used for v3, with `OPDI_RENDER=check`.
Expected: exit 0, no stale job, and every table populated. Open the rendered tables and compare three against the CSVs by eye.

- [ ] **Step 4: Add the card, update `CLAUDE.md`, commit submodule-first**

The `CLAUDE.md` note goes under "Track identity is a versioned choice": A10 exists, is not the default, and links to the paper. Also correct the Evidence-base line "gap segmentation at 5 min", which the research report could not find in either winner's active code path: say it is unverified, rather than delete it, unless you find the source.

```bash
git -C opdi add benchmarks/regenerate_track_v4.py tests/test_regenerate_track_v4.py
git -C opdi commit -m "feat(track-v4): the paper's regeneration entrypoint"
git -C opdi-portal add papers/track-construction-v4 papers/index.qmd
git -C opdi-portal commit -m "papers: track construction v4 -- one track across an en-route hole"
git add CLAUDE.md opdi opdi-portal docs/superpowers
git commit -m "track-v4: point at the arm, the study and the paper"
```

- [ ] **Step 5: Final verification**

Run the full suite from the `opdi` worktree: `.venv310/bin/python -m pytest tests/ -q -p no:cacheprovider`.
Expected: the 9 baseline failures named in Global Constraints and nothing new. Confirm with `git diff main -- src/opdi/pipeline/segmentation/methods.py` that `debounced` differs only by the extraction, and that `standard` still resolves to `debounced`.
