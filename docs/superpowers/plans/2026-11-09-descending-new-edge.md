# Descending New Edge Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let a hole link when the aircraft reappears *descending* below the cruise floor, if one small extra check brings that case within the study's limits; otherwise say, with numbers, why it stays off.

**Architecture:** The rule already has the switch (`link_new_edge_descent`, off). Version 4 turned it off because holes linked only by a descending new edge showed 3 false joins in 119 checkable cases, over the 0.1% limit, and never looked at *which* three. This plan exports those holes with their features and labels, tries a fixed short list of extra conditions on them offline, and only then touches the rule. A condition is adopted only if it meets the same three limits the climbing edge had to meet, on its own. Everything runs on the existing study tooling (`track_link_controls.py` labels and controls, `link_feature_columns` features, `track_link_grid.py` for the flight-level numbers).

**Tech Stack:** PySpark (native column expressions only), pandas for the offline scoring, pytest with the local `spark` fixture, `benchmarks/provenance.py`.

**Spec:** this plan's *Design* section below, under the v5 spec `docs/superpowers/specs/2026-11-09-cruise-linked-default-design.md` (its limits and its "no option without evidence" rule apply). Plan it extends: `docs/superpowers/plans/2026-11-09-cruise-linked-default.md`.

## Design

**What a descending new edge is.** After a hole, the lowest of the nearest five altitudes within 5 min is below `link_cruise_floor_ft` (18,000 ft) but at or above `low_alt_ft` (5,000 ft), the mean of the nearest vertical rates is below −`link_edge_vert_rate_fpm` (−500 ft/min) on at least `LINK_DIRECTION_MIN_RATES` (3) rates, and every other check passes. These are the holes "linked only by descent": the study's `linked_by_descent` count.

**Why it can be a false join.** The aircraft may have landed and taken off again inside the hole; what reappears on descent is the *next* flight approaching its destination. The direction says "arriving", not "the same flight arriving".

**Candidate extra conditions**, fixed in advance so the data cannot be searched until something fits:

| Name | Condition on a hole linked only by descent | Why it might separate the cases |
|---|---|---|
| C1 floor | the new edge's lowest altitude is at or above `link_descent_edge_floor_ft` (10,000 ft) | Higher on descent means further from the airport, so less of the hole is spent on the ground. |
| C2 callsign | a callsign is present on **both** sides and equal | Blank callsigns are where every hidden landing got through in version 4; direction is weak evidence, so demand strong identity evidence with it. |
| C3 shorter hole | hole at most 360 min when the new edge is descending | Less room for a turnaround. |
| C1+C2, C2+C3, C1+C3, C1+C2+C3 | | |

No other condition is tried. In particular nothing that needs reference data (distance to an aerodrome) or a new read behind the edge: the check must stay a function of the features the rule already reads, so `required_lookback_hours` and the two-day claim consistency are untouched.

**Limits** (the v4 per-edge-kind limits, pooled over the two days 2026-06-14 and 2025-06-14): among real holes linked only by descent *and* passing the candidate, **0 false joins** among those checkable (that is what "at most 0.1%" means below 1,000 cases); among `cruise_descent` hidden-landing controls passing the candidate, joined **at most 1%** as a 95% upper bound (Clopper–Pearson, `rule_of_three_upper`'s neighbour in `track_link_controls.py`), and **at most 5%** among those with the same or a blank callsign. A candidate that leaves fewer than 50 checkable real holes is reported but not adoptable: it would not be a measurement.

**Decision rule.** Adopt the candidate with the fewest conditions that meets all three limits and still links at least 20 of the real holes on flights crossing the box over the two days (a gain worth a setting). Ties go to C2 alone (one bool, no new threshold). If none qualifies, `link_new_edge_descent` stays off and the report states the counts for each candidate, so the next reader does not repeat the search.

**If adopted.** `SegmentationConfig.link_new_edge_descent` becomes `True`, plus only the field(s) the winning candidate needs: `link_descent_edge_floor_ft: float = 10000.0` (C1), `link_descent_needs_callsign: bool = True` (C2), `link_descent_max_gap_minutes: float = 360.0` (C3). Each in aviation units with the unit in the name. The checks go into `link_checks` beside the existing descent block, as part of `cruise_at_new_edge` (they qualify the edge; they are not new tests, so the reasons reports keep their test names). The grid script accepts them through `--grid-json` as any `link_*` field.

**Sequence with the running v5 measurement.** One Spark driver at a time on this pod. The `debounced` arm (steps 01–04b) is running now. Then: Task 1's export (cluster, both days) → Task 2 (offline) → Task 3 (code, local tests) → the v5 `two_days` grid with the four edge settings at the adopted values → the `cruise_linked` arm → `pipeline_day`. So the v5 re-measurement (v5 plan Task 5) runs once, on the final rule, and the v5 report (Task 6) carries the descent result either way. Deleting the study's state vectors waits until the last arm has run.

## Global Constraints

- Native Spark only; no `traffic`, no pandas UDFs inside the pipeline. Pandas is for the offline scoring over staged CSVs only.
- Thresholds in aviation units with the unit in the field name; compare SI storage through the existing factors (`FT_PER_M`, `FPM_PER_MPS`, `KT_PER_MPS`).
- `debounced`, `recommended`, `legacy` unchanged. `link_new_edge_descent=False` must still reproduce today's A10 exactly (a test).
- No new read behind the old edge or past the new edge: `required_lookback_hours` and the claim tests stay as they are.
- A false join is worse than a split: every new condition fails towards splitting when a value it needs is missing.
- Every CSV the report reads is staged with provenance; the report builder refuses a missing value.
- Bucket: write only under `s3a://eurocontrol/opdi-world/research/a10/`; nothing new on S3 is needed for Tasks 1–2 (they read tracks and write local CSVs).

## Review Focus

1. A hole where the new edge is descending **and** the old edge passed only by climbing: both allowances at once is the `both` setting version 4 rejected; the adopted condition must apply there too, and the test in Task 3 covers it.
2. A descending new edge whose vertical rate is NaN or on two samples only: must not pass (Task 3 test keeps the three-rate rule under the new condition).
3. C2 with a callsign present on one side only: "present on both sides and equal" fails, not "blank agrees" (Task 3 test).
4. The offline scorer and the grid must agree: the count of real holes linked only by descent under the adopted setting in `link_grid_pooled.csv` must equal the per-hole export's count after the candidate filter (Task 4 check).
5. A candidate that filters the checkable set down to a handful reads as "0 false joins": the 50-case floor in Task 2 and the upper bound both appear in the report table.

---

### Task 1: Export the holes the descent question is about

**Files:**
- Create: `benchmarks/track_link_descent.py`
- Test: `tests/test_track_link_descent.py`

**Interfaces:**
- Consumes: `track_link_controls.real_hole_candidates(tracks, p)`, `label_real_holes(holes, truth)`, `negative_controls(...)` (family `cruise_descent`), `track_link_grid.load_tracks(day)`, `link_feature_columns(p)` / `select_link_features`, `link_checks(f, p)`.
- Produces: per day, `descent_holes_{day}.csv` with one row per real hole whose new edge is below the floor and descending (all other checks passing or not), columns `track_id, t_old_edge, t_new_edge, gap_min, population` (`crosses_box`/`other`), `label` (`true_link`/`false_join`/`unknown`/`airframe_not_in_truth`), `alt_after_ft, vr_after_fpm, vr_after_samples, alt_before_ft, vr_before_fpm, implied_kt, cs_before, cs_after, callsign_state` (`both_equal`/`differ`/`one_blank`/`both_blank`), `other_checks_pass` (bool: every check except `cruise_at_new_edge`), `event_near_edge`; and `descent_controls_{day}.csv` with one row per `cruise_descent` negative control: `pair_key, requested_gap_min, depth_min, same_callsign, alt_after_ft, vr_after_fpm, vr_after_samples, implied_kt, gap_min, callsign_state, other_checks_pass, joined` (bool: the rule with `link_new_edge_descent=True` links it).

- [ ] **Step 1: Write the failing tests** (synthetic tracks from `tests/test_track_link_controls.py`'s helpers)

```python
def test_a_descending_new_edge_hole_is_exported_with_its_label_and_features(spark)
    # cruise piece, 191 min hole, new piece at 12,000 ft descending 1,000 ft/min, truth: one flight → label true_link, other_checks_pass True, callsign_state both_equal
def test_a_hole_whose_new_edge_is_at_cruise_is_not_exported(spark)
def test_callsign_state_names_the_four_cases(spark)   # both_equal, differ, one_blank, both_blank
def test_a_cruise_descent_control_row_says_whether_the_rule_joined_it(spark)
def test_the_export_writes_provenance(tmp_path)
```

- [ ] **Step 2: Run them** — `pytest tests/test_track_link_descent.py -q` — Expected: FAIL, module missing.
- [ ] **Step 3: Implement** `descent_holes(tracks, truth, p) -> DataFrame` and `descent_controls(samples, truth, p) -> DataFrame` in `benchmarks/track_link_descent.py`, built on the consumed functions (features through `select_link_features`; the descent test reproduced from `link_checks` with `link_new_edge_descent=True`), plus `main()` with `--days`, `--results-dir`, `--executors`, writing both CSVs per day via `provenance.record` with `_CODE_PATHS` naming `methods.py`, `base.py`, `track_link_controls.py`, `track_link_grid.py`, itself.
- [ ] **Step 4: Run the tests** — Expected: PASS.
- [ ] **Step 5: Run on the cluster** (after the `debounced` arm has finished, and before any other driver): `.venv310/bin/python -u benchmarks/track_link_descent.py --days 2026-06-14 2025-06-14 --results-dir <portal>/papers/track-construction-v5/data/descent` — Expected: four CSVs and a `_manifest.json`; the number of rows with `other_checks_pass` and `alt_after_ft >= 5000` equals, within the two days, the grid's `linked_by_descent` at the shipped settings with descent on (compare against `two_days/link_grid_pooled.csv` once Task 4 has run; note the figure now).
- [ ] **Step 6: Commit** (code and tests in `opdi`; CSVs in the portal paper directory).

### Task 2: Score the candidates offline

**Files:**
- Modify: `benchmarks/track_link_descent.py` (add `--score`)
- Test: `tests/test_track_link_descent.py`

**Interfaces:**
- Consumes: the CSVs of Task 1.
- Produces: `descent_candidates.csv`, one row per candidate (`none`, `C1`, `C2`, `C3`, `C1+C2`, `C2+C3`, `C1+C3`, `C1+C2+C3`): `candidate, conditions, real_linked, real_crossing_linked, real_checkable, real_false_joins, real_unknown, false_join_upper_95, controls, controls_joined, controls_joined_upper_95, same_callsign_controls, same_callsign_joined, same_callsign_upper_95, within_limits, adoptable` (within limits and `real_checkable >= 50` and `real_crossing_linked >= 20`); and `descent_false_joins.csv`, the false-join rows of Task 1 in full, so the report can describe them.

- [ ] **Step 1: Failing tests**

```python
def test_candidates_are_scored_from_the_exported_rows(tmp_path)   # hand-made CSVs; C2 removes the one false join; within_limits True for C2, False for none
def test_a_candidate_leaving_under_50_checkable_holes_is_not_adoptable(tmp_path)
def test_the_chosen_candidate_is_the_one_with_fewest_conditions(tmp_path)   # ties → C2
def test_the_upper_bounds_match_the_studys(tmp_path)   # against track_link_controls' Clopper–Pearson on a known case
```

- [ ] **Step 2: Run** — Expected: FAIL.
- [ ] **Step 3: Implement** `score_candidates(holes: pd.DataFrame, controls: pd.DataFrame) -> pd.DataFrame`, `choose(scored: pd.DataFrame) -> Optional[str]`, and the `--score` path of `main()` reading the staged CSVs and writing the two outputs with provenance. Candidate definitions as one dict `CANDIDATES = {"C1": lambda r: r.alt_after_ft >= 10000, "C2": lambda r: r.callsign_state == "both_equal", "C3": lambda r: r.gap_min <= 360}` and the combinations as conjunctions.
- [ ] **Step 4: Run tests** — Expected: PASS.
- [ ] **Step 5: Run it** (no cluster): `python benchmarks/track_link_descent.py --score --results-dir <portal>/papers/track-construction-v5/data/descent` — Expected: `descent_candidates.csv` with 8 rows. Read it. Record the decision in the ledger as `Ruling: descent <adopted candidate or none> — <the row's numbers> — cost if wrong`.
- [ ] **Step 6: Commit.**

### Task 3: The rule, if a candidate was adopted

Skip to Task 3′ if none was.

**Files:**
- Modify: `src/opdi/config.py` (`SegmentationConfig`: `link_new_edge_descent = True`, plus the winning field(s) with docstrings stating the measured counts), `src/opdi/pipeline/segmentation/methods.py:1087-1093` (`link_checks`' descent block), `src/opdi/pipeline/segmentation/__init__.py` if `SegmentationParams` lists fields explicitly, `readme.md` (the A10 section's sentence on descent).
- Test: `tests/test_segmentation_cruise_linked.py`, `tests/test_track_claim.py`.

- [ ] **Step 1: Failing tests** (at `SHIPPED`)

```python
def test_a_descending_new_edge_links_under_the_adopted_condition(spark)      # e.g. 12,000 ft, −1,000 fpm, DLH1 both sides → 1 track
def test_a_descending_new_edge_failing_the_condition_splits(spark)           # C1: 8,000 ft; C2: blank after; C3: 400 min hole — whichever was adopted
def test_a_callsign_on_one_side_only_does_not_satisfy_the_descent_condition(spark)   # if C2
def test_two_vertical_rates_do_not_make_a_descent(spark)
def test_descent_with_a_climbing_old_edge_is_also_subject_to_the_condition(spark)
def test_descent_off_reproduces_the_previous_rule_exactly(spark)             # partition equality on the HOLE/FLICKER fixtures and a descent fixture
def test_the_descent_fields_are_aviation_units_with_the_unit_in_the_name()   # extends test_detection_config's rule to the new fields
def test_required_lookback_is_unchanged_by_the_descent_condition()           # still 11
```

- [ ] **Step 2: Run** — Expected: FAIL (fields unknown / 2 tracks where 1 expected).
- [ ] **Step 3: Implement** the field(s) and extend the descent block in `link_checks`: `new_edge_by_descent = (alt_after < floor) & (alt_after >= low_alt_ft) & (vr_after < -rate) & (vr_after_samples >= 3) & <condition>`; for C2 the condition is `f["cs_before"].isNotNull() & f["cs_after"].isNotNull() & (f["cs_before"] == f["cs_after"])`. Docstrings: why, and the counts from `descent_candidates.csv`.
- [ ] **Step 4: Run** `tests/test_segmentation_cruise_linked.py tests/test_track_claim.py tests/test_detection_config.py tests/test_segmentation_default.py tests/test_tracks_method.py` — Expected: all pass.
- [ ] **Step 5: Commit.**

### Task 3′: If no candidate was adopted

- [ ] Add one paragraph to the `link_new_edge_descent` docstring in `config.py` with the candidate table's headline numbers and the file they come from; the readme sentence likewise. Commit. (The report section in Task 5 carries the full table.)

### Task 4: The v5 measurement on the final rule

This is v5 plan Task 5 with the descent settings added; it runs once.

- [ ] **Step 1:** `benchmarks/regenerate_track_v5.py`: the `two_days` job's `--grid-json` names the adopted field(s) at their shipped value(s) (and, if adopted, also the pre-adoption values so the report can show the descent case with and without the condition); `tests/test_regenerate_track_v5.py` updated accordingly. Commit.
- [ ] **Step 2:** `regenerate_track_v5.py --only two_days` (cluster). Expected: the shipped row `within_limits == True`; its `linked_by_descent` equals Task 1's exported count after the adopted filter (Review Focus 4; note both numbers in the ledger).
- [ ] **Step 3:** `run_track_link_day.py --day 2026-06-14 --arm cruise_linked --steps 02 02a 03 04 04b --timings-csv …` (cluster), then `regenerate_track_v5.py --only pipeline_day`, then `--check`. Expected: all current; `link_consistency.csv` has 0 rows without a track and 0 unchanged tracks with other airports or events; `link_level_across_hole.csv` 0 segments across a hole; `link_distance_across_hole.csv` `stored_distance_verdict == agrees`; `link_movements.csv` passed.
- [ ] **Step 4:** Delete this study's state vectors and raw tracks under `research/a10` (`$CLAUDE_JOB_DIR/tmp/prune_a10.py`). Expected: free space back above 8 GB.

### Task 5: The report says what happened to descent

**Files:**
- Modify: `papers/track-construction-v5/report_template.md` (a subsection "Reappearing on descent" under Results, and the Limits bullet), `benchmarks/build_track_v5_paper.py` (`descent/descent_candidates.csv` → `table_descent_candidates`; the adopted name; the false-join descriptions), `tests/test_build_track_v5_paper.py`, meta `CLAUDE.md` (one sentence in the track-identity paragraph).

- [ ] **Step 1:** Failing test: `test_the_descent_table_lists_every_candidate_and_names_the_adopted_one` (8 rows; the adopted candidate's row marked; `none` when nothing adopted).
- [ ] **Step 2:** Implement in `build_context`; write the subsection in plain words: what a descending edge is, why v4 turned it off (3 of 119), what was tried (the fixed list, so the reader knows the data was not searched), the table, what was adopted and what it changes for crossing flights, or why nothing was.
- [ ] **Step 3:** Build (`--check` must pass after the build), rebuild the PDF; `tests/test_build_track_v5_paper.py` passes. Commit in `opdi`, `opdi-portal`, then the meta repo.

Then the v5 plan continues at its Task 6 (report finish, card, freeze v4 — the parts not done yet) and Task 7 (whole-branch review).
