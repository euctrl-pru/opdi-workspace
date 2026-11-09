# Cruise-linked tracks as the default Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix what blocked `cruise_linked` (level segments and distance across a gap, unexplained side effects), re-check the rule, make it the default track rule, and publish a short v5 report.

**Architecture:** One new notion, a *hole* (a silence over 30 min inside a track), computed once per track as a `piece` index. Detectors that pair or interpolate samples partition on `(track_id, piece)`. Distance reads the last known position. Then the default pointer moves and the study is re-run on the final code.

**Tech Stack:** PySpark, pytest with the local `spark` fixture, reportlab in a scratch venv for the PDF.

**Spec:** `docs/superpowers/specs/2026-11-09-cruise-linked-default-design.md`

## Global Constraints

- Worktrees: `opdi/.claude/worktrees/track-construction-v4`, `opdi-portal/.claude/worktrees/track-construction-v4`, `.claude/worktrees/track-construction-v4`, all on branch `track-construction-v5`. Never `git add -A` in the code worktree (`data` is an untracked symlink).
- Python: `opdi/.venv310/bin/python -m pytest <file> -q -p no:cacheprovider`.
- Tracks without a hole must give identical events before and after. `debounced`, `recommended`, `legacy` keep their partitions.
- `track_hole_minutes` is 30.0 and equals `SegmentationConfig.gap_minutes`; a test holds them equal.
- New measurement version `distance_v0.0.3`; no event version changes.
- One Spark driver at a time. Stop under 3 GB free of 200 GB. Write only under `s3a://eurocontrol/opdi-world/research/a10/`.
- Report: plain language, structured, each new term introduced where first used, every number through the builder.
- Baseline: 9 tests fail from a nested worktree (`test_check_report_numbers`, `test_events_labelling`, `test_flight_detection`, `test_flight_list_v7_redirect`, 2 in `test_h3_airport_layouts`, 2 in `test_regenerate_track_v2`, `test_regenerate_v62`).
- Commit trailer: `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>` and `Claude-Session: https://claude.ai/code/session_01F8JMNXryGq6PJRccES7ESa`.

## Review Focus

1. A track with two holes: three pieces, level events per piece, one `first_seen`, one top of climb. Pinned in Task 2.
2. A hole whose edge samples have blank positions: distance still adds the straight line between the nearest known positions. Pinned in Task 2.
3. A track with no hole and blank positions mid-track: distance no longer loses segments, and events are otherwise identical. Pinned in Task 2.
4. A flight-level crossing that happens inside a hole: no event. Pinned in Task 2.
5. A caller that names `debounced` after the switch still gets A9 with a 3 h lookback. Pinned in Task 4.

---

### Task 1: Trace the unexplained changes (analysis)

**Files:** scratch only; findings go to the ledger and the report.

- [ ] **Step 1:** From `research/a10/{debounced,cruise_linked}` (`opdi_flight_list`, `opdi_flight_events`, `osn_tracks_clean`, 2026-06-14) and `reference/flights_202606.parquet`, list the inside-Europe flights whose row had both airports right under `debounced` and not under `cruise_linked`, with both rules' rows, track extents and the linked holes.
- [ ] **Step 2:** Do the same for the tracks behind the `entry-taxiway`/`exit-taxiway` changes.
- [ ] **Step 3:** Name the mechanism for each group. Expected output: a table of cases by cause. A cause in the rule or a detector becomes a failing test in Task 2 or 3.
- [ ] **Step 4:** On the `debounced` clean table, measure how much cumulative distance the blank-position bug loses per track (median and share of tracks over 1 NM), for the report.

### Task 2: Detectors do not work across a hole

**Files:** Create `src/opdi/pipeline/holes.py`, `tests/test_holes.py`. Modify `src/opdi/pipeline/events.py`, `src/opdi/config.py` (`EventConfig`), tests of the touched detectors.

**Interfaces:** Produces `add_piece(df, hole_minutes: float, track_col="track_id", time_col="event_time") -> DataFrame` adding integer `piece` (0 for the first), and `EventConfig.track_hole_minutes: float = 30.0`.

- [ ] **Step 1: Failing tests**

```python
def test_piece_counts_holes_per_track(spark):                 # gaps 10, 31, 45 min -> pieces 0,0,1,2
def test_a_gap_of_exactly_the_threshold_is_not_a_hole(spark)
def test_hole_threshold_equals_the_segmentation_gap():        # EventConfig().track_hole_minutes == SegmentationConfig().gap_minutes
def test_level_events_do_not_span_a_hole(spark):              # cruise, 3 h hole, cruise: level-end before, level-start after
def test_a_track_without_a_hole_gives_the_same_events(spark): # byte-identical frames, flag on and off
def test_two_holes_three_pieces_one_first_seen(spark)
def test_level_off_segments_do_not_span_a_hole(spark):        # both arms in LEVEL_ARMS
def test_a_crossing_inside_a_hole_is_not_emitted(spark):      # FL240 before, FL350 after: no xing-fl245
def test_distance_reads_the_last_known_position(spark):       # blank mid-track: total equals the sum over known positions
def test_distance_adds_the_straight_line_across_a_hole(spark) # blank edges too
def test_distance_version_is_v003()
```

- [ ] **Step 2:** Run to see them fail.
- [ ] **Step 3: Implement.** `add_piece` is one `lag` and a running sum. In `events.py`: attach `piece` once where the track frame is prepared; phase transition windows (`prev_phase`, `next_phase`, segment counting) partition on `track_id, piece`; `LEVEL_ARMS[...]`, flight-level and ring crossings get `piece` in their partition columns; `add_distance_measure` lags the last non-blank `(lat, lon)` pair. Top of climb, top of descent, `first_seen`, `last_seen` and milestones stay per track. Version string to `distance_v0.0.3`.
- [ ] **Step 4:** Run the new tests and every existing events, level, crossing and distance test file. Expected: PASS, with no existing expectation edited except the distance version.
- [ ] **Step 5:** Commit.

### Task 3: Audit and fix the rule

**Files:** `src/opdi/pipeline/segmentation/methods.py`, `tests/test_segmentation_cruise_linked.py`, `tests/test_track_claim.py`, plus whatever the audit names.

- [ ] **Step 1:** Dispatch one fresh reviewer on the most capable model over the rule (`_edge_summary`, `_link_reads`, `link_checks`, `cruise_linked`, `required_lookback_hours`, the claim path in `tracks.py`): bugs, read-window dependence, and simplifications that remove code without changing a decision.
- [ ] **Step 2:** Fix the known item: same-second edge samples ordered by time then a key that is not the value being read. Test: six samples in one second, the kept five do not depend on altitude.
- [ ] **Step 3:** For each audit bug: a failing test, the fix, the test passing. Simplifications only if they remove code and every existing test passes unchanged. No new option.
- [ ] **Step 4:** Run the segmentation, claim and study test files. Commit.

### Task 4: Make it the default

**Files:** `src/opdi/pipeline/tracks.py`, `src/opdi/config.py`, `readme.md`, `tests/test_tracks_method.py`, `tests/test_segmentation_default.py`, any benchmark that relies on `standard` meaning A9.

- [ ] **Step 1: Failing tests**

```python
def test_standard_resolves_to_cruise_linked()
def test_debounced_by_name_is_still_a9_with_a_3h_lookback()
def test_the_default_run_reads_11_hours_back()
def test_a_transatlantic_hole_is_one_track_by_default(spark)
```

- [ ] **Step 2:** Move the pointer; update the `method` docstring (A10 is the default, what it does, the boundary, how to reproduce A9 and A8), the `cruise_linked` docstring ("research only" goes), `readme.md`. Benchmarks whose meaning was "A9" name `debounced` explicitly (`track_pipeline_v2.py`, `track_continuity.py`, `regenerate_track_v2.py` keep `standard` only if their papers meant "the default").
- [ ] **Step 3:** Run `test_tracks_method`, `test_segmentation_default`, `test_track_claim`, `test_periodrun`, `test_run_track_link_day`. Commit.

### Task 5: Re-measure on the final code (cluster)

**Files:** Create `benchmarks/regenerate_track_v5.py`, `tests/test_regenerate_track_v5.py`. Modify `benchmarks/track_link_pipeline_score.py` and its test for one new output, `link_distance_no_hole.csv` (distance change on tracks without a hole) and step timings.

- [ ] **Step 1:** `regenerate_track_v5.py`, on the v4 entrypoint's pattern, paper `track-construction-v5`: job `two_days` (`track_link_grid.py`, both days, grid-json of the shipped setting and the same with climb off, to `data/two_days/`), job `pipeline_day` (`track_link_pipeline_score.py --day 2026-06-14`, to `data/pipeline_day/`). Tests as for v4.
- [ ] **Step 2:** Run `run_track_link_day.py` for `debounced` (01 02 02a 03 04 04b), then `cruise_linked` (02 02a 03 04 04b); record each step's duration from the logs.
- [ ] **Step 3:** `regenerate_track_v5.py --force`, then `--check`. Expected: all current.
- [ ] **Step 4:** Read the results against the spec's expectations: level segments across a hole 0; distance within 1 NM of the straight line; partition check passed; movements not lower; false joins as before. A miss goes back to Task 2 or 3.
- [ ] **Step 5:** Delete this study's state vectors and raw tracks.

### Task 6: The v5 report, and freezing v4

**Files:** Create `papers/track-construction-v5/report_template.md`, `benchmarks/build_track_v5_paper.py`, `tests/test_build_track_v5_paper.py`. Modify `papers/index.qmd`, `papers/track-construction-v4/` (frozen note, `OPDI_RENDER` default `skip`), meta `CLAUDE.md`.

- [ ] **Step 1:** `build_track_v5_paper.py` imports the generic parts of the v4 builder (CSV readers, placeholder rendering, Markdown to HTML, PDF, qmd) and holds only v5's context. It stops on a missing value and has `--check` and `--pdf`.
- [ ] **Step 2:** Write the report, about four pages: the problem with one example; terms (track, hole, edge, piece, false join) each defined once; the rule as a short list; results in two tables (two days, pipeline day); what changed since v4 (the two detector fixes, the distance bug on all tracks, the traced cases); what changes for users of the data (default, `track_id` boundary, distance version, how to get the old rule); limits. Plain language.
- [ ] **Step 3:** Build `index.qmd`, `index.html` and the PDF; verify the PDF text and two rendered pages; add the card; mark v4 superseded.
- [ ] **Step 4:** `CLAUDE.md`: the default is A10, the new boundary, the hole rule for detectors, `distance_v0.0.3`. Commit in all three repos, submodule-first.

### Task 7: Close

- [ ] Whole-branch review by a fresh reviewer on the most capable model over `f57cca3..HEAD`; one fix wave; re-run of anything the fixes touch; full test suite; send the PDF and HTML.
