# Blank altitudes and climb edges Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stop rejecting holes for a blank altitude, let the rule link an old edge still climbing if the safety limits allow, test the same for a descending new edge, and update the v4 report.

**Architecture:** One change to the rule's edge features, one split flag, one change to which low-edge test cases count, then a study run that decides the defaults and a regeneration of everything.

**Tech Stack:** PySpark, pytest with the local `spark` fixture, reportlab in a scratch venv for the PDF.

**Spec:** `docs/superpowers/specs/2026-11-04-climb-edges-design.md`, on top of `2026-11-03-cruise-linked-tracks-design.md`.

## Global Constraints

- Code worktree `opdi/.claude/worktrees/track-construction-v4`, portal worktree `opdi-portal/.claude/worktrees/track-construction-v4`, meta worktree `.claude/worktrees/track-construction-v4`. Branch `track-construction-v4` in all three. Never `git add -A` in the code worktree: `data` is an untracked symlink.
- Python: `opdi/.venv310/bin/python -m pytest <file> -q -p no:cacheprovider`. Export `OPDI_PAPER_DIR` to the portal paper for the regenerate and builder tests, and unset it for `test_regenerate_adsc_v1`.
- `debounced`, `recommended`, `legacy`, `standard` keep an unchanged partition.
- One Spark driver at a time on this machine: two at once fail with a broadcast error.
- Stop a cluster run under 3 GB free of the 200 GB bucket. Delete only what this study wrote under `research/a10/`.
- Report language: plain words for a data analyst. Every number comes through the builder.
- Baseline: 9 tests fail from a nested worktree (`test_check_report_numbers`, `test_events_labelling`, `test_flight_detection`, `test_flight_list_v7_redirect`, 2 in `test_h3_airport_layouts`, 2 in `test_regenerate_track_v2`, `test_regenerate_v62`).
- Commit trailer: `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>` and `Claude-Session: https://claude.ai/code/session_01F8JMNXryGq6PJRccES7ESa`.

## Review Focus

1. A hole whose first altitude arrives inside the window but after a low sample: the low sample still counts, since it is the nearest value. Pinned in Task 1.
2. A window with values on one side of a NULL run: both reads decide alike. Pinned in Task 1.
3. Climb edge at a low-altitude hole: a new edge at low altitude still splits, through the unchanged low-altitude rule. Pinned in Task 1.
4. Setting with climb on and descent off counts no `cruise_descent` case. Pinned in Task 2.
5. The study's feature cost grows with a 60-sample window: stage times must stay printed, and a run must stay under about 1 h per setting family. Checked in Task 3.

---

### Task 1: The rule

**Files:** Modify `src/opdi/pipeline/segmentation/methods.py`, `base.py`, `src/opdi/config.py`, `tests/test_segmentation_cruise_linked.py`, `tests/test_track_claim.py`.

**Interfaces:** Produces `link_old_edge_climb: bool = False`, `link_new_edge_descent: bool = False` on both config classes, replacing `link_directional_edges`.

- [ ] **Step 1: Write the failing tests**

```python
def test_altitude_is_read_past_blank_samples(spark):             # first 8 samples after the hole blank altitude, then cruise, at 1 Hz: links
def test_a_low_value_after_blanks_still_counts(spark):           # blanks, then 3,000 ft: splits
def test_no_altitude_in_the_window_splits(spark):                # blank for over 5 min: splits
def test_each_field_finds_its_own_samples(spark):                # altitude and rate blank on different rows: both read
def test_climb_edge_links_only_when_on(spark):                   # old edge 9,000 ft climbing, new cruise: 2 tracks off, 1 on
def test_descent_edge_links_only_when_on(spark):                 # new edge 9,000 ft descending: 2 tracks off, 1 on
def test_climb_on_does_not_allow_a_descent_edge(spark):
def test_a_low_new_edge_still_splits_with_climb_on(spark):       # below 5,000 ft after the hole
def test_two_days_decide_alike_with_blank_altitudes(spark):      # in test_track_claim, every sample claimed once
```

- [ ] **Step 2: Run to verify they fail.** Run the two test files. Expected: FAIL.
- [ ] **Step 3: Implement.** `_edge_summary` reads a time-bounded frame of `link_edge_position_minutes` and, per field, keeps the nearest `LINK_EDGE_SAMPLES` non-NULL values. Split the flag through `link_checks`. Update docstrings, `readme.md`.
- [ ] **Step 4: Run** the two files plus `test_segmentation_debounce`, `test_segmentation_base`, `test_tracks_method`. Expected: PASS.
- [ ] **Step 5: Commit.**

### Task 2: The study counts by what a setting allows

**Files:** Modify `benchmarks/track_link_grid.py`, `benchmarks/regenerate_track_v4.py`, `tests/test_track_link_grid.py`, `tests/test_regenerate_track_v4.py`.

- [ ] **Step 1: Failing tests**

```python
def test_climb_on_counts_only_climb_cruise():
def test_descent_on_counts_only_cruise_descent():
def test_both_on_counts_all_three_families():
def test_a_bad_climb_family_fails_a_climb_setting_only():
```

- [ ] **Step 2: Implement.** Low-edge limit counts per flags. `--grid-json` varies both flags; tie-break prefers fewer on. Real-hole columns split into `linked_by_climb*`, `linked_by_descent*`. Rename job `final_direction` to `final_edges`: speed [400], ratio [0.0], stop [0.0], gap [600], floor [18000], climb [false,true], descent [false,true]. Stage directory `data/final_edges/`. Remove `data/final_direction/`.
- [ ] **Step 3: Run** the two test files. Expected: PASS. **Commit.**

### Task 3: Decide (controller)

- [ ] **Step 1:** Run `track_link_grid.py` for the `final_edges` grid on both days, to a scratch directory. Expected: exit 0 in about 1 h; `link_recommended.csv` names a setting.
- [ ] **Step 2:** Rule on the outcome per spec; write it to the ledger. Check the crossing-flight share and the false joins, and whether the blank-altitude fix alone passes the limits.
- [ ] **Step 3:** Set the chosen flags as defaults in `base.py` and `config.py`, updating the `final_grid` job's grid to carry them; commit, with tests.

### Task 4: Re-run (controller)

- [ ] **Step 1:** `run_track_link_day.py --day 2026-06-14` for `debounced` (01 02 02a 03 04 04b), then `cruise_linked` (02 02a 03 04 04b).
- [ ] **Step 2:** `regenerate_track_v4.py --force`. Expected: exit 0 and `--check` all current.
- [ ] **Step 3:** Delete `research/a10` state vectors and `osn_tracks`.

### Task 5: Update the report

**Files:** `papers/track-construction-v4/report_template.md`, `benchmarks/build_track_v4_paper.py`, `tests/test_build_track_v4_paper.py`, `papers/index.qmd`, meta `CLAUDE.md`.

- [ ] **Step 1:** Rewrite for the final rule, keeping it about the best method. Remove the claim that the largest split group is aircraft on approach, and say what the data shows. Update the rule table, results, what keeps holes split, the section on direction (now climb and descent, one adopted or not), level segments and distance figures, choices, and reproduction. Check any claim with a number through the builder.
- [ ] **Step 2:** Rebuild `index.qmd`, `index.html` and the PDF, with `--pdf` in the scratch reportlab venv. `--check` passes. Update the card.
- [ ] **Step 3:** Run `test_build_track_v4_paper`. Commit in both repos.

### Task 6: Close

- [ ] Whole-branch review of the range since `ed3105f`, one fix wave, full suite, meta pointers, send the PDF and HTML.
