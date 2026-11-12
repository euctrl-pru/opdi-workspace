# Milestone Plausibility Checks Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove flight events that cannot be true (for example an off-block, in-block, take-off or landing time placed while the aircraft is in the air) before they reach the events table, the flight list and the published files, without removing correct ones.

**Architecture:** One new module, `src/opdi/pipeline/plausibility.py`, sits between event detection and the write in step 04. It looks up what the aircraft was doing at each event's time (height above the airport, ground speed, on-ground flag) from the track's own position reports, applies a small table of rules per event type, and splits the events into kept and rejected. Rejected events go to a working table with the reason, never to the published files. The flight-list milestone columns are derived from the kept events, so they follow. Thresholds are not guessed: Task 1 measures them on real data against airport records, and a rule is adopted only if it removes almost no event that the airport records confirm.

**Tech Stack:** PySpark column expressions and window functions (no UDF), pytest with the local `spark` fixture, the existing benchmark tooling for the measurement.

**Spec:** the owner's request of 2026-11-12, verbatim: "I noticed earlier a lot of unplausable aibt and aobt and atot and aldt. For example aibt in the air or aobt in the air. Similar for atot aldt. Could you put in place checks for all milestones to mask / delete them if they are erronous / unlikely? ... Make sure not to delete correct milestones". The owner approved the plan in advance and is not reachable; decisions are taken here and recorded.

**Worktrees:** code `/home/jupyter/work/opdi-workspace/opdi/.claude/worktrees/industrialisation` (branch `industrialisation`, draft PR 28); portal `/home/jupyter/work/opdi-workspace/opdi-portal/.claude/worktrees/publication` (branch `feature/new-topics`).

## Decisions taken

- **Remove, and keep a record.** A rejected event is left out of `opdi_flight_events` and its measurements out of `opdi_measurements`. It is written to `opdi_flight_events_rejected` (same columns plus `reject_rule`, `reject_detail`), partitioned by `dof`, so every removal can be audited and a rule can be re-examined. Nothing rejected is published.
- **Only positive evidence removes an event.** A rule fires only when the facts it needs are known and clearly contradict the event. An event whose context is unknown (no altitude near its time, no airport elevation) is kept.
- **Default on for release v2.0.0, off in the v0.0.2 settings.** `EventConfig.plausibility_checks: bool = True`; `EventConfig.legacy()` sets it `False`. The v0.0.2 golden outputs stay byte-identical.
- **Correct flights lose nothing.** The default golden outputs (six simulated, physically consistent flights) must also stay identical with the checks on. This is the first proof that correct milestones are kept; the measurement of Task 1 is the second.
- **Adoption rule for a threshold.** Among events that airport records confirm (same flight, same milestone, within 60 s), a rule may remove at most 0.1%, and each such removal is inspected and explained in the study note. Among the events a rule removes for which airport records exist, the share far from the record (more than 300 s) is reported. A rule that fails the first condition is not adopted, or its threshold is widened until it passes.
- **Event versions are unchanged** (`events_v0.3.0` has not been published yet).

## Global Constraints

- Native Spark only; thresholds in aviation units with the unit in the field name (`_ft`, `_kt`, `_s`, `_nm`, `_min`); storage units converted at the comparison with the existing constants.
- One heavy Spark job at a time; the pod has a 16 GB memory limit; cluster runs detached (`setsid nohup script`) with their own log; bucket `eurocontrol` is near its quota: write only small result tables under `s3a://eurocontrol/opdi-world/research/plausibility/` and delete them at the end; read everything else.
- Tests: `.venv310/bin/python -m pytest tests/<files> -q`, one process at a time. `tests/test_golden_outputs.py` must pass without regenerating golden files. `ruff check .` and `black --check .` (in `/home/jupyter/work/opdi-workspace/opdi/.venv/bin/`) clean.
- Writing style (owner's rule): every text is well structured and readable without background knowledge; readers are researchers and students. Define each term on first use, ordinary words, short sentences, units on numbers. No AI or internal wording: not "gate", "guard", "rung", "ladder", "arm", "pin", "load-bearing", "surface" as a verb, "robust", "leverage". No development history in code or portal pages.
- No Claude attribution in commits. No push to `main`. The portal is not merged to `main`.

## Review Focus

1. An event with no usable context (altitude blanked by cleaning, aircraft not received near the event) is kept, not removed.
2. An airport far above sea level (for example 5,000 ft elevation): heights are measured above the airport, so ground events there are kept.
3. A touch-and-go or a go-around, and a flight that lands and departs again within one track: real take-offs and landings in the middle of a track are kept.
4. A helicopter or a slow light aircraft: low speed at take-off and landing does not make its milestones implausible.
5. Removing an event removes its measurements too, and the flight-list column derived from it becomes empty rather than keeping the removed time.

---

### Task 1: Measure what implausible milestones look like

**Files:** Create `benchmarks/milestone_plausibility.py`, `tests/benchmarks/test_milestone_plausibility.py`; output CSVs staged under the portal at `papers/milestone-plausibility-v1/data/` with provenance (`benchmarks/provenance.py`).

**Interfaces — Produces** (used by Task 2 to fix thresholds):
- `event_context(events: DataFrame, tracks: DataFrame, airports: DataFrame, window_s: int) -> DataFrame` adding per event: `ctx_height_ft` (barometric altitude of the nearest position report within `window_s`, minus the elevation of the airport the event belongs to: departure airport for departure-side types, destination for arrival-side types, nearest airport for surface events), `ctx_speed_kt`, `ctx_vertical_rate_ft_min`, `ctx_on_ground` (share of reports flagged on ground in the window), `ctx_gap_s` (time to the nearest report), `ctx_airport_distance_nm`.
- CSVs: `context_by_type.csv` (per event type: count, share with known context, percentiles 1/5/25/50/75/95/99 of height, speed, vertical rate, airport distance); `sequence_by_flight.csv` (counts of each order violation and of time differences between paired milestones: AOBT→take-off roll→ATOT, ALDT→runway vacated→AIBT, ATOT→ALDT, with percentiles); `candidate_rules.csv` (per rule and threshold: events removed, share of that type, and, where airport records exist, confirmed events removed and the share of removed events more than 300 s from the record); `examples.csv` (50 removed events per rule with their context, for reading).

- [ ] Step 1: read how the flight-events study scored events against airport records (`benchmarks/event_bench.py`, `benchmarks/regenerate_*`, `opdi-benchmarker/` in the workspace, tables under `s3a://eurocontrol/opdi/research/events_2026_V07_shipped` and `opdi-world/research/benchmarker`) and choose the data: a period for which events from today's default settings, the tracks they were built from, and airport records all exist. If today's default has no such run, re-run step 04 for the smallest sufficient sample under the study prefix. Also profile one worldwide day (2026-06-14, `opdi-world/research/airspace_events/on` with tracks from `opdi-world/osn_tracks_clean`) without airport records, to see the worldwide picture. Record the choice and its limits.
- [ ] Step 2: unit tests for `event_context` on hand-built frames (event between two reports takes the nearer; no report within the window gives NULL context; airport elevation subtracted; departure-side and arrival-side types use the right airport). Red, implement, green.
- [ ] Step 3: run the measurement on the cluster (detached). Stage the CSVs. Read `examples.csv` for each candidate rule and write down, per rule, what the removed events are (overflight crossing a stand polygon, track cut by a reception gap, altitude error, ...).
- [ ] Step 4: propose the rule table with thresholds that satisfy the adoption rule, each with its evidence line. Candidate rules to evaluate (add others the data suggests; drop those the data does not support):
  - `airborne_ground_event`: a ground event (`AOBT`, `AIBT`, `line-up`, `take-off-roll`, `runway-vacated`, `runway-crossing-entry`, `runway-crossing-vacated`, every `entry-`/`exit-` surface event) while height above the airport exceeds H ft and ground speed exceeds S kt.
  - `runway_time_too_high`: `ATOT`, `ALDT`, `landing` at a height above the airport over H ft.
  - `block_after_takeoff` / `block_before_landing`: `AOBT` not before `ATOT`; `AIBT` not after `ALDT` (the block time is removed).
  - `taxi_too_long`: `ATOT − AOBT` or `AIBT − ALDT` over M minutes (the block time is removed).
  - `takeoff_after_landing`: an `ATOT` later than the same track's `ALDT` when the track holds one flight; decide from the data which of the two is wrong, or adopt nothing.
  - `top_order`: `top-of-descent` before `top-of-climb`; a top below a minimum height above both airports.
  - `far_from_airport`: a runway or block milestone more than D NM from the airport it is attributed to.
- [ ] Step 5: commit the script, tests and staged data. Report the rule table, the evidence, and the items that could not be judged.

### Task 2: The checks in the pipeline

**Files:** Create `src/opdi/pipeline/plausibility.py`, `tests/test_plausibility.py`; modify `src/opdi/config/events.py` (fields), `src/opdi/pipeline/events.py` (`_etl_flight_events_and_measures`: apply after the families are combined and before identifiers are assigned; write the rejected table), `src/opdi/pipeline/flight_list_step.py` or `flight_list_milestones.py` only if a column there is not derived from the events table, `src/opdi/orchestration.py` (prune list unchanged; the rejected table is a working table kept with events), `readme.md`.

**Interfaces — Consumes** Task 1's `event_context` (moved into the package module; the benchmark imports it from there) and rule table. **Produces:**
- `EventConfig.plausibility_checks: bool = True` and one threshold field per adopted rule, named for what it limits with its unit, each documented in plain words.
- `plausibility.RULES: tuple[Rule, ...]` with `Rule(name: str, applies_to: tuple[str, ...], description: str)`; `plausibility.check_events(events: DataFrame, tracks: DataFrame, airports: DataFrame, config: EventConfig) -> tuple[DataFrame, DataFrame]` returning (kept, rejected); `rejected` carries `reject_rule` (first rule that fired, in the order of `RULES`) and `reject_detail` (JSON of the facts used).
- Working table `opdi_flight_events_rejected` (`REJECTED_TABLE`), partition `dof`, overwritten with the day.

- [ ] Step 1: tests, one per rule, each with a failing event and its nearest passing neighbour (just inside the threshold), plus the five Review Focus cases as tests: unknown context kept; a 5,000 ft airport; a touch-and-go and a two-leg track; a 40 kt helicopter departure; the removed event's measurements absent and the flight-list column empty after step 04b on a small local warehouse. Plus: `plausibility_checks=False` returns the input unchanged; `EventConfig.legacy().plausibility_checks is False`.
- [ ] Step 2: red. Step 3: implement. Step 4: green; `tests/test_golden_outputs.py` passes untouched for both configurations; the suites of `events`, `periodrun`, `published` pass.
- [ ] Step 5: step 04 prints, per day, events kept and removed per rule. Commit `events: plausibility checks remove milestones the track contradicts`.

### Task 3: Confirm on real data

- [ ] Step 1: run step 04 with the checks on for the Task 1 data (study prefix), and the scoring against airport records before and after. Stage `before_after.csv`: per milestone, events, confirmed-within-60 s count (must not drop by more than the adoption rule allows), coverage, share within 60 s and 300 s, 90th percentile of the absolute difference.
- [ ] Step 2: if a confirmed event was removed, inspect it and either widen the threshold or record why it stays. Delete the scratch tables. Commit.

### Task 4: Tell the users

**Files (portal):** Create `papers/milestone-plausibility-v1/index.qmd` (short study note: what was measured, on which data, the rule table, results, limits; card in `papers/index.qmd`), `content/methods/plausibility-checks.qmd` (the nine-heading template; numbers registered through `tools/draw_validation_figures.py` from the staged CSVs; one figure of the height-above-airport distribution of a ground milestone with the threshold marked); modify `_quarto.yml` (sidebar, after Measurements), `content/methods/index.qmd` and `pipeline.qmd` (one sentence and link), the "How it is computed" and "Limitations" sections of `block-times.qmd`, `takeoff-landing.qmd`, `runway-milestones.qmd`, `surface-events.qmd` (one sentence each naming the checks that apply), `content/data/limitations.qmd`, `tools/check_site.py` if the new page needs registering.

- [ ] Step 1: write, run `tools/check_site.py`, `check_papers_index.py`, `pytest tools/tests`. Step 2: commit and push `feature/new-topics`.

### Task 5: Review and hand over

- [ ] Fresh review of the code range of Tasks 1-3 against this plan's Review Focus; fix Critical and Important findings test-first; full suite in chunks; push `industrialisation`; update the draft PR description and `docs/industrialisation-handover.md`.
