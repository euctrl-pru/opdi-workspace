# Threshold-Crossing Rename, Processing Speed and ADS-C Test Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rename the event `landing` to `xing-runway-threshold`; bring the processing of one worldwide day under one hour without significantly changing its results; and measure whether adding ADS-C position reports improves the results of today's pipeline.

**Architecture:** Three parts, run in this order because they share one code worktree and one cluster. Part 1 is a rename of one event type string. Part 2 first measures where the time goes (per step and per Spark stage), then applies changes to how the work is executed (reading inputs once, sharing sorts, parallel writes, Spark settings, executor count), each verified against the reference outputs and a real day. Part 3 runs the current default pipeline twice on the aircraft that send ADS-C, once without and once with those reports, and compares tracks, flight list and events.

**Tech Stack:** PySpark 4 on Kubernetes (driver on the pod), pytest with a local Spark session, the existing benchmark tooling and provenance stamping.

**Spec:** the owner's request of 2026-11-13, verbatim: "Call landing xing-runway-threshold. Also do a test to see if ads-c enhances the results. At last, also see if you can speed up all spark processing without changing results (significiantly, few changing events eg is not a problem). Check what can be done to speed up processing. Do a test. Try to get it under 1 hour of processing per worldwide day. Use /superpowers:writing-plans to write a plan and then execute it how you see fit."

**Worktrees:** code `/home/jupyter/work/opdi-workspace/opdi/.claude/worktrees/industrialisation` (branch `industrialisation`, draft PR 28); portal `/home/jupyter/work/opdi-workspace/opdi-portal/.claude/worktrees/publication` (branch `feature/new-topics`, never merged to `main`).

## Global Constraints

- The settings that reproduce release v0.0.2 give byte-identical output: the v0.0.2 frames of `tests/test_golden_outputs.py` pass with no regeneration, in every part.
- Speed changes: the default golden frames stay identical unless a change is deliberate, listed row by row, and explained in the commit message. On a real day, at least 99.9% of events of every type keep the same flight, type and time (to the second), and the flight list keeps the same flights with the same airports; every difference class is explained.
- Cluster: the pod hosts only the Spark driver and has a 16 GB memory limit with silent kills. One Spark job at a time. Long runs detached (`setsid nohup <script>`, own log ending in `run exit <code>`; wait by grepping the log for that line). Do not `source` `.env` in bash.
- Bucket `eurocontrol`: 200 GB quota, about 7 GB free on 2026-11-12. Check free space before every run. Write only under `s3a://eurocontrol/opdi-world/research/{speed,adsc_v2}/`, delete each scratch table as soon as its numbers are staged, and delete nothing that this work did not create. If a measurement does not fit, measure on a time slice or an aircraft sample, say so, and scale with the stated assumption.
- Native Spark only (no `applyInPandas`, no `pandas_udf`). Thresholds, units and published names unchanged except the rename of Part 1.
- Tests: `.venv310/bin/python -m pytest tests/<files> -q`, one process at a time; whole suite in chunks at the end of each part. Known failing set: 4 under `tests/benchmarks` that need the portal checkout, and 3 there that fail only by test order. `ruff check .` and `black --check .` clean (`/home/jupyter/work/opdi-workspace/opdi/.venv/bin/`).
- Writing style (owner's rule): every text is readable without background knowledge by researchers and students; terms defined on first use; ordinary words; short sentences; units on numbers. No "gate", "guard", "rung", "ladder", "arm", "pin", "robust", "leverage". No development history in code or on portal methods pages.
- No Claude attribution in commits or pull requests. No push to `main`.

## Review Focus

1. A stage that was made faster by caching or checkpointing: an executor lost mid-step must make the step fail and be re-run, never write partial or wrong data.
2. A parallel write: the day folder must still be replaced as a whole, and a re-run of the day must leave no file of the earlier run.
3. Settings tuned for a worldwide day on the cluster must not break the local test session or a small European day (few rows, one partition).
4. More executors: the driver stays under its 16 GB limit (no large collect, no broadcast above the limit).
5. The ADS-C comparison: a difference between the two runs must come from the ADS-C reports, not from running the two runs with different code, days or aircraft.

---

## Part 1: `landing` becomes `xing-runway-threshold`

### Task 1: Rename in the pipeline

**Files:** Modify `src/opdi/pipeline/runway_ops.py` (the type string and the milestone-number table), `src/opdi/pipeline/events.py`, `src/opdi/pipeline/plausibility.py` (type lists), `src/opdi/pipeline/flight_list_milestones.py` if it reads the type, `src/opdi/config/events.py` (docstrings), `src/opdi/output/schema.py` (descriptions), `readme.md`, tests that name the type, `benchmarks/` scripts that read today's default output by type name (not those that read v0.0.2 output, where `landing` is the older event of that name).

**Interfaces — Produces:** constant `RUNWAY_THRESHOLD_CROSSING = "xing-runway-threshold"` in `src/opdi/pipeline/runway_ops.py`, used wherever the default settings emit or read the event. The v0.0.2 event `landing` (end of descent, emitted only with `EventConfig.legacy()`) keeps its name.

- [ ] Step 1: test `test_the_threshold_crossing_is_named_xing_runway_threshold` (default settings on the golden day: the type exists, and no event of type `landing` exists); test that `EventConfig.legacy()` still emits `landing`. Red.
- [ ] Step 2: rename. Find every use with `grep -rn '"landing"\|landing' src tests benchmarks` and decide each by which event it means. Green.
- [ ] Step 3: `tests/test_golden_outputs.py`: v0.0.2 identical. Default: only the `type` value of those events (and identifiers derived from it, and file sizes in the catalogue) change; list the changed rows, then regenerate the default files with `tools/make_golden.py`.
- [ ] Step 4: suite in chunks, ruff, black. Commit `events: the runway threshold crossing is named xing-runway-threshold`.

### Task 2: Rename on the portal

**Files:** `tools/check_site.py` (`EVENT_TYPES`), `tools/gen_validation_summary.py` (`METHOD_PAGES`), `content/data/dictionary.qmd`, `content/data/releases.qmd` (the table of names moving from v0.0.2: `landing` of v0.0.2 → `ALDT`; the new event is `xing-runway-threshold`), `content/methods/takeoff-landing.qmd`, `runway-milestones.qmd`, `plausibility-checks.qmd`, `index.qmd`, `papers/milestone-plausibility-v1/index.qmd` (prose only; its CSVs keep the name used when they were measured, with one sentence saying so), `assets/validation/validation_measures.csv` through `tools/draw_validation_figures.py` if an `item` names the type.

- [ ] Step 1: edit; regenerate the dictionary (`tools/gen_data_dictionary.py --opdi <src>`) and the validation summary. Step 2: `tools/check_site.py` OK, `--list-types --opdi <src>` exit 0, `pytest tools/tests` green. Commit and push `feature/new-topics`.

---

## Part 2: one worldwide day in under one hour

### Task 3: Measure where the time goes

**Files:** Create `benchmarks/day_timing.py`, `tests/benchmarks/test_day_timing.py`; results staged at `opdi-portal/.../papers/processing-speed-v1/data/` (`step_timings.csv`, `stage_timings.csv`, `_manifest.json`).

**Interfaces — Produces:** `python benchmarks/day_timing.py run --day 2026-06-14 --steps 01 02 02a 03 04 04b 09 --executors N --label <name> [--slice-hours H] [--sample-mod M]` runs the steps through `periodrun.run_day_step` under `research/speed/<label>/`, with a Spark listener that records per stage: step, stage id, name (call site), duration, tasks, input/shuffle read and write bytes, spill, and the longest task (skew). `step_timings.csv`: label, step, seconds, executors, cores, rows in, rows out, scope (`full`, or the slice/sample and its scaling factor). `summarise` prints the ten slowest stages per step.

- [ ] Step 1: tests for the listener bookkeeping and the scaling arithmetic (local session). Red, implement, green.
- [ ] Step 2: find out what the cluster allows (executor instances, cores and memory per executor in `config/spark.py`; the Kubernetes namespace quota if readable with the pod's rights; do not change cluster objects). Record it.
- [ ] Step 3: baseline run of steps 03, 04, 04b, 09 on the full worldwide day from the existing tracks (`opdi-world/osn_tracks_clean`, `dof=2026-06-14`) with today's executor count. Steps 01, 02, 02a need about 20 GB of new tables: run them in full only if the bucket has the room; otherwise run them on a time slice (for example four hours) or an aircraft sample, record the scope, and state the scaling assumption. Delete each scratch table after its timings are staged.
- [ ] Step 4: read the stage table and write the findings: for each step, the stages that cost the most and why (repeated reads of the same input, repeated sorts over the same key, one-partition writes, skewed tasks, spills, too many or too few shuffle partitions, UDFs, wide joins not broadcast). Commit the script and staged data.

### Task 4: Make it faster

**Files:** as the findings direct; expected: `src/opdi/pipeline/events.py`, `tracks.py`, `flights.py`, `fir.py`, `holding.py`, `point_merge.py`, `cleaning/`, `utils/storage.py`, `utils/spark_helpers.py`, `config/spark.py`; tests beside each.

Candidate changes, in the order of expected gain per risk; adopt each only if the stage table supports it, and measure each on the real day:
- Read the day's tracks once for all event families and keep them partitioned by `track_id` and sorted by time, so each family's window reuses that order.
- Replace repeated wide scans by one pass where families read the same derived columns.
- Write the day's events, measurements and tracks in parallel files inside the day folder instead of one partition; the published files of step 09 stay one file each.
- Spark settings for a day of this size: adaptive execution, shuffle partitions, broadcast threshold for the reference tables, Kryo, executor cores and memory, off-heap; local test session unchanged.
- More executors, within what the cluster allows.
- Skip work that cannot change a result (for example the FIR lookup of reports with no altitude and no position), only with a test showing identical output.

- [ ] Step 1: for each adopted change: a test that the output frame is identical to the one before on a fixture (or the golden day), red where a behaviour is added, then the change, then `tests/test_golden_outputs.py` for both settings.
- [ ] Step 2: after each group of changes, re-run the timing of the affected step on the real day and append to `step_timings.csv` under a new label.
- [ ] Step 3: stop when the day is under 60 minutes in total (steps 01 to 09, full-day figures or stated scaled figures), or when the remaining stages offer no further gain; record which.

### Task 5: Prove the results are the same

**Files:** Create `benchmarks/day_compare.py`; staged `papers/processing-speed-v1/data/result_comparison.csv`.

- [ ] Step 1: run steps 03 to 09 on the full day with the code before Part 2 (commit of Task 1) and with the final code, same inputs. Compare: flight list by `(icao24, first_seen)`: flights only in one, airports differing; events by `(flight, type, time to the second)`: per type, count in each, matched, only before, only after; measurements of matched events equal within 1e-6; published files: row counts and column sums. Step 2: every difference class explained (for example the order of rows with equal keys). Meets the limit in Global Constraints, or the change that causes more is removed. Commit, stage, delete scratch.

### Task 6: Write it down

**Files:** `airflow/README.md` ("What a day costs": measured times per step, executors used, settings), `readme.md`, portal `papers/processing-speed-v1/index.qmd` (short note rendering from the staged CSVs; card in `papers/index.qmd`), `content/methods/pipeline.qmd` (one sentence on processing time with the measured figure).

---

## Part 3: does ADS-C improve the results?

### Task 7: Read what is known, then measure with today's pipeline

**Files:** Create `benchmarks/adsc_effect.py`, `tests/benchmarks/test_adsc_effect.py`; staged `opdi-portal/.../papers/adsc-effect-v1/data/`.

- [ ] Step 1: read the earlier study (`opdi-portal/papers/adsc-fusion-v1/`, `benchmarks/run_adsc_day.py`, `benchmarks/adsc_*`, `src/opdi/config/adsc.py`, `TrackProcessor._merge_adsc`, the tables under `opdi-world/research/adsc_airframes` and `opdi/research/adsc`) and write down what it measured, with which track rule, and what it left open. The question now: with today's default (tracks kept across long reception gaps, the plausibility checks), what changes when ADS-C reports are added?
- [ ] Step 2: choose days and aircraft: the aircraft that sent ADS-C reports on the chosen days (a small share of the fleet, mostly oceanic), on days after the feed carries speed and vertical rate (after about 2025-09) and for which Network Manager or airport reference data exist. Two runs with identical code, days and aircraft: `base` (`adsc.enabled = False`) and `merged` (`True`), steps 01b to 04b, under `research/adsc_v2/{base,merged}`.
- [ ] Step 3: compare, each with counts and shares: flights reconstructed as one track against the reference flights (the measure of the track study); reception gaps and their length; flights whose two airports are both found; the distance-flown measurement of flights with a long gap (against the great-circle distance and the reference where available); events gained, lost and moved by type (FIR crossings, level segments, top of climb and descent over the ocean); events that the plausibility checks remove in each run; false joins of two flights into one track; processing time added.
- [ ] Step 4: a recommendation: switch ADS-C on for release v2.0.0 or not, with the evidence and the costs (identifier change, a `source` column, the clock offset between the two feeds, the extra step in the daily run which `opdi.orchestration.DAY_STEPS` does not include today). Do not change the default. Stage the CSVs, write `papers/adsc-effect-v1/index.qmd` (short, renders from the CSVs, card in `papers/index.qmd`), delete scratch, commit, push the portal branch.

---

### Task 8: Review and hand over

- [ ] Fresh review of the code range of Parts 1 and 2 against the Review Focus; Critical and Important findings fixed test-first. Whole suite in chunks. Push `industrialisation`; update the draft PR and `docs/industrialisation-handover.md` (times per step, ADS-C recommendation, the rename).
