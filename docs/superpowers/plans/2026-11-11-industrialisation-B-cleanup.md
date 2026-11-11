# OPDI Code Clean-up Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the `opdi` package PEP 8 clean, documented for users in plain language, free of idle and development-only code, and better structured, without changing any default or v0.0.2 result.

**Architecture:** Four verified stages in this order: tooling and mechanical lint; removal of idle code and relocation of study variants to `benchmarks/`; docstring and comment rewrite; structural splits behind re-exports. Every stage ends with the full test suite and a fixed-input equality check of the pipeline's outputs.

**Tech Stack:** ruff, black (line length 100, as configured in `pyproject.toml`), pytest, PySpark.

**Spec:** `docs/superpowers/specs/2026-11-11-industrialisation-design.md` (meta repo), sections "Code clean-up" and "My calls".

**Worktree:** `/home/jupyter/work/opdi-workspace/opdi/.claude/worktrees/industrialisation`, after plan A has landed on the same branch. Linters: `/home/jupyter/work/opdi-workspace/opdi/.venv/bin/ruff` and `.../black`. One Spark test session at a time (16 GB pod limit); the full suite takes about 35 minutes, run detached.

## Global Constraints

- **Nothing changes in output.** The default pipeline and the v0.0.2 presets (`EventConfig.legacy()`, `DetectionConfig.legacy()`, `segmentation.method="legacy"`, `decimation="modulo"`, flight list from raw tracks, the `local` and `opensky` environments) produce exactly what they produce before this plan.
- The package never imports from `benchmarks/`; `benchmarks/` may import the package.
- Docstrings and comments: what it does, inputs, outputs, units, one sentence of reason where a choice is not obvious. No development history: no study or paper version names, no "A8/A9/A10", "rung", "ladder", "Task N", incident anecdotes, internal dates, names of run logs.
- Public import paths that exist today keep working (re-exports).
- No Claude attribution in commits.
- The baseline has nine known failing tests unrelated to this work (portal lookup from a worktree ×2, airport layout writer ×2, `regenerate_track_v2` ×2, events labelling helper import, flight detection border wiring, flight list v7 redirect); a stage passes when the failure set is a subset of these.
- Writing style (owner's rule): every text is well structured and readable without background knowledge; readers are researchers and students. Define each term on first use, ordinary words, short sentences, units on numbers. No AI or internal wording: not "gate", "guard", "rung", "ladder", "arm", "pin", "load-bearing", "surface" as a verb, "robust", "leverage".

## Review Focus

1. A caller that imports a moved name from its old path (`from opdi.config import EventConfig`, `from opdi.pipeline.segmentation.methods import ARMS`) still works or fails with a message naming the new home.
2. `legacy()` presets after field removal: every field they set still exists and still has the legacy effect.
3. A paper's regeneration script that used a removed variant runs through the relocated study variant with identical configuration.
4. A config field that was read through `getattr(..., default)` and is removed: no silent fallback remains.
5. Docstring rewrite must not delete a constraint a maintainer needs (units, why a threshold is scaled at point of use).

---

### Task 1: Output-equality harness and lint tooling

**Files:** Create `tests/test_golden_outputs.py`, `tests/golden/` (parquet fixtures, small), `tools/make_golden.py`; modify `pyproject.toml` (ruff rules).

**Interfaces — Produces:** `tools/make_golden.py` builds, from the synthetic flights already used by `tests/test_events_processor.py` and `tests/navigation_fixtures.py` plus a flight-list fixture, the outputs of steps 02 (track ids, default and `method="legacy"`), 03 (default and `DetectionConfig.legacy()`), 04 (default `EventConfig()` and `EventConfig.legacy()`) and writes them sorted to `tests/golden/*.parquet`; `test_golden_outputs.py` recomputes and asserts frame equality (types, ids, values) for each of the six.
- ruff config: `select = ["E","F","W","I","N","D","B","UP"]`, `convention = "google"`, per-file ignores for `tests/` and `benchmarks/` docstring rules, `N803/N806` allowed for the upper-case flight-list column names.

- [ ] Step 1: write the harness test (expects fixtures; fails "no golden files"). Step 2: run `tools/make_golden.py` on the untouched code, commit fixtures. Step 3: test passes. Step 4: fix the two real defects with a failing test each first: `ingestion/osn_statevectors.py` undefined `df_cleaned` on the row-report path; `reference/h3_airport_layouts.py` missing `geopandas` import in an annotation. Step 5: `ruff check --fix` for I001/F401/F541/E402/F402 and `black src tests benchmarks`; remaining E501 by hand. Step 6: suite + golden; commit `tooling: golden outputs, ruff and black clean`.

### Task 2: Remove idle code; relocate study variants

**Files:** Modify `src/opdi/config.py`, `src/opdi/pipeline/segmentation/{__init__,methods,base}.py`, `src/opdi/pipeline/tracks.py`, `src/opdi/pipeline/flights.py`, `src/opdi/cleaning/{cleaner,pandas_udf}.py`, `src/opdi/cli.py`, `opdi.py`, `run_period.py`; create `benchmarks/study_variants/{__init__,segmentation,events,detection}.py`, `benchmarks/README.md`; delete `task6_build_research.py`, `task6_verify.py`, `task6_build.log`, `task6b_build.log`, `publish_layouts.py`, `docs/industrialization-plan.md`, `sql/`, `src/opdi/cleaning/pandas_udf.py`, `src/opdi/reference/zone_viewer.py` (move to `benchmarks/tools/`).

**What leaves the package:**
- Segmentation: arms `no_month_suffix`, `traffic_style`, `airframe_only`, `ground_anchored`, `airport_anchored`, `vertical_profile`, `recommended`, `debounced` and the fields only they read (`ground_dwell_minutes`, `turnaround_max_*`, `descent_floor_ft`, `callsign_lookback_minutes` where unused by the default). The package keeps `standard` (the default rule, named `standard` only; the name `cruise_linked` stays as an alias) and `legacy`. The removed arms move verbatim to `benchmarks/study_variants/segmentation.py`, registered through a `register_study_arms()` function that adds them to the engine's registry when a study script calls it.
- Detection: mode `nearest`, option `trend_ooa` and `_label_trend_out_of_area`.
- Cleaning/events: `enable_pandas_stage` (both configs) and the stub module.
- Events: nothing is removed from `EventConfig` that `legacy()` sets; the benchmark-only reconstructions (`V020_BASE`, `V4_BASE`, ladders) already live in `benchmarks/event_bench.py` and stay there.
- The v0.0.2 release path that is no longer produced: `output/parquet_exporter.py`, `output/csv_exporter.py` (steps 05, 06), `monitoring/` (steps 07, 08), the `dev` and `live` environments in `for_environment` with their Kerberos/Hive settings, and the stale Iceberg DDL strings that only they used (the `local` environment and its tests keep working).
- `periodrun.run_period(allow_existing=...)` dead parameter; `runner`/`opdi.py` references to `OPDI-live/python/v2.0.0`.

- [ ] Step 1: for each removal, find its tests; move tests of study variants to `tests/benchmarks/` and point them at `study_variants`; add `tests/test_package_independence.py`: no file under `src/opdi` imports `benchmarks` or names a removed arm; `SegmentationConfig(method="recommended")` raises a `ValueError` saying the rule lives in the study scripts; `register_study_arms()` makes it available again.
- [ ] Step 2: run, expect failures. Step 3: remove/move. Step 4: run every `tests/benchmarks/test_regenerate_*` and `test_build_*_paper` to confirm the study scripts still resolve their variants. Step 5: suite + golden. Step 6: `benchmarks/README.md` (what the folder is, that it is not part of the package, how a study script enables a variant). Commit `package: only the default and the v0.0.2 reproduction remain; study variants live with the study scripts`.

### Task 3: Docstrings and comments for users

**Files:** every module under `src/opdi/`, root scripts, `readme.md`, `docker/README.md`.

**Rule per element:** module docstring = what this step or helper does in the pipeline, its inputs and outputs (table names), in three to eight lines; function/class docstring in Google style with Args/Returns and units; config field docstring = meaning, unit, default, effect, and a link `See: {PORTAL}/methods/<page>.html#<anchor>` for algorithm thresholds; inline comments short, only where the code is not self-explanatory. One constant `opdi.PORTAL_URL = "https://www.opdi.aero"`.

- [ ] Step 1: add `tests/test_docs_plain_language.py`: over `src/opdi/**/*.py` docstrings and comments, none matches `\b(A[0-9]{1,2}|V[0-9](\.[0-9])?|v6\.[0-9]|rung|ladder|Task [0-9]+|track-construction-v[0-9]|SIGKILL|bitten|EVENTS_RUN_LOG|weekrun)\b` nor a date `20[0-9]{2}-[0-9]{2}-[0-9]{2}` (version strings such as `events_v0.0.2` and AIRAC validity dates in `reference/h3_airspaces.py` are allow-listed by exact string); every public function and class has a docstring (ruff D rules enforce). Step 2: run, expect failures listing files. Step 3: rewrite file by file, largest prose first (`config.py`, `segmentation/methods.py`, `tracks.py`, `events.py`, `flights.py`, `periodrun.py`), keeping every unit and constraint statement; rewrite `readme.md` (what OPDI is, install, run a day, run a period, environments, published layout, where the documentation is). Step 4: test passes; suite + golden. Commit per group of modules, last commit `docs: user-facing docstrings across the package`.

### Task 4: Structure

**Files:** split `src/opdi/config.py` into `src/opdi/config/{__init__,project,spark,ingestion,cleaning,adsc,detection,events,segmentation,environments}.py`; create `src/opdi/versions.py` (every version string) and `src/opdi/utils/geometry.py` (haversine, bearing, wrap; used by `flights`, `holding`, `point_merge`, `events`); split `src/opdi/pipeline/events.py` into `pipeline/events/{__init__,processor,measures,crossing_events,phase_events,block_events,ids}.py` keeping `opdi.pipeline.events` imports working; tests `tests/test_public_api.py`.

- [ ] Step 1: `tests/test_public_api.py` asserts today's import surface (list generated from the current tests' imports of `opdi.*`: each name importable from the same path). Step 2: green before the move (pins the surface). Step 3: move code without editing bodies; re-export from the old paths. Step 4: public API test, suite, golden. Step 5: ruff/black; commit `structure: config package, events package, shared geometry and versions`.

### Task 5: Final verification

- [ ] Full suite detached; golden; `ruff check src benchmarks tests` and `black --check` clean; `python -m opdi --help`, `python -m opdi.orchestration --help`, `python run_period.py --help` run; a one-day local-mode smoke of steps 02-04b on the synthetic fixture through `periodrun` functions. Record results in the pull request body.
