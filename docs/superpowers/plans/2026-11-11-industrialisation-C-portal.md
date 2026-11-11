# OPDI Portal Publication Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the OPDI website ready to publish for the external research community: all branches merged into `feature/new-topics`, release v2.0.0 documented end to end (data, columns, methods, validation, limitations), the PRC Data Challenge section current, the community page hidden.

**Architecture:** Quarto site; pages under `content/`. New sections `content/data/` and `content/methods/`. The data dictionary is generated from the pipeline's declared schema. Every methods page follows one template; every number on a methods page comes from `assets/validation/validation_measures.csv`, and every figure from a script that reads committed study CSVs. Checks are plain Python scripts under `tools/` (Quarto and R are not available on the build pod; the owner renders).

**Tech Stack:** Quarto markdown (`.qmd`), R chunks already used by the site (unchanged style), Python 3 for generators and checks (pandas, matplotlib in a scratch environment), git.

**Spec:** `docs/superpowers/specs/2026-11-11-industrialisation-design.md` (meta repo), section "Portal" and the owner's answers.

**Worktree:** `/home/jupyter/work/opdi-workspace/opdi-portal/.claude/worktrees/publication` (branch `feature/new-topics`). Scratch Python with matplotlib: `/home/jupyter/.claude/jobs/616d5918/tmp/pdfenv/venv/bin/python`. Research notes for the challenge: `/home/jupyter/.claude/jobs/616d5918/tmp/research/prc_challenge.md`.

## Global Constraints

- Audience: external researchers. Plain language. No internal vocabulary: no study arm names (A8, A9, A10), "rung", "ladder", paper version numbers as method names, task numbers, internal dates of decisions, incident stories. Method studies are linked as "Full study".
- No accuracy figure without a source row in `validation_measures.csv`; an algorithm without validation says "Not validated" under the same headings.
- Every methods page has exactly these nine H2 headings in this order: `What it is`, `How it is computed`, `Reference data`, `Validation method`, `Results`, `Error distribution`, `Headline accuracy`, `Limitations`, `Full study`.
- Release name `v2.0.0`. Access through Trino (OpenSky instance; file browser URL to be defined: one config value `cfg$access$browser_url`, shown as "to be announced" while empty). One switch `cfg$current_release` (`"v0.0.2"` until the backfill has run).
- Community page: file kept, not in the sidebar, no card on the home page, no links to it.
- Not merged to `main`. No Quarto or R installation. No Claude attribution in commits.
- Do not change the content of the study papers under `papers/` except `papers/index.qmd` listings.

## Review Focus

1. A page links to an anchor or page that does not exist (the link checker must catch it).
2. A number quoted on a methods page that differs from its source CSV after a study is re-run.
3. An event type, flight-list column or measurement emitted by the pipeline that the dictionary or the validation summary does not list.
4. The home page or sidebar still exposing the community page through a card, a counter or a "Join" link.
5. A reader landing on an old URL (`/flight-list-data.html`, `/methodology.html`): aliases must still resolve to the right new page.

---

### Task 1: Bring the branches together

- [ ] Step 1: in the worktree, `git merge --ff-only airspace-events-v1` (contains `feature/new-topics`, the track-construction papers v3-v5 and the airspace paper).
- [ ] Step 2: `git merge origin/development-2026` (the flight-events-v4 rework). Resolve the add/add conflicts by taking `development-2026` for `papers/flight-events-v4/**` and `tools/`, the current branch for `papers/track-construction-v1/*.pdf` and `_site/**`, and merging `papers/index.qmd` by hand so every paper has one card (newest first).
- [ ] Step 3: `git merge origin/main`; in `R/portal_config.R` keep this branch's structure and take `origin/main`'s newer values (`refresh_label = "October 2026"`, coverage end `2026-09-30`, counts `+79M / +1.9B / +3.8B`); take `origin/main` for `_site/`.
- [ ] Step 4: verify: `git log --oneline -3`; every directory under `papers/` has a card in `papers/index.qmd` (script `tools/check_papers_index.py`, written here: lists `papers/*/index.qmd` and asserts each path appears in `papers/index.qmd`). Commit merges; push `feature/new-topics`.

### Task 2: Checks and generators

**Files:** Create `tools/check_site.py`, `tools/gen_data_dictionary.py`, `tools/draw_validation_figures.py`, `tools/README.md`, `assets/validation/validation_measures.csv` (header only here).

**Interfaces — Produces:**
- `check_site.py` (no Quarto needed), exit 1 on any failure: every relative link and `#anchor` in `index.qmd`, `content/**/*.qmd` resolves to a file/heading (Quarto heading-id rules, explicit `{#id}` honoured, `aliases` honoured); every sidebar entry in `_quarto.yml` exists; `content/community.qmd` is not referenced by `_quarto.yml`, `index.qmd` or any page; every `content/methods/*.qmd` except `index.qmd`, `pipeline.qmd`, `validation-summary.qmd` has the nine headings in order; every number in a methods page's `Results` and `Headline accuracy` sections appears in `validation_measures.csv` for that page (`page, measure_id, label, value, unit, n, reference, source_file, source_field`); no page contains the forbidden vocabulary (regex list from Global Constraints); `--list-types` prints every event type and column the checks expect.
- `gen_data_dictionary.py --opdi <path to opdi src>`: imports `opdi.output.schema` and writes `content/data/_generated/flight_list_columns.md`, `flight_events_columns.md`, `measurement_columns.md` (tables: Column, Type, Unit, Description) and `assets/data/dictionary.csv`.
- `draw_validation_figures.py`: one function per figure id, reading `papers/*/data/**.csv`, writing `assets/validation/<page>_<figure>.svg` (one style: 7×3.5 in, grey grid, one accent colour, axis labels with units, n in the title) and appending/refreshing rows of `validation_measures.csv`.

- [ ] Step 1: write `tools/tests/test_check_site.py` (pytest, tmp site): a broken link fails; a page missing a heading fails; a number not in the CSV fails; the forbidden word "rung" fails. Step 2: red. Step 3: implement. Step 4: green; run `check_site.py` on the merged site and record the current failures as the to-do list for Tasks 3-7. Commit.

### Task 3: Data section and dictionary

**Files:** Create `content/data/index.qmd` (overview: what is published, the two daily tables, release v2.0.0 vs v0.0.2, coverage, how days are assigned), `content/data/access.qmd` (Trino: catalog/schema, table and view names, example queries for the most common tasks — flights between two airports on a day, all events of one flight, holding time per destination, minutes per FIR; file access: path pattern, Python and R and DuckDB snippets using `opdi.published`), `content/data/dictionary.qmd` (includes the generated column tables; event `type` vocabulary with one line each and a link to its methods page; `info` fields per event type; `source` values; the version columns; identifiers and how tables join), `content/data/releases.qmd` (v2.0.0 changes against v0.0.2 in user terms: new events, merged table, daily files, track identity across reception gaps, worldwide coverage; where v0.0.2 lives), `content/data/limitations.qmd` (index: one paragraph per algorithm family linking to its Limitations section, plus dataset-wide limits: ADS-B coverage, single AIRAC cycle, day assignment, no flight-plan data); modify `content/data.qmd` → redirect/alias into `content/data/index.qmd`, remove `flight-list-data.qmd`, `flight-event-data.qmd`, `measurement-data.qmd` in favour of `content/data/v0.0.2.qmd` (the previous release's download tables, keeping the three old aliases), fix the entity diagram.

- [ ] Step 1: run `gen_data_dictionary.py`; Step 2: write the pages; Step 3: `check_site.py` passes for `content/data/`; Step 4: commit `data: v2.0.0 overview, access, dictionary, releases, limitations`.

### Task 4: Methods pages

**Files:** Create `content/methods/index.qmd`, `pipeline.qmd` (the steps with a diagram: state vectors → tracks → cleaning → flight list → events → flight-list milestones → published files; what each step reads and writes), and one page per family on the nine-heading template: `track-identity.qmd`, `trajectory-cleaning.qmd`, `airports.qmd` (departure and destination), `takeoff-landing.qmd`, `block-times.qmd`, `runway-milestones.qmd` (line-up, take-off roll, runway vacated, runway crossings, go-around), `surface-events.qmd` (taxiway, apron, stand, de-icing entries and exits), `vertical-events.qmd` (level segments, top of climb and descent, flight-level crossings, level-offs), `distance-rings.qmd`, `holding.qmd`, `point-merge.qmd`, `fir-crossings.qmd`, `measurements.qmd`, `first-last-seen.qmd`; `validation-summary.qmd` (one table: item, kind, headline accuracy, reference, n, links to the page's Validation and Limitations). Replace `content/methodology.qmd` and `content/concepts.qmd` content by the new section (keep the concept explanations that are still correct in `methods/index.qmd`; keep both old paths as aliases).

**Sources per page** (read the study, restate in plain language, take numbers only from its `data/` CSVs): track identity ← `papers/track-construction-v5`; cleaning ← `papers/statevector-decimation` and the cleaning notes in `papers/flight-events-v4`; airports ← `papers/adep-ades-detection-v7`; take-off/landing, block times, runway milestones, surface, vertical, rings ← `papers/flight-events-v4` (and v3 for the items it still covers); holding, point merge, FIR ← `papers/airspace-events-v1`; measurements ← `papers/track-construction-v5` (distance) and the event studies; first/last seen ← not validated.

- [ ] Step 1: write `track-identity.qmd` and `holding.qmd` first as the two reference pages (one with a classification-type validation, one with a label-set validation), with their figures added to `draw_validation_figures.py` and their rows in `validation_measures.csv`; run `check_site.py`. Step 2: write the remaining pages to the same pattern. Step 3: `validation-summary.qmd` generated by `tools/gen_validation_summary.py` from `validation_measures.csv` (one headline row per item, `headline=1` column). Step 4: `check_site.py` clean; `--list-types` output compared with the pipeline's emitted types and the schema's columns (script argument `--opdi`), no item missing. Commit per page group; last commit `methods: one page per algorithm with validation and limitations`.

### Task 5: PRC Data Challenge, home page, community, stale content

**Files:** Modify `content/data-challenges.qmd`, `R/portal_config.R`, `index.qmd`, `_quarto.yml`, `content/about.qmd`, `content/roadmap.qmd`, `content/resources.qmd`, `content/data-preview.qmd`, `content/sensors-expansion.qmd`.

- [ ] Step 1: challenges — 2026 edition card: taxi-out time prediction at ten major European airports, 1 September to 14 October 2026, data (airport movements matched to the Network Manager flight list, 4,167,797 movements, no ADS-B trajectories), metric RMSE in seconds, winners (charming-eel 176.16 s; zesty-puzzle 180.18 s; youthful-giraffe 183.66 s), presented at the 14th OpenSky Symposium, Madrid, 29-30 October 2026, data paper and open data planned for Q1 2027, links (challenge page, GitHub organisation). 2025 edition wording: "fuel burnt per flight interval" (not per-second fuel flow), RMSE without the kg/h unit. Totals in `portal_config.R`: `editions = 3`; team, country and submission totals stay at the confirmed 2024+2025 figures labelled "2024-2025" (the 2026 totals are not confirmed and are left out). Before writing, re-read the research notes and copy only items marked confirmed.
- [ ] Step 2: `_quarto.yml` sidebar: Home; Data (Overview, Access, Data dictionary, Releases, Limitations, Data preview); Methods (Overview, Pipeline, the fourteen pages, Validation summary); PRC Data Challenges; OPDI Sensors (unchanged); Resources; Roadmap; About. Community removed from the sidebar; `index.qmd` community card and counters removed; links to `community.qmd` in `about.qmd`, `resources.qmd` removed.
- [ ] Step 3: `portal_config.R`: `current_release`, `release = "v2.0.0"`, `access` block (Trino catalog/schema names, `browser_url = ""`), "July"; every hard-coded "v0.0.2 is the latest" statement rewritten against `cfg`; `roadmap.qmd` rewritten (what v2.0.0 delivers, what is next: FIR validation against Network Manager profiles, newer AIRAC cycles, more merge points, backfill to 2019) with the past-release table completed; `resources.qmd` lists every method study from `papers/index.qmd`, the software (opdi, the Airflow DAG, the reader) and the API reference; `sensors-expansion.qmd` version mention fixed.
- [ ] Step 4: `check_site.py` clean. Commit `site: 2026 challenge edition, navigation, current release, community hidden`.

### Task 6: Publication readiness

- [ ] Step 1: `README.md` — "Rendering and publishing" updated: prerequisites, `python tools/check_site.py`, `python tools/gen_data_dictionary.py --opdi ../opdi/src`, `quarto render`, what to check by eye (list of the new pages), how to flip `current_release`, how to un-hide the community page (three edits, listed).
- [ ] Step 2: a `PUBLISHING_CHECKLIST.md` at the repository root: items the owner must supply (Trino file-browser URL, confirmation of the 2026 challenge totals, community members' consent), and the render-and-merge steps.
- [ ] Step 3: full `check_site.py`, `check_papers_index.py`; push `feature/new-topics`. No merge to `main`.
