# Portal edits and short tracks Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give every track with events a flight-list row, and apply the owner's wording, navigation and version-removal edits to the portal.

**Architecture:** One opdi change (a setting on `EventConfig` that replaces the fixed 5-minute rule of the overflight pass), then four portal tasks on the owner's branch, in this order: navigation and titles, Overview page, Data Access page, version wording on every other page.

**Tech Stack:** PySpark 4.1 + pytest (`.venv310`), Quarto `.qmd` with R chunks (not rendered here), `tools/check_site.py`.

**Spec:** `docs/superpowers/specs/2026-11-19-portal-edits.md` (meta repo). It holds the owner's request verbatim, the measurements and the calls made.

## Global Constraints

- opdi worktree: `/home/jupyter/work/opdi-workspace/opdi/.claude/worktrees/industrialisation`, branch `industrialisation`. Portal worktree: `/home/jupyter/work/opdi-workspace/opdi-portal/.claude/worktrees/publication`, branch `feature/new-topics`.
- **Portal: run `git pull --rebase` before the first edit of every task.** Keep the owner's structure, brevity and design; change only what the task says.
- Never add Claude attribution (Co-Authored-By, Claude-Session, "Generated with") to commits. Push to the task branch after committing; never push to main, never force-push, never merge.
- Do not install Quarto or R; the portal is not rendered here.
- No Spark cluster job in this plan; the local test Spark session is fine.
- Text: plain, for researchers and students; no "gate, guard, rung, ladder, arm, pin, robust, leverage, crucial, comprehensive"; no development history ("now", "no longer", "was changed", "before this change"); no individual flights, call signs or track ids.
- Version wording appears only on `content/data/releases.qmd` and in the Roadmap release-history table and "Release v2.0.0 delivers" section. Everywhere else: no "release v2.0.0", "v0.0.2", `cfg$release`, `cfg$previous_release`, `cfg$current_release`. Data values (`version` column values such as `time_v0.0.2`) may stay as values.
- Page names in link text: "Data Access" for `content/data/access.qmd`, "OPDI Algorithms" for `content/algorithms/index.qmd`.
- v0.0.2 golden output (`tests/golden/v002`) must stay byte-identical.

## Review Focus

- A track with a single report (span 0 s): it gets a row with first_seen = last_seen, and no crash on empty call sign. Test in Task 1.
- `legacy()` keeps the 300 s rule in both branches of `_fetch_overflights` (measured and unmeasured first/last seen). Test in Task 1.
- Links to anchors that disappear with removed sections (`#python`, `#r`, `#duckdb`, `#release-v002` from non-Releases pages are fine, but `access.qmd#python` etc. must not remain anywhere). Checked by grep in Task 3.
- R chunks left referencing variables whose setup was removed (`v2_current`, `rel`) — every remaining inline `` `r ...` `` must still have its variable defined in that page's setup chunk. Checked in Tasks 2-4 by grep.
- `check_site.py` number checks against prose that was rounded or removed. Run in every portal task.

---

### Task 1: Short tracks get a flight-list row (opdi)

**Files:**
- Modify: `src/opdi/config/events.py` (new field after `flight_stop_min_ground_s`, ~line 1712; `legacy()` ~line 1779-1890)
- Modify: `src/opdi/pipeline/flights.py:1790-1910` (`_fetch_overflights`: both `>= 300` comparisons and the docstring "tracks of at least 5 minutes")
- Modify: `src/opdi/output/schema.py` descriptions that say a flight-list row needs 5 minutes or an airport, if any (grep `5 min\|five minutes\|300`)
- Test: `tests/test_flight_list_corrections.py`
- Golden: `tests/golden/default/*` regenerated only if the default fixture has a track under 5 minutes

**Interfaces:**
- Produces: `EventConfig.overflight_min_duration_s: float = 0.0`; `EventConfig.legacy().overflight_min_duration_s == 300.0`. Read in `_fetch_overflights` through the same `getattr(getattr(self, "config", None), "events", None)` pattern the method already uses, falling back to 300.0 when no config is present.

- [ ] **Step 1: Write the failing tests** in `tests/test_flight_list_corrections.py`, following how that file already builds a `FlightListProcessor` stub and tracks frame:
  - `test_short_track_gets_an_overflight_row`: a track whose reports span 120 s, no airport → `_fetch_overflights` returns one row for it with `ADEP`/`ADES` NULL and both sources `undetermined`, under the default `EventConfig()`.
  - `test_single_report_track_gets_a_row`: one report → one row, `first_seen == last_seen`.
  - `test_legacy_keeps_the_five_minute_rule`: same 120 s track under `EventConfig.legacy()` → no row; a 300 s track → a row. Run with `seen_from_measured_reports` both True and False (parametrize).
  - `test_overflight_min_duration_defaults`: `EventConfig().overflight_min_duration_s == 0.0` and `EventConfig.legacy().overflight_min_duration_s == 300.0`.
- [ ] **Step 2: Run** `.venv310/bin/python -m pytest tests/test_flight_list_corrections.py -q -k "short_track or single_report or five_minute or overflight_min"` — Expected: FAIL (attribute missing / row missing).
- [ ] **Step 3: Implement** the field (docstring in the file's style: "Shortest time, in seconds, between the first and last report of a track with no departure and no destination for it to get a flight-list row. At 0 every track with reports gets one, so every published event has its flight. 300 in release v0.0.2.") and use it in both comparisons of `_fetch_overflights`; update that method's docstring.
- [ ] **Step 4: Run** the four tests — Expected: PASS.
- [ ] **Step 5: Golden check.** Run `.venv310/bin/python -m pytest tests/test_golden_outputs.py -q`. Expected: v002 identical. If default differs, the only allowed difference is added `opdi_flight_list` rows for tracks under 300 s; inspect with `/home/jupyter/.claude/jobs/616d5918/tmp/next/int/gd.py` (run from the repo root) and regenerate the default golden the way `tests/test_golden_outputs.py` documents. Any other difference: stop and report.
- [ ] **Step 6: Full suite** `.venv310/bin/python -m pytest tests/ -q -x --deselect nothing > /home/jupyter/.claude/jobs/616d5918/tmp/portal6/t1.log 2>&1; tail -3 …` — Expected: only the 9 failures known before (7 needing portal study files, 2 in `test_window_frames.py`); name any other.
- [ ] **Step 7: Commit and push** `git commit -m "flight list: tracks under five minutes get a row (overflight_min_duration_s, 300 in v0.0.2)"`; `git push`.

### Task 2: Navigation, titles and link names (portal)

**Files:**
- Modify: `_quarto.yml:53-100` (OPDI Data section)
- Modify: titles of `content/data/index.qmd` ("OPDI Data"), `content/data/access.qmd` ("Data Access"), `content/algorithms/index.qmd` ("OPDI Algorithms")
- Modify: link text across `content/**/*.qmd` and `index.qmd` (24 links point at `access.qmd` or `algorithms/index.qmd`)

- [ ] **Step 1:** `git pull --rebase`.
- [ ] **Step 2: Reorder and rename** the OPDI Data section to exactly: `Overview` (content/data/index.qmd), `Data Access` (content/data/access.qmd), `Data dictionary`, section `Data quality` (its three pages unchanged), `Data preview` (content/data-preview.qmd), section `OPDI Algorithms` (its contents unchanged, in their current order), `Releases`. Move whole YAML blocks; keep indentation.
- [ ] **Step 3: Titles** as listed under Files. Keep each page's `description`, but remove version wording from it if any (Task 5 owns the rest of the page).
- [ ] **Step 4: Link text:** every `[Access](…access.qmd…)` → `[Data Access](…)`, every `[Algorithms](…algorithms/index.qmd…)` or `[Algorithms](…algorithms/)` → `[OPDI Algorithms](…)`. Do not change other link targets.
- [ ] **Step 5: Verify:** `python3 -c "import yaml;yaml.safe_load(open('_quarto.yml'))"` → no error; `grep -rnE '\[(Access|Algorithms)\]\(' --include=*.qmd . | grep -v _site` → empty; `python3 tools/check_site.py` → `OK`.
- [ ] **Step 6: Commit and push** `git commit -m "menu: Data Access and OPDI Algorithms, pages under OPDI Data in reading order"`.

### Task 3: Overview page (portal)

**Files:**
- Modify: `content/data/index.qmd`
- Modify (only if the pages-table fix needs it): `assets/` stylesheet used by the site

- [ ] **Step 1:** `git pull --rebase`.
- [ ] **Step 2: Remove** the `status` R chunk (the release callout) entirely; drop setup variables that become unused (`v2_current`, `rel`) only if nothing else on the page uses them.
- [ ] **Step 3: Pages table** — first sentence of the page becomes neutral (no release). Table rows, in the new menu order (Data Access, Data dictionary, Data quality, Limitations, Airports, Data preview, OPDI Algorithms, Releases), with these exact descriptions where the owner gave them:
  - Data Access: "Where the data can be accessed and how."
  - Releases: "Release information."
  - Data preview: "A preview of what is included in the OPDI data and what can be done with it."
  - Limitations: "An overview of which limitations apply to the flight list, flight events and measurements."
  - Data quality: "An overview of the data quality, incl. availability and accuracy. Benchmarking is done using EUROCONTROL data." followed by " Per airport: [Airports](../accuracy/airports.qmd)." 
  - Data dictionary: keep its current description. OPDI Algorithms: "How each item is computed."
  Widen the Page column: add `: {tbl-colwidths="[25,75]"}` on the line after the table (Quarto pipe-table column widths).
- [ ] **Step 4: "What is published":** "OPDI starts from the position reports that aircraft broadcast by ADS-B, received by …" (no "radio"); delete ", and the columns of its flight, so one row stands alone" (end the sentence after the measurements example); "how each is computed is explained in [OPDI Algorithms](../algorithms/index.qmd)". State that the flight list has a row for every flight, also those for which no departure or destination airport could be named (one short clause in the flight-list bullet).
- [ ] **Step 5: Daily files:** "Release `r cfg$release` has two files per day:" → "The data is published in daily files:". Replace "The sizes were measured on 14 June 2026: 161,881 flights and 6,692,084 events worldwide." with "A day holds about 160,000 flights and 6.7 million events worldwide."
- [ ] **Step 6: Which day:** delete the paragraph "A few events belong to a track with no row in the flight list …" entirely.
- [ ] **Step 7: Coverage:** "The data cover the whole world: every flight that the OpenSky Network received. Reception depends on where receivers are and is thin over the oceans and near the ground at many airports; see [Limitations](../limitations.qmd#ads-b-reception)." (no version, no Europe box).
- [ ] **Step 8: Verify:** `grep -nE 'v2\.0\.0|v0\.0\.2|cfg\$(release|previous_release|current_release)|radio|stands alone|3,338' content/data/index.qmd` → empty; every `` `r `` expression's variable is defined in the setup chunk; `python3 tools/check_site.py` → `OK` (if it checks a number removed here, update the check's expectation in `tools/check_site.py` and say so in the report).
- [ ] **Step 9: Commit and push** `git commit -m "OPDI Data overview: owner's wording, no release names, every flight in the flight list"`.

### Task 4: Data Access page (portal)

**Files:**
- Modify: `content/data/access.qmd`
- Modify: `content/resources.qmd` (remove the opdi-reader mention, ~line 249, with its paragraph or list item)
- Delete only if nothing else includes them: `content/data/_v002_download_py.txt`, `content/data/_v002_download_r.txt` (grep first)

- [ ] **Step 1:** `git pull --rebase`.
- [ ] **Step 2: Remove** the `status` callout chunk; the opening sentence becomes "Tables and files of the OPDI data, and how to query or read them. Columns: [Data dictionary](dictionary.qmd)."; `description` without "Python, R and DuckDB" and without examples promise.
- [ ] **Step 3: "Which way to use" table:** keep Trino, file browser and Download rows; Download row: "Read the daily files with any Parquet tool. Suits repeated reads of the same days, or tools without a database connection." (no links to removed sections).
- [ ] **Step 4: Trino:** keep Getting access, Catalog and schema, Tables and views, and the example "flights between two airports on a day". Delete the other examples (all events of one flight; holding time; FIR crossings; measurements in the shape of release v0.0.2). In Tables and views, remove the rows and sentences about the views in the shape of the earlier release (`flight_list_v0` and any `*_v0`).
- [ ] **Step 5: Files:** keep "Where the files are" with "The files are in one published folder, …" (no release) and the folder tree; delete `### Python`, `### R`, `### DuckDB` with all their code; keep Tips. Remove every mention of opdi-reader and of v0.0.2 downloads.
- [ ] **Step 6: Verify:** `grep -rnE 'opdi-reader|opdi_reader|duckdb_views|access\.qmd#(python|r|duckdb)' --include=*.qmd . | grep -v _site` → empty; `grep -nE 'v2\.0\.0|v0\.0\.2|cfg\$(release|previous_release|current_release)|_v0\b' content/data/access.qmd` → empty; `python3 tools/check_site.py` → `OK`.
- [ ] **Step 7: Commit and push** `git commit -m "Data Access: Trino and the daily files, one example"`.

### Task 5: No release names outside Releases (portal)

**Files (each with what to change):**
- `index.qmd:90-110` — home callout and button: describe the data without release names ("Worldwide, a flight list and a file of flight events for every day."); button text "Release information →" to `content/data/releases.qmd`. Keep counts logic that does not print a version.
- `content/data/dictionary.qmd` — description and intro without release; remove the status callout chunk; "Event types of release v0.0.2 that release v2.0.0 no longer publishes" → "Event types of earlier releases that are not published, … see [Releases](releases.qmd#moving-from-v0.0.2)"; table header "Values in release v2.0.0" → "Values"; `release` row: "The data release; the same on every row. See [Releases](releases.qmd)."; "Event versions of release v2.0.0:" → "Event versions:"; the paragraph on days processed with the v0.0.2 settings (~383-386) → delete; "Views of the release v0.0.2 shape" bullet → delete; line ~422 → "Identifiers of different releases do not match; see [Releases](releases.qmd)."; line ~362 "v2.0.0 always `OSN`" → "always `OSN`".
- `content/methodology.qmd` — rewrite as: methods are on [OPDI Algorithms](algorithms/index.qmd) (tracks, airports, events, measurements links as now, without the "Release v2.0.0:" labels); "Methods of earlier releases: [Releases](data/releases.qmd)." Description likewise.
- `content/data-preview.qmd` — "a snapshot of the release `r cfg$previous_release` files" → "a snapshot of the files of an earlier release"; "available for download in the [Release v0.0.2 section](…)" → "available for download on [Releases](data/releases.qmd)" (keep anchors to `#release-v002` as link targets; only the visible text changes).
- `content/sensors-expansion.qmd` — "the OPDI flight list of release v0.0.2" → "the OPDI flight list of an earlier release"; "The algorithm of release v0.0.2, which built…" → "The algorithm of that release, which built…"; "Release v2.0.0 uses a newer method" → "The current method"; keep link targets.
- `content/limitations.qmd:59` — rephrase without the release.
- `content/algorithms/processing-steps.qmd:11` — "describes the steps of release v2.0.0 with their default settings" → "describes the steps with their default settings".
- `content/roadmap.qmd` — keep the release-history table and "Release v2.0.0 delivers" section; the status callout chunk (lines ~24-34) → remove.
- Any page that says a flight-list row needs a departure or destination airport, or that tracks under five minutes have no row (grep `five minutes\|5 minutes\|no row\|neither`, in `content/algorithms/airports.qmd`, `tracks.qmd`, `first-last-seen.qmd`, `content/limitations.qmd`) — state that every track has a row; a track with no airport has empty departure and destination.

- [ ] **Step 1:** `git pull --rebase`.
- [ ] **Step 2:** Apply the list above.
- [ ] **Step 3: Verify:** `grep -rnE 'v2\.0\.0|v0\.0\.2|[Rr]elease v[0-9]|cfg\$(release|previous_release|current_release)\b' --include=*.qmd . | grep -v '^./_site' | grep -v 'content/data/releases.qmd' | grep -v 'content/roadmap.qmd'` → only data-value lines (version strings such as `time_v0.0.2` in the dictionary's version list, link targets `#release-v002`, `#moving-from-v0.0.2`); list any remaining line in the report with why it stays. `content/roadmap.qmd` lines outside the history table and "delivers" section → none. Every `` `r `` variable still defined in its page's setup. `python3 tools/check_site.py` → `OK`.
- [ ] **Step 4: Commit and push** `git commit -m "release names only on Releases and the roadmap history"`.

### Task 6: Pointers (meta repo)

- [ ] In the meta worktree `/home/jupyter/work/opdi-workspace/.claude/worktrees/industrialisation`, append a dated section "2026-11-19 portal edits" to `docs/industrialisation-handover.md` (5-8 lines: the new setting and its measured effect, the opdi and portal commits, the calls listed in the spec). Commit; push.
