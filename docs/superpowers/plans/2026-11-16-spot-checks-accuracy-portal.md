# Spot Checks, Corrections, Accuracy per Airport and Item Pages Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Find the remaining odd cases in every flight event by reading individual examples, correct them in the code without slowing the processing or degrading any event, and make the portal tell a user quickly, for every published item, how it is computed, what its limits are, how accurate it is overall, and at which airports.

**Architecture:** Five parts in sequence. (1) Process one worldwide day with today's code and draw, for every event type, flight-list item and measurement, a few dozen examples with the reports around them. (2) Readers go through the examples family by family and list odd cases with their cause. (3) The causes worth correcting are fixed in the detectors, each with a test, a check that confirmed events are not lost, and a timing of the events step. (4) Three days are processed with the corrected code and scored against airport and Network Manager records, per item overall and per airport. (5) The portal gets one algorithm page per item on one template, an at-a-glance table, and per-airport accuracy tables generated from the staged results.

**Tech Stack:** PySpark on Kubernetes (driver on the pod), pandas for the reading and scoring, pytest; Quarto pages with generated markdown tables, Python tools under the portal's `tools/`.

**Spec:** the owner's request of 2026-11-16, verbatim: "Do a few dozens of spotchecks for each of the flight events to see if there are any more odd cases that need to be rectified. Then rectify in code, but make sure that none of the code edits make the process much longer and that it doesn't introduce any degradations of the flight events. In addition, in the opdi portal, update the flight event, measurements, flight list info with the limitations of each of the flight events (or calculated item) and give a measure of accuracy overall. In addition each of flight events or things how the flight list is constructed should be explained simply (but completely) on an algorithm page for each item. If you can or need to, give the accuracy for each of the top 100 busiest airports in Europe. If you can add a top 100 worldwide - add it. The user should quickly be able to know what the limitations are and at which airport they are accurately calculated." The owner said "Go ahead".

**Worktrees:** code `/home/jupyter/work/opdi-workspace/opdi/.claude/worktrees/industrialisation` (branch `industrialisation`, draft PR 28); portal `/home/jupyter/work/opdi-workspace/opdi-portal/.claude/worktrees/publication` (branch `feature/new-topics`, never merged to `main`).

## Decisions taken

- **Days.** Spot checks and corrections use 14 June 2026 (a Sunday; the day every earlier study used). The accuracy tables use three days so that smaller airports have enough movements: 14 June plus one weekday before and one after for which airport records exist (chosen in Task 1 from the records file; default 10 and 17 June 2026). Each day is processed from the raw archive with the current track rule.
- **Examples per item.** For each event type, flight-list item and measurement: 30 drawn at random and up to 20 drawn from the tails (highest and lowest speed and height at the event, longest and shortest durations, events at small airports, events far from any record), because odd cases sit in the tails. An item with fewer than 50 events on the day is read in full.
- **What counts as worth correcting.** A cause that produces a wrong value (not a missing one) on more than a handful of flights a day, or any cause that puts an event in a physically impossible place. Missing values and rare cases are documented as limits. The owner's earlier decision stands: `line-up`, `take-off-roll` and `runway-vacated` are often placed at speed because reception starts or ends mid-roll; they stay as they are and are documented.
- **No degradation.** After every correction: the v0.0.2 golden frames byte-identical; the default golden frames unchanged or changed only as explained row by row; on the real day no event that airport records confirm (within 60 s) is lost, each lost or moved one read individually; every event type the correction does not concern is unchanged.
- **Not much longer.** The events step on the full day may not take more than 5% longer than before the corrections (wall time and executor-seconds, each measured twice); the whole day stays within 5%.
- **Accuracy per airport.** Europe: the 100 airports with the most flights in June 2026 in the Network Manager flight records. For each airport and item: reference movements, share found (coverage), share within 60 s, median difference, 90th percentile, and a three-level label. Airports without airport-operator records get the items that Network Manager records can score (departure and destination airport) and the share of flights for which each event was found. Worldwide: the 100 airports with the most flights in OPDI's own flight list on the three days; outside Europe there are no records to compare with, so the table gives how often each event is found there, and the airport accuracy for flights to and from Europe where Network Manager records them. This is stated on the page: found is not the same as correct.
- **Labels.** `good`: at least 80% of reference movements have the event within 60 s; `fair`: 50 to 80%; `limited`: under 50%; `no reference`: fewer than 30 reference movements. Shown beside the numbers, never instead of them.
- **Portal structure.** Three separate entries: Algorithms (one page per published item, all on one template; items that are two ends of one thing share a page with a section each), Limitations (one page, a section per item) and Accuracy (overall per item, and per airport). The present methods pages become pointers and keep their addresses. No examples from the spot checks appear on the portal.

## Global Constraints

- The v0.0.2 settings give byte-identical output throughout: `tests/test_golden_outputs.py` v0.0.2 frames pass with no regeneration.
- Cluster: the pod hosts only the driver (16 GB limit, silent kills). One Spark job at a time. At most the `opensky` configuration's 13 executors. Detached runs (`setsid nohup <script>`, own log ending in `run exit <code>`; wait by grepping the log). Do not `source` `.env` in bash.
- Bucket `eurocontrol`: 200 GB quota, about 46 GB free. A day needs about 33 GB at its peak. Write only under `s3a://eurocontrol/opdi-world/research/{speed,spot,accuracy}/`; delete position reports after the track step and raw tracks after cleaning; keep cleaned tracks of at most one day at a time; delete all scratch at the end; delete nothing else.
- Native Spark only. No new dependency on the executors.
- Tests: `.venv310/bin/python -m pytest tests/<files> -q`, one process at a time; whole suite in chunks at the end of each code part. Known failing set: 4 under `tests/benchmarks` that need the portal checkout and 3 there that fail only by test order. `ruff check .`, `black --check .` clean.
- Writing style (owner's rule): readable without background knowledge by researchers and students; every term defined on first use; ordinary words; short sentences; units on numbers. No "gate", "guard", "rung", "ladder", "arm", "pin", "robust", "leverage", "crucial", "comprehensive". No development history in code or on methods and data pages. Each item page must be simple and complete: a reader can follow every step of the method without the code.
- Every number on a portal page comes from a staged CSV with provenance. Found is never presented as correct.
- No Claude attribution in commits or pull requests. No push to `main`. The portal is not rendered here (no Quarto, no R).

## Review Focus

1. A correction that helps the common case and silently breaks a rare real one (touch-and-go, helicopter, training circuit, turnaround in one track, airport at high elevation, track starting or ending in the air).
2. A per-airport figure built on too few movements presented as if it were solid.
3. An airport table that mixes "event found" with "event correct", or airports with and without records, without saying so.
4. An item page whose description no longer matches the code after the corrections.
5. A correction that adds a join or a window over the whole day and slows the events step.

---

## Part 1: a processed day and the examples

### Task 1: Process the day and draw the examples

**Files:** Create `benchmarks/spot_checks.py`, `tests/benchmarks/test_spot_checks.py`. Output on the pod's disk: `/home/jupyter/.claude/jobs/616d5918/tmp/spot/samples/<item>.parquet` and `<item>.md` (one readable sheet per example), plus `inventory.csv`.

**Interfaces — Produces:**
- `spot_checks.py run-day` (steps 01 to 04b and 09 for a day under `research/spot/<label>` with the current code, deleting reports and raw tracks as it goes, keeping cleaned tracks, flight list, events, measurements and published files), `sample` (draws the examples), `sheet` (writes the reading sheets), `clean`.
- `inventory.csv`: one row per item: kind (event, flight-list item, measurement), name, count on the day, count with a reference record, methods page.
- A sheet holds, for one example: the item and its value; the flight's row of the flight list; all events of the track in time order with the example marked; the position reports from 3 minutes before to 3 minutes after the event (time, position, altitude, height above the airport, ground speed, vertical rate, ground flag, heading) thinned to at most 40 lines with the nearest ones kept; the airport's elevation and the runway or layout element concerned; the reference record where one exists (airport record or Network Manager) and the difference; and for a flight-list item the whole track's summary (first and last report, airports found, candidates).
- Items: every event type the default settings publish (the families `entry-`/`exit-` × 6 elements, `xing-fl` × 4, `xing-…nm` × 6 each count as types), the flight-list columns in groups (identity and times; departure airport; destination airport; alternative airports; aircraft data; take-off and landing times and runways; block times and stands; ring times; superseded and movement flags; track duration), and the measurements (distance flown, time passed, the holding and point-merge measures).

- [ ] Step 1: choose the two extra days from the airport records file (same month, a weekday before and after, full archive hours); record the choice.
- [ ] Step 2: tests for the sampling (random part reproducible from a seed; tails drawn from the named measures; an item with fewer than 50 events returned whole) and for the sheet text on a small fixture. Red, implement, green.
- [ ] Step 3: run the day, draw the examples, write the sheets. Commit the script.

## Part 2: reading the examples

### Task 2: Read every example and list the odd cases

Six readers, each given one family's sheets and the code of its detector, read-only: (a) take-off, landing, runway milestones and go-around; (b) block times and airport-surface events; (c) tops of climb and descent, level segments, level-offs, flight-level crossings; (d) distance rings, FIR crossings, first and last seen, measurements; (e) holding and point merge; (f) the flight list (airports, alternatives, aircraft data, runway, stand and ring columns, flags).

- [ ] Each reader writes `/home/jupyter/.claude/jobs/616d5918/tmp/spot/findings/<family>.md`: per item, how many examples were read, how many look right, and each odd case with the example's identifier, what is wrong, the cause in the code (file and line), how often it is likely to occur on the day (with the query that would count it), and whether it is a wrong value, a missing value or a matter of definition. No code is changed in this task.
- [ ] The findings are merged into one list ordered by flights affected per day; each is counted on the whole day before deciding.

## Part 3: corrections

### Task 3: Correct the causes worth correcting

**Files:** as the findings direct, in `src/opdi/pipeline/`; tests beside each; `src/opdi/config/events.py` for switches.

- [ ] For each correction: a unit test built from the real example that fails first; the change, behind a config field that is on by default and off in `EventConfig.legacy()` when the code is reachable under the v0.0.2 settings; the tests of the cases named in Review Focus 1; golden frames.
- [ ] Real-day check for the set of corrections: the events step with the code before and after on the same cleaned tracks; scoring against airport records (`benchmarks/milestone_plausibility.py`); `benchmarks/day_compare.py` for all event types; every confirmed event lost or moved read individually; timing of the events step twice each (limit: 5%).
- [ ] A correction that loses a confirmed event or slows the step beyond the limit is reworked or dropped and documented as a limit instead.
- [ ] Independent review of the corrections against Review Focus 1 and 5; findings fixed test-first.

## Part 4: accuracy overall and per airport

### Task 4: Score three days

**Files:** Create `benchmarks/airport_accuracy.py`, `tests/benchmarks/test_airport_accuracy.py`; staged at `opdi-portal/.../papers/accuracy-by-airport-v1/data/`.

**Interfaces — Produces:**
- `item_accuracy.csv`: one row per item: reference used, days, reference movements, found, coverage %, within 30 / 60 / 300 s %, median difference s, 90th percentile s (or the item's own measure: share of airports right for airports, share of runways right, share of stands right, distance ratio), and `not validated` with the reason where no reference exists.
- `airport_accuracy_europe.csv`: the 100 European airports with the most flights in June 2026 (Network Manager records), one row per airport and item with the same measures, the count behind each, and the label.
- `airport_accuracy_world.csv`: the 100 airports with the most flights in OPDI's flight list on the three days: share of departures and arrivals for which each event is found; departure and destination airport accuracy for the flights Network Manager records; `has_airport_records` flag.
- `airports.csv`: rank, ICAO code, name, country, flights, whether airport-operator records exist.

- [ ] Step 1: tests of the scoring arithmetic and the labels (pandas). Step 2: process the two extra days with the corrected code and re-run the events step of 14 June; score; stage with provenance; delete scratch.

## Part 5: the portal

The owner's addition of 2026-11-16: "Don't put the examples on opdi portal. Clearly add limitations page, algorithm page and accuracy page."

- **No examples on the portal.** The reading sheets, the sampled events and any individual flight read during the spot checks are working material. They are not staged under the portal, not linked, and no page names an individual flight, track or call sign from them.
- **Three clearly separate entries in the navigation**, each answering one question: **Algorithms** (how is it computed?), **Limitations** (when is it missing or wrong?), **Accuracy** (how good is it, overall and at which airports?).

### Task 5: Algorithms, Limitations, Accuracy

**Files:** Create `content/algorithms/index.qmd` (overview: every item in one table with a one-line description and links to its algorithm page, its limitations entry and its accuracy entry) and `content/algorithms/<item>.qmd` (one per item, one template); `content/limitations.qmd` (one page, one section per item, same order as the overview); `content/accuracy/index.qmd` (overall accuracy of every item in one table, how to read it, the labels), `content/accuracy/europe.qmd` (the 100 busiest European airports), `content/accuracy/world.qmd` (the 100 busiest airports worldwide); `papers/accuracy-by-airport-v1/index.qmd` (short study note: data, method, results; no individual flights); `tools/gen_item_tables.py` (writes the generated tables under `content/*/_generated/` from the staged CSVs). Modify `_quarto.yml` (sidebar: Algorithms, Limitations, Accuracy as three entries next to Data), the present pages under `content/methods/` (they become short pointers to the new pages and keep their addresses), `content/data/dictionary.qmd` (each type and column links to its algorithm page), `content/data/limitations.qmd` (dataset-wide limits stay; item limits move to `content/limitations.qmd` with a pointer), `tools/check_site.py`.

**Template of an algorithm page** (exact headings, in this order): `In one sentence`, `What it needs` (which reports or other items must exist), `How it is computed` (numbered steps, every threshold with its unit, complete enough to follow without the code), `What is published` (the event type or columns, and the fields of `info`), `Use cases` (the owner's additions: "Add use case examples as well for each flight event / flight list and measurement" and "No need to fully work them out" — two or three short use cases per item, one or two sentences each: what the item can be used for, which other items it is combined with, and the limitation to keep in mind; no worked-out queries; for example "Taxi-out time per airport: the take-off time minus the off-block time of the same flight. Both must exist, which depends on ground reception at the airport."), `Limitations` (two lines and a link to the item's section on the Limitations page), `Accuracy` (the headline figure and a link to the item's row on the Accuracy page), `Full study`.

Use cases are short ideas for using the data and belong on the portal. They name published items correctly (checked against `src/opdi/output/schema.py`) and never show an individual flight from the spot checks. Worked query examples stay on the data access page only. `content/algorithms/index.qmd` also lists every use case in one table (question, items used, link), so a user can start from the question.

**Each section of the Limitations page** (exact sub-headings): `When it is missing`, `When it can be wrong`, `What not to conclude from it`.

**The Accuracy pages:** the overall table has one row per item: reference used, movements compared, share found, share within 60 s, median difference, 90th percentile (or the item's own measure), label; "Not validated" with what would be needed where no reference exists. The airport pages: how to read them, the labels and their thresholds, the count behind every figure, one table per item group with the airports as rows, and the CSV to download. The world page says in its first paragraph that outside Europe it shows how often an event is found, not whether it is correct.

`tools/check_site.py` checks: every published item has an algorithm page on the template, a section on the Limitations page and a row on the Accuracy page; every number on these pages is in a staged CSV; no page under `content/` or `papers/accuracy-by-airport-v1/` contains a track identifier or links to the spot-check material.

- [ ] Step 1: the generator and the checks, tests first. Step 2: the algorithm pages, written from the code as it is after Part 3. Step 3: the Limitations page, from the code and from the findings of Part 2 that were left as limits. Step 4: the Accuracy pages from the staged CSVs. Step 5: navigation, pointers from the old pages, cross-links. Step 6: `tools/check_site.py`, `check_papers_index.py`, `pytest tools/tests`; commit; push.
- [ ] Independent read-through of the new pages against the code and the CSVs (truth, completeness, readability, Review Focus 2 to 4); findings fixed.

## Part 6: hand over

### Task 6

- [ ] Whole test suite in chunks; push `industrialisation`; update the draft PR, `docs/industrialisation-handover.md` and `CLAUDE.md`; all scratch deleted.
