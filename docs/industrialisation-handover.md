# OPDI release v2.0.0: handover notes

Written 2026-11-11. This page lists what was built, what the owner still has to
decide or supply, and the commands to free the old data in the bucket.

## Where the work is

| Repository | Branch | Contents |
|---|---|---|
| `opdi` | `industrialisation` | Daily publishing step, reader, Trino table and view definitions, Airflow DAGs, code clean-up and documentation |
| `opdi-portal` | `feature/new-topics` | All earlier branches merged; new Data and Methods sections; 2026 PRC Data Challenge; not merged to `main` |
| `opdi-workspace` | `industrialisation` | Design, the three plans, this page, `CLAUDE.md` |

## What the owner still has to supply or decide

1. **Bucket space.** One worldwide day is about 0.42 GB of published files.
   The first backfill (January 2025 to November 2026) needs about 290 GB for
   the published files alone. The quota is 200 GB. Raise the quota or publish
   to a larger warehouse before starting.
2. **Trino names.** The catalog `minio` and schema `opdi` in
   `opdi/src/opdi/sql/trino/` and in the portal configuration are placeholders.
3. **Addresses.** The Trino file-browser address and the public download
   address are empty configuration values (`R/portal_config.R` in the portal,
   `PUBLISHED_BASE_URL` in `opdi/src/opdi/published.py`).
4. **Airflow.** Create the pool `opdi` with one slot and set the Variables
   listed in `opdi/airflow/README.md`. How the Airflow workers reach the
   pipeline is set by one Variable, `opdi_command_prefix`.
5. **Take-off and landing times.** The pipeline publishes the time found on
   the runway first, and the time found from the climb or descent otherwise.
   The validation study found more times within 10 seconds with the opposite
   order. Changing the order changes published times, so it was left as it is.
6. **Portal.** The site has not been rendered since the restructure (Quarto
   and R are not on the pod). `PUBLISHING_CHECKLIST.md` in the portal lists the
   steps and the facts to confirm.

## Freeing the old data in the bucket

Inventory on 2026-11-11 (bucket `eurocontrol`, 192.7 GB of 200 GB used):

| Prefix | Size | Note |
|---|---|---|
| `opdi-world/` | 101.7 GB | Earlier worldwide runs and study tables |
| `opdi/osn_symposium_paper_2026/` | 80.7 GB | Data of the symposium paper. Check before deleting. |
| `opdi/` (everything else) | 10.3 GB | Earlier European tables and study tables |
| `opdi-prod/` | does not exist | Nothing to delete |

Before deleting, know what goes with it:

- **Reference tables** (airport zones, airport layouts, runway zones, airport
  and aircraft lists, FIR cells) live in `opdi-world/`. The new release
  rebuilds them with `python -m opdi.orchestration reference`. Copying them
  first (step 1 below) saves that time.
- **Study tables** under `opdi/research/` and `opdi-world/research/` are the
  inputs of the method studies on the portal. Without them a study can no
  longer be re-run from scratch; it still renders from its committed data with
  `OPDI_RENDER=allow-stale`.

The commands use the MinIO client `mc`. Set the alias once:

```bash
mc alias set osn https://s3.opensky-network.org "$AWS_ACCESS_KEY_ID" "$AWS_SECRET_ACCESS_KEY"
```

Step 1 (optional), keep the reference tables for the new release:

```bash
for t in h3_airport_detection_zones h3_runway_zones hexaero_airport_layouts oa_airports oa_runways osn_aircraft_db; do
  mc cp --recursive "osn/eurocontrol/opdi-world/$t/" "osn/eurocontrol/opdi/v2.0.0/$t/"
done
mc cp --recursive osn/eurocontrol/opdi-world/research/airspace_events/ref/opdi_h3_airspace_ref/ \
                  osn/eurocontrol/opdi/v2.0.0/opdi_h3_airspace_ref/
```

Do not copy `coverage_marker`: the first run writes it. After copying, run
`python -m opdi.orchestration reference`; it builds only what is missing.

Step 2, look before deleting:

```bash
mc du --depth 2 osn/eurocontrol/
```

Step 3, delete the old worldwide warehouse (101.7 GB):

```bash
mc rm --recursive --force osn/eurocontrol/opdi-world/
```

Step 4, delete the old contents of `opdi/`, keeping the new release folder:

```bash
# Everything directly under opdi/ except v2.0.0/ and the symposium folder (10.3 GB)
mc ls osn/eurocontrol/opdi/ | awk '{print $NF}' | grep -v -e '^v2.0.0/$' -e '^osn_symposium_paper_2026/$' |
  while read -r p; do mc rm --recursive --force "osn/eurocontrol/opdi/$p"; done

# The symposium paper data (80.7 GB): only when it is no longer needed
mc rm --recursive --force osn/eurocontrol/opdi/osn_symposium_paper_2026/
```

`opdi-prod/` does not exist, so there is nothing to run for it.

## Plausibility of milestones (added 2026-11-12)

Plan: `docs/superpowers/plans/2026-11-12-milestone-plausibility.md`. Study note
and data: `opdi-portal/papers/milestone-plausibility-v1/`. Portal page:
`content/methods/plausibility-checks.qmd`.

- **Block times are read from the ground.** An off-block time is the start of a
  movement from a standstill before the take-off of its own leg; an in-block
  time is the end of the last movement after the landing.
- **Crossings over missing values** are placed between the last known value and
  the current one, and are not given when those are more than 30 minutes apart.
- **Five checks** remove an event only when known facts contradict it: no time;
  a ground event while clearly flying; a take-off or landing time more than
  1,500 ft above the airport; an off-block time not before its take-off; an
  in-block time not after its landing. Removed events go to the working table
  `opdi_flight_events_rejected` and are not published.
- **The v0.0.2 settings are unchanged**: the three switches are off there.

Measured on one worldwide day (14 June 2026) against airport records:

| | Before | After |
|---|---|---|
| Off-block times placed in flight | 4,470 | 27 |
| In-block times placed in flight | 2,934 | 106 |
| Off-block times confirmed within 60 s | 2,167 | 2,172 |
| In-block times confirmed within 60 s | 2,204 | 2,319 |
| Take-off times confirmed within 60 s | 13,737 | 13,768 |
| Landing times confirmed within 60 s | 15,746 | 15,750 |

The checks removed 312 events that day and none that a record confirms. Five
block times that matched a record before are no longer published; each was
read individually and was placed while the aircraft was flying or on the
take-off roll, so the match was by chance.

Limits: one day and about 90 European airports; the height limit for take-off
and landing times removed nothing on that day; landing events at the runway
threshold above 500 ft are left as they are.

## Processing time (measured, one worldwide day, eight executors)

Step 03 takes 29 minutes and step 04 about 69 to 87 minutes, of which holding,
point merge and FIR crossings are about 51 minutes. Steps 01, 02 and 02a were
not timed on a full day. A whole day is roughly three to four hours, so the
2025-2026 backfill run one day at a time is about three months. Speeding it up
without changing results has been discussed and is waiting for the owner's
go-ahead; nothing was started.

## Rename, processing speed and ADS-C (added 2026-11-14)

Plan: `docs/superpowers/plans/2026-11-13-rename-adsc-speed.md`. Study notes on
the portal: `papers/processing-speed-v1/` and `papers/adsc-effect-v1/`.

**Rename.** Under the default settings the runway threshold crossing is now
the event `xing-runway-threshold`. The v0.0.2 settings keep their own
`landing` event.

**Processing time of one worldwide day (14 June 2026).**

| Step | Before (8 executors) | After (12 executors) | Figure |
|---|---|---|---|
| 01 Ingest | 1,092 s | 396 s | scaled from a 4-hour slice |
| 02 Tracks | 4,173 s | 2,133 s | scaled from one aircraft in 12 |
| 02a Cleaning | 1,641 s | 1,225 s | scaled from one aircraft in 12 |
| 03 Flight list | 940 s | 387 s | measured on the full day |
| 04 Events | 4,687 s | 1,741 s | measured on the full day |
| 04b Flight-list milestones | 196 s | 55 s | measured on the full day |
| 09 Publish | 322 s | 246 s | measured on the full day |
| Total | 13,051 s (3.6 h) | 6,183 s (1.7 h) | |

- The one-hour target was not reached. About 120,000 executor-seconds of work
  remain and the cluster quota allows 12 executors (24 cores). Getting under
  one hour needs about twice the cores, or a track step that works
  incrementally from day to day.
- Steps 01 to 02a could not be timed on a full day: their output (about 20 GB)
  does not fit in the bucket. A separate run ingested 72 hours of the archive
  in 2,058 s, which suggests step 01 is nearer 690 s than 396 s per day.
  Re-time these steps on a full day when the bucket has room.
- Results are the same. Steps 03 to 09 on the full day: the same 175,615
  flights and airports, all 6.74 million events equal on flight, type and
  time, measurements equal. Steps 01, 02 and 02a on a slice and a sample:
  every row and every column equal. Two runs of the same code now give
  identical output, after two order-dependent choices were made fixed (which
  taxiway, apron or stand an event names; the order of the list of other
  candidate airports).
- To look at: in one run of step 09, 13 executors were stopped for lack of
  memory and Spark repeated their tasks. The output was correct, but the
  publishing step may need more executor memory or smaller tasks.

**ADS-C.** One day, the 1,884 aircraft with ADS-C, the pipeline run without
and with those reports. With ADS-C: reference flights rebuilt as one track
85.2% to 87.5%; flight time without any report 5,016 h to 4,131 h; flights
with both airports right 900 to 902; published distance within 5% of the
reference route 71.7% to 75.0%; no track holding two flights in either run.
Against that: FIR crossings over the ocean are placed on the first report in
the new region and can be many minutes late; some flights gain a short
separate piece made only of ADS-C reports; identifiers change where a track's
start moves or tracks are joined or split; the Airflow run has no ADS-C step.
Recommendation: keep ADS-C off in release v2.0.0. The setting was not changed.

## Settings, a measured full day, and reliability (added 2026-11-15)

**A whole worldwide day, measured.** 14 June 2026 was processed from the raw
archive through `python -m opdi.orchestration day`, the command the Airflow
DAGs use, with 13 executors:

| Step | Seconds |
|---|---|
| 01 Ingest (one new day) | 384 |
| 02 Tracks | 1,677 |
| 02a Cleaning | 744 |
| 03 Flight list | 356 |
| 04 Events | 1,550 |
| 04b Flight-list milestones | 79 |
| 09 Publish | 209 |
| Total | 4,999 (1.4 h); a second run took 5,117 |

**Settings kept** (results identical): 13 executors instead of 12; 104 shuffle
partitions; executors of 11 GiB heap plus 2,560 MiB of memory outside the
heap; storage retries with a 120-second request timeout. Tried and not kept:
Arrow-based Python functions, Kryo. The memory outside the heap had never
reached the cluster before (it was only set for another environment), which
is why every executor was killed for memory in the publishing step of each
earlier full-day run.

**Reliability.**
- A step that fails for a passing reason (storage throttling, a lost
  executor, a timeout) runs again, up to 2 times, 120 seconds apart. A full
  bucket, an empty table, a schema error or a lack of memory stops at once
  with the cause.
- Step 04 survives a lost executor: what it keeps in memory is held twice.
- A table folder left empty by a failed write is reported as empty.
- A day of position reports counts as fetched only when its fetch is recorded
  as finished.
- Fault test: one executor was deleted during step 02 and one during step 04
  of a full day. The day finished without a step retry, with results identical
  to the undisturbed run, about 460 seconds later.
- A day needs about 29 GB of temporary space; the run refuses to start with
  less than 32 GB free.
- What can still stop a day is listed in `opdi/airflow/README.md`; each case
  stops with a message and the next run continues from the last finished step.

**To check before the backfill.** The full day built from the raw archive has
161,881 flights. The comparisons of the last weeks started from tracks stored
on 17 October, which give 175,615. The difference sits at the ends of tracks
and was not traced. The likely reason is that the stored tracks were built
with the earlier track rule, before tracks were kept across long reception
gaps; this should be confirmed. The numbers of the plausibility study and of
the before/after comparisons were measured on those stored tracks.

## Track-step partitions and plausibility on current tracks (added 2026-11-15)

**Track step.** With 104 shuffle partitions the track step wrote about 166 GB
to the executors' local disk while sorting. With 416 it writes none and is
about 9% faster (1,465 s against 1,613 s, mean of two runs each), with
identical tracks. Only the track step uses 416; cleaning and events stay at
104, where they are faster. The local disk is the node's own (527 GB, no
per-pod limit); the peak use of one executor was about 12 GB, so there was no
risk of eviction.

**A full day in this session:** 01 383 s, 02 1,446 s, 02a 706 s, 03 344 s,
04 1,519 s, 04b 48 s, 09 163 s; total 4,609 s (77 minutes).

**Plausibility figures, re-measured on tracks built with the current rule**
(161,881 flights on 14 June 2026; the earlier figures used tracks stored on
17 October with the earlier rule, kept under
`papers/milestone-plausibility-v1/data/tracks_of_17_october/`):

| | Before | After |
|---|---|---|
| Off-block times placed in flight | 4,947 | 31 |
| In-block times placed in flight | 3,127 | 111 |
| Off-block times confirmed within 60 s | 2,169 | 2,173 |
| In-block times confirmed within 60 s | 2,201 | 2,320 |
| Take-off times confirmed within 60 s | 13,767 | 13,764 |
| Landing times confirmed within 60 s | 15,752 | 15,752 |

The checks removed 332 events and none that a record confirms.

**A take-off time placed on the landing roll: corrected (2026-11-16).** After
touchdown some reports flagged on the ground carry a height 20 to 60 ft above
the airport, and the runway pass was read as a departure. A pass now counts
as a take-off only if it ends in the air (setting
`runway_take_off_needs_air_report`, off in the v0.0.2 settings). On the day:
take-off times confirmed by airport records 13,764 to 13,782, no confirmed
take-off, landing or block time lost; tracks with a take-off after their own
landing 508 to 428 (the 80 removed were all landing rolls; the rest are
turnarounds and touch-and-goes, and 8 landing rolls whose last reports were
not flagged on the ground); 91 flights no longer have the destination as
their departure airport. Three real circuit take-offs at small airfields lose
their take-off events; none has an airport record.

**To look at.** When publishing (step 09) runs in the same Spark session
straight after the events step it can run short of memory in one task; run
alone it passes. The Airflow path starts a new session per step, so it is not
affected, but study scripts that chain steps in one session are.

## Spot checks, corrections, accuracy per airport (added 2026-11-17)

About 3,000 example events (50 for each of 63 items) from one worldwide day were read, the odd cases were reduced to shared causes, and the causes were corrected in six groups, merged, reviewed independently and fixed. Code: `opdi` branch `industrialisation` at `4b59a63`; portal: `feature/new-topics` at `396cf09`. The decisions are in `docs/superpowers/specs/2026-11-17-spot-check-corrections.md`, the plan in `docs/superpowers/plans/2026-11-16-spot-checks-accuracy-portal.md`, the way the corrections were measured in `opdi/benchmarks/CORRECTIONS.md`.

**Result on three days (10, 14 and 17 June 2026, worldwide), against airport records and Network Manager flights:**

| Item | Found among reference movements | Within 60 s (median) |
|---|---|---|
| Take-off time | 80.8% | 93.2% (21 s) |
| Landing time | 81.7% | 94.5% (18 s) |
| Off-block time | 35.0% | 40.3% (99 s) |
| In-block time | 33.4% | 72.0% (34 s) |
| Departure / destination airport | 85.9% / 78.5% | 98.6% / 98.5% equal |

Before the corrections, on 14 June: off-block 34.4% within 60 s, in-block 31.7%, take-off runway equal to the record 87.4% (now about 97%), threshold crossings found for 7.5% of landings (now about 41%).

**New version strings:** flight list v6.0.0, events_v0.4.0, holding / point merge / FIR v0.2.0, distance_v0.0.4, time_v0.0.2. Every correction is one config field, on by default and off in the v0.0.2 settings, whose golden output is byte-identical.

**Cost:** on the full day the events step used 3.8% fewer executor-seconds than before the corrections and the three steps together 3.4% more; wall time about 11% longer. The review fixes added about 1% to the events step on the sample; this was not measured again on a full day in isolation.

**Portal:** Algorithms (index and 26 item pages, each with its sources and short use cases), Limitations, and Accuracy and availability (overall, 100 busiest European airports, 100 busiest worldwide). All tables are generated from `assets/accuracy/*.csv` by `tools/gen_accuracy_tables.py` and `tools/gen_item_figures.py`. The site has not been rendered here (no Quarto).

**Limits that remain (also on the portal's Limitations page):**
- A track holding several flights gets one flight-list row naming the track's first and last airport; rings and block times at a middle stop are not on the row. Solving it means splitting such tracks, which changes flight identifiers.
- Off-block times are limited by ground reception at the stand: good at 2 of the 100 busiest European airports, limited at 34.
- Ground events are nearly absent at some large airports (Istanbul, Heathrow, Rome Fiumicino, Palma, Copenhagen); the cause is not established.
- Ten of the 100 busiest European airports have no take-off or landing time on any of the three days (LGIR, LIRN, LICC, LATI, LTAC, LTBJ, GCTS, LEBB, LGKR, LUKK), and OPDI rarely or never names them as destination. This looks like a reference or airport-zone problem and should be investigated.
- At ENBR, ENZV and ENVA the runway named never equals the airport record although the times agree (probably a difference in runway names); at Malta most take-off times and runways disagree.
- A landing where reception ends in the flare, and a take-off first received more than 1,500 ft above the ground, get no time.

**Open questions for the owner:**
- Where the T00 to T25 milestone numbers come from: the A-CDM Implementation Manual (v5.0, 2017) numbers 1 to 16 and has no T codes; code comments call them "ICAO milestone numbers" and were left as they are.
- Whether the runway-pass method derives from the traffic library's runway alignment (the pages say "comparable").
- Whether US patent 11,935,419 B2 is the patent behind the holding thresholds.
- Whether to remove `papers/milestone-plausibility-v1/data/examples.csv` (individual flights) from the portal.

**Scratch of this work in the bucket** (all under `opdi-world/research/spot/`, about 13 GB; nothing else was written). It can be deleted once the figures are accepted; the tables on the portal cannot be rebuilt without re-processing the days:

```bash
mc rm --recursive --force osn/eurocontrol/opdi-world/research/spot/
```

## Off-block, one row per flight, airport checks (added 2026-11-19)

Code: `opdi` `industrialisation` at `e0d9c7b`; portal `feature/new-topics` at `8bad855` (on top of the owner's restructure of 2026-11-18). Measured on 10, 14 and 17 June 2026.

- **Off-block time.** Only about 15% of departures in the airport records are received at the stand before they leave (about 38 of the 100 busiest European airports have almost no ground reception; where stands are received, 38% of departures are received before off-block against 73% of arrivals after in-block). The rule now takes the start of a slow push-back (`off_block_at_start_of_push_back`): within 60 s 40.3% → 48.6%, median 99 → 64 s. `info.first_seen_on` (`stand` 67.4% within 60 s, `apron` 63.1%, `taxiway` 9.6%) lets users leave out the unreliable times. 13 off-block times that were within 60 s of the record moved beyond it; accepted against the gain.
- **One flight-list row per flight** (`flight_list_one_row_per_flight`, flight list v6.1.0, events_v0.5.0). A track with a landing and, at least 300 s later, a take-off is divided; flights are named `<track_id>-L<n>`; tracks and `track_id` are unchanged; working table `opdi_flight_legs`. About 0.5% of tracks (874 to 937 a day). To join flights to tracks, strip `-L<n>`. Circuits with full stops at one airport stay one row.
- **Airports with no times** (LGIR, LIRN, LICC, LATI, LTAC, LTBJ, GCTS, LEBB, LGKR, LUKK, LDZA): the OpenSky archive holds no reports near the ground there. Not a pipeline error.
- **Runway reference.** ENBR, ENZV and ENVA records hold a placeholder runway; Malta's records disagree with the tracks. The benchmark compares a record's runway only when it is a runway of that airport: runway equal 97.0% → 99.0% (take-off), 96.6% → 98.6% (landing). The fallback method's choice between parallel runways was corrected (Marseille).
- **T04…T21** are labels of the PRU flight diagram with no official status; code comments and pages say so.

**Open:** whether the plausibility checks should be described on the portal (their page was removed); the portal has still not been rendered here; nine pipeline tests fail as they did before these changes (seven need the portal's study files; two in `test_window_frames.py` not examined); `content/data/index.qmd` quotes row counts of an earlier build of 14 June.

**Scratch in the bucket** (about 6.3 GB, all under `opdi-world/research/spot/`): `d0610n`, `d0617n`, `d0614c`, `runs/next_day`, `runs/next_1`. The command in the section above deletes it; afterwards the portal tables cannot be rebuilt without processing the days again.
