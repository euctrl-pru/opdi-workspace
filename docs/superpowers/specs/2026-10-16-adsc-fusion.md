# ADS-C / ADS-B fusion — spec

**Date:** 2026-10-16
**Branch:** `adsc` (meta-repo, `opdi`, `opdi-benchmarker`, `opdi-portal`)

## Problem

OPDI builds every track from ADS-B state vectors. OpenSky also publishes an
ADS-C feed that OPDI has never read. The hypothesis to test: **merging ADS-C
into the ADS-B trajectory improves coverage and therefore milestone
extraction.**

## What the source actually is

Measured on `s3a://readsb-aircraft-states/topics/readsb.aircraft.adsc.avro`.
First read end to end for 2026-06-01 (251 objects, 4.9 MB), then by
`benchmarks/adsc_census.py` over **eleven days spanning 2023-09-01 to
2026-10-15** (`benchmarks/out/adsc_census/adsc_census.csv`). Ranges below are
across all eleven; the parenthesised figure is the three study days
(2026-06-05/06/07), each of which read all 24 hourly partitions.

| Fact | Value |
|---|---|
| Rows, worldwide | 409 – 32,206 / day (25,714 – 27,891) |
| Rows inside `EUROPE_BBOX` | 140 – 5,420 / day (4,725 – 5,420) |
| Share of rows in box | 6.7 – 34.2% (18.4 – 19.4%) |
| Distinct airframes in box | 74 – 994 (953 – 994) |
| Median cadence per airframe | 277 – 1,139 s (**543 – 568 s**) |
| In-box rows above FL300 | **94.4 – 98.6%** (96.8 – 98.0%) |
| In-box rows below 10,000 ft | 1 – 67 / day (44 – 67) |
| In-box rows west of 10°W | **65.8 – 83.8%** (75.2 – 76.9%) |
| In-box rows with `on_ground` true | **0 on every day** |
| Exact duplicate rows | **0 on every day** |
| Partition coverage | 2023-07-07 → now, 982 days, 24 gaps >1 day |

The 2026-03-22 day read only 6 of 24 partitions — it was picked deliberately
from the gap list — and its 277 s cadence is a partial-coverage artefact, not a
denser feed. The study days are the ones to read.

**The feed's schema has grown, and that was not anticipated.** Fill rates by
day show two step changes, so a historical ADS-C study gets materially less
than a 2026 one:

| Column | 2023-09 → 2025-03 | 2025-09 → 2026-03 | 2026-06 → 2026-10 |
|---|---|---|---|
| `time`, `hex`, `lat`, `lon`, `alt_baro_ft` | 100% | 100% | 100% |
| `track` | **0%** | 99.9% | 99.7 – 99.9% |
| `gs`, `baro_rate` | **0%** | 65 – 82% | 58 – 71% |
| `flight`, `squawk`, `on_ground` | **0%** | **0%** | 97 – 98% / 38 – 51% / 100% |

So before roughly 2025-09 the feed is position and barometric altitude only:
no ground speed, no vertical rate, no callsign. For this study that is mostly
harmless — the callsign is deliberately dropped anyway — but a merged arm over
2023–2024 would write ADS-C rows with NULL `velocity` and `vert_rate`, and any
speed-based detector sees NULL rather than a value on those rows.

**0% on every one of the eleven days** — the gate the schema decision rests
on: `alt_geom`, `ias`, `tas`, `mach`, `mag_heading`, `true_heading`, `roll`,
`track_rate`, `geom_rate`, `oat`, `tat`, `wd`, `ws`, `nav_qnh`,
`nav_altitude_mcp`, `nav_altitude_fms`, `nav_heading`, `registration`,
`ac_type`, `ac_desc`, `category`. Also NULL throughout: `nav_modes`,
`version`, `nic_baro`, `nac_p`, `nac_v`, `sil`, `sil_type`, `gva`, `sda`,
`rr_lat`, `rr_lon`, `last_position`, `gps_ok_before`, `db_flags`.
Constant placeholders rather than measurements: `nic`=0, `rc`=0, `seen`=0,
`seen_pos`=0, `rssi`=−49.5, `mlat`=`[]`, `tisb`=`[]`, `type`=`'adsc'`.

Objects are **snappy Parquet** despite the `.avro` in the topic name, carrying
the Avro schema `org.opensky.avro.readsb.Aircraft` in the footer.

A sibling topic `topics/readsb.aircraft.mlat.avro` exists (multilateration, not
ADS-C; 2026-05-18 onward only, 374 files/day) and is **out of scope**.

## The three facts that drive the design

1. **The ADS-C `flight` field is the IATA number** (`LH133`, `AF805`, `AY68`),
   not the ICAO callsign ADS-B broadcasts (`DLH133`). The production
   segmentation arm A9 `debounced` breaks a track when the last non-blank
   callsign changes, so carrying this field through would split exactly the
   tracks the merge exists to join. ADS-C rows therefore enter with
   `callsign = NULL`, which A9 bridges over by design
   (`F.last(..., ignorenulls=True)`).
2. **Step 02a would erase ADS-C.** `CleaningConfig.isolated_max_gap_seconds`
   is 20.0 s and `mask_isolated_points` NULLs any value whose nearest
   same-column neighbour is further than that on *both* sides. At 600 s
   cadence that is every ADS-C sample. The cleaner must become source-aware.
3. **The thinning rule must not touch ADS-C.** `decimation = "bucket"` keeps
   the newest row per (airframe, 5 s bin) and `"modulo"` keeps only rows at
   `event_time % 5 == 0`; ADS-C timestamps are arbitrary doubles, so the modulo
   arm would delete almost all of it. ADS-C is ingested unthinned.

## Time synchronisation — measured

The two feeds carry different clocks and neither states a position age that can
be trusted: ADS-C's `seen`/`seen_pos` are constant 0, i.e. the feed *asserts*
zero age for every report, and the readsb `Aircraft` record does not expose the
ADS-C report's own time-of-position. So the skew was measured instead, by
comparing ADS-C reports against ADS-B interpolated to the same instant.

**Method.** For each ADS-C report, take the two ADS-B samples of the same
`icao24` bracketing it within 60 s, interpolate position linearly to the ADS-C
timestamp, and decompose the offset `p_adsc − p_adsb` into **along-track** and
**cross-track** components using the direction of the ADS-B pair itself.
Cross-track is the position-disagreement floor; along-track divided by ground
speed is the implied time offset. Measured on 2026-06-01, 11:00–14:00 UTC, full
1 Hz ADS-B (all seven row groups per hour): **46 bracketed pairs, 25 airframes**
out of 1,088 ADS-C reports — the overlap is intrinsically small because ADS-C
reports mostly arrive where ADS-B cannot hear the aircraft.

| Quantity | Value |
|---|---|
| Median \|cross-track\| | **9 m** |
| Median \|along-track\| | **377 m** |
| Along:cross ratio | **43×** |
| Implied offset, median | **+1.48 s** |
| Implied offset, IQR | +0.32 … +1.96 s |
| Implied offset, σ (core) | 0.86 s |
| ADS-B's own position age (`time − lastPosUpdate`) | median 0.22 s, max 0.92 s |
| corr(implied offset, ADS-B position age) | 0.23 |

**Three conclusions, each load-bearing.**

1. **The residual is timing, not position.** A 43× along-to-cross ratio cannot
   be a position error, which would be isotropic. The two feeds agree about
   *where the track is* to within about nine metres.
2. **It is not ADS-B staleness.** The OSN archive carries positions forward,
   which would bias exactly this way — but `time − lastPosUpdate` is at most
   0.92 s and correlates with the offset at only 0.23. The displacement is on
   the ADS-C side.
3. **The skew is per-airframe, not global.** Within an airframe the implied
   offset is tight (σ 0.10–0.31 s over 2–6 reports); between airframes it
   ranges from 0.01 s to +2.09 s, with one airframe (`345318`) persistently at
   **−12.9 s** across three reports. So a single global correction is wrong for
   nearly every aircraft and would make the well-synchronised ones worse.

| Airframe | Reports | Median implied offset | σ |
|---|---|---|---|
| `47c968` | 6 | +0.01 s | 0.10 |
| `06a0b5` | 3 | +0.24 s | 0.11 |
| `461f59` | 3 | +1.19 s | 0.25 |
| `3c6565` | 2 | +1.33 s | 0.11 |
| `aac32f` | 3 | +1.72 s | 0.21 |
| `347386` | 5 | +1.86 s | 0.24 |
| `34659a` | 4 | +2.09 s | 0.26 |
| `345318` | 3 | **−12.93 s** | 3.15 |

**Magnitude in context.** At 240 m/s a 1.5 s offset is 360 m and the −12.9 s
airframe is 3.1 km. The alternative to having an ADS-C sample at all is a
straight line interpolated across a ten-minute reception hole, whose own error
is tens of kilometres. The timing skew is therefore a correction worth
reporting, not a reason to withhold the merge.

**What is done about it**

| Decision | Choice | Why |
|---|---|---|
| Measure it | New job `benchmarks/adsc_time_sync.py`, own paper chapter | A skew nobody has sized cannot be reasoned about |
| Global time correction | `AdscConfig.time_offset_seconds = 0.0`, **off** | The skew is per-airframe; a constant would harm the synchronised aircraft |
| Shadow window | Split into `shadow_before_seconds` / `shadow_after_seconds` | "The same observation" is not centred on zero once a feed lags; a −12.9 s airframe falls outside a window a symmetric one would centre wrongly |
| Derivative-spike filter | **Unchanged** | `latlon_d1_max_deg_s = 0.01` is ~1,100 m/s of apparent ground speed; 377 m (or 3.1 km) over a 600 s gap adds 0.0006 (or 0.005) deg/s. The filter is blind to this, and a guard would be machinery for a risk that is not there |
| Per-airframe correction | **Not in v1** | The method is this measurement; applying it needs a stability study over weeks, and 25 airframes on one day cannot support it |
| `last_pos_update` | Stays `time − seen_pos`, documented as the feed's *assertion* | `seen_pos` is constant 0 and the measurement shows that assertion is false by ~1.5 s |

**The interpolation exposure, stated rather than fixed.** `segment_id` exists
(`cleaning/native.py::add_segment_id`, splitting at 300 s holes) and is read by
**no detector** — `grep -rn segment_id src/opdi/pipeline/` finds it in two
comments only. So nothing in the pipeline today prevents an event from being
interpolated between an ADS-B and an ADS-C sample, and any event so derived
inherits the along-track displacement. That is a pre-existing property (the
same detectors already interpolate across 30-minute ADS-B holes), it is not
introduced by this change, and this branch does not fix it: making `segment_id`
load-bearing changes published detector behaviour and belongs in its own study.
It is reported in the paper's limitations chapter.

## Measured outcome (campaign of 2026-10-19/20)

Both arms built for 2026-06-05/06/07, ~5h05m, zero fatal errors. All seven
outputs staged under `papers/adsc-fusion-v1/data/`.

### Trajectories

| | base | merged |
|---|---|---|
| Rows | 152,071,295 | 152,082,840 |
| ADS-C rows surviving cleaning | 0 | **12,941** |
| Tracks | 140,437 | 141,232 |
| Tracks carrying ADS-C | 0 | 3,832 |
| **Tracks made only of ADS-C** | 0 | **905** |
| Track-hours | 216,208 | 218,228 |
| Two-callsign tracks (fusion guard) | 9,244 | **9,244** |

**The track count rose by 795, and that decomposes into +905 new tracks
against a net −110 among shared tracks.** The 905 are airframes ADS-B never
heard in the window, which had no baseline track at all; the −110 is bridging
joining tracks. The headline rise is new coverage, not fragmentation. The
original gate in this spec ("a rise means splitting") was wrong, and
`tracks_all_adsc` exists because the bare count cannot distinguish the three
mechanisms. Both figures are nets, so they bound a dominant effect rather than
excluding an absent one.

Of the 12,941 ADS-C samples, **12,498 (96.6%) sat >300 s from an ADS-B sample
on both sides**; **12,138** of those extended a track past its last ADS-B
report, and **360** bridged a genuine interior hole (median smaller side 19
min, p90 32 min). Of 41,710 matched airframe-days, 1,951 start earlier and
1,258 end later. **Fusion: exactly zero.**

### Time synchronisation at scale

764 bracketed pairs over 293 airframes, against the prototype's 46/25:

| | Prototype | Campaign |
|---|---|---|
| Median \|cross-track\| | 9 m | **8.7 m** |
| Median \|along-track\| | 377 m | **415.4 m** |
| Along:cross ratio | 43× | **48×** |
| Median implied offset | +1.48 s | **+1.41 s** |

The central finding reproduces: the residual is **timing, not position**. Two
claims in this spec needed narrowing:

- **"ADS-B staleness cannot explain it (max 0.92 s)" is withdrawn for the
  tail.** Position age reaches **152.3 s** — 36.6 km at cruise. At the median
  (0.34 s, ≈82 m, correlation 0.21) carry-forward cannot account for a 415 m
  displacement, so the centre is ADS-C skew; the extreme offsets are at least
  partly a stale reference.
- **"Tight within an airframe (σ 0.10–0.31 s)" overstated it.** Within-airframe
  σ spans **0.00–44.76 s** and per-airframe medians **−184.6 to +58.7 s**. The
  conclusion that a global constant is the wrong fix is *strengthened*, not
  weakened — the between-aircraft spread dwarfs anything one value could track.

### Milestones

APDF scores four milestones, all A-CDM/runway: `ATOT`, `AOBT`, `AIBT`, `ALDT`.

| Milestone | Truth | base | merged | Δ |
|---|---|---|---|---|
| ATOT | 11,675 | 11,235 | 11,235 | **0** |
| AOBT | 11,674 | 10,521 | 10,521 | **0** |
| AIBT | 11,657 | 10,113 | 10,111 | −2 |
| ALDT | 11,657 | 11,087 | 11,085 | −2 |

Bias and MAD identical to one decimal throughout. Per aerodrome: **2 of 80
cells changed**, both at EGCC, −2 each, bias unchanged. Total −4 of 42,956
detections (0.009%) — plausibly the claim filter moving a track whose first
sample shifted before midnight out of the day that owned it, which would also
explain the 1,396 ADS-B rows the merged arm has fewer of; not verified.

**The hypothesis as originally stated is not supported** — and a stronger
point: **this scoring cannot test it.** Every milestone APDF holds is in the
family ADS-C physically cannot reach. `first_seen`, `last_seen`, the FL
crossings and ADEP/ADES are outside the reference's vocabulary, so the null is
confirmed where a null was predicted and the coverage gain has *no independent
reference* against which it can be checked. Closing that needs a different
reference (Network Manager flight plans for oceanic entry fixes), not more
APDF days.

## Honest expectation

ADS-C cannot reach the runway or A-CDM milestone families: it has no
`on_ground`, no sub-FL100 samples to speak of, and a cadence three orders of
magnitude coarser than the detectors assume. Where it *can* help is the
coverage edge — `first_seen`/`last_seen`, the FL crossings, ADEP/ADES for North
Atlantic arrivals, and track continuity across oceanic reception holes. A null
result on the A-CDM family is an expected and publishable outcome; the paper
must report per-family so the two cannot be confused.

## Decisions taken

| Decision | Choice | Why |
|---|---|---|
| Arm isolation | Merged arm writes to `s3a://eurocontrol/opdi/research/adsc/{base,merged}` | Production `track_id` untouched; same pattern as the A9 study's `research/a9` |
| Merge point | Before segmentation, inside `TrackProcessor.process_month` | The hypothesis is about `track_id` and coverage; merging after segmentation cannot test it |
| Default | `AdscConfig.enabled = False` | Every existing caller keeps getting ADS-B only without saying so |
| Schema | Populated fields mapped into existing SI columns, plus `source` | Nothing invented; no column added for data that does not exist |
| `source` column | Added **only** when `adsc.enabled` | A column appearing in new partitions of a production table and not old ones breaks `spark.read.parquet` schema inference silently |
| ADS-C callsign | `NULL` | See fact 1 |
| Extras struct | Not added | All 21 candidate fields measured 0.0% on all eleven census days, 2023-09 to 2026-10 — **gate passed** |
| Paper scope | Full flight-events-v4 comparison, both arms, same 20 aerodromes | What was asked; per-family split makes a null result legible |
| Benchmark period | `2026-06-05`…`07`, APDF `202606` | `events_gt.PERIODS["2026"]` — the only month whose extract covers all twenty study aerodromes |
| Both arms rebuilt | Yes, baseline too | The existing V4 2026 tables were built by A8 `recommended`; reusing them would mix the segmentation change into the ADS-C delta |

## Deliverables

1. `opdi`: ADS-C ingestion (step 01b), gated pre-segmentation merge,
   source-aware cleaning, a two-arm experiment runner, three benchmark jobs,
   and a paper regeneration entrypoint.
2. `opdi-benchmarker`: `source` carried into the trajectory cache; the Flight
   page draws ADS-B and ADS-C as distinct traces on map and vertical profile.
3. `opdi-portal`: `papers/adsc-fusion-v1/` comparing milestone extraction
   between the two arms, rendering by running its own analysis.

## Non-goals

- IATA→ICAO callsign mapping.
- The `readsb.aircraft.mlat.avro` topic.
- Any change to a published `version` string, to `EUROPE_BBOX`, or to a
  detector.
- Synthetic gap filling between an ADS-B and an ADS-C sample (a documented PRC
  negative result).
- Per-airframe time-offset correction, and making `segment_id` load-bearing.
  Both are named in the paper as follow-ups with the measurement that
  motivates them.
