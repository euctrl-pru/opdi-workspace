# ADS-C / ADS-B fusion — spec

**Date:** 2026-10-16
**Branch:** `adsc` (meta-repo, `opdi`, `opdi-benchmarker`, `opdi-portal`)

## Problem

OPDI builds every track from ADS-B state vectors. OpenSky also publishes an
ADS-C feed that OPDI has never read. The hypothesis to test: **merging ADS-C
into the ADS-B trajectory improves coverage and therefore milestone
extraction.**

## What the source actually is

Measured on `s3a://readsb-aircraft-states/topics/readsb.aircraft.adsc.avro`,
day 2026-06-01 (251 objects, 4.9 MB, read end to end):

| Fact | Value |
|---|---|
| Rows, worldwide | 23,116 / day |
| Rows inside `EUROPE_BBOX` | 4,973 / day (21.5%) |
| Distinct airframes in box | 941 |
| Median cadence per airframe | **~600 s** |
| In-box rows above FL300 | 97.6% |
| In-box rows below 10,000 ft | 48, all day |
| In-box rows west of 10°W | 74.5% |
| `on_ground` true | never |
| Partition coverage | 2023-07-07 → now, 982 days, 24 gaps >1 day |

Populated columns: `time`, `hex`, `lat`, `lon`, `alt_baro_ft` (100%),
`track` (99.7%), `flight` (97.7%), `gs` (58%, kt), `baro_rate` (58%, ft/min),
`squawk` (44%), `messages`.

**100% NULL** on the sampled day: `alt_geom`, `ias`, `tas`, `mach`,
`mag_heading`, `true_heading`, `roll`, `track_rate`, `geom_rate`, `oat`, `tat`,
`wd`, `ws`, `nav_qnh`, `nav_altitude_mcp`, `nav_altitude_fms`, `nav_heading`,
`nav_modes`, `registration`, `ac_type`, `ac_desc`, `category`, `version`,
`nic_baro`, `nac_p`, `nac_v`, `sil`, `sil_type`, `gva`, `sda`, `rr_lat`,
`rr_lon`, `last_position`, `gps_ok_before`, `db_flags`.
Constant placeholders: `nic`=0, `rc`=0, `seen`=0, `seen_pos`=0, `rssi`=−49.5,
`mlat`=`[]`, `tisb`=`[]`, `type`=`'adsc'`.

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
| Extras struct | Not added | All candidate fields are 100% NULL; Task 1 confirms across history |
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
