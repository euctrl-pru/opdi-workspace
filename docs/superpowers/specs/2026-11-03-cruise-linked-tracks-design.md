# Cruise-linked tracks: one `track_id` across an en-route reception hole

**Date:** 2026-11-03
**Branch:** `track-construction-v4` (meta-repo, `opdi`, `opdi-portal`)
**Paper:** `opdi-portal/papers/track-construction-v4/`, based on `track-construction-v3`
**Evidence:** `docs/superpowers/research/linking-fragmented-flight-trajectories.md`

## Why

A flight crossing the Atlantic loses ADS-B reception for hours. Today any gap over
30 minutes ends the track, so one flight becomes two `track_id`s, one in the USA
and one in Europe, and two rows in the flight list. Per-flight ADEP/ADES analysis
is then wrong: neither row has both ends.

Measured on 2026-06-05, among airframes with ADS-C: 1,204 of 1,334 flights crossing
the European box have a gap over 30 min, median 191 min, hole edges almost never
below 5,000 ft, and none of those flights has both ends right on one row.

ADS-C does not solve this. It closes 30 of 1,680 holes (`adsc-fusion-v1`, spec
2026-10-16). The literature has no validated method for it either, so OPDI measures
its own false-join rate rather than adopting a threshold.

## What is decided

**By the owner:**

- Change track generation. Keep the single notion of `track_id`. No separate
  `flight_id`, no second identifier, no flight-grouping layer.
- A linked flight is one `track_id`, and therefore one flight-list row with ADEP
  from its start and ADES from its end.
- The test looks at en-route evidence at the hole: whether the two edges can be one
  continuous flight, including implied speed being normal and not too slow.
- ADEP/ADES detection itself is not being optimised.
- The paper is `track-construction-v4`. `adep-ades-detection-v8` is dropped.

**By the author, for the owner to overrule when reading this:**

- A wrongly glued flight is worse than a split one, so unclear evidence means split.
- The rule is research-only. Making it `standard` is a separate decision, after the
  paper, because it creates another silent `track_id` boundary.
- No airport-reachability ellipse in this version (see Non-goals).
- The acceptance gates below.

## The rule: arm A10 `cruise_linked`

A new named arm in `src/opdi/pipeline/segmentation/methods.py`, registered in `ARMS`.
It is A9 `debounced` with one change. A9 breaks when:

    callsign_change OR low_altitude_gap OR general_gap

A10 breaks when:

    callsign_change OR low_altitude_gap OR (general_gap AND NOT linkable)

`group_cols=["icao24"]`, `month_suffix=False`, `id_from_start_time=True`, as A9.
`debounced` and `standard` must produce an unchanged partition.

`linkable` is evaluated at the first sample after a gap, against the sample before
it. **All** of these must hold, and a NULL in any required input means not linkable:

| Test | Definition | Parameter (default) |
|---|---|---|
| Gap not too long | `gap <= link_max_gap_minutes` | 360.0 |
| Cruise at the old edge | min altitude of the 5 samples before the gap `>=` floor | `link_cruise_floor_ft` 25000.0 |
| Cruise at the new edge | min altitude of the first 5 samples from the gap on `>=` floor | same |
| Not descend-then-climb | NOT (mean vert. rate before `< -r` AND mean after `> +r`). NULL vert. rate is neutral, because ADS-B sends it in separate messages | `link_edge_vert_rate_fpm` 500.0 |
| Callsign agrees | the last debounced callsign before the gap and the first non-blank callsign within the lookahead after it are not both non-blank and different. All blank is neutral. A callsign older than the lookback counts as blank, so two days' reads always decide alike | `link_callsign_lookahead_minutes` 10.0, `link_callsign_lookback_minutes` 60.0 |
| Implied speed not too slow | `gc_nm / (position_gap/60) >= floor` | `link_min_implied_speed_kt` 315.0 |
| Implied speed not impossible | `gc_nm / (position_gap/60) <= cap`. Above this is a duplicate address, not wind | `link_max_implied_speed_kt` 750.0 |
| Not slow for this flight | implied speed `>=` ratio x edge ground speed, where edge speed is the mean over the 5 samples before the gap. Off at 0.0 | `link_min_speed_ratio` 0.0 |
| No time for a stop | `position_gap - gc_nm / V_fast * 60 < link_min_stop_minutes`, with `V_fast` the fastest plausible ground speed. Off at 0.0 | `link_min_stop_minutes` 0.0, `link_vfast_kt` 650.0 |
| Real address | `icao24` not `000000`, `000001`, `ffffff` | constant |

Edge positions read through blank samples: the old edge is the last non-blank position within `link_edge_position_minutes` (5.0) before the hole, and the new edge the first within that span after it, because a position message often arrives after altitude and speed. Implied speed and the time-for-a-stop test use those samples' times. Round 2 found 15% of cruise holes had a blank position right at the edge.

The lookahead closes a false join: at `X, hole, blank, Y`, the blank first sample
agrees with anything, and A9's own callsign break does not fire at Y because X is
hours old. A callsign first appearing later than the lookahead still slips through.
The real-hole measurement catches that case, and the paper reports it.

The defaults are starting values. The study's grid (below) chooses the recommended
set, and the defaults are then updated to it.

Conventions: thresholds are aviation units with the unit in the field name, compared
against SI data by scaling (`altitude_ft()`, `speed_kt()`, `* 196.850394` for ft/min).
New fields go on both `SegmentationParams` and `SegmentationConfig`, because
`test_segmentation_base.py` asserts they match. Distance is `flights.py:haversine_nm`,
which handles NULL correctly. The callsign logic is extracted from `debounced`, not
copied, so A9 and A10 share one definition.

## The midnight duplicate

Day D reads from `D - DEFAULT_LOOKBACK_HOURS` (3 h) and claims tracks starting in D.
If a linked track's old piece ends before D's read window and its new piece starts
inside D, then D sees a new track and claims it, while D-1 claims the same samples
inside the linked track. Samples are published twice under two ids.

So under A10, `process_day` must use a lookback of at least
`ceil(link_max_gap_minutes / 60) + ceil(max(link_callsign_lookback_minutes, link_edge_position_minutes) / 60)` hours, which is 7 h at the default. An explicit
smaller `lookback_hours` raises. Other arms keep 3 h. Lookahead stays 24 h: a track
over it is reported by `tracks_touching_window_end`, whose margin widens to `link_max_gap_minutes` under A10, because a hole straddling the read end leaves the last sample far from the edge.

Step 01 reads the whole of D-1, D and D+1, so ingestion already covers this.

## Evidence plan

Worldwide coverage only: under the European box the far piece is never ingested.

**Storage.** The bucket had 11.04 GB free of 200 GB on 2026-11-03. Nothing existing
is deleted. Stage 1 writes nothing to S3. Stage 2 is limited to one day. Stage 2 is
estimated at about 5 GB: 2 GB per arm plus 0.8 GB of shared inputs at 10%. Runs check
free space first and stop under 3 GB. If space runs short, delete only what this study
wrote under `research/a10/`, starting with the state vectors after the second arm's
step 02, then each arm's `osn_tracks` once its clean table exists.

### Stage 1: re-segmentation, read-only (2026-06-14, 2025-06-14)

Re-segment `opdi-world/osn_tracks` for both days. They are the only worldwide full
days with both `osn_tracks` and `osn_tracks_clean`, and truth exists in
`flights_202606` and `flights_202506`. Only tracks starting on the day exist, so the
scored population is flights that **take off and land on the day**, as
`track_truth.load_flight_intervals` already defines it. That removes eastbound
crossings leaving before midnight UTC, so the sample leans westbound, and the paper
states it. This is not a from-raw run, and the paper says so, as
`track_fragmentation.py` does.

Three measurements, for the grid of parameter values:

1. **Real holes against Network Manager.** Attach truth by `icao24` and time, never
   callsign, because scoring the callsign test by a callsign join is circular. Each
   linked hole is a true link if it lies within one flight's `[t_off, t_land]`, a
   **false join** if any truth landing or take-off of that airframe falls inside it,
   since with both edges at cruise either one proves a stop, and unknown otherwise. Report the unknown share.
2. **Negative controls: a real landing hidden in a hole.** For consecutive truth
   flights of one airframe, delete from a cut point in flight 1 to a cut point in
   flight 2 and apply the rule there. A link is a false join. The base cut is the last
   sample at or above the cruise floor in flight 1 and the first in flight 2. That
   alone is too easy: both edges sit in descent or climb near one airport, so speed
   rejects nearly all of it. So each pair also gets deep cuts, moved 30, 60 and 120 min
   further into cruise on each side where the samples stay at or above the floor. This
   is the case that matters: A to B then B to C, with cruise edges and a plausible
   implied speed. Stratify by synthetic gap (30-120, 120-240, 240-360 min), cut depth
   and same or different callsign. Controls of 30 min or less are excluded, as they are
   never candidates. Longer than the point's `link_max_gap_minutes` counts as a correct
   rejection, and shows in its own stratum. Report the rate with a rule-of-three upper
   bound, because zero of n only bounds it at about 3/n.
3. **Positive controls.** Delete the middle of continuous truth flights at 60, 120,
   180 and 360 min, starting at or above the floor. No link is a missed link.

Per grid point also report tracks per truth flight, share of flights in one track,
and both V3 two-callsign counts. Those are reported, not gated: the span-based count
rose from 47 to 259 between A8 and A9 with no real merge, and longer tracks inflate it
further, so measurement 1 is the false-join gate instead.

The grid, 3 x 2 x 3 x 3 = 54 points: `link_min_implied_speed_kt` {250, 315, 400},
`link_min_speed_ratio` {0.0, 0.85}, `link_min_stop_minutes` {0.0, 20.0, 35.0},
`link_max_gap_minutes` {120, 240, 360}.

### Stage 2: one day through the real pipeline (2026-06-14)

Steps 01, 02, 02a, 03, 04, 04b for `debounced` and `cruise_linked`, into
`s3a://eurocontrol/opdi-world/research/a10/{debounced,cruise_linked}`, on a 10% sample
of airframes chosen by `abs(xxhash64(icao24)) % 10 == 0`, at the recommended
parameters. Sampling by airframe is sound because segmentation partitions by airframe.
Reference tables are read from `opdi-world`, and the write guard allows only
`research/a10/`.

Scoring is limited to flights that take off and land on the day. With one day built,
the arms differ at both ends: A10 moves an overnight piece to the day its track started,
which was not run, and picks up after-midnight pieces that `debounced` gives to the next
day. Flights inside the day are complete in both. Measures:

1. **The owner's goal.** Flight list against Network Manager, for flights crossing the
   European box: rows per flight, and flights with both ADEP and ADES right on one row.
2. **Detector damage.** Detectors have never seen a track with a 3-hour hole. Compare
   events per type between arms on the airframes with a linked track. Level segments
   are the main worry, since a hole can read as hours of level flight, so report any
   level segment spanning a hole. Check the cumulative distance across a hole against
   the great circle: it is expected to carry over and has not been verified.
3. **Nothing else moved.** Re-segment the sample's state vectors under both rules. Every
   `debounced` boundary missing from `cruise_linked` must be a linked hole, and
   `cruise_linked` must add no boundary of its own. APDF movement counts on the scored
   flights must not fall by more than 0.1%.

Detectors are not changed in this work. If damage appears, the paper reports it and
the fix is a follow-up with its own `version`.

### Acceptance gates (author's call)

Stage 1 picks the recommended set: the grid point with the most flights crossing the
European box in one track, then the most flights in one track overall, then the more
conservative settings, subject to, pooled over both days and also reported per day:

- negative-control false-join upper bound at most 1% overall,
- at most 5% in the same-callsign stratum, among gaps the point allows,
- false joins at most 1% of real linked holes with a known label.

A same-callsign stratum under 60 controls cannot reach 5% even at zero joins. It reads
"insufficient", does not fail the point, and marks the recommendation provisional.

**If no point passes,** the paper says so and recommends none, a valid outcome. Defaults
stay at the starting values, and Stage 2 still runs at the point with the lowest
false-join upper bound, labelled not recommended, so the detector question is answered.

**If a point passes,** its values become the defaults in one commit before Stage 2.

**Stage 2 can withdraw it.** The paper withdraws the recommendation if measure 1 shows
no gain, if measure 2 shows detector damage on most linked tracks, or if measure 3 fails.
Otherwise it stands with caveats. Which detector damage is acceptable stays the owner's
decision after reading the paper.

## The paper

`opdi-portal/papers/track-construction-v4/`, on the structure of v3: the page renders
by running `benchmarks/regenerate_track_v4.py`, each CSV comes from one declared job,
provenance is in `data/_manifest.json`. A card in `papers/index.qmd` and a rendered PDF.

It covers: the problem and the measured scale; what others do, from the research
report, keeping its evidence caveats; the rule; the midnight duplicate; the three
Stage 1 measurements; Stage 2; limitations; and what is not decided here.

Required limitations: truth is European, so transfer to other regions is unmeasured;
two days; Stage 1 is not from raw; Stage 2 is a sample; the population is flights inside
one day, which leans westbound; hard strata may be too small to bound, since the
research report warns they may need months of data. It states that promoting A10 would be a further silent `track_id`
boundary, and that nothing says which rule produced a row.

## Non-goals

- Making A10 `standard`, or moving anything published. `opdi-prod` is untouched.
- A `flight_id` or any second identifier.
- ADEP/ADES detection changes, and changes to any detector or published `version`.
- Airport-reachability ellipse and dark-airport map. It is the report's most promising
  and least proven idea, but it needs an airport join outside the break-expression
  contract. It is the follow-up if the gates show that time budget alone is not enough.
- The OpenSky API or a coverage map (dropped by the owner).
- ADS-C in the rule. A10 runs on whatever frame segmentation receives.
- Synthetic points in the hole (a documented PRC negative result). Linking assigns an
  identity and adds no sample.
- Machine learning, dynamic programming, or a per-group function.

## Left to the builder

File layout of the benchmark scripts and tests; how the Stage 1 grid is computed
efficiently, provided it stays native Spark; CSV column names; the prose of the paper.
