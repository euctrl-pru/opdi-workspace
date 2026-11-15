# Corrections decided after the spot checks of 2026-06-14

Seventeen readers went through about 3,000 examples of the 63 published items. Their findings are in this folder, one file per reading group (`a1.md` … `f3.md`), each with the cause in the code (file and line), the rate among random examples, and a query that counts the cause on the whole day. This file records what is corrected in the code, in six work packages, and what is documented as a limit instead.

## Rules for every correction
- Each correction sits behind its own config field in `EventConfig` / `DetectionConfig` / the relevant config class: on by default, off in the v0.0.2 settings (`legacy()` presets), named for what it does, documented in plain words. The v0.0.2 golden frames stay byte-identical with no regeneration.
- A unit test built from the real example fails first, then passes. The cases that must keep working are tested too: touch-and-go, go-around, helicopter and slow aircraft, training circuit, turnaround in one track, airport at high elevation, track starting or ending in the air, reception starting or ending mid-roll.
- No degradation: on the real day, no event that airport records confirm (within 60 s) is lost; each lost or moved one is read individually. Measured with the scorecard (`benchmarks/corrections_scorecard.py`).
- Not much slower: the events step may not take more than 5% longer.
- No change to how tracks are built (no new `track_id` boundary) and no new published column. New facts go into `info`.
- Native Spark only.

## Work package 1: altitude the detectors read (findings c1, c2, c3, d2, a1, f3, a3)
1. Every detector of step 04 reads an altitude that has been through the cleaning of step 02a (today they read `baro_altitude_c`, which step 02a passes through untouched; cleaning masks `baro_altitude` only). Decide by the code which is cleaner to do without re-running step 02a: build the altitude used in step 04 from the cleaned column at read time.
2. A plausible range: an altitude below -2,000 ft or above 60,000 ft is not used.
3. An altitude on a report flagged on the ground is not used as a flight altitude (ghost altitudes of 9,800 to 126,000 ft at the stand).
4. The level method (`level_segments.py`, `vertical_pru.py`) does not fill altitude before the first or after the last measured value, does not hold a value flat across a gap, and treats a point as not level when the measured values either side are more than about 60 s apart (false 5-minute level-offs where OpenSky repeats the last state; hours-long level-offs on parked aircraft).
5. Level segments: the existing minimum duration is applied (a quarter last 20 s or less; single-report segments); no level segment on the ground or the take-off or landing roll.
6. Tops: not placed on an implausible altitude; a relocated top takes its position from the nearest report with a position within 60 s (else none) and its time-passed from its own time; `info.observed` = false when the track shows no climb of at least 3,000 ft before the top of climb (or no descent of at least 3,000 ft after the top of descent), i.e. the top sits at the edge of reception.
7. Level-offs get a position, a distance and a time like other events (today NULL by construction).
Must not degrade: take-off and landing times (they read altitude near the ground). Documented limits: light-aircraft cruise splits into many level-offs; tops on flights without a cruise are the highest report; one pair of tops per track.

## Work package 2: runway events (findings a1, a2, a3, b3, f3) — `runway_ops.py`, `runways.py`, `elevation.py`
1. Height above the runway is measured from the same track's own altitude on the ground at that airport (reports on the runway or flagged on the ground), not from barometric altitude minus the published elevation; the published elevation is the fallback when the track has no ground report there. This removes the air-pressure error (the ground reading ranged from -571 to +232 ft on the day).
2. The runway direction is chosen by the direction the aircraft moves (bearing error), not by sideways distance at entry; a pass is classified per runway strip and one runway per pass is published. This corrects the reciprocal runway name (half of the runway-pass take-offs) and removes one event id on two rows (passes numbered per strip; parallels compared).
3. `runway-vacated` and the runway crossings are taken from reports on the runway only (not the approach corridor); nothing is published when the aircraft never had a report on the runway. `runway-vacated` in the air on approach was 10 of 30.
4. `take-off-roll` uses the ground speed derived from consecutive positions where reports carry no speed (99% of ground reports have none); a report without speed does not break the run; the event has a position.
5. `line-up` and `take-off-roll` only on a real departure (a slow or ground report before the fast run), not on a touch-and-go or a low approach.
6. `xing-runway-threshold`: only the last inbound crossing before a touchdown, below about 300 ft above the track's ground reference; not on low passes, circuits or overhead flights.
7. Go-around: a ground-flagged report, or a slow report without altitude, counts as a touchdown; no go-around when the low excursion lasts more than about 600 s or spans a gap of more than about 60 s; the low point must be in the approach corridor of a runway of the destination airport.
8. Runway crossings: an empty height on a report flagged on the ground counts as on the ground (today airliners are dropped); a landing, take-off or turn-off on the aircraft's own runway is not a crossing of it.
9. The fallback take-off and landing times (`method=legacy`): publish the position and altitude of the report the time comes from; `info.at_track_edge` = true when it is the first or last report of the track; do not accept a report more than about 500 ft above the ground reference or off the runway's extended centre line; for a landing, require the ground afterwards where reports exist.
The owner's earlier decision stands in part: where reception truly starts or ends mid-roll, `line-up`, `take-off-roll` and `runway-vacated` mark the first or last report on the runway; that is documented. Items 3 and 4 are corrections of the method, not of that limit.
Documented limits: one take-off and one landing time per track; the fallback method gives about 60% of the times.

## Work package 3: block times and stands (findings b1, f3, b2) — `ground.py`
1. Ground speed is derived between consecutive reports that have a position (gap of at most about 30 s); a report with unknown speed keeps the previous moving state instead of ending the movement. This is the real cause of off-block late / in-block early: 40% of ground reports have no position and under 1% a speed, so a taxi looked like a standstill and the take-off or landing roll was taken as the movement.
2. A standstill is judged on speed (at most 30 kt), with the ground flag only when no speed is known; a block time within 60 s of its take-off or landing, or between line-up and take-off, or between landing and runway-vacated, is not given.
3. The stand passage that licenses a block time is at the same airport as the block time, with a dwell of at least 30 s.
4. Off-block is the movement leaving the last long stand stop before the take-off; in-block the movement into the first long stand stop after the landing (not a tow hours earlier or later).
5. The stand named is the one with the longest dwell in the same part and airport.
6. The comparison that decides whether a one-sided track gets an extra part is NULL-safe, so every block time carries the take-off or landing of its leg in `info`.
7. The text "None" (also "nan", empty) as a stand name becomes NULL, at the reference build and in the detector (23% of stand values).
Documented limits: when the transponder is switched on during push-back the off-block time is the first report; vehicles and towed aircraft.

## Work package 4: flight list (findings f1, f2, f3) — `flights.py`, `flight_list_milestones.py`, step 00d
1. A runway event replaces the departure airport only if it is the first departure on the track and within 30 NM of the track's first report; the destination only if it is the last arrival, after every departure, and within 30 NM of the last report; a `method=legacy` time does not replace an airport (it was searched only at the airport already chosen); otherwise the airport found from the trajectory stays. This removes most of the 3,000 daily flights published with departure equal to destination.
2. The trend method picks the last landing airport on the track, not any.
3. The penalty for airports without scheduled service is not applied when the end report is on the ground or within about 2 NM and 1,500 ft of the field.
4. An airborne first report names a departure airport only when the track climbs in its first minutes.
5. Airport records whose name contains "Duplicate" are dropped in step 00d (CA-1291 "(Duplicate)YEG").
6. `FLT_ID` is trimmed (today padded with spaces to 8 characters).
7. `SUPERSEDED_*` needs an airport; an airborne leg is not a fragment; the ghost rule accepts a climb above the field or a last position more than 40 NM away as evidence of leaving.
8. The endpoint join keeps `icao24` and `flight_id` from either side; no row with a NULL day.
9. `AOBT`/`AIBT`/stand columns are taken at the departure / destination airport and on the right side of the take-off / landing; ring columns from outbound crossings after the take-off and inbound crossings before the landing.
10. `first_seen` / `last_seen` (flight list and events): one definition everywhere — the first and last report with a measured position after cleaning; for a track with no position at all, the first and last report with any measured value. Not the repeated state OpenSky sends for about 300 s after last contact.
11. `adep_p` is published in ranked order, cut to 30 NM; the wide candidate set stays internal for the runway and surface search.
Documented limits: only medium and large airports are candidates; tracks holding several flights; aircraft data from the October 2024 database; about one row in ten has no position; placeholder addresses.

## Work package 5: crossings, rings, FIR, distance (findings c3, d1, d2, d3)
1. A crossing (flight level, ring, FIR) whose two bracketing positions imply more than 1,000 kt is not given (spoofed or jumping positions fired all six rings within seconds).
2. Rings are computed around the airports after the runway-event correction, so a ring never names an airport that is not in the flight list.
3. FIR lookup: only reports whose position survived cleaning (today the map cell survives when cleaning blanks a position); the flight level from the cleaned altitude (work package 1); `info.vertical` = true when the change is between a lower and an upper region of the same area.
4. Crossings get their position from the reports that have one (interpolated within about 30 s), and their altitude from its own last known value.
5. Distance flown: a step between positions implying more than 1,000 kt is skipped; an event at a report without a position gets the distance interpolated in time between positioned reports of the same piece (today flat, so distance can go down between two events); distance is NULL before the first known position.
6. Every event gets both measurements from its own time, including block times, level-offs and fallback take-off and landing times; no measurement row with a NULL value.
7. Time passed counts from `first_seen` as defined in work package 4.
8. The internal track column `cumulative_distance_nm` of `osn_tracks_clean` is left as it is (changing it needs steps 02/02a); note in its comment that the published distance is computed in step 04.
Documented limits: a crossing with a large `bracket_s` is an estimate; rings on every leg of a multi-leg track; AIRAC cycle 524 for every day.

## Work package 6: surface events, holding, point merge (findings b2, b3, e1, e2)
1. Layout events: the element is chosen per passage before entry and exit are split, so every entry has its exit on the same element (13% of taxiway entries had none); a report not flagged on the ground is admitted only if slow (at most 50 kt from positions, or no speed) — no surface events on approach or climb-out; consecutive passages of the same element are merged when the aircraft moved less than about 50 m and entered no other element (42 entry/exit pairs for one parked aircraft); `info.at_track_start` / `info.at_track_end` when the entry or exit is the first or last report of the track.
2. Tracks that never leave the ground and whose aircraft type is missing or a vehicle type: `info.ground_only` = true on their layout events; no block times for them.
3. Holding: `info.context` = `arrival` (a landing at the destination follows within about 60 min and 150 NM), `en-route`, or `other` (local flight, no landing, three or more episodes, or more than half the airborne time turning); the `orbit` / `racetrack` label decided on the share of straight reports alone; start and end trimmed to reports turning in the episode's main direction; altitude from the first and last report that has one. No cap on length.
4. Point merge: the leg exit extended report by report while still in the band and tangential (today about 25 s early, leg time 15% short); two runs of the same track and merge point at most 120 s apart are one leg; a leg must turn (heading change at least 0.3 of the bearing sweep for runs under 3 min) and not climb more than 500 ft; `pms-merge-point` only after any take-off, descending or level, at least 150 kt, one per approach and for a system that serves the runway landed on where that is known.
Documented limits: a de-icing pad event means the pad area was passed; threshold-area events are aircraft on the ground near the threshold point (lining up), not the landing threshold crossing; vehicles carry transponders; aerial work that circles meets the holding definition (use `info.context`).

## Reliability
`spark.excludeOnFailure.enabled=true` and `spark.excludeOnFailure.task.maxTaskAttemptsPerExecutor=1` in the `opensky` configuration: the events step failed twice when all eight retries of a task went to the same dying executor.

## Not corrected now (limits, and why)
- **Tracks holding several flights** (a turnaround under an unchanged call sign with continuous ground reception). The correction is a new rule in how tracks are built, which changes flight identifiers; it needs its own study.
- **Placeholder aircraft addresses** (`000000`, `000001`, `ffffff`, `000400`, …) merging several aircraft. Documented; a flag needs a published column.
- **Small airfields and heliports** are not airport candidates.
- **Aircraft data** come from an October 2024 snapshot.
- **Spoofed positions** that do not jump (a frozen position) still mislead.
