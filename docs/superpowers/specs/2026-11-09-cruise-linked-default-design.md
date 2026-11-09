# Cruise-linked tracks become the default

**Date:** 2026-11-09. **Branch:** `track-construction-v5` (meta-repo, `opdi`, `opdi-portal`), on top of `track-construction-v4`.
**Paper:** `opdi-portal/papers/track-construction-v5/`, short, replacing v4 as the current statement.

## Why
The v4 study built `cruise_linked` (A10): one `track_id` across a reception gap when its two edges fit one flight. Flights crossing the European box in one track rise from 25.3% to 80.7%. It was not made the default because:

1. a level segment spans the gap on 88% of linked tracks;
2. cumulative distance is short by a median 1,508 NM across a gap;
3. 7 flights inside Europe lost both airports right, and a few surface events moved, unexplained;
4. the switch is a silent `track_id` boundary.

The owner asks to fix these, re-check the rule for bugs without making it more complex, make it the default, and write a to-the-point v5 report.

## Decisions

**A hole.** A silence longer than `EventConfig.track_hole_minutes` (30.0, equal to the segmentation gap) between two consecutive samples of one track. Samples between two holes form a **piece**. Tracks from earlier rules have no holes, so everything below leaves their output unchanged.

**No detector pairs or interpolates across a hole.** Level segments (fuzzy phase, ICAO, PRU), flight-level crossings and ring crossings work per piece. Per-track facts stay per track: `first_seen`, `last_seen`, top of climb, top of descent, block and runway milestones, the flight list.

**Cumulative distance uses the last known position.** Today a blank position zeroes the two segments around it, on every track. Reading the last known position instead fixes that, and across a hole it adds the straight-line distance between the edges, a lower bound on what was flown. This changes a published measurement, so its version goes from `distance_v0.0.2` to `distance_v0.0.3`. Event versions do not change: on tracks without holes the detectors' output is identical.

**The 7 flights and the surface events** are traced on the tables kept under `research/a10/`. A bug found is fixed; otherwise the report states what they are.

**The rule** gets an independent audit before the re-run. Bugs are fixed. No new option is added. Known item: within one second, edge samples are ordered by altitude; order by time then a stable key instead. Options that the v4 study left off stay in the code, since removing them breaks the v4 study for no change in behaviour.

**Default.** `standard` resolves to `cruise_linked`. `debounced` (A9), `recommended` (A8) and `legacy` stay selectable by name. A day's run reads 11 h back instead of 3 h.

**v4 paper** is frozen: it renders from its committed data and says it is superseded.

## Evidence for v5
Same days and method as v4, re-run on the final code: both worldwide days for the shipped setting, and 2026-06-14 through the real pipeline on a 10% sample under `debounced` and `cruise_linked`, both with the detector fixes. New measurements: level segments across a hole (expected none), distance across a hole against the straight line (expected within 1 NM), distance change on tracks without holes, and step timings for both rules.

## Limits kept
Bucket: stop under 3 GB free of 200 GB; delete only this study's tables. One Spark driver at a time. Nothing pushed or merged.
