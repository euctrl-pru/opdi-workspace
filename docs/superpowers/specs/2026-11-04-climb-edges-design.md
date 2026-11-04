# Cruise-linked tracks: blank altitudes and climb edges

**Date:** 2026-11-04. **Extends:** `2026-11-03-cruise-linked-tracks-design.md`. **Branch:** `track-construction-v4`.

## Why
The owner asked whether the rule can reach climb and descent as well as cruise. A read of every real hole on 2026-06-14 and 2025-06-14 (`docs/superpowers/research/edge_census_probe.py`), with the callsign, speed and gap checks passed, gives:

| Edges | True links | False joins |
|---|---|---|
| Cruise, cruise | 4,668 | 2 |
| Cruise, altitude blank in the first 5 samples after the hole | 948 | 1 |
| Climbing before the hole, then cruise or blank | 186 | 0 |
| Cruise, then descending after the hole | 121 | 4 |

**The second row overturns a claim in the v4 report.** The report reads the largest split group as aircraft reappearing on approach. Most are simply blank: altitude arrives in a later message, as position does. That is why lowering the floor barely helped.

Climb is clean on this evidence and descent is not. Why they differ is not known, and truth-time error near a descent edge may explain some; the tests below decide, not this table.

## What changes
1. **Edge altitude and vertical rate read past blanks.** Within `link_edge_position_minutes` (5) of the edge, use the nearest `LINK_EDGE_SAMPLES` (5) samples that carry a value, for each of altitude, vertical rate and speed separately. No value in the window: altitude fails closed, rate stays neutral as now. Both days' reads must still hold identical evidence.
2. **`link_directional_edges` splits in two:** `link_old_edge_climb: bool` and `link_new_edge_descent: bool`, both default False until the study sets them. The branch is unmerged, so nothing depends on the old name. The existing 3-rate requirement, the 5,000 ft guard on a descent new edge, and the ratio exemption at a climb edge stay.
3. **The safety limits count low-edge test cases by what the setting allows:** `climb_cruise` when climb is on, `cruise_descent` when descent is on, `climb_descent` when both are on. Own limits stay as now: 1%, 5% same or blank, 0.1% on real holes linked only that way, under 300 labelled reads insufficient and provisional.

## Decisions
- **Adopt item 1 regardless,** subject to the existing limits: it is a bug fix, the same as position.
- **Climb and descent are decided separately** by the limits, at 18,000 ft, 400 kt, 600 min. Descent is expected to fail. If so it stays off and the report says so plainly.
- **Stage 2 is re-run** on the final rule, both rules, same day and sample, then all study outputs regenerate.
- **The recommendation stays unchanged unless the data changes it:** not the default until level segments and distance are fixed. Measure them again: more low edges may change them.

## Non-goals
Detector changes; lowering the floor again; an airport test; changing `debounced`.
