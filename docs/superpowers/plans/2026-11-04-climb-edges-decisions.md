# Blank altitudes and climb edges: decisions

Copied from the run's working ledger when the workspace was removed.

Ruling: Implement without waiting for plan review — the owner said to implement if something good is found — if wrong, the branch is unmerged and easy to drop.
Task 1: Ruling: the 5,000 ft guard on a descent new edge reads the lowest altitude of the edge samples rather than the first sample only, so a blank first sample cannot slip past it — a low new edge should never link — if wrong, a few fewer descent links.
Task 2: Ruling: a real-hole rate above 0.1% fails a setting at any sample size; under 300 labelled reads insufficient only when the rate is within the limit — otherwise a small sample passes at many times the limit — if wrong, a setting with one unlucky false join is rejected, which is the safe direction.
Task 2: Ruling: at a both-on setting each allowed low-edge family meets the 1% bound on its own — pooling dilutes a bad family — if wrong, slightly stricter.
Task 2: Ruling: data/final_direction is removed in the report rewrite, with the lines that name it.
Ruling: Blank-altitude reading stays on; climb edges on, reported as provisional since 185 labelled is under 300; descent off, failing its own 0.1% limit at 2.5% — climb gains 1.6 points with no checkable false join, but adds a hidden-landing path of 6 of 82,974 — if wrong, one flag.
Ruling: Climb stays on as a judgement call, stated as such, and the report says within-limit at this sample means none observed; the owner can switch it off with one flag — the owner asked for climb and descent, it gains 1.6 points and no checkable false join — if wrong, one flag, and a few percent of the 5,773 labelled links' worth of risk in unlabelled regions.
Final review: parked: methods.py tie order by altitude within a second, and config.py long line: real and cosmetic, not changed, since changing those files would mark the regenerated outputs stale. Fix on the next code change.
