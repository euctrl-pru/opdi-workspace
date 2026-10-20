# Counting movements in `opdi_flight_list`

A hand-off note for anyone building dashboards or reports on the OPDI flight
list. Read the first section even if you skip the rest — it is where the
counting mistakes come from.

## The data model, which is not what you expect

**One row is one *track*, not one *flight*.**

A track is a continuous run of ADS-B observations of a single aircraft. OPDI
derives an origin (`ADEP`) and a destination (`ADES`) for each track where it
can. So a row contributes:

| Row looks like | `ADEP` | `ADES` | Counts as |
|---|---|---|---|
| a complete flight | set | set | 1 departure **and** 1 arrival |
| left the coverage area | set | null | 1 departure |
| entered the coverage area | null | set | 1 arrival |
| an overflight or a fragment | null | null | nothing |

There is no single "number of flights" column, and `COUNT(*)` is not a movement
count. **Count each leg separately**, which is also how APDF (the EUROCONTROL
reference) is structured: it has one row per *movement*, tagged `SRC_PHASE =
'DEP'` or `'ARR'`.

## The two flags, and why they exist

A real departure is routinely cut into **two** tracks: a ground fragment that
sits at the stand and never leaves, then the flight itself. This happens when
the transponder drops out between stand and pushback. Both tracks begin at the
aerodrome, so OPDI gives **both** an `ADEP`, and a naive count sees two
departures where one aircraft left.

Measured over 2026-06-01..03 against APDF, that made OPDI report **1.23×**
APDF's departures. Arrivals were only **1.03×** over, because after landing the
reception usually runs continuously into the stand, so the flight and its
ground remnant stay one track.

`SUPERSEDED_DEP` marks a row whose departure is **already counted on another
row**. It is true when all of:

* the row has no `ATOT` (this track never took off), **and**
* another track of the **same aircraft** departs the **same aerodrome**
  within **3 hours** afterwards.

`SUPERSEDED_ARR` is the mirror, reversed in time — for arrivals the fragment
*follows* the flight, so the rule looks backwards.

**The flags mark; they never drop.** A row can be a superseded departure and
still be a perfectly good arrival, so the two legs are filtered independently.
Both flags are always `true` or `false`, never null.

## How to count — copy these

**Filter on `MOVEMENT_DEP` / `MOVEMENT_ARR`.** They compose every rule, so
counting is one predicate rather than three, and the definition lives in one
place when it changes again. `SUPERSEDED_*` remain beside them to say *why* a
row does not count.

```sql
-- Departures
SELECT COUNT(*) FROM opdi_flight_list WHERE MOVEMENT_DEP;

-- Arrivals
SELECT COUNT(*) FROM opdi_flight_list WHERE MOVEMENT_ARR;

-- Total movements (what an airport means by "movements")
SELECT
  SUM(CASE WHEN MOVEMENT_DEP THEN 1 ELSE 0 END)
+ SUM(CASE WHEN MOVEMENT_ARR THEN 1 ELSE 0 END)
FROM opdi_flight_list;

-- Movements at one aerodrome, per day
SELECT DOF, ADEP AS apt, 'DEP' AS phase, COUNT(*) AS n
FROM opdi_flight_list
WHERE MOVEMENT_DEP
GROUP BY DOF, ADEP
UNION ALL
SELECT DOF, ADES, 'ARR', COUNT(*)
FROM opdi_flight_list
WHERE MOVEMENT_ARR
GROUP BY DOF, ADES;

-- "Flights" in the loose sense: rows that are at least one real movement
SELECT COUNT(*) FROM opdi_flight_list
WHERE MOVEMENT_DEP OR MOVEMENT_ARR;
```

## Do not do these

**Do not subtract a correction from a total.** The flags are per-leg. There is
no single number to take off `COUNT(*)`, because a row may be superseded on one
leg and valid on the other.

**Do not use `COUNT(*)` as a flight count.** About a quarter of rows are not
flights at all — overflights, five-minute fragments, towing, circuits. Measured
on one day: 13,429 of 52,400 rows had no milestone and no ring crossing, and
1,914 rows had `ADEP = ADES`.

**Do not filter on `TRACK_DURATION_MIN` by default.** It is published so you
*can*, not because you should. It is a proxy and it leaks both ways: a 30-minute
cut discards 4,364 departures that demonstrably took off, while still admitting
hour-long circuits. And no single threshold fixes both legs, because departures
start 20 points further out than arrivals — the setting that fixes one breaks
the other. If you want a further cut, 10 minutes costs ~1.4% of real departures
and is the cheapest useful one.

**Do not compare OPDI to APDF across all aerodromes.** APDF reports ~91
aerodromes; OPDI sees ~872. Restrict to the aerodromes APDF actually covers or
you are measuring scope, not accuracy.

**Check the day is processed before trusting the flags.** The table is
legitimately half-processed while a campaign is running: step 03 writes a day's
flight list before step 04 extracts its events. On such a day every milestone is
null, and the flags deliberately abstain — `SUPERSEDED_DEP`/`SUPERSEDED_ARR` are
`false` everywhere because there is no evidence, not because nothing is
superseded. A day is ready when it has milestones at all:

```sql
-- days safe to count
SELECT DOF FROM opdi_flight_list
GROUP BY DOF HAVING COUNT(ATOT) + COUNT(ALDT) > 0;
```

A day with zero `ATOT` across every row is **not yet processed**. Counting it
will understate movements badly, because no departure will have a take-off and
no arrival a landing.

**Exclude the dateless partition.** `DOF = '__HIVE_DEFAULT_PARTITION__'` holds
rows with no determinable date. They carry no milestones and largely duplicate
rows that appear with a real date.

## What the numbers should look like

Against APDF, restricted to APDF-covered aerodromes, over 2026-06-01..03:

| | departures | arrivals |
|---|---|---|
| every row | 1.23× | 1.03× |
| **`MOVEMENT_*` applied** | **1.03×** | **1.02×** |

Applying the flags marked **zero** flights that had a detected take-off or
landing — that is the property that makes the rule safe, and
`benchmarks/movement_counts.py` re-checks it rather than trusting it.

**The aggregate is not the whole story, and you should not quote it alone.**
Per-aerodrome errors do not all point the same way, so they cancel in a total.
Of the forty busiest departure aerodromes, the furthest from APDF are:

| | APDF | OPDI | ratio |
|---|---|---|---|
| GCLP | 534 | 343 | 0.64× |
| EGGW | 655 | 816 | 1.25× |
| LFPG | 2,065 | 2,475 | 1.20× |
| EDDK | 531 | 627 | 1.18× |

None of these is explained. **Treat a single aerodrome's movement count as
±20–35%**, and do not present OPDI counts as equivalent to an official
aerodrome statistic. The network total is reliable to a few percent; an
individual aerodrome is not.

This matters because it has bitten already: an earlier version of the rule
scored 0.96× network-wide while counting **0.39×** of Istanbul's departures.
The aggregate looked better than the rule it replaced and was much worse.

## One gotcha: `DOF` is a partition column

The table is Hive-partitioned as `.../opdi_flight_list/DOF=2026-06-01/*.parquet`,
so **`DOF` is in the directory name, not inside the parquet files**. It appears
automatically if you read the table as a table:

```python
# works -- pyarrow reads the partitioning
pd.read_parquet("s3://eurocontrol/opdi-prod/opdi_flight_list/")
```
```sql
-- works -- Spark, DuckDB, Athena all infer it
SELECT DOF, COUNT(*) FROM opdi_flight_list WHERE MOVEMENT_DEP GROUP BY DOF;
```

It is **absent** if you read individual files and concatenate them, which is
easy to do when pulling from S3 by key. In that case recover it from the key:

```python
df["DOF"] = key.split("/")[-2].split("=", 1)[1]
```

Note also that the flight list spells it `DOF` while `opdi_flight_events`
spells it `dof`. That is not a typo to fix on one side; the two tables really
differ, and so do their partition directories.

## Column reference

| Column | Type | Meaning |
|---|---|---|
| `ID` | string | Track identifier, the primary key. One row per value. |
| `DOF` | date | **Partition column — lives in the path, not in the file.** See the note below. |
| `ADEP` / `ADES` | string | ICAO code of origin / destination, null where undetermined |
| `MOVEMENT_DEP` | boolean | **Count departures on this.** True iff the row is a departure movement |
| `MOVEMENT_ARR` | boolean | **Count arrivals on this.** True iff the row is an arrival movement |
| `SUPERSEDED_DEP` | boolean | Why not: this row's departure is already counted on another row |
| `SUPERSEDED_ARR` | boolean | Why not: this row's arrival is already counted on another row |
| `TRACK_DURATION_MIN` | double | `LAST_SEEN - FIRST_SEEN` in minutes |
| `ATOT` / `ALDT` | timestamp | Take-off / landing time, null where not detected |
| `AOBT` / `AIBT` | timestamp | Off-block / on-block time |
| `RWY_DEP` / `RWY_ARR` | string | Runway designator used |
| `RWY_DEP_BEARING_DEG` / `RWY_ARR_BEARING_DEG` | double | Runway centreline **true** bearing |
| `STND_DEP` / `STND_ARR` | string | Stand identifier |
| `C{40,50,60,100,110,120}_{DEP,ARR}` | timestamp | Crossing of that NM ring outbound / inbound |

A milestone column being null means the event was not detected — not that it
did not happen. Coverage varies a lot by aerodrome, so **never present a null
milestone as an operational fact** (e.g. do not report "no off-block time" as
"the aircraft did not push back"). Only `C40` and `C100` have any APDF
counterpart; `C50`, `C60`, `C110` and `C120` cannot be validated at all.
