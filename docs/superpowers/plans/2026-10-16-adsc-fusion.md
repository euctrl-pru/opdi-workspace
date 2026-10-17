# ADS-C / ADS-B Fusion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Merge OpenSky's ADS-C feed into OPDI's ADS-B trajectories behind a default-off config flag, show the merged track with its source on the benchmarker's Flight page, and publish a paper measuring whether the merge improves flight-event extraction against the flight-events-v4 benchmark.

**Architecture:** A new step 01b ingests `s3a://readsb-aircraft-states/topics/readsb.aircraft.adsc.avro` into `osn_adsc_statevectors`, mapped into OPDI's existing SI state-vector schema plus a `source` column. `TrackProcessor.process_month` unions that table into the ADS-B frame **before** segmentation, so `track_id` sees both feeds; the cleaner is made source-aware so its 20-second isolated-point rule does not erase a 600-second feed. Nothing is enabled by default, and the experiment writes to `s3a://eurocontrol/opdi/research/adsc/{base,merged}` so no production table or published `track_id` changes.

**Tech Stack:** PySpark 3.5 / Spark 4.1 (native column expressions only — no `applyInPandas`, no `pandas_udf`, no `traffic`), Python 3.10 (`.venv310`), pytest with a local Spark session, Streamlit + Plotly + DuckDB (benchmarker), Quarto + R (portal papers).

**Spec:** `docs/superpowers/specs/2026-10-16-adsc-fusion.md` — read it first. Every number quoted in this plan comes from there.

## Global Constraints

- **Branch `adsc`** in all four repos (meta, `opdi`, `opdi-benchmarker`, `opdi-portal`). Already created. Commit inside a submodule first, then commit the pointer in the meta-repo.
- **`AdscConfig.enabled` defaults to `False`.** Every existing caller must keep getting ADS-B only without saying so.
- **The merged arm writes only to `s3a://eurocontrol/opdi/research/adsc/...`.** No script in this plan may write to `opdi-prod`, `opdi-world`, or `s3a://eurocontrol/opdi/` directly. Guard with `assert "research/" in warehouse_path`.
- **Units: storage stays SI, thresholds stay aviation.** ADS-C arrives in aviation units and is converted *in*: ft→m `/ 3.28084`, kt→m/s `/ 1.94384`, ft/min→m/s `/ 196.850394`. Reuse those exact literals — do not introduce new constants.
- **`EUROPE_BBOX = (-25.86653, 26.74617, 49.65699, 70.25976)` is a published contract.** Never edit it. ADS-C is filtered through `config.coverage.bbox` like everything else.
- **Never mutate a published `version` string.** No event type's `version` changes in this plan.
- **The `source` column is added only when `adsc.enabled` is true.** A column present in new partitions of a production table and absent from old ones breaks `spark.read.parquet` schema inference silently. The Iceberg DDLs in `tracks.py` and `cleaning/cleaner.py` are **not** changed.
- **ADS-C rows carry `callsign = NULL`.** The feed's `flight` field is the IATA number (`LH133`), not the ICAO callsign ADS-B broadcasts (`DLH133`).
- **ADS-C is never thinned.** The 5 s decimation rules (`modulo`, `bucket`) apply to ADS-B only.
- **The ADS-C clock is skewed per airframe, by a measured median +1.48 s (IQR +0.32…+1.96 s, one airframe persistently −12.9 s).** Cross-track agreement with ADS-B is 9 m against 377 m along-track — a 43× ratio, so the residual is timing and not position. No global correction is applied (`time_offset_seconds = 0.0`): a constant would harm the airframes that are already synchronised to 0.01 s. See the spec's "Time synchronisation — measured" section.
- **Benchmark period:** `2026-06-05`, `2026-06-06`, `2026-06-07`; APDF month `202606`; the twenty aerodromes in `events_gt.STUDY_AIRPORTS`. This is `events_gt.PERIODS["2026"]` — the flight-events-v4 period.
- **Tests:** `opdi` → `.venv310/bin/python -m pytest tests/ -q`; `opdi-benchmarker` → `.venv/bin/python -m pytest tests/ -q`. Both run on a local Spark session with no credentials. Anything needing S3 belongs in `benchmarks/`, not `tests/`.
- **Every committed figure carries provenance.** Benchmark outputs are staged through `benchmarks/provenance.py` and stamped in `data/_manifest.json`.

---

## File Structure

### `opdi` (the pipeline)

| Path | Responsibility |
|---|---|
| `src/opdi/config.py` | **Modify.** Add `AdscConfig`; add `CleaningConfig.sparse_sources`; wire `adsc` into `OPDIConfig`. |
| `src/opdi/ingestion/osn_adsc.py` | **Create.** `to_opdi_schema()` (pure mapping), `shadow_filter()` (pure), `AdscIngestion` (S3 read + write). |
| `src/opdi/ingestion/__init__.py` | **Modify.** Export `AdscIngestion`. |
| `src/opdi/pipeline/tracks.py` | **Modify.** `_merge_adsc()` called from `process_month` before `original_columns` is captured. |
| `src/opdi/cleaning/native.py` | **Modify.** `_sparse_mask()`; exempt sparse sources from the isolated-point and stale-broadcast stages; prefer ADS-B in the dedup tie-break. |
| `src/opdi/runner.py` | **Modify.** `_step_01b_ingest_adsc`, registered in `STEPS`. |
| `src/opdi/periodrun.py` | **Modify.** `"01b"` in `DAY_STEPS` and `run_day_step`. |
| `benchmarks/adsc_census.py` | **Create.** What the feed contains, per sampled day. No pipeline dependency. |
| `benchmarks/run_adsc_day.py` | **Create.** Runs both arms for one day into `research/adsc/{base,merged}`. |
| `benchmarks/adsc_coverage_gain.py` | **Create.** Samples and track-minutes added, gaps bridged, fragmentation and merge-neutrality guards. |
| `benchmarks/adsc_time_sync.py` | **Create.** Along/cross decomposition against interpolated ADS-B: how far apart the two clocks are, per airframe. |
| `benchmarks/event_bench.py` | **Modify.** `PERIOD_TRACKS` entries for the two arms. |
| `benchmarks/events_gt.py` | **Modify.** `PERIODS` entries for the two arms. |
| `benchmarks/regenerate_adsc_v1.py` | **Create.** The paper's executable definition. |
| `tests/test_adsc_config.py` | **Create.** |
| `tests/test_adsc_mapping.py` | **Create.** |
| `tests/test_adsc_shadow.py` | **Create.** |
| `tests/test_adsc_merge.py` | **Create.** |
| `tests/test_cleaning_sparse_sources.py` | **Create.** |
| `tests/test_adsc_steps.py` | **Create.** |
| `tests/test_run_adsc_day.py` | **Create.** |
| `tests/test_adsc_coverage_gain.py` | **Create.** |
| `tests/test_adsc_time_sync.py` | **Create.** |
| `tests/test_adsc_periods.py` | **Create.** |
| `tests/test_regenerate_adsc_v1.py` | **Create.** |

### `opdi-benchmarker`

| Path | Responsibility |
|---|---|
| `src/opdi_benchmarker/build/trajectories.py` | **Modify.** Carry `source`, defaulting to `ADS-B`. |
| `src/opdi_benchmarker/read.py` | **Modify.** `union_by_name=1` so pre-`source` cached days still read. |
| `src/opdi_benchmarker/app/queries.py` | **Modify.** Select `source` in `flight_trajectories`. |
| `src/opdi_benchmarker/app/pages/1_Flight.py` | **Modify.** One trace per (flight, source) on map and profile; a caption naming the cadence difference. |
| `tests/test_trajectories.py` | **Modify.** |
| `tests/test_queries_flight.py` | **Modify.** |
| `tests/test_read.py` | **Modify.** |
| `tests/test_pages.py` | **Modify.** |

### `opdi-portal`

| Path | Responsibility |
|---|---|
| `papers/adsc-fusion-v1/index.qmd` | **Create.** The paper. |
| `papers/adsc-fusion-v1/data/` | **Create.** Staged CSVs + `_manifest.json`, written by `regenerate_adsc_v1.py`. |
| `papers/index.qmd` | **Modify.** Publication card, newest first. |

### meta-repo

| Path | Responsibility |
|---|---|
| `CLAUDE.md` | **Modify.** The ADS-C source, the three design facts, the research prefixes. |
| `docs/superpowers/specs/2026-10-16-adsc-fusion.md` | Already written. |
| `docs/superpowers/plans/2026-10-16-adsc-fusion.md` | This file. |

---

## Task 1: ADS-C census

Establishes the facts the paper's framing rests on, and checks that the
2026-06-01 measurement generalises. No pipeline code changes, so this task is
safe to run first and its result can redirect later tasks.

**Files:**
- Create: `benchmarks/adsc_census.py`
- Create: `tests/test_adsc_census.py`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces: `census(df, bbox) -> DataFrame` — one row per sampled day with columns `day STRING`, `hour_partitions INT`, `rows LONG`, `rows_in_box LONG`, `airframes LONG`, `airframes_in_box LONG`, `median_cadence_s DOUBLE`, `in_box_above_fl300 LONG`, `in_box_below_10000ft LONG`, `in_box_west_of_10w LONG`, `in_box_on_ground LONG`, `exact_duplicate_rows LONG`, plus one `fill_<column>` DOUBLE per name in `TRACKED` — **0.0 for a column absent from the frame, never omitted**. Also `hour_paths`, `existing`, `read_day` (returning `(frame, n_partitions)`). Task 12 reads its CSV.
- **Status: DONE.** Built, tested (6 tests) and run over eleven days; `benchmarks/out/adsc_census/adsc_census.csv` is committed. The notes below record what changed against the plan as written.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_adsc_census.py
"""The census aggregation, on a frame small enough to reason about."""

import pytest
from pyspark.sql import Row

import adsc_census

EUROPE_BBOX = (-25.86653, 26.74617, 49.65699, 70.25976)


@pytest.fixture
def feed(spark):
    # Two airframes. 4a… is inside the box at FL350 with 600 s between
    # samples; 3c… is outside it (mid-Atlantic) and appears once, twice over
    # -- an exact duplicate, which is the thing the duplicate counter exists
    # to find.
    return spark.createDataFrame([
        Row(time=1780000000.0, hex="4acb41", lat=52.0, lon=-5.0,
            alt_baro_ft=35000, track=270.0, gs=480.0, baro_rate=0,
            flight="SK4414", squawk="2000", on_ground=False),
        Row(time=1780000600.0, hex="4acb41", lat=52.1, lon=-6.0,
            alt_baro_ft=35000, track=270.0, gs=None, baro_rate=None,
            flight="SK4414", squawk=None, on_ground=False),
        Row(time=1780001200.0, hex="4acb41", lat=52.2, lon=-7.0,
            alt_baro_ft=9000, track=270.0, gs=None, baro_rate=-1200,
            flight="SK4414", squawk=None, on_ground=False),
        Row(time=1780000000.0, hex="3c6568", lat=45.0, lon=-40.0,
            alt_baro_ft=38000, track=90.0, gs=500.0, baro_rate=0,
            flight="LH133", squawk="0014", on_ground=False),
        Row(time=1780000000.0, hex="3c6568", lat=45.0, lon=-40.0,
            alt_baro_ft=38000, track=90.0, gs=500.0, baro_rate=0,
            flight="LH133", squawk="0014", on_ground=False),
    ])


def test_counts_rows_inside_and_outside_the_box(spark, feed):
    r = adsc_census.census(feed, EUROPE_BBOX).collect()[0]
    assert r["rows"] == 5
    assert r["rows_in_box"] == 3
    assert r["airframes"] == 2
    assert r["airframes_in_box"] == 1


def test_median_cadence_is_per_airframe_and_ignores_duplicates(spark, feed):
    # 4acb41 has two 600 s steps, so its own median is 600. 3c6568's only step
    # is the exact duplicate, a 0 s gap, which is a redelivery and not a
    # cadence -- so that airframe contributes nothing and the headline is 600.
    #
    # The median is taken over *airframe* medians, so one busy airframe cannot
    # set the number for a day with 1,855 of them. And `percentile_approx` is
    # nearest-rank rather than interpolating, which is exactly why the zero
    # gaps have to be excluded rather than averaged away: over {0, 600} it
    # returns 0, and the day would read as a 1 Hz feed.
    r = adsc_census.census(feed, EUROPE_BBOX).collect()[0]
    assert r["median_cadence_s"] == pytest.approx(600.0, abs=0.1)


def test_altitude_and_geography_bands_are_counted_inside_the_box(spark, feed):
    # In the box, not over the whole feed: the box is the population OPDI would
    # ingest, so it is the one a claim about what ADS-C can reach has to be
    # made over. 3c6568's two FL380 rows sit at 40W and must not be counted.
    r = adsc_census.census(feed, EUROPE_BBOX).collect()[0]
    assert r["in_box_above_fl300"] == 2       # the two FL350 rows, not the FL380 pair
    assert r["in_box_below_10000ft"] == 1
    assert r["in_box_west_of_10w"] == 0       # -5/-6/-7 are all east of 10W
    assert r["in_box_on_ground"] == 0


def test_exact_duplicates_are_counted(spark, feed):
    r = adsc_census.census(feed, EUROPE_BBOX).collect()[0]
    assert r["exact_duplicate_rows"] == 1


def test_a_column_absent_from_the_feed_is_not_silently_omitted(spark, feed):
    # The census exists to answer "is this field ever populated". A column that
    # vanished from the feed must read as 0.0, not disappear from the report --
    # an absent row and a zero mean different things and only one of them is a
    # measurement.
    r = adsc_census.census(feed, EUROPE_BBOX).collect()[0]
    for absent in ("ias", "tas", "oat", "nav_altitude_mcp", "registration"):
        assert r[f"fill_{absent}"] == pytest.approx(0.0)


def test_fill_rate_is_reported_per_column(spark, feed):
    r = adsc_census.census(feed, EUROPE_BBOX).collect()[0]
    assert r["fill_gs"] == pytest.approx(60.0, abs=0.1)
    assert r["fill_squawk"] == pytest.approx(60.0, abs=0.1)
    assert r["fill_alt_baro_ft"] == pytest.approx(100.0, abs=0.1)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv310/bin/python -m pytest tests/test_adsc_census.py -q`
Expected: FAIL with `ModuleNotFoundError: No module named 'adsc_census'`

- [ ] **Step 3: Write the census module**

```python
#!/usr/bin/env python
"""What the ADS-C feed actually contains, per sampled day.

The paper's framing rests on three measured facts -- the feed is sparse
(~600 s per airframe), high (97.6% of in-box rows above FL300) and oceanic
(74.5% of in-box rows west of 10W) -- and on a fourth, negative one: the
interesting columns (`ias`, `tas`, `mach`, `oat`, `wd`/`ws`,
`nav_altitude_mcp`, `registration`, `ac_type`, `alt_geom`) are empty. All four
were first measured on 2026-06-01 alone. This job re-measures them across the
archive, because "empty on one day in June 2026" and "never populated since
2023" are different claims and only the second one justifies not building a
passthrough column for them.

Reads the raw topic directly -- no pipeline table, no reference data, no
ground truth -- so it needs S3 credentials and nothing else.

    python benchmarks/adsc_census.py --days 2026-06-01 2026-03-01 \
        --out-name adsc_census.csv --results-dir /tmp/census
"""

import argparse
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "src"))
sys.path.insert(0, str(REPO / "benchmarks"))

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from opdi.coverage import EUROPE_BBOX

#: Columns whose fill rate decides whether a passthrough struct is worth
#: building. Listed explicitly rather than taken from the frame, so a column
#: that disappears from the feed reads as 0% rather than vanishing from the
#: report.
TRACKED = (
    "time", "hex", "lat", "lon", "alt_baro_ft", "alt_geom", "track", "gs",
    "baro_rate", "flight", "squawk", "ias", "tas", "mach", "mag_heading",
    "true_heading", "roll", "track_rate", "geom_rate", "oat", "tat", "wd",
    "ws", "nav_qnh", "nav_altitude_mcp", "nav_altitude_fms", "nav_heading",
    "registration", "ac_type", "ac_desc", "category", "on_ground",
)


def census(df: DataFrame, bbox) -> DataFrame:
    """One row describing *df*. See the module docstring for why each column.

    ``median_cadence_s`` is the median over **per-airframe** medians, not over
    all gaps: one airframe with 467 samples would otherwise set the headline
    number for a day with 1,855 airframes.
    """
    from pyspark.sql import Window

    min_lon, min_lat, max_lon, max_lat = bbox
    in_box = (
        (F.col("lon") >= min_lon) & (F.col("lon") <= max_lon)
        & (F.col("lat") >= min_lat) & (F.col("lat") <= max_lat)
    )

    w = Window.partitionBy("hex").orderBy("time")
    # Zero gaps are excluded, not averaged in: a 0 s step is an at-least-once
    # redelivery of one report, counted separately as `exact_duplicate_rows`.
    # `percentile_approx` is nearest-rank rather than interpolating, so over
    # {0, 600} it returns 0 -- leaving the duplicates in would report a
    # ten-minute feed as a 1 Hz one.
    gaps = (
        df.withColumn("_gap", F.col("time") - F.lag("time").over(w))
        .where(F.col("_gap").isNotNull() & (F.col("_gap") > 0))
        .groupBy("hex")
        .agg(F.expr("percentile_approx(_gap, 0.5)").alias("_median"))
    )
    cadence = gaps.agg(
        F.expr("percentile_approx(_median, 0.5)").alias("median_cadence_s")
    )

    # An exact duplicate is the same airframe, instant and position twice --
    # Kafka at-least-once delivery, not a second observation.
    dupes = df.groupBy("hex", "time", "lat", "lon").count()
    dupe_rows = dupes.agg(
        F.coalesce(F.sum(F.col("count") - 1), F.lit(0)).alias("exact_duplicate_rows")
    )

    fills = [
        (
            (F.sum(F.col(c).isNotNull().cast("double")) * 100.0 / F.count("*"))
            if c in df.columns
            else F.lit(0.0)
        ).alias(f"fill_{c}")
        for c in TRACKED
    ]

    # The altitude and geography bands are counted **inside the box**, not over
    # the whole feed. That is the population OPDI would ingest, so it is the
    # one a claim about what ADS-C can reach has to be made over. Counting them
    # globally gives a similar-looking percentage for a different population --
    # and two tables quoting "97.6% above FL300" about different denominators
    # is worse than one table quoting nothing.
    base = df.agg(
        F.count("*").alias("rows"),
        F.sum(in_box.cast("long")).alias("rows_in_box"),
        F.countDistinct("hex").alias("airframes"),
        F.countDistinct(F.when(in_box, F.col("hex"))).alias("airframes_in_box"),
        F.sum((in_box & (F.col("alt_baro_ft") > 30000)).cast("long"))
        .alias("in_box_above_fl300"),
        F.sum((in_box & (F.col("alt_baro_ft") < 10000)).cast("long"))
        .alias("in_box_below_10000ft"),
        F.sum((in_box & (F.col("lon") < -10.0)).cast("long"))
        .alias("in_box_west_of_10w"),
        F.sum((in_box & F.col("on_ground")).cast("long"))
        .alias("in_box_on_ground")
        if "on_ground" in df.columns else F.lit(0).cast("long").alias("in_box_on_ground"),
        *fills,
    )
    return base.crossJoin(cadence).crossJoin(dupe_rows)
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `.venv310/bin/python -m pytest tests/test_adsc_census.py -q`
Expected: PASS (6 tests)

- [ ] **Step 5: Add the S3 reader and CLI**

Append to `benchmarks/adsc_census.py`:

```python
ADSC_BASE = "s3a://readsb-aircraft-states/topics/readsb.aircraft.adsc.avro"


def hour_paths(base: str, day) -> list:
    """The 24 hourly partition paths for *day*, in order.

    Every object in the topic sits under `hour=<unix seconds>`, one directory
    per UTC hour. Paths are handed to Spark as explicit leaf directories, not
    as a glob with a parent root: partition discovery only looks *below* the
    path it is given, so a leaf path yields no `hour` partition column -- which
    matters because the Avro schema already carries a data column of that name
    and the two would collide.
    """
    import datetime as dt

    start = dt.datetime(day.year, day.month, day.day, tzinfo=dt.timezone.utc)
    return [f"{base}/hour={int((start + dt.timedelta(hours=h)).timestamp())}"
            for h in range(24)]


def existing(spark, paths: list) -> list:
    """The subset of *paths* that exists.

    The archive has 24 gaps longer than a day since 2023-07 and averages 22.3
    of 24 hourly partitions per day in 2026, so a missing hour is normal.
    `spark.read.parquet` raises PATH_NOT_FOUND on the first absent one, which
    would make a routine gap look like a credentials or bucket failure.
    """
    jvm = spark._jvm
    hconf = spark._jsc.hadoopConfiguration()
    out = []
    for p in paths:
        path = jvm.org.apache.hadoop.fs.Path(p)
        if path.getFileSystem(hconf).exists(path):
            out.append(p)
    return out


def read_day(spark, base: str, day):
    """One day of the raw topic, or None when the archive has no hour at all."""
    paths = existing(spark, hour_paths(base, day))
    if not paths:
        return None
    print(f"  {day}: {len(paths)}/24 hourly partitions present")
    return spark.read.option("mergeSchema", "true").parquet(*paths)


def main() -> int:
    import csv
    import datetime as dt

    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--days", nargs="+", required=True,
                    help="YYYY-MM-DD, one census row each")
    ap.add_argument("--base", default=ADSC_BASE)
    ap.add_argument("--results-dir", required=True)
    ap.add_argument("--out-name", default="adsc_census.csv")
    ap.add_argument("--cores", type=int, default=4)
    ap.add_argument("--driver-memory", default="6g")
    args = ap.parse_args()

    from osn_sample import build_spark, load_dotenv

    load_dotenv()
    spark = build_spark(args.cores, args.driver_memory, distributed=True)
    rows = []
    try:
        for d in args.days:
            day = dt.date.fromisoformat(d)
            df = read_day(spark, args.base, day)
            if df is None:
                print(f"  {day}: no partitions at all; skipped")
                continue
            r = census(df, EUROPE_BBOX).collect()[0].asDict()
            r["day"] = d
            rows.append(r)
    finally:
        spark.stop()

    if not rows:
        raise SystemExit("no day produced a census row -- nothing written")

    out = Path(args.results_dir)
    out.mkdir(parents=True, exist_ok=True)
    fields = ["day"] + [k for k in rows[0] if k != "day"]
    with open(out / args.out_name, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=fields)
        w.writeheader()
        w.writerows(rows)

    import provenance
    provenance.record(out, args.out_name, "benchmarks/adsc_census.py",
                      sys.argv[1:], ["benchmarks/adsc_census.py"],
                      inputs={"days": args.days, "base": args.base},
                      input_tables=[args.base])
    print(f"wrote {out / args.out_name} ({len(rows)} day(s))")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 6: Verify `hour_paths` against a known partition**

Run:
```bash
.venv310/bin/python -c "
import sys, datetime as dt; sys.path.insert(0, 'benchmarks')
import adsc_census as c
print(c.hour_paths(c.ADSC_BASE, dt.date(2026,10,16))[9])
"
```
Expected: the path ends `hour=1792141200` — the partition listed for 2026-10-16 09:00 UTC in the spec.

- [ ] **Step 7: Run the whole suite, then commit**

Run: `.venv310/bin/python -m pytest tests/ -q`
Expected: the pre-existing suite plus 5 new tests, all passing.

```bash
git add benchmarks/adsc_census.py tests/test_adsc_census.py
git commit -m "feat(adsc): census job for the raw ADS-C topic"
```

- [ ] **Step 8: Run the census on the cluster and record the answer**

Run (needs credentials; cheap — the whole topic is ~5 MB/day):
```bash
export PYSPARK_DRIVER_PYTHON=$PWD/.venv310/bin/python
unset PYSPARK_PYTHON
.venv310/bin/python -u benchmarks/adsc_census.py \
  --days 2023-09-01 2024-03-01 2024-09-02 2025-03-01 2025-09-01 \
         2026-03-22 2026-06-01 2026-06-05 2026-06-06 2026-06-07 2026-10-15 \
  --results-dir benchmarks/out/adsc_census --out-name adsc_census.csv
```

Read `fill_ias`, `fill_tas`, `fill_oat`, `fill_wd`, `fill_nav_altitude_mcp`,
`fill_registration`, `fill_alt_geom` across the eleven days. **If every one is
0.0, proceed with the plan unchanged.** If any is materially non-zero on any
day, stop and report: the spec's "populated fields only" decision was taken on
the assumption they are always empty, and a populated field is a reason to
revisit it with the user rather than to quietly add a column.

**Result: the gate passed.** All 21 candidate fields measured 0.0% on all
eleven days (2023-09-01 → 2026-10-15). The "populated fields only" schema in
Task 3 stands as written.

**Two findings the plan did not anticipate, both recorded in the spec:**

1. **The feed's schema has grown.** `track`, `gs` and `baro_rate` are 0% until
   somewhere between 2025-03-01 and 2025-09-01; `flight`, `squawk` and
   `on_ground` are 0% until between 2026-03-22 and 2026-06-01. Before roughly
   2025-09 the feed is position and barometric altitude only. For this study
   that is harmless -- the three study days are fully populated and the
   callsign is dropped anyway -- but a merged arm over 2023-2024 would write
   ADS-C rows with NULL `velocity` and `vert_rate`. If the study period ever
   moves back, that has to move with it.
2. **Zero exact duplicates on any of the eleven days.** The
   `dropDuplicates(["icao24", "event_time", "lat", "lon"])` in Task 3 has
   never had anything to remove. Keep it -- it is one hash aggregate over five
   thousand rows and the topic is Kafka-sourced, so the guarantee is
   at-least-once whatever today's data shows -- but say in its comment that it
   has never fired, so a later reader does not infer a problem that was never
   observed.

```bash
git add benchmarks/out/adsc_census/adsc_census.csv
git commit -m "data(adsc): census over eleven days of the archive"
```

---

## Task 2: `AdscConfig` and `CleaningConfig.sparse_sources`

Both new config surfaces land together because they are one decision — "there
is a second, sparse source" — and a reviewer judging one without the other
cannot see whether the defaults are consistent.

**Files:**
- Modify: `src/opdi/config.py` (add `AdscConfig` after `CleaningConfig`; add one field to `CleaningConfig`; add one field to `OPDIConfig`)
- Create: `tests/test_adsc_config.py`

**Interfaces:**
- Consumes: nothing.
- Produces: `opdi.config.AdscConfig(enabled: bool, s3_base: str, table: str, shadow_before_seconds: int, shadow_after_seconds: int, time_offset_seconds: float)`; `OPDIConfig.adsc: AdscConfig`; `CleaningConfig.sparse_sources: Tuple[str, ...]`. Tasks 3–8 and 11 read these.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_adsc_config.py
"""ADS-C configuration: off everywhere, in every environment."""

import pytest

from opdi.config import AdscConfig, OPDIConfig


@pytest.mark.parametrize("env", ["dev", "live", "local", "opensky"])
def test_adsc_is_off_in_every_environment(env):
    # The whole safety argument for this feature is that no existing caller
    # changes behaviour. An environment preset that quietly enabled it would
    # change `track_id` for every airframe with an ADS-C sample.
    assert OPDIConfig.for_environment(env).adsc.enabled is False


def test_defaults_name_the_readsb_topic():
    cfg = AdscConfig()
    assert cfg.s3_base == (
        "s3a://readsb-aircraft-states/topics/readsb.aircraft.adsc.avro")
    assert cfg.table == "osn_adsc_statevectors"


def test_shadow_window_is_positive_on_both_sides():
    # Zero would mean "drop an ADS-C row only on an exact timestamp
    # collision", which no satellite report will ever produce.
    cfg = AdscConfig()
    assert cfg.shadow_before_seconds > 0 and cfg.shadow_after_seconds > 0


def test_no_global_time_correction_by_default():
    # The measured skew is per-airframe (+0.01 s to +2.09 s, one at -12.9 s).
    # A constant would be wrong for nearly every aircraft and would damage the
    # ones already synchronised, so the knob ships at zero.
    assert AdscConfig().time_offset_seconds == 0.0


def test_adsc_is_exempt_from_the_density_stages_by_default():
    # 600 s cadence against a 20 s isolated-point rule: without this, step 02a
    # NULLs every ADS-C lat/lon/altitude and the merge silently does nothing.
    cfg = OPDIConfig.for_environment("opensky")
    assert "ADS-C" in cfg.cleaning.sparse_sources


def test_worldwide_does_not_change_the_adsc_default():
    assert OPDIConfig.for_environment("opensky", worldwide=True).adsc.enabled is False
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv310/bin/python -m pytest tests/test_adsc_config.py -q`
Expected: FAIL with `ImportError: cannot import name 'AdscConfig'`

- [ ] **Step 3: Add `AdscConfig` to `src/opdi/config.py`**

Insert immediately after the `CleaningConfig` class body ends and before
`class DetectionConfig`:

```python
@dataclass
class AdscConfig:
    """ADS-C ingestion (step 01b) and fusion into the ADS-B frame (step 02).

    **Off by default, and that is the point.** Enabling it unions a second
    source into the frame `_add_track_id` sees, so `track_id` changes for every
    airframe that has an ADS-C sample. That is a fourth `track_id` boundary
    after legacy -> A8 -> A9, and like those it is silent to a joiner (an empty
    join, not an error). It must therefore be a deliberate act, taken in a
    research warehouse, not a default anyone inherits.

    **What the source is.** `readsb`'s ADS-C topic, ~23k rows/day worldwide and
    ~5k inside `EUROPE_BBOX`, at a median cadence of ~600 s per airframe --
    three orders of magnitude sparser than the 5 s ADS-B grid. 97.6% of in-box
    rows are above FL300 and 74.5% sit west of 10W, so this is an oceanic
    cruise feed: it can extend a track's ends and bridge reception holes over
    the Atlantic, and it cannot contribute to a runway or A-CDM milestone,
    which needs samples the feed does not contain.
    """

    enabled: bool = False
    """Union ADS-C into the state-vector frame before segmentation."""

    s3_base: str = "s3a://readsb-aircraft-states/topics/readsb.aircraft.adsc.avro"
    """Hourly-partitioned raw topic. Snappy parquet, despite the `.avro` name."""

    table: str = "osn_adsc_statevectors"
    """Where step 01b writes the mapped rows, in the run's own warehouse."""

    shadow_before_seconds: int = 60
    """Drop an ADS-C row when an ADS-B row for the same airframe is this far
    **behind** it.

    The two feeds report the same aircraft from different links, so a
    near-coincident pair is two renderings of one position, not two
    measurements. Keeping both puts a second point a few hundred metres away at
    nearly the same instant. ADS-B wins by construction: it is denser, it is
    what every detector was tuned on, and it carries `on_ground`.

    Split from a single symmetric window because the window is defined in a
    **skewed** time base. Measured against interpolated ADS-B, ADS-C reports sit
    a median +1.48 s ahead of where ADS-B puts the aircraft at the same
    timestamp, and one airframe in twenty-five is persistently −12.9 s. "The
    same observation" is therefore not centred on zero, and the two sides have
    to be settable independently once that is known.
    """

    shadow_after_seconds: int = 60
    """Drop an ADS-C row when an ADS-B row for the same airframe is this far
    **ahead** of it. See :attr:`shadow_before_seconds`."""

    time_offset_seconds: float = 0.0
    """Seconds added to every ADS-C `event_time`. **Deliberately zero.**

    The skew is real -- a median +1.48 s along-track displacement against
    interpolated ADS-B, with a 43:1 along-to-cross ratio that rules out a
    position error, and ADS-B's own position age (median 0.22 s, max 0.92 s)
    too small to explain it. But it is a property of the **individual
    airframe**, not of the feed: within an airframe the implied offset holds to
    sigma 0.10-0.31 s, while between airframes it runs from +0.01 s to +2.09 s
    with one at -12.9 s. A single constant would therefore be wrong for nearly
    every aircraft and would actively damage the ones already synchronised.

    The knob exists so a third arm can test a correction, not because one is
    recommended. A per-airframe offset, estimated on coverage-overlap data and
    applied where ADS-B is absent, is the real fix; it needs a stability study
    over weeks before it could be believed, and 25 airframes on one day cannot
    support it. See ``benchmarks/adsc_time_sync.py`` and the paper's
    time-synchronisation chapter.
    """
```

- [ ] **Step 4: Add `sparse_sources` to `CleaningConfig`**

Add inside `CleaningConfig`, immediately after `isolated_max_gap_seconds`:

```python
    sparse_sources: Tuple[str, ...] = ("ADS-C",)
    """`source` values exempt from the stages that assume a dense feed.

    Two stages here encode "a real measurement has neighbours": the
    isolated-point rule discards a value whose nearest same-column neighbour is
    more than `isolated_max_gap_seconds` (20 s) away on both sides, and the
    stale-broadcast rule reads an unchanged value as carried forward rather
    than re-measured. Both are true of ADS-B and false of ADS-C, whose median
    cadence is ~600 s and which has no second message type to carry anything
    forward from. Left unexempted, step 02a NULLs the position, altitude and
    speed of **every** ADS-C sample, and the fusion would measure as a no-op
    for a reason having nothing to do with its idea.

    Matched against the `source` column, which exists only in a run with
    `adsc.enabled`; where the column is absent this setting has no effect.
    """
```

- [ ] **Step 5: Wire `adsc` into `OPDIConfig`**

In `class OPDIConfig`, add after the `cleaning` field:

```python
    adsc: AdscConfig = field(default_factory=AdscConfig)
```

- [ ] **Step 6: Run the tests**

Run: `.venv310/bin/python -m pytest tests/test_adsc_config.py tests/test_detection_config.py -q`
Expected: PASS (5 new + the existing config tests)

- [ ] **Step 7: Commit**

```bash
git add src/opdi/config.py tests/test_adsc_config.py
git commit -m "feat(adsc): AdscConfig and the sparse-source exemption, both off-path by default"
```

---

## Task 3: ADS-C → OPDI schema mapping and ingestion

**Files:**
- Create: `src/opdi/ingestion/osn_adsc.py`
- Modify: `src/opdi/ingestion/__init__.py`
- Create: `tests/test_adsc_mapping.py`

**Interfaces:**
- Consumes: `opdi.config.AdscConfig` (Task 2), `opdi.coverage.CoverageConfig.bbox`.
- Produces:
  - `ADSB_SOURCE = "ADS-B"`, `ADSC_SOURCE = "ADS-C"`
  - `to_opdi_schema(df: DataFrame) -> DataFrame` — raw topic frame in, `osn_statevectors_v2` column set plus `source` out, `event_time` still a **double** (unix seconds), so the bbox filter runs before the timestamp cast exactly as `StateVectorIngestion._apply_filters` requires.
  - `AdscIngestion(spark, config, bbox=_UNSET)` with `.hour_paths(start_date, end_date) -> List[str]`, `.existing(paths) -> List[str]`, `.ingest_from_s3(start_date, end_date) -> int`.
  - Task 4 adds `shadow_filter` to this module; Task 5 calls `AdscIngestion`; Task 6 reads `ADSB_SOURCE`.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_adsc_mapping.py
"""The raw ADS-C topic, mapped into OPDI's state-vector schema."""

import pytest
from pyspark.sql import Row

from opdi.ingestion.osn_adsc import ADSC_SOURCE, to_opdi_schema

#: Every column `osn_statevectors_v2` carries, plus `source`.
EXPECTED = {
    "event_time", "icao24", "lat", "lon", "velocity", "heading", "vert_rate",
    "callsign", "on_ground", "alert", "spi", "squawk", "baro_altitude",
    "geo_altitude", "last_pos_update", "last_contact", "serials", "source",
}


@pytest.fixture
def raw(spark):
    return spark.createDataFrame([
        Row(time=1780000000.0, hex="4ACB41", lat=52.0, lon=-5.0,
            alt_baro_ft=35000, alt_geom=None, track=270.0, gs=480.0,
            baro_rate=-1200, flight="SK4414", squawk="2000",
            on_ground=False, seen=0.0, seen_pos=0.0, alert=0, spi=0),
    ])


def test_column_set_matches_the_statevector_schema(spark, raw):
    # A column the ADS-B frame does not have would survive `unionByName` and
    # land in `osn_tracks` unannounced; one it does have and this lacks would
    # become NULL for every ADS-C row without anything saying so.
    assert set(to_opdi_schema(raw).columns) == EXPECTED


def test_altitude_is_converted_to_metres(spark, raw):
    r = to_opdi_schema(raw).collect()[0]
    assert r["baro_altitude"] == pytest.approx(35000 / 3.28084, abs=0.01)


def test_ground_speed_is_converted_to_metres_per_second(spark, raw):
    r = to_opdi_schema(raw).collect()[0]
    assert r["velocity"] == pytest.approx(480.0 / 1.94384, abs=0.01)


def test_vertical_rate_is_converted_to_metres_per_second(spark, raw):
    r = to_opdi_schema(raw).collect()[0]
    assert r["vert_rate"] == pytest.approx(-1200 / 196.850394, abs=0.001)


def test_event_time_stays_a_double(spark, raw):
    # The bbox and thinning filters compare against unix seconds and must run
    # before the timestamp cast; a frame arriving as TIMESTAMP would make the
    # modulo comparison a type error at best and a silent no-op at worst.
    assert dict(to_opdi_schema(raw).dtypes)["event_time"] == "double"


def test_icao24_is_lowercased(spark, raw):
    # ADS-B `icao24` is lowercase hex and it is the join key to everything --
    # the flight list, APDF's AIRCRAFT_ADDRESS, the aircraft database. A
    # mixed-case key joins to nothing and raises no error.
    assert to_opdi_schema(raw).collect()[0]["icao24"] == "4acb41"


def test_callsign_is_dropped(spark, raw):
    # `flight` is the IATA number (SK4414), not the ICAO callsign ADS-B
    # broadcasts (SAS4414). Carried through, it would read as a callsign change
    # on every ADS-C sample and the A9 debounce would split exactly the tracks
    # this fusion exists to join.
    assert to_opdi_schema(raw).collect()[0]["callsign"] is None


def test_unavailable_fields_are_null_not_zero(spark, raw):
    r = to_opdi_schema(raw).collect()[0]
    # `alert`/`spi` arrive as integer 0 for every row: a readsb default, not an
    # observation. `serials` has no meaning for a satellite link.
    assert r["alert"] is None and r["spi"] is None
    assert r["serials"] is None
    assert r["geo_altitude"] is None


def test_position_and_contact_ages_are_absolute_instants(spark, raw):
    # The ADS-B columns hold unix timestamps, not ages; `seen`/`seen_pos` hold
    # ages. Subtracting is what makes the two comparable.
    r = to_opdi_schema(raw).collect()[0]
    assert r["last_pos_update"] == pytest.approx(1780000000.0)
    assert r["last_contact"] == pytest.approx(1780000000.0)


def test_source_is_stamped(spark, raw):
    assert to_opdi_schema(raw).collect()[0]["source"] == ADSC_SOURCE
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv310/bin/python -m pytest tests/test_adsc_mapping.py -q`
Expected: FAIL with `ModuleNotFoundError: No module named 'opdi.ingestion.osn_adsc'`

- [ ] **Step 3: Write the module**

```python
# src/opdi/ingestion/osn_adsc.py
"""ADS-C ingestion (pipeline step 01b).

OpenSky publishes a `readsb` ADS-C topic beside the ADS-B archive:
`s3a://readsb-aircraft-states/topics/readsb.aircraft.adsc.avro`, hourly
partitions named `hour=<unix seconds>`, snappy **parquet** despite the `.avro`
in the topic name, carrying the Avro schema
`org.opensky.avro.readsb.Aircraft` in the footer.

**What arrives.** ~23k rows/day worldwide, ~5k inside `EUROPE_BBOX`, from ~940
in-box airframes at a median cadence of ~600 s. Populated: `time`, `hex`,
`lat`, `lon`, `alt_baro_ft`, `track` (99.7%), `flight` (97.7%), `gs` (58%),
`baro_rate` (58%), `squawk` (44%). Empty on every day sampled: `alt_geom`,
`ias`, `tas`, `mach`, `oat`, `tat`, `wd`, `ws`, `nav_*`, `registration`,
`ac_type`, `true_heading`, `mag_heading`, `roll`, `geom_rate`. Constant
placeholders rather than measurements: `nic`=0, `rc`=0, `seen`=0, `seen_pos`=0,
`rssi`=-49.5, `mlat`=[], `tisb`=[].

So the mapping below is short because the feed is narrow, not because anything
was left out. `benchmarks/adsc_census.py` is the standing check on that claim.

**Units are converted in, not out.** The feed is aviation (ft, kt, ft/min);
OPDI storage is SI, mirroring OpenSky's own schema. The three factors are the
same literals `events.py` and `cleaning/native.py` use, so the two cannot
drift.
"""

import os
from datetime import date, datetime, timedelta
from typing import List, Optional, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, from_unixtime, lit, to_date

from opdi.config import OPDIConfig
from opdi.utils.storage import StorageManager

__all__ = [
    "ADSB_SOURCE",
    "ADSC_SOURCE",
    "AdscIngestion",
    "to_opdi_schema",
]

#: Values of the `source` column. Named rather than inlined: the cleaner's
#: exemption list, the merge, the trajectory cache and the Flight page's legend
#: all have to agree on the spelling, and a typo in any one of them is a silent
#: behaviour change rather than an error.
ADSB_SOURCE = "ADS-B"
ADSC_SOURCE = "ADS-C"

#: Aviation -> SI. Reciprocals of the factors in `events.py`.
_FT_TO_M = 1.0 / 3.28084
_KT_TO_MS = 1.0 / 1.94384
_FT_MIN_TO_MS = 1.0 / 196.850394

#: Distinguishes "the caller said nothing" from "the caller said worldwide",
#: matching `StateVectorIngestion`.
_UNSET = object()


def to_opdi_schema(df: DataFrame) -> DataFrame:
    """Raw ADS-C rows in, `osn_statevectors_v2` columns plus `source` out.

    `event_time` is left as a **double** of unix seconds, because the bbox
    filter runs before the timestamp cast -- the same ordering
    `StateVectorIngestion._apply_filters` documents and depends on.

    Three fields are deliberately NULL rather than carried:

    * `callsign` -- the feed's `flight` is the IATA number (`LH133`), not the
      ICAO callsign ADS-B broadcasts (`DLH133`). The production segmentation
      arm breaks a track when the last non-blank callsign changes, so carrying
      it would split every merged track at its first ADS-C sample. A9 bridges
      NULLs by design (`F.last(..., ignorenulls=True)`), so NULL is not a loss
      of grouping information -- it is the absence of a false signal.
    * `alert`, `spi` -- integer 0 on every row, a readsb default rather than an
      observation of an ATC indicator.
    * `serials` -- the receiving-station list of a terrestrial network has no
      counterpart on a satellite link.
    """
    return df.select(
        col("time").cast("double").alias("event_time"),
        F.lower(F.trim(col("hex"))).alias("icao24"),
        col("lat").cast("double").alias("lat"),
        col("lon").cast("double").alias("lon"),
        (col("gs").cast("double") * lit(_KT_TO_MS)).alias("velocity"),
        col("track").cast("double").alias("heading"),
        (col("baro_rate").cast("double") * lit(_FT_MIN_TO_MS)).alias("vert_rate"),
        lit(None).cast("string").alias("callsign"),
        F.coalesce(col("on_ground").cast("boolean"), lit(False)).alias("on_ground"),
        lit(None).cast("boolean").alias("alert"),
        lit(None).cast("boolean").alias("spi"),
        col("squawk").cast("string").alias("squawk"),
        (col("alt_baro_ft").cast("double") * lit(_FT_TO_M)).alias("baro_altitude"),
        (col("alt_geom").cast("double") * lit(_FT_TO_M)).alias("geo_altitude"),
        (col("time").cast("double")
         - F.coalesce(col("seen_pos").cast("double"), lit(0.0))
         ).alias("last_pos_update"),
        (col("time").cast("double")
         - F.coalesce(col("seen").cast("double"), lit(0.0))
         ).alias("last_contact"),
        lit(None).cast("array<int>").alias("serials"),
        lit(ADSC_SOURCE).alias("source"),
    )


class AdscIngestion:
    """Reads the ADS-C topic and writes `config.adsc.table`.

    Deliberately **not** a subclass of `StateVectorIngestion`, and deliberately
    not routed through its `_apply_filters`: that method's job includes the 5 s
    thinning, and both of its rules are wrong here. `modulo` keeps only rows
    whose timestamp is congruent to zero -- on a feed whose instants are
    arbitrary doubles that deletes nearly everything -- and `bucket` keeps one
    row per 5 s bin, which at a 600 s cadence is every row and therefore a
    shuffle for nothing. ADS-C is ingested unthinned.
    """

    def __init__(
        self,
        spark: SparkSession,
        config: OPDIConfig,
        bbox: Optional[Tuple[float, float, float, float]] = _UNSET,
    ):
        self.spark = spark
        self.config = config
        self.storage = StorageManager(spark, config)
        self.bbox = config.coverage.bbox if bbox is _UNSET else bbox
        self.base = config.adsc.s3_base
        self.table = config.adsc.table

    def hour_paths(self, start_date: date, end_date: date) -> List[str]:
        """Leaf partition paths for `[start_date, end_date)`, in order.

        Leaf directories, not a parent root plus a filter: Spark's partition
        discovery only looks *below* the path it is given, so naming the leaf
        yields no `hour` partition column -- which matters because the Avro
        schema already carries a data column called `hour` and the two would
        collide with `Found duplicate column(s)`.
        """
        current = datetime(start_date.year, start_date.month, start_date.day)
        end_dt = datetime(end_date.year, end_date.month, end_date.day)
        out = []
        while current < end_dt:
            out.append(f"{self.base}/hour={int(current.timestamp())}")
            current += timedelta(hours=1)
        return out

    def existing(self, paths: List[str]) -> List[str]:
        """The subset of *paths* that exists.

        The archive has 24 gaps longer than a day since 2023-07 and averages
        22.3 of 24 hourly partitions per day in 2026, so an absent hour is
        routine. `spark.read.parquet` raises PATH_NOT_FOUND on the first one,
        which would make a normal gap look like a bucket or credentials
        failure.
        """
        jvm = self.spark._jvm
        hconf = self.spark._jsc.hadoopConfiguration()
        out = []
        for p in paths:
            path = jvm.org.apache.hadoop.fs.Path(p)
            if path.getFileSystem(hconf).exists(path):
                out.append(p)
        return out

    def ingest_from_s3(self, start_date: date, end_date: date) -> int:
        """Ingest `[start_date, end_date)` into `config.adsc.table`.

        Returns the number of rows written. Unlike the ADS-B ingest this counts
        unconditionally: the whole day is ~5 MB, so the second action costs
        nothing worth hiding behind an environment variable.
        """
        wanted = self.hour_paths(start_date, end_date)
        paths = self.existing(wanted)
        print(f"ADS-C: {len(paths)}/{len(wanted)} hourly partitions present "
              f"({start_date} to {end_date})")
        if not paths:
            print("ADS-C: nothing to ingest; table left untouched")
            return 0

        df = to_opdi_schema(
            self.spark.read.option("mergeSchema", "true").parquet(*paths)
        )

        if self.bbox:
            min_lon, min_lat, max_lon, max_lat = self.bbox
            df = df.filter(
                (col("lon") >= min_lon) & (col("lon") <= max_lon)
                & (col("lat") >= min_lat) & (col("lat") <= max_lat)
            )

        # Exact duplicates only -- same airframe, instant and position. The
        # topic is Kafka-sourced and at-least-once, so a redelivered report
        # appears twice; two genuinely different positions at the same instant
        # are a data problem for step 02a's dedup to arbitrate, not something
        # to discard here.
        #
        # Measured: **zero** duplicates on each of the eleven census days
        # (2023-09 to 2026-10), so this has never had anything to remove. Kept
        # because the delivery guarantee is at-least-once whatever today's data
        # shows, and it is one hash aggregate over a few thousand rows -- but
        # noted, so a later reader does not infer a problem that was never
        # observed.
        df = df.dropDuplicates(["icao24", "event_time", "lat", "lon"])

        # The global time correction, normally a no-op. Applied here rather
        # than at merge time so the written table is self-describing: whatever
        # `osn_adsc_statevectors` holds is what the merge will union, and a run
        # cannot silently mean two different things by one timestamp.
        offset = float(self.config.adsc.time_offset_seconds)
        if offset:
            print(f"ADS-C: applying a global time offset of {offset:+.3f} s. "
                  f"The measured skew is per-airframe -- see AdscConfig.")
            df = df.withColumn("event_time", col("event_time") + lit(offset))

        df = df.withColumn(
            "event_time", from_unixtime(col("event_time")).cast("timestamp"))
        df = df.withColumn("event_time_day", to_date(col("event_time")))

        rows = df.count()
        days = [r[0] for r in df.select("event_time_day").distinct().collect()]
        self.storage.write_table(
            df, self.table, mode="overwrite",
            partition_by=["event_time_day"],
            partition_values=[{"event_time_day": d} for d in days],
        )
        print(f"ADS-C: written {rows:,} rows to {self.table} "
              f"({len(days)} day partition(s))")
        return rows
```

- [ ] **Step 4: Export it**

In `src/opdi/ingestion/__init__.py`, add `AdscIngestion` to the imports and
`__all__` beside `StateVectorIngestion`.

- [ ] **Step 5: Run the tests**

Run: `.venv310/bin/python -m pytest tests/test_adsc_mapping.py -q`
Expected: PASS (10 tests)

- [ ] **Step 6: Check the column set against the real table, not just the test**

Run:
```bash
.venv310/bin/python -c "
import re, pathlib
ddl = pathlib.Path('src/opdi/ingestion/osn_statevectors.py').read_text()
body = ddl.split('CREATE TABLE IF NOT EXISTS')[1].split('USING iceberg')[0]
cols = set(re.findall(r'^\s+(\w+)\s+(?:TIMESTAMP|STRING|DOUBLE|BOOLEAN|ARRAY)', body, re.M))
import sys; sys.path.insert(0, 'tests')
from test_adsc_mapping import EXPECTED
print('missing from mapping :', sorted(cols - EXPECTED))
print('extra in mapping     :', sorted(EXPECTED - cols))
"
```
Expected: `missing from mapping : []` and `extra in mapping : ['source']`.

- [ ] **Step 7: Commit**

```bash
git add src/opdi/ingestion/osn_adsc.py src/opdi/ingestion/__init__.py tests/test_adsc_mapping.py
git commit -m "feat(adsc): map the readsb ADS-C topic into the OPDI state-vector schema"
```

---

## Task 4: The shadow filter

**Files:**
- Modify: `src/opdi/ingestion/osn_adsc.py` (append `shadow_filter`, add to `__all__`)
- Create: `tests/test_adsc_shadow.py`

**Interfaces:**
- Consumes: `ADSC_SOURCE` (Task 3).
- Produces: `shadow_filter(adsc: DataFrame, adsb: DataFrame, before_seconds: int, after_seconds: int) -> DataFrame` — the ADS-C rows with no ADS-B row for the same `icao24` in the window `[t − before_seconds, t + after_seconds]`. Both frames must carry `icao24` and a **timestamp** `event_time`. Task 6 calls it.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_adsc_shadow.py
"""ADS-C rows an ADS-B sample already covers are dropped."""

import datetime as dt

import pytest
from pyspark.sql import Row

from opdi.ingestion.osn_adsc import shadow_filter

T0 = dt.datetime(2026, 6, 5, 12, 0, 0)


def _adsb(spark, rows):
    return spark.createDataFrame(
        [Row(icao24=i, event_time=t) for i, t in rows])


def _adsc(spark, rows):
    return spark.createDataFrame(
        [Row(icao24=i, event_time=t, lat=52.0, lon=-5.0) for i, t in rows])


def test_a_row_inside_the_window_is_dropped(spark):
    kept = shadow_filter(
        _adsc(spark, [("4acb41", T0 + dt.timedelta(seconds=10))]),
        _adsb(spark, [("4acb41", T0)]),
        60, 60,
    )
    assert kept.count() == 0


def test_a_row_outside_the_window_is_kept(spark):
    kept = shadow_filter(
        _adsc(spark, [("4acb41", T0 + dt.timedelta(seconds=600))]),
        _adsb(spark, [("4acb41", T0)]),
        60, 60,
    )
    assert kept.count() == 1


def test_the_window_is_exact_at_both_edges(spark):
    # Exactly the half-window away is covered; one second beyond is not. An
    # off-by-a-bin implementation passes the two tests above and fails here.
    inside = shadow_filter(
        _adsc(spark, [("4acb41", T0 + dt.timedelta(seconds=60))]),
        _adsb(spark, [("4acb41", T0)]), 60, 60)
    outside = shadow_filter(
        _adsc(spark, [("4acb41", T0 + dt.timedelta(seconds=61))]),
        _adsb(spark, [("4acb41", T0)]), 60, 60)
    assert inside.count() == 0
    assert outside.count() == 1


def test_the_window_is_symmetric(spark):
    # An ADS-C report can precede the ADS-B sample that covers it: the
    # satellite link's latency is not a constant.
    kept = shadow_filter(
        _adsc(spark, [("4acb41", T0 - dt.timedelta(seconds=30))]),
        _adsb(spark, [("4acb41", T0)]), 60, 60)
    assert kept.count() == 0


def test_another_airframe_does_not_shadow_this_one(spark):
    kept = shadow_filter(
        _adsc(spark, [("4acb41", T0)]),
        _adsb(spark, [("3c6568", T0)]), 60, 60)
    assert kept.count() == 1


def test_no_adsb_at_all_keeps_everything(spark):
    # The whole point of the merge: an airframe with no reception keeps every
    # ADS-C row it has.
    empty = _adsb(spark, [("x", T0)]).where("icao24 = 'nothing'")
    kept = shadow_filter(_adsc(spark, [("4acb41", T0)]), empty, 60, 60)
    assert kept.count() == 1


def test_the_window_can_be_asymmetric(spark):
    # The reason the two halves are separate parameters: an airframe whose
    # ADS-C clock runs 12.9 s slow has its true partner 12.9 s away on one
    # side only, and a window centred on zero either misses it or has to be
    # widened on both sides -- which discards ADS-C that is genuinely alone.
    before_only = shadow_filter(
        _adsc(spark, [("4acb41", T0 + dt.timedelta(seconds=30))]),
        _adsb(spark, [("4acb41", T0)]), 60, 5)
    after_only = shadow_filter(
        _adsc(spark, [("4acb41", T0 - dt.timedelta(seconds=30))]),
        _adsb(spark, [("4acb41", T0)]), 5, 60)
    # ADS-B 30 s *behind* the report: covered by `before`, not by `after`.
    assert before_only.count() == 0
    assert after_only.count() == 0
    # And the mirror image of each is kept.
    assert shadow_filter(
        _adsc(spark, [("4acb41", T0 - dt.timedelta(seconds=30))]),
        _adsb(spark, [("4acb41", T0)]), 60, 5).count() == 1


def test_a_half_window_of_zero_is_refused(spark):
    with pytest.raises(ValueError, match="positive"):
        shadow_filter(_adsc(spark, [("4acb41", T0)]),
                      _adsb(spark, [("4acb41", T0)]), 60, 0)


def test_output_columns_are_unchanged(spark):
    adsc = _adsc(spark, [("4acb41", T0 + dt.timedelta(seconds=600))])
    assert shadow_filter(adsc, _adsb(spark, [("4acb41", T0)]), 60, 60).columns \
        == adsc.columns


def test_duplicate_adsb_rows_do_not_duplicate_the_output(spark):
    # An anti-join against a non-distinct key set is the classic way to turn a
    # filter into a fan-out.
    adsc = _adsc(spark, [("4acb41", T0 + dt.timedelta(seconds=600))])
    adsb = _adsb(spark, [("4acb41", T0), ("4acb41", T0), ("4acb41", T0)])
    assert shadow_filter(adsc, adsb, 60, 60).count() == 1
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv310/bin/python -m pytest tests/test_adsc_shadow.py -q`
Expected: FAIL with `ImportError: cannot import name 'shadow_filter'`

- [ ] **Step 3: Implement it**

Append to `src/opdi/ingestion/osn_adsc.py`, and add `"shadow_filter"` to
`__all__`:

```python
def shadow_filter(
    adsc: DataFrame, adsb: DataFrame, before_seconds: int, after_seconds: int
) -> DataFrame:
    """ADS-C rows with no ADS-B row for the same airframe inside the window.

    The window is `[t - before_seconds, t + after_seconds]` around each ADS-C
    report. **Asymmetric on purpose**: it is defined in a time base the two
    feeds do not share. Measured against interpolated ADS-B, ADS-C reports sit
    a median +1.48 s ahead of where ADS-B puts the aircraft at the same
    timestamp, and the skew is a property of the individual airframe -- one in
    twenty-five is persistently -12.9 s. A window centred on zero therefore
    centres on the wrong instant for some aircraft, and the two sides have to
    be settable independently.

    Both frames carry `icao24` and a **timestamp** `event_time`; the returned
    frame has exactly `adsc`'s columns.

    Two stages, because the exact test is a range predicate and a range join
    over a day of state vectors is not affordable:

    1. Bin both sides to `floor(t / w)` where `w` is the **larger** half-window,
       and join the ADS-C side's three candidate bins (`b-1`, `b`, `b+1`)
       against the ADS-B side's. Three bins is what makes the prefilter
       *complete*: with a bin at least as wide as either half-window, a partner
       inside the window can only be one bin away.
    2. Apply the exact asymmetric test to the survivors, which is a handful of
       rows per airframe.

    The ADS-C side is the broadcast side -- a few thousand rows a day against
    hundreds of millions -- so this is one broadcast hash join, not a shuffle
    of the state vectors.
    """
    before, after = int(before_seconds), int(after_seconds)
    if before <= 0 or after <= 0:
        raise ValueError(
            f"both half-windows must be positive, got before={before!r} "
            f"after={after!r}")
    s = max(before, after)

    bin_of = lambda t: F.floor(t / F.lit(s)).cast("long")  # noqa: E731

    adsc_t = F.unix_timestamp("event_time").cast("double")
    adsc_keys = adsc.select(
        col("icao24"),
        col("event_time"),
        adsc_t.alias("_t_adsc"),
        F.explode(F.array(
            bin_of(adsc_t) - 1, bin_of(adsc_t), bin_of(adsc_t) + 1,
        )).alias("_bin"),
    )

    adsb_t = F.unix_timestamp("event_time").cast("double")
    adsb_keys = adsb.select(
        col("icao24").alias("_icao24"),
        adsb_t.alias("_t_adsb"),
        bin_of(adsb_t).alias("_bin"),
    )

    shadowed = (
        adsb_keys.join(
            F.broadcast(adsc_keys),
            (col("_icao24") == col("icao24")) & (adsb_keys["_bin"] == adsc_keys["_bin"]),
            "inner",
        )
        # `t_adsb - t_adsc` in `[-before, +after]`: an ADS-B sample up to
        # `before` seconds behind the report, or up to `after` ahead of it.
        .where((col("_t_adsb") - col("_t_adsc")) >= F.lit(-float(before)))
        .where((col("_t_adsb") - col("_t_adsc")) <= F.lit(float(after)))
        # Distinct before the anti-join: a key set with repeats turns a filter
        # into a fan-out, and an ADS-C row near a dense stretch of ADS-B has
        # dozens of partners.
        .select(col("icao24"), col("event_time"))
        .distinct()
    )

    return adsc.join(shadowed, ["icao24", "event_time"], "left_anti")
```

- [ ] **Step 4: Run the tests**

Run: `.venv310/bin/python -m pytest tests/test_adsc_shadow.py -q`
Expected: PASS (8 tests)

- [ ] **Step 5: Commit**

```bash
git add src/opdi/ingestion/osn_adsc.py tests/test_adsc_shadow.py
git commit -m "feat(adsc): drop ADS-C reports an ADS-B sample already covers"
```

---

## Task 5: Step 01b — the pipeline entry point

**Files:**
- Modify: `src/opdi/runner.py` (add `_step_01b_ingest_adsc` after `_step_01_ingest_statevectors`; add `"01b"` to `STEPS`)
- Modify: `src/opdi/periodrun.py` (add `"01b"` to `DAY_STEPS`; add the branch to `run_day_step`)
- Create: `tests/test_adsc_steps.py`

**Interfaces:**
- Consumes: `AdscIngestion` (Task 3), `config.adsc.enabled` (Task 2).
- Produces: `runner.STEPS["01b"]`; `periodrun.DAY_STEPS` containing `"01b"`. Task 8 drives both.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_adsc_steps.py
"""Step 01b: registered everywhere, and a no-op when ADS-C is off."""

import datetime as dt

import pytest

from opdi import periodrun, runner
from opdi.config import OPDIConfig


def test_step_01b_is_registered():
    assert "01b" in runner.STEPS


def test_step_01b_runs_between_01_and_02():
    # `run_pipeline` with no --step iterates `sorted(STEPS)`, so the ordering
    # is lexical and not declared anywhere. ADS-C has to be ingested after the
    # state vectors it is merged into and before the tracks that merge them.
    order = sorted(runner.STEPS)
    assert order.index("01") < order.index("01b") < order.index("02")


def test_step_01b_is_a_per_day_step():
    assert "01b" in periodrun.DAY_STEPS


def test_step_01b_does_nothing_when_adsc_is_off(capsys):
    # No Spark session is passed, and none may be touched: the gate has to be
    # checked before anything is read. A session argument of None that reaches
    # the ingestion would raise AttributeError and fail this test.
    cfg = OPDIConfig.for_environment("local")
    assert cfg.adsc.enabled is False
    runner.STEPS["01b"](None, cfg, dt.date(2026, 6, 5), dt.date(2026, 6, 6))
    assert "disabled" in capsys.readouterr().out


def test_run_day_step_01b_does_nothing_when_adsc_is_off(capsys):
    cfg = OPDIConfig.for_environment("local")
    periodrun.run_day_step(None, cfg, "01b", dt.date(2026, 6, 5), {})
    assert "disabled" in capsys.readouterr().out
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv310/bin/python -m pytest tests/test_adsc_steps.py -q`
Expected: FAIL with `KeyError: '01b'` / `AssertionError`

- [ ] **Step 3: Add the step to `runner.py`**

Insert after `_step_01_ingest_statevectors`:

```python
def _step_01b_ingest_adsc(spark, config, start_date, end_date, **kwargs):
    """Step 01b: Ingest ADS-C state vectors from the readsb topic.

    A no-op unless `config.adsc.enabled`. Registered unconditionally so that a
    full run names it and skips it out loud: a step that exists only when a
    flag is set is a step nobody remembers to run.
    """
    if not config.adsc.enabled:
        print("\nSTEP 01b - ADS-C ingestion: disabled (config.adsc.enabled is False)")
        return

    print("\n" + "=" * 70)
    print(f"STEP 01b - ADS-C ingestion ({start_date} to {end_date})")
    print("=" * 70)

    from opdi.ingestion.osn_adsc import AdscIngestion

    AdscIngestion(spark, config).ingest_from_s3(
        start_date=start_date, end_date=end_date)
```

and in `STEPS`, between `"01"` and `"02"`:

```python
    "01b": _step_01b_ingest_adsc,
```

- [ ] **Step 4: Add the per-day branch to `periodrun.py`**

Change `DAY_STEPS`:

```python
DAY_STEPS: Tuple[str, ...] = ("01", "01b", "02", "02a", "03", "04", "04b")
```

and add this branch to `run_day_step`, after the `"01"` branch and before `"02"`:

```python
    elif step == "01b":
        if not config.adsc.enabled:
            print("  ADS-C ingestion: disabled (config.adsc.enabled is False)")
            return

        from opdi.ingestion.osn_adsc import AdscIngestion

        # The same three-day sliding window step 01 uses, and for the same
        # reason: day D is segmented over [D-1, D+2), so the merge needs ADS-C
        # for all three days or the tracks at the window's edges would see one
        # source on one side of midnight and two on the other.
        #
        # No partition pruning of the kind step 01 does, because the whole
        # table is ~5 MB per day: refetching three days costs less than the
        # listing that would avoid it.
        adsc = AdscIngestion(spark, config)
        adsc.ingest_from_s3(
            start_date=day - timedelta(days=1),
            end_date=day + timedelta(days=2),
        )
```

- [ ] **Step 5: Run the tests**

Run: `.venv310/bin/python -m pytest tests/test_adsc_steps.py tests/test_periodrun.py -q`
Expected: PASS (5 new, plus the existing per-period tests)

- [ ] **Step 6: Check the CLI help still describes the step range truthfully**

Run: `grep -n "00-08" src/opdi/cli.py src/opdi/runner.py`

For each hit, the text now undersells the registry. Update each to `00-08
(including 01b and 02a)`.

- [ ] **Step 7: Commit**

```bash
git add src/opdi/runner.py src/opdi/periodrun.py src/opdi/cli.py tests/test_adsc_steps.py
git commit -m "feat(adsc): step 01b, a no-op unless ADS-C is enabled"
```

---

## Task 6: The pre-segmentation merge

The hypothesis is about `track_id` and coverage, so the union has to happen
before `_add_track_id`. This is the task that changes `track_id`, and the only
protection against that reaching published data is the default-off flag plus
the research warehouse — both of which this task's tests pin down.

**Files:**
- Modify: `src/opdi/pipeline/tracks.py` (add `_merge_adsc`; call it in `process_month`; add a comment to the `osn_tracks` DDL)
- Create: `tests/test_adsc_merge.py`

**Interfaces:**
- Consumes: `shadow_filter`, `ADSB_SOURCE` (Tasks 3–4); `config.adsc` (Task 2).
- Produces: `TrackProcessor._merge_adsc(df_sv, start_time_str, end_time_str) -> DataFrame` — `df_sv` unchanged when the flag is off, and `df_sv` plus a `source` column plus the unshadowed ADS-C rows when it is on. Task 7 consumes the `source` column; Task 9 measures the difference.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_adsc_merge.py
"""The ADS-C union, and the guarantee that it is invisible when off."""

import datetime as dt

import pytest
from pyspark.sql import Row

from opdi.config import OPDIConfig
from opdi.pipeline.tracks import TrackProcessor

T0 = dt.datetime(2026, 6, 5, 12, 0, 0)


@pytest.fixture
def sv(spark):
    """Two ADS-B samples of one airframe, 5 s apart."""
    return spark.createDataFrame([
        Row(event_time=T0, icao24="4acb41", callsign="SAS4414 ",
            lat=52.0, lon=-5.0, baro_altitude=10668.0, geo_altitude=10700.0,
            velocity=240.0, heading=270.0, vert_rate=0.0, on_ground=False,
            alert=False, spi=False, squawk="2000", last_pos_update=1.0,
            last_contact=1.0, serials=None,
            event_time_day=dt.date(2026, 6, 5)),
        Row(event_time=T0 + dt.timedelta(seconds=5), icao24="4acb41",
            callsign="SAS4414 ", lat=52.0, lon=-5.1, baro_altitude=10668.0,
            geo_altitude=10700.0, velocity=240.0, heading=270.0,
            vert_rate=0.0, on_ground=False, alert=False, spi=False,
            squawk="2000", last_pos_update=1.0, last_contact=1.0,
            serials=None, event_time_day=dt.date(2026, 6, 5)),
    ])


@pytest.fixture
def adsc(spark):
    """One ADS-C row far from any ADS-B sample, and one that shadows."""
    def row(seconds, lon):
        return Row(event_time=T0 + dt.timedelta(seconds=seconds),
                   icao24="4acb41", callsign=None, lat=52.0, lon=lon,
                   baro_altitude=10668.0, geo_altitude=None, velocity=240.0,
                   heading=270.0, vert_rate=0.0, on_ground=False, alert=None,
                   spi=None, squawk="2000", last_pos_update=1.0,
                   last_contact=1.0, serials=None, source="ADS-C")
    return spark.createDataFrame([row(600, -6.0), row(10, -5.05)])


def _processor(spark, enabled):
    cfg = OPDIConfig.for_environment("local")
    cfg.adsc.enabled = enabled
    return TrackProcessor(spark, cfg)


def test_merge_off_returns_the_frame_untouched(spark, sv):
    out = _processor(spark, False)._merge_adsc(sv, "2026-06-05", "2026-06-06")
    # Identity, not equivalence: a `source` column added "harmlessly" when the
    # flag is off would appear in new partitions of a production table and not
    # in old ones, and `spark.read.parquet` infers its schema from a sample of
    # footers -- so the failure would be an intermittent missing column
    # downstream, not an error here.
    assert out.columns == sv.columns
    assert out.count() == sv.count()


def test_merge_on_adds_source_to_the_adsb_rows(spark, sv, adsc, monkeypatch):
    p = _processor(spark, True)
    monkeypatch.setattr(p, "_read_adsc", lambda *a: adsc)
    out = p._merge_adsc(sv, "2026-06-05", "2026-06-06")
    sources = {r["source"] for r in out.collect()}
    assert sources == {"ADS-B", "ADS-C"}
    assert out.where("source = 'ADS-B'").count() == 2


def test_merge_on_drops_the_shadowed_row(spark, sv, adsc, monkeypatch):
    p = _processor(spark, True)
    monkeypatch.setattr(p, "_read_adsc", lambda *a: adsc)
    out = p._merge_adsc(sv, "2026-06-05", "2026-06-06")
    # The +10 s row is inside the 60 s shadow of the T0 ADS-B sample.
    assert out.where("source = 'ADS-C'").count() == 1
    assert out.where("source = 'ADS-C'").collect()[0]["lon"] == pytest.approx(-6.0)


def test_merged_rows_get_the_partition_column(spark, sv, adsc, monkeypatch):
    # `event_time_day` is a partition column of the frame being read back, and
    # `process_month` carries every column of its input through a `select`. A
    # NULL there would write ADS-C rows into a `event_time_day=__HIVE_DEFAULT`
    # partition that no day's read would ever find.
    p = _processor(spark, True)
    monkeypatch.setattr(p, "_read_adsc", lambda *a: adsc)
    out = p._merge_adsc(sv, "2026-06-05", "2026-06-06")
    assert out.where("event_time_day IS NULL").count() == 0


def test_merge_on_with_no_adsc_table_raises(spark, sv):
    # Silently falling back to ADS-B only would publish the baseline arm under
    # the merged arm's name -- the one failure mode that cannot be detected
    # after the fact, because the two arms' tables are schema-identical.
    p = _processor(spark, True)
    with pytest.raises(RuntimeError, match="01b"):
        p._merge_adsc(sv, "2026-06-05", "2026-06-06")


def test_segmentation_sees_one_track_across_the_merge(spark, sv, adsc, monkeypatch):
    # The point of merging before `_add_track_id`: an ADS-C sample 600 s after
    # the last ADS-B one is inside the 30-minute gap rule, so it extends the
    # track rather than starting a new one. A NULL callsign must not break it
    # either -- A9 bridges NULLs with `F.last(..., ignorenulls=True)`.
    p = _processor(spark, True)
    monkeypatch.setattr(p, "_read_adsc", lambda *a: adsc)
    merged = p._merge_adsc(sv, "2026-06-05", "2026-06-06")
    out = p._add_track_id(merged)
    assert out.select("track_id").distinct().count() == 1
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv310/bin/python -m pytest tests/test_adsc_merge.py -q`
Expected: FAIL with `AttributeError: 'TrackProcessor' object has no attribute '_merge_adsc'`

- [ ] **Step 3: Implement `_read_adsc` and `_merge_adsc`**

Add to `TrackProcessor` in `src/opdi/pipeline/tracks.py`, immediately before
`_add_track_id`:

```python
    def _read_adsc(self, start_time_str: str, end_time_str: str) -> DataFrame:
        """The ADS-C state vectors for this batch's window.

        Separate from `_merge_adsc` so the union logic is testable without a
        table: the test replaces this method, which is the only part that
        touches storage.
        """
        ref = self.storage.table_ref(self.config.adsc.table)
        return self.spark.sql(
            f"""
            SELECT * FROM {ref}
            WHERE (event_time >= TIMESTAMP('{start_time_str}'))
              AND (event_time < TIMESTAMP('{end_time_str}'));
        """
        )

    def _merge_adsc(
        self, df_sv: DataFrame, start_time_str: str, end_time_str: str
    ) -> DataFrame:
        """Union ADS-C into the state-vector frame, before segmentation.

        **Before**, deliberately. The question this fusion exists to answer is
        whether a second source extends tracks and closes reception holes, and
        both of those are properties of `track_id` -- which `_add_track_id`
        assigns from the frame it is given. Merging afterwards could only
        attach ADS-C to tracks ADS-B had already drawn, which is not the
        hypothesis.

        Returns `df_sv` **unchanged, column for column**, when
        `config.adsc.enabled` is false. The `source` column is not added in
        that case: it would appear in the partitions a new run writes and not
        in the ones already on S3, and `spark.read.parquet` infers a table's
        schema from a sample of footers -- so a consumer would intermittently
        see a column that intermittently does not exist.
        """
        cfg = self.config.adsc
        if not cfg.enabled:
            return df_sv

        from opdi.ingestion.osn_adsc import ADSB_SOURCE, shadow_filter

        if not self.storage.table_exists(cfg.table):
            raise RuntimeError(
                f"config.adsc.enabled is True but {cfg.table!r} is absent from "
                f"{self.config.project.warehouse_path!r}. Run step 01b for this "
                f"window first. Falling back to ADS-B only here would write the "
                f"baseline arm under the merged arm's name, and the two arms' "
                f"tables are schema-identical -- nothing downstream could tell."
            )

        df_adsc = self._read_adsc(start_time_str, end_time_str)
        kept = shadow_filter(df_adsc, df_sv,
                             cfg.shadow_before_seconds,
                             cfg.shadow_after_seconds)

        # The partition column of the table `process_month` read back. ADS-C
        # rows must carry their own day, not NULL: `process_month` selects
        # every input column through to the write, and a NULL partition value
        # lands in `__HIVE_DEFAULT_PARTITION__` where no day's read finds it.
        if "event_time_day" in df_sv.columns:
            kept = kept.withColumn("event_time_day", to_date(col("event_time")))

        return (
            df_sv.withColumn("source", lit(ADSB_SOURCE))
            .unionByName(kept, allowMissingColumns=True)
        )
```

- [ ] **Step 4: Call it from `process_month`**

In `process_month`, directly after the `df_month = self.spark.sql(...)` that
reads `osn_statevectors_v2` and **before** `original_columns` is captured:

```python
        # Before `original_columns`, so `source` survives the select below,
        # and before `_add_track_id`, so segmentation sees both feeds.
        df_month = self._merge_adsc(df_month, start_time_str, end_time_str)
```

- [ ] **Step 5: Document the column in the DDL without adding it**

In `create_table_if_not_exists`, above the `create_table_sql` assignment:

```python
        # `source` is deliberately absent from this DDL.
        #
        # A run with `config.adsc.enabled` writes an extra `source` column
        # ("ADS-B" / "ADS-C") into the parquet it produces. That arm runs in
        # the `opensky` environment, where `create_table` is a no-op and the
        # schema is whatever the files carry -- so the column costs nothing
        # there. Adding it here would put it on the Iceberg tables in dev and
        # live, where the writer does not supply it and a by-name write would
        # fail. The merged arm is a research arm; when it becomes a production
        # one, this DDL and `cleaning/cleaner.py`'s change together.
```

- [ ] **Step 6: Run the tests**

Run: `.venv310/bin/python -m pytest tests/test_adsc_merge.py tests/test_tracks_method.py tests/test_track_identity.py tests/test_segmentation_default.py -q`
Expected: PASS (6 new; the three existing suites unchanged — they are the
`track_id` contract and must not move)

- [ ] **Step 7: Run the whole suite**

Run: `.venv310/bin/python -m pytest tests/ -q`
Expected: no regression. Any failure here is the flag leaking into the
default path — fix that, not the test.

- [ ] **Step 8: Commit**

```bash
git add src/opdi/pipeline/tracks.py tests/test_adsc_merge.py
git commit -m "feat(adsc): union ADS-C into the frame segmentation sees, behind the flag"
```

---

## Task 7: Source-aware cleaning

Without this the merge measures as a no-op: `isolated_max_gap_seconds` is 20 s
and ADS-C's median cadence is 600 s, so step 02a would NULL the position,
altitude and speed of every ADS-C sample it had just been handed.

**Files:**
- Modify: `src/opdi/cleaning/native.py` (add `_sparse_mask`; exempt the two density stages; order the dedup tie-break)
- Create: `tests/test_cleaning_sparse_sources.py`

**Interfaces:**
- Consumes: `CleaningConfig.sparse_sources` (Task 2); the `source` column (Task 6).
- Produces: `_sparse_mask(df, cfg) -> Column`, internal. `clean_tracks`'s behaviour on a frame carrying `source` is the contract Task 9 measures.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_cleaning_sparse_sources.py
"""ADS-C survives step 02a; ADS-B behaviour is untouched."""

import datetime as dt

import pytest
from pyspark.sql import Row

from opdi.cleaning.native import (
    clean_tracks, drop_duplicate_statevectors, mask_isolated_points,
    mask_stale_broadcasts,
)
from opdi.config import CleaningConfig

T0 = dt.datetime(2026, 6, 5, 12, 0, 0)


def _row(seconds, source, lat=52.0, lon=-5.0, alt=10668.0, **over):
    base = dict(
        event_time=T0 + dt.timedelta(seconds=seconds), track_id="t1",
        icao24="4acb41", callsign=None, lat=lat, lon=lon, baro_altitude=alt,
        geo_altitude=None, velocity=240.0, heading=270.0, vert_rate=0.0,
        on_ground=False, last_contact=1.0, last_pos_update=1.0, source=source,
    )
    base.update(over)
    return Row(**base)


def test_an_isolated_adsc_sample_survives(spark):
    # Three ADS-C samples 600 s apart: every one is >20 s from its nearest
    # neighbour on both sides, so the unexempted rule NULLs all of them.
    df = spark.createDataFrame([
        _row(0, "ADS-C", lon=-5.0), _row(600, "ADS-C", lon=-6.0),
        _row(1200, "ADS-C", lon=-7.0),
    ])
    out = mask_isolated_points(df, CleaningConfig())
    assert out.where("lat IS NULL").count() == 0
    assert out.where("baro_altitude IS NULL").count() == 0


def test_an_isolated_adsb_sample_is_still_discarded(spark):
    # The exemption must be keyed on the source, not on the gap: this is the
    # Alligier rule that the cleaning study measured, and it stays.
    df = spark.createDataFrame([
        _row(0, "ADS-B", lon=-5.0), _row(600, "ADS-B", lon=-6.0),
    ])
    out = mask_isolated_points(df, CleaningConfig())
    assert out.where("lat IS NULL").count() == 2


def test_a_repeated_adsc_value_is_not_read_as_stale(spark):
    # The stale rule exists because ADS-B sends position and velocity in
    # separate message types, so an unchanged value was carried forward rather
    # than re-measured. ADS-C has no second message type to carry anything
    # forward from: an unchanged altitude 600 s later is a measurement.
    df = spark.createDataFrame([
        _row(0, "ADS-C", lon=-5.0), _row(600, "ADS-C", lon=-6.0),
    ])
    out = mask_stale_broadcasts(df, CleaningConfig())
    assert out.where("velocity IS NULL").count() == 0


def test_a_repeated_adsb_value_is_still_read_as_stale(spark):
    df = spark.createDataFrame([
        _row(0, "ADS-B", lon=-5.0), _row(5, "ADS-B", lon=-5.0),
    ])
    out = mask_stale_broadcasts(df, CleaningConfig())
    assert out.where("lat IS NULL").count() == 1


def test_adsb_wins_an_exact_timestamp_collision(spark):
    # Two rows for one instant is what the shadow filter's window is for, but
    # the window is finite and a collision at 0 s is still possible. ADS-B is
    # denser, carries on_ground, and is what every detector was tuned on.
    df = spark.createDataFrame([
        _row(0, "ADS-B", lon=-5.0), _row(0, "ADS-C", lon=-5.9),
    ])
    out = drop_duplicate_statevectors(df, CleaningConfig())
    assert out.count() == 1
    assert out.collect()[0]["source"] == "ADS-B"


def test_the_exemption_is_configurable_off(spark):
    df = spark.createDataFrame([
        _row(0, "ADS-C", lon=-5.0), _row(600, "ADS-C", lon=-6.0),
    ])
    cfg = CleaningConfig(sparse_sources=())
    assert mask_isolated_points(df, cfg).where("lat IS NULL").count() == 2


def test_a_frame_without_source_behaves_exactly_as_before(spark):
    # Every existing caller -- production, and every test in this suite --
    # passes a frame with no `source` column. The exemption must be inert
    # there, not raise and not change a mask.
    df = spark.createDataFrame([
        Row(event_time=T0, track_id="t1", icao24="4acb41", callsign=None,
            lat=52.0, lon=-5.0, baro_altitude=10668.0, geo_altitude=None,
            velocity=240.0, heading=270.0, vert_rate=0.0, on_ground=False,
            last_contact=1.0, last_pos_update=1.0),
        Row(event_time=T0 + dt.timedelta(seconds=600), track_id="t1",
            icao24="4acb41", callsign=None, lat=52.0, lon=-6.0,
            baro_altitude=10668.0, geo_altitude=None, velocity=240.0,
            heading=270.0, vert_rate=0.0, on_ground=False, last_contact=1.0,
            last_pos_update=1.0),
    ])
    out = mask_isolated_points(df, CleaningConfig())
    assert out.where("lat IS NULL").count() == 2


def test_clean_tracks_end_to_end_keeps_the_adsc_rows(spark):
    df = spark.createDataFrame([
        _row(0, "ADS-B", lon=-5.00), _row(5, "ADS-B", lon=-5.01),
        _row(10, "ADS-B", lon=-5.02), _row(610, "ADS-C", lon=-6.00),
        _row(1210, "ADS-C", lon=-7.00),
    ])
    out = clean_tracks(df, CleaningConfig())
    adsc = out.where("source = 'ADS-C'")
    assert adsc.count() == 2
    assert adsc.where("lat IS NULL OR lon IS NULL").count() == 0
    # `segment_id` still splits at the 300 s coverage hole. Note what that
    # does and does not buy: it marks the discontinuity, and no detector reads
    # it (`grep -rn segment_id src/opdi/pipeline/` finds two comments), so it
    # does not today prevent an event being interpolated across the boundary.
    # Asserted because the column is a published contract, not because it
    # protects anything.
    assert out.select("segment_id").distinct().count() == 3
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv310/bin/python -m pytest tests/test_cleaning_sparse_sources.py -q`
Expected: FAIL — the isolated-point and stale tests fail because the stages
are unconditional; the collision test fails because the tie-break only looks
at `last_contact`.

- [ ] **Step 3: Add `_sparse_mask`**

In `src/opdi/cleaning/native.py`, after `_with_epoch`:

```python
def _sparse_mask(df: DataFrame, cfg: CleaningConfig) -> Column:
    """True where the row's source is exempt from the density stages.

    Two stages here encode "a measurement has neighbours": the isolated-point
    rule and the stale-broadcast rule. Both are properties of ADS-B's 5 s grid
    and separate message types, and neither holds for a 600 s satellite feed
    with a single report type. Applied to ADS-C they do not clean it, they
    delete it -- every value of every row.

    Inert, by construction, on a frame with no `source` column: that is every
    existing caller, including production and the rest of this suite.
    """
    if "source" not in df.columns or not cfg.sparse_sources:
        return F.lit(False)
    return F.col("source").isin(list(cfg.sparse_sources))
```

- [ ] **Step 4: Exempt the isolated-point stage**

In `mask_isolated_points`, after `sentinel = F.lit(_NO_NEIGHBOUR)`:

```python
    exempt = _sparse_mask(df, cfg)
```

and change the masking line to:

```python
        df = df.withColumn(name, F.when((gap <= limit) | exempt, value))
```

- [ ] **Step 5: Exempt the stale-broadcast stage**

In `mask_stale_broadcasts`, replace the `_stale_*` assignment block with:

```python
    # A sparse source cannot be stale: there is no second message type for a
    # value to have been carried forward from.
    exempt = _sparse_mask(df, cfg)
    df = (
        df.withColumn("_stale_latlon",
                      ~exempt & ~F.coalesce(lat_or_lon, F.lit(False)))
        .withColumn("_stale_position",
                    ~exempt & ~F.coalesce(position, F.lit(False)))
        .withColumn("_stale_speed",
                    ~exempt & ~F.coalesce(speed, F.lit(False)))
    )
```

- [ ] **Step 6: Order the dedup tie-break**

In `drop_duplicate_statevectors`, replace the `ordering` assignment with:

```python
    # ADS-B first, then the freshest observation. The source term is the outer
    # key: where both feeds report one instant the two values are the same
    # position rendered by two links, and the dense feed is the one every
    # detector was tuned on -- and the only one carrying `on_ground`.
    ordering = Window.partitionBy("track_id", "event_time").orderBy(
        F.when(_sparse_mask(df, cfg), 1).otherwise(0).asc(),
        F.col("last_contact").desc_nulls_last(),
    )
```

- [ ] **Step 7: Run the tests**

Run: `.venv310/bin/python -m pytest tests/test_cleaning_sparse_sources.py tests/test_cleaning_native.py tests/test_cleaning_next_valid.py tests/test_altitude_cleaning.py -q`
Expected: PASS (8 new; the three existing cleaning suites unchanged)

- [ ] **Step 8: Run the whole suite and commit**

Run: `.venv310/bin/python -m pytest tests/ -q`

```bash
git add src/opdi/cleaning/native.py tests/test_cleaning_sparse_sources.py
git commit -m "feat(adsc): exempt sparse sources from the stages that assume a dense feed"
```

---

## Task 8: The two-arm experiment runner

Both arms are rebuilt from the same state vectors, differing in exactly one
setting. The existing flight-events-v4 2026 tables are **not** reused as the
baseline: they were built by the A8 `recommended` segmentation arm, and
reusing them would mix that change into the ADS-C delta.

**Files:**
- Create: `benchmarks/run_adsc_day.py`
- Create: `tests/test_run_adsc_day.py`

**Interfaces:**
- Consumes: `periodrun.run_day_step` (Task 5), `config.adsc` (Task 2).
- Produces:
  - `ARM_PREFIX = "s3a://eurocontrol/opdi/research/adsc"`
  - `SHARED_INPUTS`, `DERIVED`, `PASSTHROUGH` — the three disjoint table-name sets
  - `arm_path(arm, table) -> str`
  - `redirect_arm(arm, reference_warehouse) -> None`
  - Tasks 9, 10 and 12 read the tables it writes at
    `s3a://eurocontrol/opdi/research/adsc/{base,merged}/<table>`.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_run_adsc_day.py
"""The arm redirect: every write lands in research, every input is shared."""

import pytest

import run_adsc_day as r
from opdi.utils.storage import StorageManager


def test_the_three_table_sets_are_disjoint_and_complete():
    # Every table name `src/opdi` resolves has to be in exactly one set. A
    # name in none of them is the failure the A9 study hit twice: the table is
    # written to the research prefix and read back from production, or the
    # reverse, and the run completes with a wrong answer.
    sets = [set(r.SHARED_INPUTS), set(r.DERIVED), set(r.PASSTHROUGH)]
    for a, b in ((0, 1), (0, 2), (1, 2)):
        assert not sets[a] & sets[b]
    assert set(r.ALL_TABLES) == sets[0] | sets[1] | sets[2]


def test_every_table_src_uses_is_classified():
    # Read off `src/opdi` rather than trusted: a new table added to the
    # pipeline must fail this test, not silently default to one side.
    import re
    from pathlib import Path

    src = Path(r.REPO) / "src" / "opdi"
    found = set()
    pattern = re.compile(
        r"""(?:read_table|table_ref|table_exists)\(\s*["']([a-z_0-9]+)["']"""
        r"""|write_table\([^,]+,\s*["']([a-z_0-9]+)["']""")
    for p in src.rglob("*.py"):
        if ".ipynb_checkpoints" in str(p):
            continue
        for m in pattern.finditer(p.read_text()):
            found.add(m.group(1) or m.group(2))
    assert found <= set(r.ALL_TABLES), sorted(found - set(r.ALL_TABLES))


def test_derived_tables_go_to_the_arm_prefix():
    assert r.arm_path("merged", "osn_tracks") == (
        "s3a://eurocontrol/opdi/research/adsc/merged/osn_tracks")


def test_shared_inputs_are_not_per_arm():
    # The scientific claim is that the arms differ in one setting. Sharing the
    # ADS-B state vectors makes that structurally true rather than asserted,
    # and halves the ingest.
    assert r.arm_path("base", "osn_statevectors_v2") == \
        r.arm_path("merged", "osn_statevectors_v2")
    assert "/input/" in r.arm_path("base", "osn_statevectors_v2")


def test_redirect_sends_derived_writes_to_research(monkeypatch):
    r.redirect_arm("merged", "s3a://eurocontrol/opdi-prod")
    try:
        sm = StorageManager.__new__(StorageManager)
        sm.base_path = "s3a://eurocontrol/opdi-prod"
        assert sm._s3_path("osn_tracks").startswith(
            "s3a://eurocontrol/opdi/research/adsc/merged/")
        assert sm._s3_path("oa_airports") == \
            "s3a://eurocontrol/opdi-prod/oa_airports"
    finally:
        r.restore()


def test_redirect_refuses_an_unknown_table(monkeypatch):
    # Loud, not defaulted: an unclassified table is a plan error and the only
    # cheap moment to catch it is the first time it is resolved.
    r.redirect_arm("merged", "s3a://eurocontrol/opdi-prod")
    try:
        sm = StorageManager.__new__(StorageManager)
        sm.base_path = "s3a://eurocontrol/opdi-prod"
        with pytest.raises(RuntimeError, match="unclassified"):
            sm._s3_path("some_new_table")
    finally:
        r.restore()


def test_a_write_outside_research_is_refused():
    r.redirect_arm("merged", "s3a://eurocontrol/opdi-prod")
    try:
        with pytest.raises(AssertionError, match="research/adsc"):
            r.guard_write_path("s3a://eurocontrol/opdi-prod/osn_tracks")
        r.guard_write_path("s3a://eurocontrol/opdi/research/adsc/merged/osn_tracks")
    finally:
        r.restore()


def test_restore_is_idempotent():
    r.redirect_arm("base", "s3a://eurocontrol/opdi-prod")
    r.restore()
    r.restore()
    sm = StorageManager.__new__(StorageManager)
    sm.base_path = "s3a://eurocontrol/opdi-prod"
    assert sm._s3_path("osn_tracks") == "s3a://eurocontrol/opdi-prod/osn_tracks"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv310/bin/python -m pytest tests/test_run_adsc_day.py -q`
Expected: FAIL with `ModuleNotFoundError: No module named 'run_adsc_day'`
(`tests/conftest.py` already puts `benchmarks/` on `sys.path`.)

- [ ] **Step 3: Write the runner**

```python
#!/usr/bin/env python
"""Run one day through the real per-day pipeline, twice: ADS-B only, and
ADS-B + ADS-C. Into research prefixes, never production.

    export PYSPARK_DRIVER_PYTHON=$PWD/.venv310/bin/python
    unset PYSPARK_PYTHON    # executors must use the opdi-spark image's own
                            # python -- it has h3, and this venv does not
                            # exist in the executor container, which kills
                            # step 02's h3_res_12 UDF with "Cannot run
                            # program .../.venv310/bin/python: Exec failed".
    .venv310/bin/python -u benchmarks/run_adsc_day.py --day 2026-06-05 --arm both

**Why both arms are rebuilt.** The published 2026 tables
(`research/tracks_2026`) were built by the A8 `recommended` segmentation arm;
the current default is A9 `debounced`. Using them as the ADS-B-only baseline
would put two changes in one delta and the study could not say which moved the
number -- the mistake the track-construction V1 study documents.

**Why the arms share their inputs.** `osn_statevectors_v2` and
`osn_adsc_statevectors` live under `research/adsc/input/`, written once and
read by both arms. The claim "the arms differ in exactly one setting" is then
structurally true rather than asserted, and the ADS-B ingest is paid once.

**How the redirect works.** `StorageManager` resolves every table against one
`warehouse_path`, so a run cannot read production reference data and write
research output without help. The help is a patched `_s3_path`, the idiom
`event_bench.redirect_event_tables` and `flight_list_v7.redirect_candidates`
already use -- patching the resolver rather than the table name, because
`table_ref` registers a temp view named after the table and
`research/adsc/merged/osn_tracks` is not a legal SQL identifier.

Three disjoint sets, and a table in none of them raises. That is the whole
lesson of the A9 run, which failed twice because `opdi_endpoint_candidates`
was written to the research prefix and read back from production: a
classification that defaults is a classification that is wrong silently.
"""

import argparse
import sys
from datetime import date
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "src"))
sys.path.insert(0, str(REPO / "benchmarks"))

from opdi.config import OPDIConfig  # noqa: E402
from opdi.periodrun import run_day_step  # noqa: E402
from opdi.utils.spark_helpers import SparkSessionManager  # noqa: E402
from opdi.utils.storage import StorageManager  # noqa: E402

#: Root of every table this script writes. Appears in `guard_write_path`, so a
#: typo here cannot send a write to production -- it fails the guard instead.
ARM_PREFIX = "s3a://eurocontrol/opdi/research/adsc"

#: Reference data, read and never written. Resolved against the run's own
#: `warehouse_path` (production), because re-deriving an H3 airport layout for
#: a two-arm comparison would cost more than the comparison.
PASSTHROUGH = (
    "h3_airport_detection_zones",
    "h3_runway_zones",
    "hexaero_airport_layouts",
    "oa_airports",
    "oa_runways",
    "osn_aircraft_db",
    "opdi_h3_airspace_ref",
    "coverage_marker",
)

#: Written once, read by both arms.
SHARED_INPUTS = (
    "osn_statevectors_v2",
    "osn_adsc_statevectors",
)

#: Everything the arms produce. Per-arm, because these are the comparison.
DERIVED = (
    "osn_tracks",
    "osn_tracks_clean",
    "opdi_flight_list",
    "opdi_endpoint_candidates",
    "opdi_flight_list_milestones_staging",
    "opdi_flight_events",
    "opdi_measurements",
)

ALL_TABLES = tuple(sorted(PASSTHROUGH + SHARED_INPUTS + DERIVED))

ARMS = ("base", "merged")
STEPS = ("01", "01b", "02", "02a", "03", "04", "04b")

_ORIG_S3_PATH = None
_ORIG_WRITE = None


def arm_path(arm: str, table: str) -> str:
    """Absolute S3 path for *table* in *arm*.

    Shared inputs ignore *arm*: that is what makes them shared.
    """
    if arm not in ARMS:
        raise ValueError(f"unknown arm {arm!r}; expected one of {ARMS}")
    if table in SHARED_INPUTS:
        return f"{ARM_PREFIX}/input/{table}"
    return f"{ARM_PREFIX}/{arm}/{table}"


def guard_write_path(path: str) -> None:
    """Refuse a write outside the research prefix.

    Not paranoia: this script runs steps 01 through 04b, which between them
    replace seven production tables, and the difference between the right and
    the wrong prefix is one string.
    """
    assert path.startswith(ARM_PREFIX), (
        f"refusing to write {path!r}: every table this script writes must sit "
        f"under {ARM_PREFIX!r}. A passthrough table is read-only and must "
        f"never be written by an arm run."
    )


def redirect_arm(arm: str, reference_warehouse: str) -> None:
    """Resolve derived and shared tables to research; pass reference through."""
    global _ORIG_S3_PATH, _ORIG_WRITE
    if _ORIG_S3_PATH is None:
        _ORIG_S3_PATH = StorageManager._s3_path
    if _ORIG_WRITE is None:
        _ORIG_WRITE = StorageManager.write_table
    orig_path, orig_write = _ORIG_S3_PATH, _ORIG_WRITE

    def _s3_path(self, table_name):
        if table_name in PASSTHROUGH:
            return orig_path(self, table_name)
        if table_name in SHARED_INPUTS or table_name in DERIVED:
            return arm_path(arm, table_name)
        raise RuntimeError(
            f"unclassified table {table_name!r}: add it to PASSTHROUGH, "
            f"SHARED_INPUTS or DERIVED in benchmarks/run_adsc_day.py. "
            f"Defaulting it to either side is how the A9 run produced a wrong "
            f"answer twice."
        )

    def write_table(self, df, table_name, mode="append", partition_by=None,
                    partition_values=None):
        guard_write_path(_s3_path(self, table_name))
        return orig_write(self, df, table_name, mode, partition_by,
                          partition_values)

    StorageManager._s3_path = _s3_path
    StorageManager.write_table = write_table


def restore() -> None:
    """Undo the patch. Idempotent, so a test can call it in a finally block."""
    global _ORIG_S3_PATH, _ORIG_WRITE
    if _ORIG_S3_PATH is not None:
        StorageManager._s3_path = _ORIG_S3_PATH
        _ORIG_S3_PATH = None
    if _ORIG_WRITE is not None:
        StorageManager.write_table = _ORIG_WRITE
        _ORIG_WRITE = None


def run_arm(arm: str, day: date, executors: int, reference_warehouse: str,
            steps=STEPS) -> None:
    """One arm, one day, through the real per-day entry points."""
    cfg = OPDIConfig.for_environment("opensky")
    cfg.project.warehouse_path = reference_warehouse
    cfg.adsc.enabled = (arm == "merged")
    cfg.spark.executor_instances = str(executors)

    print("=" * 72)
    print(f"  arm        : {arm}  (adsc.enabled={cfg.adsc.enabled})")
    print(f"  day        : {day}")
    print(f"  reference  : {reference_warehouse}  (read-only)")
    print(f"  inputs     : {ARM_PREFIX}/input")
    print(f"  outputs    : {ARM_PREFIX}/{arm}")
    print(f"  segment'n  : {cfg.segmentation.method}")
    print("=" * 72)

    redirect_arm(arm, reference_warehouse)
    spark = SparkSessionManager.create_session(
        app_name=f"adsc {arm} {day}", config=cfg, distributed=True)
    try:
        kwargs = {
            "airports_hex_path": str(
                REPO / "data" / "airport_hex" / "zones_res7_processed.parquet"),
        }
        for step in steps:
            print(f"\n{'-' * 72}\n>>> arm {arm}  step {step}  day {day}\n"
                  f"{'-' * 72}")
            run_day_step(spark, cfg, step, day, kwargs)
    finally:
        spark.stop()
        restore()


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--day", required=True, help="YYYY-MM-DD")
    ap.add_argument("--arm", choices=(*ARMS, "both"), default="both")
    ap.add_argument("--steps", nargs="+", default=list(STEPS),
                    help="subset of the per-day steps, in order")
    ap.add_argument("--executors", type=int, default=8)
    ap.add_argument(
        "--reference-warehouse", default="s3a://eurocontrol/opdi-prod",
        help="where the read-only reference tables live. opdi-prod, not opdi: "
             "the bare `opdi` prefix holds an unpartitioned scratch copy of "
             "the pipeline tables.")
    args = ap.parse_args()

    day = date.fromisoformat(args.day)
    arms = ARMS if args.arm == "both" else (args.arm,)
    for arm in arms:
        run_arm(arm, day, args.executors, args.reference_warehouse, args.steps)
    print(f"\nDone. {', '.join(arms)} written for {day} under {ARM_PREFIX}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 4: Run the tests**

Run: `.venv310/bin/python -m pytest tests/test_run_adsc_day.py -q`
Expected: PASS (8 tests). If `test_every_table_src_uses_is_classified` fails,
it has found a table name this plan missed — add it to the right set rather
than relaxing the test.

- [ ] **Step 5: Commit**

```bash
git add benchmarks/run_adsc_day.py tests/test_run_adsc_day.py
git commit -m "feat(adsc): two-arm day runner over research prefixes"
```

- [ ] **Step 6: Prove the ADS-C ingest works before spending the day on ADS-B**

Run (needs credentials; cheap -- the ADS-C topic is a few megabytes):
```bash
export PYSPARK_DRIVER_PYTHON=$PWD/.venv310/bin/python
unset PYSPARK_PYTHON OPDI_WAREHOUSE
.venv310/bin/python -u benchmarks/run_adsc_day.py \
  --day 2026-06-05 --arm merged --steps 01b --executors 4
```

Expected: `ADS-C: 72/72 hourly partitions present (2026-06-04 to 2026-06-07)`
and roughly 15,000 rows written across 3 day partitions -- the census says
4,725-5,420 in-box rows a day. **A step 01b that reports 0 rows is a stop:**
the bbox filter, the topic path or the credentials are wrong, and every later
number would be a baseline measured twice.

- [ ] **Step 7: Build both arms, one day at a time, all steps**

**Day-major, not step-major, and this is not a preference.** Step 01 keeps a
three-day sliding window and drops the partitions the window has moved past,
so ingesting 2026-06-07 (window 06-08) deletes the state vectors for 06-04 and
06-05. A loop that ingested all three days first and segmented afterwards would
hand day 05's step 02 a window that is two-thirds absent -- which claims almost
no tracks and writes an `osn_tracks` whose schema cannot be inferred
downstream, the failure `StateVectorIngestion.ingest_from_s3` documents.
`periodrun.run_period` is day-major for exactly this reason
(`periodrun.py:876-878`).

```bash
for d in 2026-06-05 2026-06-06 2026-06-07; do
  .venv310/bin/python -u benchmarks/run_adsc_day.py \
    --day $d --arm both --executors 8          # all seven steps, both arms
done
```

Within one invocation `--arm both` runs the baseline fully and then the merged
arm, so steps 01 and 01b run twice per day. That is deliberate and nearly free:
01 finds its window already present and fetches nothing, and 01b rewrites the
same three ADS-C partitions in seconds. The alternative -- hoisting them out --
would mean the two arms no longer provably read the same input.

Roughly an hour per arm-day; budget six cluster-hours for the three days.

- [ ] **Step 8: Verify the arms differ, and differ only where expected**

Run:
```bash
.venv310/bin/python -u - <<'EOF'
import sys; sys.path[:0] = ["src", "benchmarks"]
from osn_sample import build_spark, load_dotenv
load_dotenv(); spark = build_spark(4, "8g", distributed=True)
P = "s3a://eurocontrol/opdi/research/adsc"
for arm in ("base", "merged"):
    t = spark.read.parquet(f"{P}/{arm}/osn_tracks_clean")
    print(arm, "rows", t.count(), "tracks", t.select("track_id").distinct().count(),
          "sources", sorted(r[0] for r in t.select("source").distinct().collect())
          if "source" in t.columns else "(no source column)")
spark.stop()
EOF
```

Expected: `base` has no `source` column and `merged` has `['ADS-B', 'ADS-C']`;
`merged` has more rows and **fewer or equal** distinct `track_id` (ADS-C
bridges gaps, so it can only join tracks or leave them alone — a larger track
count means the ADS-C callsign or timestamps are splitting tracks and Task 6
is wrong). Record both numbers; Task 9 turns them into the paper's table.

```bash
git commit --allow-empty -m "run(adsc): both arms built for 2026-06-05..07"
```

---

## Task 9: Coverage-gain and merge-neutrality measurement

What the fusion did to the trajectories, before asking what it did to the
milestones. If this task reports no added samples, Task 10's null result means
nothing — the two findings have to be read together.

**Files:**
- Create: `benchmarks/adsc_coverage_gain.py`
- Create: `tests/test_adsc_coverage_gain.py`

**Interfaces:**
- Consumes: the arm tables from Task 8 at `s3a://eurocontrol/opdi/research/adsc/{base,merged}/osn_tracks_clean`.
- Produces three pure functions and two CSVs:
  - `arm_summary(tracks: DataFrame) -> DataFrame` — one row: `rows`, `rows_adsc`, `tracks`, `tracks_with_adsc`, `segments`, `track_hours`, `median_samples_per_track`, `tracks_with_two_callsigns`.
  - `bridged_holes(tracks: DataFrame, hole_seconds: int) -> DataFrame` — one row per ADS-C sample sitting in a reception hole: `track_id`, `icao24`, `event_time`, `gap_before_s`, `gap_after_s`.
  - `extension_by_airframe(base: DataFrame, merged: DataFrame) -> DataFrame` — per `icao24` and day: `first_shift_s` (negative = merged starts earlier), `last_shift_s` (positive = merged ends later).
  - Task 12 reads `adsc_gain_2026.csv` and `adsc_holes_2026.csv`.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_adsc_coverage_gain.py
"""What the merge did to the trajectories."""

import datetime as dt

import pytest
from pyspark.sql import Row

from adsc_coverage_gain import arm_summary, bridged_holes, extension_by_airframe

T0 = dt.datetime(2026, 6, 5, 12, 0, 0)


def _r(track, seconds, source, callsign="SAS4414", segment=None):
    return Row(
        track_id=track, icao24="4acb41", callsign=callsign,
        event_time=T0 + dt.timedelta(seconds=seconds), source=source,
        segment_id=segment or f"{track}_1", lat=52.0, lon=-5.0,
    )


@pytest.fixture
def merged(spark):
    # One track: three ADS-B samples, a 600 s hole, one ADS-C sample in it,
    # then two more ADS-B samples.
    return spark.createDataFrame([
        _r("t1", 0, "ADS-B"), _r("t1", 5, "ADS-B"), _r("t1", 10, "ADS-B"),
        _r("t1", 310, "ADS-C", callsign=None, segment="t1_2"),
        _r("t1", 610, "ADS-B", segment="t1_3"), _r("t1", 615, "ADS-B", segment="t1_3"),
    ])


@pytest.fixture
def base(spark):
    # The same track without the ADS-C sample, and with no `source` column --
    # which is what the baseline arm actually writes.
    return spark.createDataFrame([
        Row(track_id="t1", icao24="4acb41", callsign="SAS4414",
            event_time=T0 + dt.timedelta(seconds=s),
            segment_id=seg, lat=52.0, lon=-5.0)
        for s, seg in ((0, "t1_1"), (5, "t1_1"), (10, "t1_1"),
                       (610, "t1_2"), (615, "t1_2"))
    ])


def test_summary_counts_the_adsc_contribution(spark, merged):
    r = arm_summary(merged).collect()[0]
    assert r["rows"] == 6
    assert r["rows_adsc"] == 1
    assert r["tracks"] == 1
    assert r["tracks_with_adsc"] == 1
    assert r["segments"] == 3


def test_summary_works_on_a_frame_with_no_source_column(spark, base):
    # The baseline arm has no `source` column at all. A summary that raised
    # there would make the comparison impossible to compute.
    r = arm_summary(base).collect()[0]
    assert r["rows"] == 5
    assert r["rows_adsc"] == 0
    assert r["tracks_with_adsc"] == 0


def test_track_hours_spans_first_to_last_sample(spark, merged):
    r = arm_summary(merged).collect()[0]
    assert r["track_hours"] == pytest.approx(615 / 3600.0, abs=1e-6)


def test_two_callsign_tracks_are_counted(spark):
    # The merge-neutrality guard, taken from the A9 study: a track holding two
    # distinct real callsigns is two flights fused into one, which is the way
    # this change could do harm. NULL is not a callsign and must not count.
    df = spark.createDataFrame([
        _r("t1", 0, "ADS-B", callsign="SAS4414"),
        _r("t1", 5, "ADS-C", callsign=None),
        _r("t1", 10, "ADS-B", callsign="SAS9001"),
        _r("t2", 0, "ADS-B", callsign="KLM1234"),
        _r("t2", 5, "ADS-C", callsign=None),
    ])
    assert arm_summary(df).collect()[0]["tracks_with_two_callsigns"] == 1


def test_a_hole_filling_sample_is_found(spark, merged):
    holes = bridged_holes(merged, hole_seconds=300).collect()
    assert len(holes) == 1
    assert holes[0]["gap_before_s"] == pytest.approx(300.0)
    assert holes[0]["gap_after_s"] == pytest.approx(300.0)


def test_an_adsc_sample_beside_adsb_is_not_a_bridge(spark):
    # Inside a dense stretch an ADS-C sample adds a point, not coverage. The
    # shadow filter should have removed it; if one survives, it must not be
    # counted as a closed hole.
    df = spark.createDataFrame([
        _r("t1", 0, "ADS-B"), _r("t1", 70, "ADS-C", callsign=None),
        _r("t1", 140, "ADS-B"),
    ])
    assert bridged_holes(df, hole_seconds=300).count() == 0


def test_extension_measures_both_ends(spark, base, merged):
    # Arms cannot be joined on `track_id` -- the merge changes it, which is the
    # whole point. `icao24` plus day is the key that survives.
    ext = extension_by_airframe(base, merged).collect()[0]
    assert ext["first_shift_s"] == pytest.approx(0.0)
    assert ext["last_shift_s"] == pytest.approx(0.0)


def test_extension_is_signed_so_a_regression_is_visible(spark, base):
    later = base.selectExpr(
        "track_id", "icao24", "callsign",
        "event_time + INTERVAL 100 SECONDS AS event_time", "segment_id",
        "lat", "lon")
    ext = extension_by_airframe(base, later).collect()[0]
    assert ext["first_shift_s"] == pytest.approx(100.0)
    assert ext["last_shift_s"] == pytest.approx(100.0)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv310/bin/python -m pytest tests/test_adsc_coverage_gain.py -q`
Expected: FAIL with `ModuleNotFoundError: No module named 'adsc_coverage_gain'`

- [ ] **Step 3: Write the module**

```python
#!/usr/bin/env python
"""What the ADS-C merge did to the trajectories, arm against arm.

Three questions, in the order they have to be answered:

1. **Did anything arrive?** Rows, tracks and track-hours per arm, and how many
   of each carry an ADS-C sample. If this is zero the study is over and
   Task 10's milestone scores are two measurements of the same thing.
2. **Did it arrive where coverage was missing?** An ADS-C sample whose nearest
   ADS-B neighbour on *both* sides is further than `hole_seconds` away is
   closing a reception hole; one sitting inside a dense stretch is adding a
   point to a trajectory that already had one. Only the first is coverage.
3. **Did it do harm?** A track holding two distinct real callsigns is two
   flights fused into one. This is the guard the A9 debounce study used, and it
   is the plausible way a gap-bridging sample hurts: an ADS-C report during a
   turnaround can hold a gap below the 30-minute split threshold and join an
   arrival to the next departure.

Arms cannot be joined on `track_id`: the merge changes it, which is the point
of merging before segmentation. `icao24` and the calendar day is the key that
survives, so the extension figures are per airframe-day rather than per track.

    python benchmarks/adsc_coverage_gain.py --days 2026-06-05 2026-06-06 \
        2026-06-07 --results-dir /tmp/gain
"""

import argparse
import csv
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "src"))
sys.path.insert(0, str(REPO / "benchmarks"))

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

ARM_PREFIX = "s3a://eurocontrol/opdi/research/adsc"

#: A reception hole, in seconds. 300 is `CleaningConfig.segment_gap_seconds` --
#: the same threshold `add_segment_id` uses to decide a detector must not
#: interpolate across a gap. Using one number for both means "a hole ADS-C
#: closed" and "a hole that stopped a detector" are the same hole.
HOLE_SECONDS = 300


def _source_col(df: DataFrame):
    """`source`, or the constant ADS-B where the column does not exist.

    The baseline arm writes no `source` column -- deliberately, see
    `TrackProcessor._merge_adsc`. Both arms have to go through the same
    summary or the comparison is between two different measurements.
    """
    if "source" in df.columns:
        return F.col("source")
    return F.lit("ADS-B")


def arm_summary(tracks: DataFrame) -> DataFrame:
    """One row describing one arm."""
    src = _source_col(tracks)
    is_adsc = (src == F.lit("ADS-C"))
    df = tracks.withColumn("_adsc", is_adsc.cast("long"))

    per_track = df.groupBy("track_id").agg(
        F.count(F.lit(1)).alias("_n"),
        F.max("_adsc").alias("_has_adsc"),
        F.min("event_time").alias("_first"),
        F.max("event_time").alias("_last"),
        F.countDistinct(
            F.when(F.trim(F.coalesce(F.col("callsign"), F.lit(""))) != "",
                   F.trim(F.col("callsign")))
        ).alias("_callsigns"),
    )

    # Two aggregations, crossJoined: `rows`, `rows_adsc` and `segments` are
    # row-level facts and the rest are track-level, so computing both from one
    # grouping would either double-count the rows or lose the track medians.
    totals = df.agg(
        F.count(F.lit(1)).alias("rows"),
        F.sum("_adsc").alias("rows_adsc"),
        F.countDistinct("segment_id").alias("segments"),
    )
    per_track_totals = per_track.agg(
        F.count(F.lit(1)).alias("tracks"),
        F.sum("_has_adsc").alias("tracks_with_adsc"),
        F.expr("percentile_approx(_n, 0.5)").alias("median_samples_per_track"),
        (F.sum(F.unix_timestamp("_last") - F.unix_timestamp("_first"))
         / F.lit(3600.0)).alias("track_hours"),
        F.sum((F.col("_callsigns") >= 2).cast("long"))
        .alias("tracks_with_two_callsigns"),
    )
    return totals.crossJoin(per_track_totals)


def bridged_holes(tracks: DataFrame, hole_seconds: int = HOLE_SECONDS) -> DataFrame:
    """ADS-C samples that closed a reception hole.

    The neighbour distance is measured to the nearest **ADS-B** sample of the
    same track on each side, not to the nearest sample of any source: two
    consecutive ADS-C reports ten minutes apart are both inside the same hole,
    and measuring them against each other would report the hole as closed
    twice and six hundred seconds narrower than it is.
    """
    src = _source_col(tracks)
    t = (tracks
         .withColumn("_t", F.unix_timestamp("event_time").cast("double"))
         .withColumn("_src", src))
    adsb_t = F.when(F.col("_src") == F.lit("ADS-B"), F.col("_t"))

    w = Window.partitionBy("track_id").orderBy("_t")
    before = Window.partitionBy("track_id").orderBy("_t").rowsBetween(
        Window.unboundedPreceding, -1)
    after = Window.partitionBy("track_id").orderBy("_t").rowsBetween(
        1, Window.unboundedFollowing)

    out = (t
           .withColumn("_prev_adsb", F.max(adsb_t).over(before))
           .withColumn("_next_adsb", F.min(adsb_t).over(after))
           .where(F.col("_src") == F.lit("ADS-C"))
           .withColumn("gap_before_s", F.col("_t") - F.col("_prev_adsb"))
           .withColumn("gap_after_s", F.col("_next_adsb") - F.col("_t")))

    # Both sides, not either: a sample `hole_seconds` after the last ADS-B
    # report and five seconds before the next one is inside a dense stretch
    # seen from one direction only.
    #
    # A missing neighbour -- the sample extends the track past its last ADS-B
    # report -- counts as an infinite gap. That is the clearest case of added
    # coverage, and treating NULL as "not a hole" would discard exactly it.
    inf = F.lit(1.0e9)
    return (out
            .withColumn("gap_before_s", F.coalesce(F.col("gap_before_s"), inf))
            .withColumn("gap_after_s", F.coalesce(F.col("gap_after_s"), inf))
            .where((F.col("gap_before_s") >= F.lit(float(hole_seconds)))
                   & (F.col("gap_after_s") >= F.lit(float(hole_seconds))))
            .select("track_id", "icao24", "event_time",
                    "gap_before_s", "gap_after_s"))


def extension_by_airframe(base: DataFrame, merged: DataFrame) -> DataFrame:
    """How much earlier and later each airframe-day is seen in *merged*.

    Positive `last_shift_s` means merged sees the airframe longer; negative
    `first_shift_s` means it sees it earlier. Both are reported signed, so a
    regression -- the merge somehow losing an end -- is visible rather than
    clipped to zero.
    """
    def ends(df, tag):
        return (df
                .withColumn("day", F.to_date("event_time"))
                .groupBy("icao24", "day")
                .agg(F.min("event_time").alias(f"first_{tag}"),
                     F.max("event_time").alias(f"last_{tag}")))

    b, m = ends(base, "base"), ends(merged, "merged")
    return (b.join(m, ["icao24", "day"], "inner")
            .select(
                "icao24", "day",
                (F.unix_timestamp("first_merged")
                 - F.unix_timestamp("first_base")).cast("double")
                .alias("first_shift_s"),
                (F.unix_timestamp("last_merged")
                 - F.unix_timestamp("last_base")).cast("double")
                .alias("last_shift_s"),
            ))


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--days", nargs="+", required=True)
    ap.add_argument("--prefix", default=ARM_PREFIX)
    ap.add_argument("--hole-seconds", type=int, default=HOLE_SECONDS)
    ap.add_argument("--results-dir", required=True)
    ap.add_argument("--gain-name", default="adsc_gain_2026.csv")
    ap.add_argument("--holes-name", default="adsc_holes_2026.csv")
    ap.add_argument("--cores", type=int, default=4)
    ap.add_argument("--driver-memory", default="8g")
    args = ap.parse_args()

    from osn_sample import build_spark, load_dotenv

    load_dotenv()
    spark = build_spark(args.cores, args.driver_memory, distributed=True)
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    gain_rows, hole_rows = [], []
    try:
        days = F.array(*[F.lit(d) for d in args.days])
        arms = {}
        for arm in ("base", "merged"):
            df = (spark.read.parquet(f"{args.prefix}/{arm}/osn_tracks_clean")
                  .where(F.array_contains(days, F.col("dof").cast("string"))))
            arms[arm] = df.cache()
            r = arm_summary(df).collect()[0].asDict()
            r["arm"] = arm
            r["days"] = " ".join(args.days)
            gain_rows.append(r)

        holes = bridged_holes(arms["merged"], args.hole_seconds)
        hole_rows = [r.asDict() for r in holes.collect()]

        ext = extension_by_airframe(arms["base"], arms["merged"])
        shifts = ext.agg(
            F.count(F.lit(1)).alias("airframe_days"),
            F.expr("percentile_approx(first_shift_s, 0.5)").alias("first_shift_median_s"),
            F.expr("percentile_approx(last_shift_s, 0.5)").alias("last_shift_median_s"),
            F.sum((F.col("first_shift_s") < 0).cast("long")).alias("seen_earlier"),
            F.sum((F.col("last_shift_s") > 0).cast("long")).alias("seen_later"),
        ).collect()[0].asDict()
        gain_rows.append({"arm": "extension", "days": " ".join(args.days), **shifts})
    finally:
        spark.stop()

    out = Path(args.results_dir)
    out.mkdir(parents=True, exist_ok=True)

    def write(name, rows):
        if not rows:
            print(f"  {name}: no rows -- writing a header-only file, which is "
                  f"a result and not a missing output")
        fields = sorted({k for r in rows for k in r}) or ["arm"]
        with open(out / name, "w", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=fields)
            w.writeheader()
            w.writerows(rows)
        print(f"  wrote {out / name} ({len(rows)} rows)")

    write(args.gain_name, gain_rows)
    write(args.holes_name, hole_rows)

    import provenance
    for name in (args.gain_name, args.holes_name):
        provenance.record(
            out, name, "benchmarks/adsc_coverage_gain.py", sys.argv[1:],
            ["benchmarks/adsc_coverage_gain.py",
             "src/opdi/ingestion/osn_adsc.py",
             "src/opdi/pipeline/tracks.py",
             "src/opdi/cleaning/native.py"],
            inputs={"days": args.days, "hole_seconds": args.hole_seconds},
            input_tables=[f"{args.prefix}/base/osn_tracks_clean",
                          f"{args.prefix}/merged/osn_tracks_clean"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 4: Run the tests**

Run: `.venv310/bin/python -m pytest tests/test_adsc_coverage_gain.py -q`
Expected: PASS (8 tests)

- [ ] **Step 5: Commit**

```bash
git add benchmarks/adsc_coverage_gain.py tests/test_adsc_coverage_gain.py
git commit -m "feat(adsc): measure coverage gain, bridged holes and merge-neutrality"
```

- [ ] **Step 6: Run it against the built arms**

```bash
export PYSPARK_DRIVER_PYTHON=$PWD/.venv310/bin/python
unset PYSPARK_PYTHON
.venv310/bin/python -u benchmarks/adsc_coverage_gain.py \
  --days 2026-06-05 2026-06-06 2026-06-07 \
  --results-dir benchmarks/out/adsc_gain
```

Read `tracks_with_two_callsigns` for both arms. **A materially higher value in
`merged` is a stop**: the merge is fusing separate flights, and the fix is a
rule on ADS-C samples during turnarounds, not a footnote in the paper.

---

## Task 9b: Time-synchronisation measurement

The two feeds do not share a clock, and neither states a usable position age:
ADS-C's `seen`/`seen_pos` are constant 0 — the feed *asserts* zero age — and the
readsb record does not expose the ADS-C report's own time-of-position. This task
measures the skew instead of assuming it away. Named `9b` rather than
renumbering, following the pipeline's own `01b`/`02a` convention: it belongs
between the trajectory measurement and the milestone scoring, and its result
changes how both are read.

A prototype of this measurement has already been run by hand on 2026-06-01
11:00–14:00 UTC (46 bracketed pairs, 25 airframes) and is the source of every
number quoted in the spec. This task is that prototype in Spark, over the three
study days, staged with provenance.

**Files:**
- Create: `benchmarks/adsc_time_sync.py`
- Create: `tests/test_adsc_time_sync.py`

**Interfaces:**
- Consumes: the shared ADS-B input at `s3a://eurocontrol/opdi/research/adsc/input/osn_statevectors_v2` and the ADS-C table beside it (Task 8), both written before either arm ran.
- Produces:
  - `bracket(adsc, adsb, max_gap_s) -> DataFrame` — one row per ADS-C report that has an ADS-B sample within `max_gap_s` on **both** sides, carrying the nearest each side.
  - `decompose(bracketed) -> DataFrame` — adds `along_m`, `cross_m`, `dist_m`, `implied_dt_s`, `adsb_pos_age_s`.
  - `per_airframe(decomposed) -> DataFrame` — `icao24`, `n`, `median_implied_dt_s`, `std_implied_dt_s`, `median_abs_cross_m`.
  - CSVs `adsc_time_sync_pairs_2026.csv` and `adsc_time_sync_2026.csv`. Tasks 12 and 13 read both.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_adsc_time_sync.py
"""The along/cross decomposition, against a track with a known injected lag."""

import datetime as dt

import pytest
from pyspark.sql import Row

from adsc_time_sync import bracket, decompose, per_airframe

T0 = dt.datetime(2026, 6, 5, 12, 0, 0)

#: Due east at a round speed, at a latitude where a degree of longitude is a
#: convenient length. Everything below is checked against hand arithmetic, so
#: the test fails on a wrong projection rather than agreeing with itself.
LAT = 50.0
V_MS = 250.0
R = 6371000.0


def _lon_at(seconds):
    """Longitude after *seconds* of due-east flight at V_MS from lon 0."""
    import math

    metres = V_MS * seconds
    return math.degrees(metres / (R * math.cos(math.radians(LAT))))


def _adsb(spark, times):
    return spark.createDataFrame([
        Row(icao24="4acb41", event_time=T0 + dt.timedelta(seconds=s),
            lat=LAT, lon=_lon_at(s), velocity=V_MS,
            last_pos_update=(T0 + dt.timedelta(seconds=s)).timestamp())
        for s in times])


def _adsc(spark, at_seconds, lag_seconds=0.0, cross_m=0.0):
    """An ADS-C report tagged `at_seconds` whose position is the aircraft's
    position at `at_seconds + lag_seconds`, offset `cross_m` to the north."""
    import math

    return spark.createDataFrame([
        Row(icao24="4acb41",
            event_time=T0 + dt.timedelta(seconds=at_seconds),
            lat=LAT + math.degrees(cross_m / R),
            lon=_lon_at(at_seconds + lag_seconds),
            velocity=V_MS)])


def test_a_synchronised_report_shows_no_offset(spark):
    out = decompose(bracket(_adsc(spark, 30), _adsb(spark, [0, 60]), 60))
    r = out.collect()[0]
    assert r["along_m"] == pytest.approx(0.0, abs=1.0)
    assert r["cross_m"] == pytest.approx(0.0, abs=1.0)
    assert r["implied_dt_s"] == pytest.approx(0.0, abs=0.02)


def test_a_known_lag_is_recovered(spark):
    # The whole instrument: a report whose position is 1.5 s further along
    # than its timestamp claims must read as +1.5 s, and as +375 m along track.
    out = decompose(bracket(_adsc(spark, 30, lag_seconds=1.5),
                            _adsb(spark, [0, 60]), 60))
    r = out.collect()[0]
    assert r["implied_dt_s"] == pytest.approx(1.5, abs=0.05)
    assert r["along_m"] == pytest.approx(375.0, abs=5.0)
    assert r["cross_m"] == pytest.approx(0.0, abs=2.0)


def test_a_negative_lag_is_recovered_with_its_sign(spark):
    out = decompose(bracket(_adsc(spark, 30, lag_seconds=-12.9),
                            _adsb(spark, [0, 60]), 60))
    assert out.collect()[0]["implied_dt_s"] == pytest.approx(-12.9, abs=0.1)


def test_a_cross_track_error_does_not_read_as_a_lag(spark):
    # The discriminator the conclusion rests on. A position error is isotropic;
    # a timing error is purely along track. If a 400 m northward offset leaked
    # into `implied_dt_s`, the 43:1 ratio would be an artefact of the maths.
    out = decompose(bracket(_adsc(spark, 30, cross_m=400.0),
                            _adsb(spark, [0, 60]), 60))
    r = out.collect()[0]
    assert r["cross_m"] == pytest.approx(400.0, abs=5.0)
    assert r["along_m"] == pytest.approx(0.0, abs=5.0)
    assert r["implied_dt_s"] == pytest.approx(0.0, abs=0.05)


def test_an_unbracketed_report_is_dropped(spark):
    # Only one side: extrapolating is how a measurement invents its own answer.
    assert bracket(_adsc(spark, 30), _adsb(spark, [0, 10]), 60).count() == 0
    assert bracket(_adsc(spark, 30), _adsb(spark, [40, 50]), 60).count() == 0


def test_a_loose_bracket_is_dropped(spark):
    # 300 s either side at 250 m/s is 75 km of interpolation: the instrument
    # would be measuring its own straight line, not the aircraft.
    assert bracket(_adsc(spark, 300), _adsb(spark, [0, 600]), 60).count() == 0


def test_the_nearest_sample_each_side_is_the_one_used(spark):
    out = decompose(bracket(_adsc(spark, 30),
                            _adsb(spark, [0, 25, 35, 60]), 60))
    r = out.collect()[0]
    assert r["bracket_s"] == pytest.approx(10.0, abs=0.01)


def test_another_airframe_cannot_bracket_this_one(spark):
    other = _adsb(spark, [0, 60]).withColumn(
        "icao24", __import__("pyspark.sql.functions", fromlist=["lit"]).lit("3c6568"))
    assert bracket(_adsc(spark, 30), other, 60).count() == 0


def test_per_airframe_summarises_each_separately(spark):
    pairs = decompose(bracket(
        _adsc(spark, 30, lag_seconds=1.5), _adsb(spark, [0, 60]), 60))
    summary = per_airframe(pairs).collect()
    assert len(summary) == 1
    assert summary[0]["icao24"] == "4acb41"
    assert summary[0]["n"] == 1
    assert summary[0]["median_implied_dt_s"] == pytest.approx(1.5, abs=0.05)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv310/bin/python -m pytest tests/test_adsc_time_sync.py -q`
Expected: FAIL with `ModuleNotFoundError: No module named 'adsc_time_sync'`

- [ ] **Step 3: Write the module**

```python
#!/usr/bin/env python
"""How far apart the ADS-B and ADS-C clocks are, measured rather than assumed.

Neither feed states a position age that can be used. ADS-B's `last_pos_update`
is real but tiny (median 0.22 s, max 0.92 s on the sample measured). ADS-C's
`seen`/`seen_pos` are constant 0 -- the feed *asserts* zero age for every
report -- and the readsb `Aircraft` record does not expose the ADS-C report's
own time-of-position, so there is nothing in band to correct from.

**The instrument.** For each ADS-C report, take the nearest ADS-B sample of the
same airframe on each side within `max_gap_s`, interpolate the ADS-B position
linearly to the ADS-C timestamp, and decompose the offset
`p_adsc - p_adsb` into along-track and cross-track components using the
direction of the ADS-B pair itself. Then:

* **cross-track** is the position-disagreement floor -- what the two feeds
  disagree about that no timing explanation can absorb;
* **along-track / ground speed** is the implied time offset.

A position error is isotropic, so it lands equally in both. A timing error
lands only along track. The ratio between them is therefore the discriminator,
and it is what makes the conclusion falsifiable rather than a story:
`test_a_cross_track_error_does_not_read_as_a_lag` pins the maths down on a
synthetic track with a known injected offset.

**What the prototype found** (2026-06-01 11:00-14:00 UTC, 46 bracketed pairs,
25 airframes, full 1 Hz ADS-B): median \\|cross\\| 9 m against median
\\|along\\| 377 m -- a 43x ratio -- and an implied offset of +1.48 s with an
IQR of +0.32 to +1.96 s. ADS-B's own staleness cannot explain it (correlation
0.23, and the age never exceeds 0.92 s). And the offset is **per airframe**:
within one it holds to sigma 0.10-0.31 s, between them it runs +0.01 s to
+2.09 s, with one airframe persistently at -12.9 s. That is why
`AdscConfig.time_offset_seconds` ships at zero -- a constant correction would
be wrong for nearly every aircraft and would damage the synchronised ones.

**Why the sample is small, and why that is not a defect.** The measurement
needs an airframe heard by *both* feeds at the same minute, and ADS-C exists
precisely where ADS-B cannot hear the aircraft. 46 of 1,088 reports bracketed
on the prototype day. More days widen it; nothing widens it a lot.

    python benchmarks/adsc_time_sync.py --days 2026-06-05 2026-06-06 \\
        2026-06-07 --results-dir /tmp/sync
"""

import argparse
import csv
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "src"))
sys.path.insert(0, str(REPO / "benchmarks"))

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

INPUT_PREFIX = "s3a://eurocontrol/opdi/research/adsc/input"

#: Earth radius for the local tangent-plane projection, metres. The offsets
#: being measured are hundreds of metres over baselines of tens of kilometres,
#: so a spherical tangent plane is three orders of magnitude more accurate
#: than it needs to be and a geodesic solver would be ceremony.
R_EARTH = 6371000.0

#: Ground speeds below this are not usable: the implied offset divides by
#: speed, so a taxiing or stationary aircraft turns a 50 m position
#: disagreement into minutes of apparent skew. ADS-C is a cruise feed, so this
#: discards almost nothing.
MIN_SPEED_MS = 50.0


def _east_north(lat, lon, lat0, lon0):
    """Local east/north metres of (lat, lon) relative to (lat0, lon0)."""
    east = (F.radians(lon - lon0) * F.lit(R_EARTH)
            * F.cos(F.radians(lat0)))
    north = F.radians(lat - lat0) * F.lit(R_EARTH)
    return east, north


def bracket(adsc: DataFrame, adsb: DataFrame, max_gap_s: int = 60) -> DataFrame:
    """ADS-C reports with the nearest ADS-B sample on **each** side.

    Both sides, never one: extrapolating from a single sample would measure the
    instrument's own straight line rather than the aircraft, and the sign of
    the result would depend on which side happened to exist.

    The join is the shadow filter's shape -- bin to `max_gap_s`, expand the
    ADS-C side to three candidate bins, broadcast it, then test exactly -- for
    the same reason: ADS-C is a few thousand rows a day against hundreds of
    millions, so this is one broadcast hash join rather than a shuffle of the
    state vectors. `max(struct(...))` then picks the nearest sample on each
    side in a single hash aggregate, the idiom
    `StateVectorIngestion._bucket_decimate` already uses.
    """
    w = int(max_gap_s)
    if w <= 0:
        raise ValueError(f"max_gap_s must be positive, got {w!r}")

    tc = F.unix_timestamp("event_time").cast("double")
    bin_c = F.floor(tc / F.lit(w)).cast("long")
    keys = adsc.select(
        F.col("icao24"),
        F.col("event_time").alias("t_c_ts"),
        tc.alias("t_c"),
        F.col("lat").alias("lat_c"),
        F.col("lon").alias("lon_c"),
        F.explode(F.array(bin_c - 1, bin_c, bin_c + 1)).alias("_bin"),
    )

    tb = F.unix_timestamp("event_time").cast("double")
    samples = adsb.select(
        F.col("icao24").alias("_icao24"),
        tb.alias("t_b"),
        F.col("lat").alias("lat_b"),
        F.col("lon").alias("lon_b"),
        F.col("velocity").alias("v_b"),
        F.col("last_pos_update").cast("double").alias("lpu_b"),
        F.floor(tb / F.lit(w)).cast("long").alias("_bin"),
    ).where(F.col("lat_b").isNotNull() & F.col("lon_b").isNotNull())

    j = (
        samples.join(
            F.broadcast(keys),
            (F.col("_icao24") == F.col("icao24"))
            & (samples["_bin"] == keys["_bin"]),
            "inner",
        )
        .withColumn("_dt", F.col("t_b") - F.col("t_c"))
        .where(F.abs(F.col("_dt")) <= F.lit(float(w)))
    )

    # `max` over a struct compares field by field, so leading with the signed
    # gap picks the nearest sample on each side: the largest negative gap
    # before, the smallest positive gap after.
    before = F.max(F.when(F.col("_dt") <= 0, F.struct(
        F.col("_dt").alias("dt"), F.col("t_b"), F.col("lat_b"),
        F.col("lon_b"), F.col("v_b"), F.col("lpu_b"))))
    after = F.min(F.when(F.col("_dt") >= 0, F.struct(
        F.col("_dt").alias("dt"), F.col("t_b"), F.col("lat_b"),
        F.col("lon_b"), F.col("v_b"), F.col("lpu_b"))))

    out = (
        j.groupBy("icao24", "t_c_ts", "t_c", "lat_c", "lon_c")
        .agg(before.alias("_p"), after.alias("_n"))
        # A report with only one side is dropped here rather than filled: see
        # the docstring. `max`/`min` of an all-NULL group is NULL, so this is
        # the whole test.
        .where(F.col("_p").isNotNull() & F.col("_n").isNotNull())
        .select(
            "icao24", "t_c_ts", "t_c", "lat_c", "lon_c",
            F.col("_p.t_b").alias("t_prev"), F.col("_p.lat_b").alias("lat_prev"),
            F.col("_p.lon_b").alias("lon_prev"), F.col("_p.v_b").alias("v_prev"),
            F.col("_p.lpu_b").alias("lpu_prev"),
            F.col("_n.t_b").alias("t_next"), F.col("_n.lat_b").alias("lat_next"),
            F.col("_n.lon_b").alias("lon_next"), F.col("_n.v_b").alias("v_next"),
        )
        .withColumn("bracket_s", F.col("t_next") - F.col("t_prev"))
    )
    # A zero-length bracket cannot be interpolated and a long one interpolates
    # across more than the instrument can justify: at 250 m/s a 120 s bracket
    # is 30 km of assumed straight line, against the hundreds of metres being
    # measured.
    return out.where((F.col("bracket_s") > 0)
                     & (F.col("bracket_s") <= F.lit(2.0 * w)))


def decompose(bracketed: DataFrame) -> DataFrame:
    """Add the along/cross decomposition and the implied time offset."""
    w = ((F.col("t_c") - F.col("t_prev"))
         / (F.col("t_next") - F.col("t_prev")))
    lat_i = F.col("lat_prev") + w * (F.col("lat_next") - F.col("lat_prev"))
    lon_i = F.col("lon_prev") + w * (F.col("lon_next") - F.col("lon_prev"))

    df = (bracketed
          .withColumn("_w", w)
          .withColumn("lat_i", lat_i)
          .withColumn("lon_i", lon_i))

    # Track direction from the ADS-B pair itself, not from the broadcast
    # `heading`: heading is what the aircraft says it is pointing at, which in
    # wind is not where it is going, and the decomposition needs the direction
    # it actually moved.
    dx, dy = _east_north(F.col("lat_next"), F.col("lon_next"),
                         F.col("lat_prev"), F.col("lon_prev"))
    df = df.withColumn("_dx", dx).withColumn("_dy", dy)
    norm = F.sqrt(F.col("_dx") * F.col("_dx") + F.col("_dy") * F.col("_dy"))
    df = df.withColumn("_norm", norm).where(F.col("_norm") > F.lit(1.0))
    df = (df.withColumn("_ux", F.col("_dx") / F.col("_norm"))
            .withColumn("_uy", F.col("_dy") / F.col("_norm")))

    ox, oy = _east_north(F.col("lat_c"), F.col("lon_c"),
                         F.col("lat_i"), F.col("lon_i"))
    df = df.withColumn("_ox", ox).withColumn("_oy", oy)

    df = (
        df.withColumn("along_m",
                      F.col("_ox") * F.col("_ux") + F.col("_oy") * F.col("_uy"))
        .withColumn("cross_m",
                    -F.col("_ox") * F.col("_uy") + F.col("_oy") * F.col("_ux"))
        .withColumn("dist_m", F.sqrt(F.col("_ox") * F.col("_ox")
                                     + F.col("_oy") * F.col("_oy")))
        .withColumn("v_ms", (F.col("v_prev") + F.col("v_next")) / F.lit(2.0))
        # ADS-B's own position age at the bracketing sample. Reported because
        # the archive carries positions forward, which would bias the result
        # in exactly the direction measured -- so the alternative explanation
        # has to be checkable from the output rather than argued away.
        .withColumn("adsb_pos_age_s", F.col("t_prev") - F.col("lpu_prev"))
        .where(F.col("v_ms") >= F.lit(MIN_SPEED_MS))
        .withColumn("implied_dt_s", F.col("along_m") / F.col("v_ms"))
    )

    return df.select("icao24", F.col("t_c_ts").alias("event_time"),
                     "along_m", "cross_m", "dist_m", "implied_dt_s",
                     "v_ms", "bracket_s", "adsb_pos_age_s")


def per_airframe(decomposed: DataFrame) -> DataFrame:
    """Per-airframe summary -- the table the per-airframe conclusion rests on.

    Reported per airframe and not only in aggregate because that is where the
    finding is: the network median says +1.48 s, and an airframe-level view
    says one aircraft is at +0.01 s and another at -12.9 s. A single number
    would have hidden the only thing that decides whether a correction is
    possible.
    """
    return (decomposed.groupBy("icao24").agg(
        F.count(F.lit(1)).alias("n"),
        F.expr("percentile_approx(implied_dt_s, 0.5)").alias("median_implied_dt_s"),
        F.stddev("implied_dt_s").alias("std_implied_dt_s"),
        F.expr("percentile_approx(abs(cross_m), 0.5)").alias("median_abs_cross_m"),
        F.expr("percentile_approx(abs(along_m), 0.5)").alias("median_abs_along_m"),
    ).orderBy("icao24"))


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--days", nargs="+", required=True)
    ap.add_argument("--prefix", default=INPUT_PREFIX)
    ap.add_argument("--max-gap-s", type=int, default=60)
    ap.add_argument("--results-dir", required=True)
    ap.add_argument("--pairs-name", default="adsc_time_sync_pairs_2026.csv")
    ap.add_argument("--out-name", default="adsc_time_sync_2026.csv")
    ap.add_argument("--cores", type=int, default=4)
    ap.add_argument("--driver-memory", default="8g")
    ap.add_argument("--executors", type=int, default=8)
    args = ap.parse_args()

    import osn_sample
    from osn_sample import build_spark, load_dotenv

    load_dotenv()
    osn_sample.RESEARCH_EXECUTORS = args.executors
    spark = build_spark(args.cores, args.driver_memory, distributed=True)
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    days = F.array(*[F.lit(d) for d in args.days])
    try:
        adsc = (spark.read.parquet(f"{args.prefix}/osn_adsc_statevectors")
                .where(F.array_contains(days, F.col("event_time_day").cast("string"))))
        adsb = (spark.read.parquet(f"{args.prefix}/osn_statevectors_v2")
                .where(F.array_contains(days, F.col("event_time_day").cast("string"))))
        n_adsc = adsc.count()
        pairs = decompose(bracket(adsc, adsb, args.max_gap_s)).cache()
        pair_rows = [r.asDict() for r in pairs.collect()]
        print(f"bracketed {len(pair_rows)} of {n_adsc} ADS-C reports "
              f"({len({r['icao24'] for r in pair_rows})} airframes)")
        summary = [r.asDict() for r in per_airframe(pairs).collect()]
        overall = pairs.agg(
            F.count(F.lit(1)).alias("n"),
            F.expr("percentile_approx(implied_dt_s, 0.5)").alias("median_implied_dt_s"),
            F.stddev("implied_dt_s").alias("std_implied_dt_s"),
            F.expr("percentile_approx(abs(cross_m), 0.5)").alias("median_abs_cross_m"),
            F.expr("percentile_approx(abs(along_m), 0.5)").alias("median_abs_along_m"),
        ).collect()[0].asDict()
        overall["icao24"] = "ALL"
        summary.append(overall)
    finally:
        spark.stop()

    out = Path(args.results_dir)
    out.mkdir(parents=True, exist_ok=True)

    def write(name, rows, required):
        if not rows and required:
            raise SystemExit(
                f"{name}: no bracketed pairs at all. That is not a null "
                f"result -- it means the two tables share no airframe-minute, "
                f"so either the ADS-C ingest or the state-vector window is "
                f"wrong for these days.")
        fields = sorted({k for r in rows for k in r}) or ["icao24"]
        with open(out / name, "w", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=fields)
            w.writeheader()
            w.writerows(rows)
        print(f"  wrote {out / name} ({len(rows)} rows)")

    write(args.pairs_name, pair_rows, required=True)
    write(args.out_name, summary, required=True)

    import provenance
    for name in (args.pairs_name, args.out_name):
        provenance.record(
            out, name, "benchmarks/adsc_time_sync.py", sys.argv[1:],
            ["benchmarks/adsc_time_sync.py", "src/opdi/ingestion/osn_adsc.py"],
            inputs={"days": args.days, "max_gap_s": args.max_gap_s,
                    "min_speed_ms": MIN_SPEED_MS},
            input_tables=[f"{args.prefix}/osn_adsc_statevectors",
                          f"{args.prefix}/osn_statevectors_v2"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 4: Run the tests**

Run: `.venv310/bin/python -m pytest tests/test_adsc_time_sync.py -q`
Expected: PASS (9 tests). The two that matter most are
`test_a_known_lag_is_recovered` and
`test_a_cross_track_error_does_not_read_as_a_lag` — together they prove the
instrument separates timing from position rather than assuming the answer.

- [ ] **Step 5: Commit**

```bash
git add benchmarks/adsc_time_sync.py tests/test_adsc_time_sync.py
git commit -m "feat(adsc): measure the ADS-B/ADS-C clock skew along and across track"
```

- [ ] **Step 6: Run it against the shared inputs**

Needs only Task 8's Step 6 (the shared ingest), not the arm builds:
```bash
export PYSPARK_DRIVER_PYTHON=$PWD/.venv310/bin/python
unset PYSPARK_PYTHON OPDI_WAREHOUSE
.venv310/bin/python -u benchmarks/adsc_time_sync.py \
  --days 2026-06-05 2026-06-06 2026-06-07 \
  --results-dir benchmarks/out/adsc_sync
```

Expected, from the prototype: a few tens of bracketed pairs per day, median
`|cross_m|` around ten metres, median `|along_m|` in the hundreds, and a
per-airframe table whose within-airframe `std_implied_dt_s` is far smaller than
the spread of `median_implied_dt_s` between airframes.

**Three readings that change what follows:**

- **Median `|cross_m|` comparable to median `|along_m|`** — the offset is a
  position error after all, the 43× ratio was a sampling artefact, and the
  timing story in the spec must be withdrawn rather than reworded.
- **`adsb_pos_age_s` large and correlated with `implied_dt_s`** — the offset is
  ADS-B carry-forward, not ADS-C skew. Check the correlation explicitly; the
  prototype measured 0.23 with ages never above 0.92 s.
- **`std_implied_dt_s` as large as the between-airframe spread** — the skew is
  not a per-airframe property, and a single global
  `time_offset_seconds` becomes defensible after all. That would be a reason to
  add a corrected third arm, which is what the knob exists for.

```bash
git add benchmarks/out/adsc_sync
git commit -m "data(adsc): clock-skew measurement over the three study days"
```

---

## Task 10: Score both arms against APDF

Reuses `events_gt` for the truth and `events_score` for the arithmetic — the
bridge, the Europe/Paris timestamps, the IATA-versus-ICAO vocabularies and the
eight-character callsign padding are not re-derived. Deliberately does **not**
register a new period: the two arms share a month, a day set and a study
airport set, so the truth frame is the same frame and building it twice would
be the easiest way to make the arms incomparable.

**Files:**
- Create: `benchmarks/adsc_events_compare.py`
- Create: `tests/test_adsc_events_compare.py`

**Interfaces:**
- Consumes: `events_gt.build`, `events_score.align`/`score`/`score_by_airport`, `event_bench.detected_events`/`milestone_map`; the arm tables from Task 8.
- Produces: `arm_tables(prefix, arm) -> dict` with keys `events`, `flight_list`; `score_arm(spark, storage, truth, prefix, arm, mapping) -> DataFrame` with an added `arm` column; CSVs `adsc_events_2026.csv` (per arm × milestone) and `adsc_events_by_airport_2026.csv` (per arm × airport × milestone). Task 12 reads both.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_adsc_events_compare.py
"""The two-arm scorer's wiring. The arithmetic is events_score's own tests."""

import pytest

import adsc_events_compare as a


def test_arm_tables_point_at_the_arm_prefix():
    t = a.arm_tables("s3a://eurocontrol/opdi/research/adsc", "merged")
    assert t["events"] == (
        "s3a://eurocontrol/opdi/research/adsc/merged/opdi_flight_events")
    assert t["flight_list"] == (
        "s3a://eurocontrol/opdi/research/adsc/merged/opdi_flight_list")


def test_the_event_table_is_an_absolute_path():
    # `detected_events` branches on `table.startswith("s3a://")`: an absolute
    # path is read with spark.read.parquet, anything else as a catalog table.
    # A relative name here would be resolved against OPDI_WAREHOUSE, which
    # production exports -- so the scorer would silently read opdi-prod.
    for arm in a.ARMS:
        assert a.arm_tables(a.ARM_PREFIX, arm)["events"].startswith("s3a://")


def test_both_arms_are_scored():
    assert a.ARMS == ("base", "merged")


def test_the_period_is_the_v4_period():
    # Comparability with flight-events-v4 is the reason this period and not
    # another: 202606 is the only APDF extract covering all twenty study
    # aerodromes.
    import events_gt
    assert a.PERIOD == "2026"
    assert events_gt.PERIODS[a.PERIOD]["month"] == "202606"


def test_an_unknown_arm_is_refused():
    with pytest.raises(ValueError, match="unknown arm"):
        a.arm_tables(a.ARM_PREFIX, "mergd")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv310/bin/python -m pytest tests/test_adsc_events_compare.py -q`
Expected: FAIL with `ModuleNotFoundError: No module named 'adsc_events_compare'`

- [ ] **Step 3: Write the scorer**

```python
#!/usr/bin/env python
"""Score both ADS-C fusion arms against APDF, per milestone and per aerodrome.

The question is narrow: with the same state vectors, the same segmentation,
the same cleaning thresholds and the same detectors, does adding ADS-C change
what the pipeline detects? So both arms are scored through one truth frame,
built once -- `events_gt.build(spark, "2026", airports="study")`, the
flight-events-v4 period and study set.

**Nothing about the truth is re-derived here.** The APDF-to-aircraft-address
bridge, the Europe/Paris timestamps behind the `_UTC` suffixes, the
IATA-versus-ICAO callsign vocabularies and the eight-character padding on ADS-B
callsigns each cost this project a run; `events_gt` closes all four and is
imported rather than imitated.

**Read the result against Task 9's, not alone.** ADS-C is a cruise feed: ~5k
rows a day inside the box, 97.6% of them above FL300, none on the ground. It
cannot reach a runway or A-CDM milestone, and a flat table for that family is
the expected outcome rather than a failure of the measurement. The families
where a change is physically possible are the ones anchored at the edges of
coverage: `first_seen`, `last_seen`, the FL crossings and -- through the flight
list -- ADEP/ADES on North Atlantic arrivals.

    unset OPDI_WAREHOUSE    # table paths here are absolute, but events_gt's
                            # reference reads are not: production exports this
                            # variable and it would move the truth frame.
    python benchmarks/adsc_events_compare.py --results-dir /tmp/adsc_score
"""

import argparse
import csv
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "src"))
sys.path.insert(0, str(REPO / "benchmarks"))

from pyspark.sql import functions as F  # noqa: E402

import events_gt  # noqa: E402
import events_score  # noqa: E402
from event_bench import detected_events, milestone_map  # noqa: E402

ARM_PREFIX = "s3a://eurocontrol/opdi/research/adsc"
ARMS = ("base", "merged")

#: The flight-events-v4 period: APDF 202606, days 2026-06-05/06/07. Named here
#: rather than passed, because a comparison between arms over different days
#: is not a comparison.
PERIOD = "2026"


def arm_tables(prefix: str, arm: str) -> dict:
    """Absolute paths to one arm's event table and flight list.

    Absolute, not warehouse-relative: `detected_events` branches on the
    `s3a://` prefix, and a relative name would be resolved against
    `OPDI_WAREHOUSE` -- which production exports.
    """
    if arm not in ARMS:
        raise ValueError(f"unknown arm {arm!r}; expected one of {ARMS}")
    return {
        "events": f"{prefix}/{arm}/opdi_flight_events",
        "flight_list": f"{prefix}/{arm}/opdi_flight_list",
    }


def score_arm(spark, storage, truth, prefix: str, arm: str, mapping: dict):
    """`(per_milestone, per_airport)` for one arm, each carrying `arm`."""
    tables = arm_tables(prefix, arm)
    det = detected_events(
        spark, tables["events"], storage=storage, identity="flight_list",
        flight_list=tables["flight_list"], mapping=mapping,
    )
    aligned = events_score.align(truth, det).cache()
    per_ms = events_score.score(aligned).withColumn("arm", F.lit(arm))
    per_ap = events_score.score_by_airport(aligned).withColumn("arm", F.lit(arm))
    events_score.guard_not_all_zero(per_ms)
    return per_ms, per_ap


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--prefix", default=ARM_PREFIX)
    ap.add_argument("--arms", nargs="+", default=list(ARMS), choices=list(ARMS))
    ap.add_argument("--results-dir", required=True)
    ap.add_argument("--out-name", default="adsc_events_2026.csv")
    ap.add_argument("--by-airport-name",
                    default="adsc_events_by_airport_2026.csv")
    ap.add_argument("--cores", type=int, default=4)
    ap.add_argument("--driver-memory", default="8g")
    ap.add_argument("--executors", type=int, default=6)
    ap.add_argument("--ui-port", type=int, default=4066)
    args = ap.parse_args()

    import osn_sample
    from osn_sample import build_spark, load_dotenv

    load_dotenv()
    osn_sample.UI_PORT = args.ui_port
    osn_sample.RESEARCH_EXECUTORS = args.executors
    spark = build_spark(args.cores, args.driver_memory, distributed=True)
    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    from opdi.config import OPDIConfig
    from opdi.utils.storage import StorageManager

    config = OPDIConfig.for_environment("opensky")
    storage = StorageManager(spark, config)

    # The shipped vocabulary, which is what both arms' step 04 ran. `landing`
    # means two different physical things either side of
    # `emit_runway_milestones`, so the map has to come from the configuration
    # that produced the table rather than from a version string.
    mapping = milestone_map(config.events)

    ms_rows, ap_rows = [], []
    try:
        truth, _rings, _report = events_gt.build(
            spark, PERIOD, airports="study")
        truth.cache()
        for arm in args.arms:
            print(f"\n=== scoring arm {arm} ===")
            per_ms, per_ap = score_arm(
                spark, storage, truth, args.prefix, arm, mapping)
            ms_rows += [r.asDict() for r in per_ms.collect()]
            ap_rows += [r.asDict() for r in per_ap.collect()]
    finally:
        spark.stop()

    out = Path(args.results_dir)
    out.mkdir(parents=True, exist_ok=True)

    def write(name, rows):
        if not rows:
            raise SystemExit(
                f"{name}: no rows. An empty score table is not a null result "
                f"-- it means the identity bridge found nothing, and a paper "
                f"must not report it as agreement.")
        fields = ["arm"] + sorted(set(rows[0]) - {"arm"})
        with open(out / name, "w", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=fields)
            w.writeheader()
            w.writerows(rows)
        print(f"  wrote {out / name} ({len(rows)} rows)")

    write(args.out_name, ms_rows)
    write(args.by_airport_name, ap_rows)

    import provenance
    for name in (args.out_name, args.by_airport_name):
        provenance.record(
            out, name, "benchmarks/adsc_events_compare.py", sys.argv[1:],
            ["benchmarks/adsc_events_compare.py", "benchmarks/events_gt.py",
             "benchmarks/events_score.py", "benchmarks/event_bench.py",
             "src/opdi/pipeline/events.py"],
            inputs={"period": PERIOD, "arms": args.arms, "airports": "study"},
            input_tables=[arm_tables(args.prefix, a)["events"]
                          for a in args.arms])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 4: Run the tests**

Run: `.venv310/bin/python -m pytest tests/test_adsc_events_compare.py -q`
Expected: PASS (5 tests)

- [ ] **Step 5: Commit**

```bash
git add benchmarks/adsc_events_compare.py tests/test_adsc_events_compare.py
git commit -m "feat(adsc): score both arms against APDF through one truth frame"
```

- [ ] **Step 6: Run it**

```bash
export PYSPARK_DRIVER_PYTHON=$PWD/.venv310/bin/python
unset PYSPARK_PYTHON
unset OPDI_WAREHOUSE
.venv310/bin/python -u benchmarks/adsc_events_compare.py \
  --results-dir benchmarks/out/adsc_score
```

Expected: a bridge report naming the reach percentage (~97%), then two tables.
Compare `n_detected` per milestone between the arms. **If the two arms are
byte-identical on every milestone**, that is a finding, not a bug — but
confirm it against Task 9's `rows_adsc` first: identical scores with zero ADS-C
rows means the merge never ran, and identical scores with thousands of ADS-C
rows is the null result the paper reports.

---

## Task 11: Show the source on the Flight page

**Files:**
- Modify: `opdi-benchmarker/src/opdi_benchmarker/build/trajectories.py`
- Modify: `opdi-benchmarker/src/opdi_benchmarker/read.py`
- Modify: `opdi-benchmarker/src/opdi_benchmarker/app/queries.py`
- Modify: `opdi-benchmarker/src/opdi_benchmarker/app/pages/1_Flight.py`
- Modify: `opdi-benchmarker/README.md`
- Modify: `opdi-benchmarker/tests/test_trajectories.py`
- Modify: `opdi-benchmarker/tests/test_read.py`
- Modify: `opdi-benchmarker/tests/test_queries_flight.py`
- Modify: `opdi-benchmarker/tests/test_pages.py`

All paths below are relative to `opdi-benchmarker/`. **No new configuration is
added:** the two environment variables that point the build at the merged arm
already exist (`OPDI_WAREHOUSE` for the source, `OPDI_BENCHMARKER_CACHE` for
the cache).

**Interfaces:**
- Consumes: the `source` column written by Task 6, present in the merged arm's `osn_tracks` and absent from the baseline's.
- Produces: `trajectory` cache table gains a `source STRING` column; `queries.flight_trajectories` returns it; `1_Flight.SOURCE_STYLE` is the per-source drawing spec.

- [ ] **Step 1: Write the failing tests**

```python
# append to tests/test_trajectories.py
def test_source_is_carried_through(spark):
    tracks = spark.createDataFrame([
        Row(track_id="f1", icao24="4acb41", callsign=None,
            event_time=dt.datetime(2026, 6, 5, 8, 10, 0), lat=50.9, lon=4.48,
            baro_altitude=304.8, geo_altitude=None, velocity=100.0,
            vert_rate=0.0, on_ground=False, cumulative_distance_nm=12.5,
            source="ADS-C"),
    ])
    assert trajectories.build(tracks).collect()[0]["source"] == "ADS-C"


def test_source_defaults_to_adsb_when_the_tracks_lack_it(spark, tracks):
    # The baseline arm and every production table have no `source` column.
    # The app colours by source, so a NULL there would draw an unlabelled
    # third category -- the chart has to say ADS-B, which is what it is.
    assert trajectories.build(tracks).collect()[0]["source"] == "ADS-B"
```

```python
# append to tests/test_read.py
def test_trajectory_scan_unions_by_name():
    # Cached days built before `source` existed have one column fewer. Without
    # union_by_name DuckDB's read_parquet over the directory fails on the
    # schema mismatch, and the failure takes out the whole Flight page rather
    # than one column of it.
    assert "union_by_name" in read.scan("trajectory")
```

```python
# append to tests/test_queries_flight.py
def test_flight_trajectories_selects_source(monkeypatch):
    captured = {}
    monkeypatch.setattr(queries.read, "query",
                        lambda sql, params=None: captured.setdefault("sql", sql))
    queries.flight_trajectories(["f1"], None)
    assert "source" in captured["sql"]


def test_empty_result_frame_has_the_source_column():
    # The page groups by `source`; an empty frame without it raises KeyError
    # on a day with no cached trajectory, which is a normal day.
    assert "source" in queries.flight_trajectories([], None).columns
```

```python
# append to tests/test_pages.py
def test_flight_page_styles_every_source():
    # Loaded through this suite's existing page-loading helper -- `1_Flight` is
    # not an importable module name, which is why one exists.
    mod = load_page("1_Flight")
    assert set(mod.SOURCE_STYLE) == {"ADS-B", "ADS-C"}
    # ADS-C is drawn with markers: at a 600 s cadence a bare line is a
    # straight-line "trajectory" between two points ten minutes apart, which
    # is a path the aircraft was never measured on.
    assert "markers" in mod.SOURCE_STYLE["ADS-C"]["mode"]
    assert mod.SOURCE_STYLE["ADS-B"]["mode"] == "lines"
```

If `tests/test_pages.py` has no `load_page` helper, read the file and use
whatever mechanism it already uses to import a page module; do not add a second
one.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `.venv/bin/python -m pytest tests/test_trajectories.py tests/test_read.py tests/test_queries_flight.py tests/test_pages.py -q`
Expected: FAIL — `source` missing from the built frame, `union_by_name` absent
from the scan, `SOURCE_STYLE` undefined.

- [ ] **Step 3: Carry `source` through the build**

In `src/opdi_benchmarker/build/trajectories.py`, add to the module docstring:

```
Carries `source` where the pipeline wrote one. The merged ADS-C arm writes
"ADS-B"/"ADS-C"; every other table has no such column, and those rows are
ADS-B by construction -- so the default is stated here rather than left NULL,
because the page draws one trace per source and a NULL would be an
unlabelled third category.
```

and add to the `select`, after `F.col("on_ground")`:

```python
        (F.col("source") if "source" in df.columns
         else F.lit(DEFAULT_SOURCE)).alias("source"),
```

with, above `build`:

```python
#: What a table without a `source` column is. Named rather than inlined so the
#: app's legend and this default cannot disagree about the spelling.
DEFAULT_SOURCE = "ADS-B"
```

- [ ] **Step 4: Union by name in the cache scan**

In `src/opdi_benchmarker/read.py`, in `scan`, change the fragment to:

```python
    # `union_by_name` so a day cached before a column existed still reads, with
    # the column NULL for those rows rather than the whole scan failing on the
    # schema mismatch. It costs a schema union across the files, which is
    # cheaper than invalidating every cached day whenever a column is added.
    frag = (
        f"read_parquet('{_uri(table)}/**/*.parquet', hive_partitioning=1, "
        f"union_by_name=1) AS {table}"
    )
```

- [ ] **Step 5: Select it in the query**

In `src/opdi_benchmarker/app/queries.py`, in `flight_trajectories`, add
`"source"` to the empty-frame column list and `COALESCE(source, 'ADS-B') AS
source` to the `SELECT`:

```python
    if len(flight_ids) == 0:
        return pd.DataFrame(columns=[
            "flight_id", "event_time", "lat", "lon", "altitude_ft",
            "speed_kt", "roc_ft_min", "on_ground", "source"])
    sql = f"""
    SELECT track_id AS flight_id, event_time, lat, lon, altitude_ft,
           speed_kt, roc_ft_min, on_ground,
           -- NULL where the day was cached before the column existed, which
           -- union_by_name fills in. Those rows are ADS-B.
           COALESCE(source, 'ADS-B') AS source
    FROM {read.scan('trajectory', days)}
    WHERE track_id IN ({_id_list(flight_ids)})
    ORDER BY track_id, event_time
    """
```

- [ ] **Step 6: Draw one trace per (flight, source)**

In `src/opdi_benchmarker/app/pages/1_Flight.py`, beside `MAP_TRACK_WIDTH`:

```python
#: How each source is drawn, on both charts.
#:
#: ADS-B is the dense 5 s line the page has always shown. ADS-C arrives from a
#: satellite datalink at a median of ten minutes between reports, so a bare
#: line between consecutive samples would draw a great-circle segment the
#: aircraft was never measured on -- and draw it in the same ink as a measured
#: one. Markers say "these points"; the dotted line between them says
#: "interpolated, and widely".
SOURCE_STYLE = {
    "ADS-B": dict(mode="lines", dash="solid", width=MAP_TRACK_WIDTH,
                  marker_size=0),
    "ADS-C": dict(mode="lines+markers", dash="dot", width=3, marker_size=9),
}
DEFAULT_SOURCE = "ADS-B"
```

Replace the map's track loop with:

```python
st.subheader("Track")
fig = go.Figure()
for fid in flight_ids:
    t = traj_all[traj_all["flight_id"] == fid]
    if t.empty:
        continue
    for src in [s for s in SOURCE_STYLE if (t["source"] == s).any()]:
        part = t[t["source"] == src]
        style = SOURCE_STYLE[src]
        fig.add_trace(go.Scattermap(
            lat=part["lat"], lon=part["lon"], mode=style["mode"],
            line=dict(width=style["width"], color=colours[fid]),
            marker=dict(size=style["marker_size"], color=colours[fid]),
            opacity=0.85,
            name=f"{names.get(fid, fid)} track ({src})",
            hovertemplate=f"{names.get(fid, fid)} · {src}<br>"
                          "%{lat:.4f}, %{lon:.4f}<extra></extra>",
        ))
```

and the vertical-profile loop with:

```python
for fid in flight_ids:
    t = traj_all[traj_all["flight_id"] == fid]
    if t.empty:
        continue
    for src in [s for s in SOURCE_STYLE if (t["source"] == s).any()]:
        part = t[t["source"] == src]
        style = SOURCE_STYLE[src]
        prof.add_trace(go.Scatter(
            x=part["event_time"], y=part["altitude_ft"], mode=style["mode"],
            line=dict(width=2, color=colours[fid], dash=style["dash"]),
            marker=dict(size=style["marker_size"], color=colours[fid]),
            name=f"{names.get(fid, fid)} ({src})",
            hovertemplate=f"{names.get(fid, fid)} · {src}<br>%{{x|%H:%M:%S}}"
                          "<br>%{y:,.0f} ft<extra></extra>",
        ))
```

Note `go.Scattermap` takes `line.dash` in neither Plotly 5 nor 6 for the map
trace, which is why `dash` is applied on the profile only and the map
distinguishes the two by markers. Keep it that way rather than passing an
unsupported property.

After `st.plotly_chart(fig, width="stretch")`, add:

```python
if (traj_all["source"] == "ADS-C").any():
    n_adsc = int((traj_all["source"] == "ADS-C").sum())
    st.caption(
        f"**{n_adsc} of {len(traj_all)} samples are ADS-C** — satellite "
        "datalink reports, drawn as dots. They arrive at a median of about ten "
        "minutes apart against ADS-B's five seconds, so the dotted segments "
        "between them are interpolation across minutes, not a measured path. "
        "Almost all of them are above FL300 and over water."
    )
```

- [ ] **Step 7: Document how to point the benchmarker at the merged arm**

Add a section to `README.md` after "Worldwide coverage":

```markdown
## The ADS-C fusion arm

The `opdi` pipeline can merge OpenSky's ADS-C feed into the ADS-B
trajectories (`config.adsc.enabled`, off by default — see
`opdi/src/opdi/config.py::AdscConfig`). The two arms of that study live at
`s3a://eurocontrol/opdi/research/adsc/{base,merged}`, and this benchmarker
reads one of them with the two environment variables it already has — no new
configuration:

```bash
OPDI_WAREHOUSE=s3a://eurocontrol/opdi/research/adsc/merged \
OPDI_BENCHMARKER_CACHE=s3a://eurocontrol/opdi/research/adsc/benchmarker/merged \
  .venv/bin/opdi-benchmarker build --from 2026-06-05 --to 2026-06-07

OPDI_WAREHOUSE=s3a://eurocontrol/opdi/research/adsc/merged \
OPDI_BENCHMARKER_CACHE=s3a://eurocontrol/opdi/research/adsc/benchmarker/merged \
  PORT=8503 ./run_app.sh
```

**Point the cache somewhere else as well as the source.** The cache prefix is
what the app reads; leaving it at the default would write research trajectories
into the published European cache, where nothing marks them as belonging to a
different arm.

The Flight page draws ADS-C as dots on a dotted line and says how many samples
of the flight came from it. There are not many: ~5,000 rows a day inside the
European box, 97.6% of them above FL300, against hundreds of millions of ADS-B
rows. A flight that never left radar coverage will show none at all — that is
the expected case, not a build failure.
```

- [ ] **Step 8: Run the tests**

Run: `.venv/bin/python -m pytest tests/ -q`
Expected: the existing suite plus 6 new tests, all passing.

- [ ] **Step 9: Commit**

```bash
git add src/opdi_benchmarker tests README.md
git commit -m "feat(adsc): carry the signal source into the trajectory cache and draw it"
```

- [ ] **Step 10: Build the merged-arm cache and look at a flight**

```bash
set -a; . ../opdi/.env; set +a
OPDI_WAREHOUSE=s3a://eurocontrol/opdi/research/adsc/merged \
OPDI_BENCHMARKER_CACHE=s3a://eurocontrol/opdi/research/adsc/benchmarker/merged \
  .venv/bin/opdi-benchmarker build --from 2026-06-05 --to 2026-06-07
```

Then launch the app on port 8503 with the same two variables and find a North
Atlantic arrival — a flight whose ADEP is in North or Central America — on
2026-06-05. Confirm the dots appear west of Ireland and that the caption
reports a non-zero ADS-C count. **Screenshot it**; the paper's figure chapter
uses that screenshot.

```bash
git commit --allow-empty -m "run(adsc): merged-arm benchmarker cache built for 2026-06-05..07"
```

---

## Task 12: The paper's regeneration entrypoint

**Files:**
- Create: `opdi/benchmarks/regenerate_adsc_v1.py`
- Create: `opdi/tests/test_regenerate_adsc_v1.py`

**Interfaces:**
- Consumes: `adsc_census.py` (Task 1), `adsc_coverage_gain.py` (Task 9), `adsc_time_sync.py` (Task 9b), `adsc_events_compare.py` (Task 10), `provenance`.
- Produces: `jobs() -> list[Job]`, `PAPER`, `DATA`, `main()` with `--check` / `--force` / `--only` / `--allow-stale`. Task 13's `.qmd` invokes it.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_regenerate_adsc_v1.py
"""The ADS-C paper's entrypoint: every staged CSV has exactly one job."""

import os
from pathlib import Path

import pytest

import regenerate_adsc_v1 as R


def test_every_job_declares_its_outputs_and_dependencies():
    for job in R.jobs():
        assert job.outputs, f"{job.name} stages nothing"
        assert job.code_paths, f"{job.name} declares no dependencies"
        for p in job.code_paths:
            assert (Path(R.REPO) / p).is_file(), f"{job.name}: missing {p}"


def test_job_names_are_unique():
    names = [j.name for j in R.jobs()]
    assert len(names) == len(set(names))


def test_no_two_jobs_stage_the_same_file():
    # The property the whole provenance scheme rests on: one output, one job,
    # one command. Two jobs writing one CSV makes its manifest entry a lie
    # about which run produced it.
    staged = [s for j in R.jobs() for s in j.outputs.values()]
    assert len(staged) == len(set(staged))


def test_the_arm_build_is_declared_as_a_prerequisite_not_a_job():
    # run_adsc_day.py costs ~6 cluster-hours for three days and two arms. It is
    # deliberately not a job here -- but it must be named, or a reader
    # concludes that `--force` reproduces the paper from nothing.
    assert "run_adsc_day" in R.__doc__


def test_every_job_depends_on_the_code_that_can_change_its_answer():
    # A job whose dependency set omits the mapping, the merge or the cleaner
    # would read as current after a change that invalidates it, and the page
    # would render stale numbers with a clean provenance table.
    core = {
        "src/opdi/ingestion/osn_adsc.py",
        "src/opdi/pipeline/tracks.py",
        "src/opdi/cleaning/native.py",
    }
    for job in R.jobs():
        if job.name == "census":
            continue  # reads the raw topic; no pipeline code can change it
        assert core <= set(job.code_paths), job.name


def test_paper_dir_is_overridable():
    # Inside a git worktree the sibling walk lands somewhere that does not
    # exist, every output reads as missing, and the staleness report is a
    # confident lie.
    assert "OPDI_PAPER_DIR" in Path(R.__file__).read_text()


def test_check_needs_no_credentials(monkeypatch, capsys):
    monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
    monkeypatch.delenv("AWS_SECRET_ACCESS_KEY", raising=False)
    monkeypatch.setattr("sys.argv", ["regenerate_adsc_v1.py", "--check",
                                     "--allow-stale"])
    R.main()
    assert "analysis at" in capsys.readouterr().out
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv310/bin/python -m pytest tests/test_regenerate_adsc_v1.py -q`
Expected: FAIL with `ModuleNotFoundError: No module named 'regenerate_adsc_v1'`

- [ ] **Step 3: Write the entrypoint**

Copy `benchmarks/regenerate_track_v3.py` to
`benchmarks/regenerate_adsc_v1.py` and replace its docstring, `PAPER`, `SEG`
and `jobs()` with:

```python
"""
The executable definition of the ADS-C fusion V1 analysis.

Every number in ``papers/adsc-fusion-v1/`` is produced by exactly one job
listed here, with exactly the arguments listed here. The report renders by
calling this module, so the numbers on the page are the numbers this code
produces.

Each job declares the source files it depends on. An output is **stale** when
the fingerprint over those files differs from the one recorded when the output
was written, so editing ``osn_adsc.py`` marks every pipeline job for re-run
while editing this docstring marks nothing. Age is not staleness.

    python benchmarks/regenerate_adsc_v1.py --check     # what is stale
    python benchmarks/regenerate_adsc_v1.py             # run only what is stale
    python benchmarks/regenerate_adsc_v1.py --force     # run everything

``--check`` needs no credentials and no cluster.

**What the study asks.** Whether merging OpenSky's ADS-C feed into the ADS-B
trajectories improves flight-event extraction. Three jobs, in the order the
question has to be answered:

  * ``census`` -- what the feed contains, over eleven days spread across the
    archive: rows, in-box rows, cadence, altitude bands, and the fill rate of
    every column. The paper's framing rests on this and on nothing else.
  * ``gain_2026`` -- what the merge did to the trajectories on 2026-06-05/06/07:
    samples and track-hours added, reception holes closed, and the
    merge-neutrality guard (tracks holding two real callsigns, i.e. two flights
    fused).
  * ``time_sync_2026`` -- how far apart the two clocks are. For each ADS-C
    report bracketed by ADS-B within 60 s, the offset is decomposed into
    along-track and cross-track; cross-track is the position floor and
    along-track over ground speed is the implied time offset. Reported per
    airframe, because that is where the finding is.
  * ``events_2026`` -- what it did to the milestones, both arms scored against
    APDF 202606 over the twenty flight-events-v4 study aerodromes, per
    milestone and per aerodrome.

**The pipeline build is a prerequisite, not a job.** ``gain_2026`` and
``events_2026`` read the two arms at
``s3a://eurocontrol/opdi/research/adsc/{base,merged}``, built by
``benchmarks/run_adsc_day.py --day <d> --arm both`` for each of the three days
-- roughly six cluster-hours in total. It is not a job here for the same reason
the A9 study's day run was not: a staleness check cannot key on it (it writes
tables, not files under ``data/``), and re-running it would not be a no-op when
current. If a job below reports table contents that disagree with the paper,
rebuild the arms first.

**What this module does not measure.** Nothing is scored outside the European
box or outside June 2026, and no sweep of the shadow half-windows was run. Both are
stated in the paper's limitations chapter rather than implied by their absence.
"""
```

```python
PAPER = Path(
    os.environ.get(
        "OPDI_PAPER_DIR",
        REPO.parent / "opdi-portal" / "papers" / "adsc-fusion-v1",
    )
)
DATA = PAPER / "data"

#: The code that decides what ADS-C contributes: the mapping and shadow filter,
#: the merge that puts it in front of segmentation, and the cleaning exemption
#: without which every ADS-C value is NULLed. A job that reads a pipeline table
#: depends on all three, because a change to any one changes its answer.
CORE = [
    "src/opdi/ingestion/osn_adsc.py",
    "src/opdi/pipeline/tracks.py",
    "src/opdi/cleaning/native.py",
]

#: The days both arms were built for. The flight-events-v4 period.
DAYS = ["2026-06-05", "2026-06-06", "2026-06-07"]

#: One day per quarter plus the three study days plus the two gap days the
#: archive listing showed, so "the extra columns are always empty" is a claim
#: about the archive and not about one Sunday in June.
CENSUS_DAYS = [
    "2023-09-01", "2024-03-01", "2024-09-02", "2025-03-01", "2025-09-01",
    "2026-03-22", "2026-06-01", "2026-06-05", "2026-06-06", "2026-06-07",
    "2026-10-15",
]


def jobs() -> list:
    return [
        Job(
            name="census",
            script="benchmarks/adsc_census.py",
            args=["--days", *CENSUS_DAYS, "--out-name", "adsc_census.csv"],
            outputs={"adsc_census.csv": "adsc_census.csv"},
            code_paths=[],
            notes="What the raw ADS-C topic contains, eleven days across the "
                  "archive: rows, in-box rows, airframes, median cadence, "
                  "altitude bands, exact duplicates, and the fill rate of "
                  "every column. Reads the topic directly -- no pipeline code "
                  "can change this answer, which is why it depends on none.",
        ),
        Job(
            name="gain_2026",
            script="benchmarks/adsc_coverage_gain.py",
            args=["--days", *DAYS,
                  "--gain-name", "adsc_gain_2026.csv",
                  "--holes-name", "adsc_holes_2026.csv"],
            outputs={"adsc_gain_2026.csv": "adsc_gain_2026.csv",
                     "adsc_holes_2026.csv": "adsc_holes_2026.csv"},
            code_paths=CORE,
            notes="Rows, tracks, track-hours and segments per arm; the "
                  "reception holes ADS-C closed; the two-callsign "
                  "merge-neutrality guard. Reads both arms at "
                  "research/adsc/{base,merged}, built by run_adsc_day.py.",
        ),
        Job(
            name="time_sync_2026",
            script="benchmarks/adsc_time_sync.py",
            args=["--days", *DAYS,
                  "--pairs-name", "adsc_time_sync_pairs_2026.csv",
                  "--out-name", "adsc_time_sync_2026.csv"],
            outputs={"adsc_time_sync_pairs_2026.csv":
                         "adsc_time_sync_pairs_2026.csv",
                     "adsc_time_sync_2026.csv": "adsc_time_sync_2026.csv"},
            code_paths=CORE,
            notes="Along/cross decomposition of each ADS-C report against "
                  "ADS-B interpolated to the same instant, per airframe. "
                  "Reads the shared inputs at research/adsc/input, so it does "
                  "not depend on either arm having been built.",
        ),
        Job(
            name="events_2026",
            script="benchmarks/adsc_events_compare.py",
            args=["--out-name", "adsc_events_2026.csv",
                  "--by-airport-name", "adsc_events_by_airport_2026.csv"],
            outputs={"adsc_events_2026.csv": "adsc_events_2026.csv",
                     "adsc_events_by_airport_2026.csv":
                         "adsc_events_by_airport_2026.csv"},
            code_paths=CORE + [
                "benchmarks/events_gt.py",
                "benchmarks/events_score.py",
                "benchmarks/event_bench.py",
                "src/opdi/pipeline/events.py",
            ],
            notes="Both arms against APDF 202606 over the twenty V4 study "
                  "aerodromes, through one truth frame: per milestone and per "
                  "aerodrome.",
        ),
    ]
```

Also change the temp-directory prefix in `Job.run` from `tcv3_` to `adscv1_`,
and the `--results-dir` flag stays as it is — all three scripts take it.

- [ ] **Step 4: Run the tests**

Run: `.venv310/bin/python -m pytest tests/test_regenerate_adsc_v1.py -q`
Expected: PASS (7 tests)

- [ ] **Step 5: Check staleness reporting works with no data staged**

Run: `.venv310/bin/python benchmarks/regenerate_adsc_v1.py --check --allow-stale`
Expected: four `STALE` lines naming the seven missing CSVs, exit 0.

- [ ] **Step 6: Commit**

```bash
git add benchmarks/regenerate_adsc_v1.py tests/test_regenerate_adsc_v1.py
git commit -m "feat(adsc): regeneration entrypoint for the fusion paper"
```

- [ ] **Step 7: Regenerate for real**

```bash
export PYSPARK_DRIVER_PYTHON=$PWD/.venv310/bin/python
unset PYSPARK_PYTHON OPDI_WAREHOUSE
mkdir -p ../opdi-portal/papers/adsc-fusion-v1/data
.venv310/bin/python -u benchmarks/regenerate_adsc_v1.py
```

Expected: four jobs run, seven CSVs staged under
`../opdi-portal/papers/adsc-fusion-v1/data/`, each with a `_manifest.json`
entry naming the script, argv, git SHA and dirty flag. Re-running must then
report all four `current` and do nothing.

---

## Task 13: The paper

**Files:**
- Create: `opdi-portal/papers/adsc-fusion-v1/index.qmd`
- Modify: `opdi-portal/papers/index.qmd`
- Modify: `opdi-workspace/CLAUDE.md`

**Interfaces:**
- Consumes: the five staged CSVs and `_manifest.json` from Task 12; `benchmarks/regenerate_adsc_v1.py` as the render-time entrypoint.
- Produces: `papers/adsc-fusion-v1/index.qmd` and its PDF; a publication card in `papers/index.qmd`.

- [ ] **Step 1: Write the header and the regeneration chunk**

Create `papers/adsc-fusion-v1/index.qmd`:

````markdown
---
title: "ADS-C in the OPDI trajectory"
subtitle: "Version 1 — what a satellite cruise feed adds to flight-event extraction"
author: "EUROCONTROL Performance Review Unit — Open Performance Data Initiative"
date: last-modified
abstract: |
  OPDI builds every track from ADS-B state vectors. OpenSky also publishes an
  ADS-C feed, and the obvious hypothesis is that merging the two improves
  coverage and therefore milestone extraction. This paper tests it. ADS-C is
  merged into the state-vector frame *before* segmentation, so `track_id` sees
  both sources; the trajectory cleaner is made source-aware, because its
  twenty-second isolated-point rule would otherwise delete a feed whose median
  cadence is ten minutes. Both arms — ADS-B only, and ADS-B plus ADS-C — are
  rebuilt from the same state vectors with the same segmentation and the same
  detectors, and scored against EUROCONTROL APDF over the twenty aerodromes
  and three days of flight-events-v4.

  The feed is narrow and the result is correspondingly narrow. ADS-C delivers
  about five thousand rows a day inside OPDI's European box, from roughly nine
  hundred airframes, at a median of six hundred seconds between reports;
  97.6% of those rows are above FL300 and three quarters sit west of 10°W. It
  carries no `on_ground` flag and almost nothing below ten thousand feet. The
  A-CDM runway family therefore cannot move, and the paper reports that as a
  measured null rather than omitting it. Where change is physically possible —
  the ends of a track, the oceanic reception holes, and the flight list that
  depends on both — the measured effect is in the chapters below.

  Every figure is produced by `benchmarks/regenerate_adsc_v1.py`. Where an
  output is not staged the table says so in place rather than being omitted.
format:
  html:
    toc: true
    toc-depth: 2
  pdf:
    documentclass: scrreprt
    toc: true
    toc-depth: 2
---

```{r regenerate}
#| include: false
#| cache: false

# Rendering this page runs the analysis behind it.
#
# `regenerate_adsc_v1.py` is the executable definition of the study: each CSV
# read below is produced by exactly one job it declares, with exactly the
# arguments it declares. It re-runs a job when the *code that job depends on*
# has changed since the output was written -- not when the file is old, which
# says nothing. A render with everything current is a fast no-op; a render
# after editing `osn_adsc.py`, `tracks.py` or `cleaning/native.py` re-runs the
# pipeline jobs before the first table is drawn.
#
# OPDI_RENDER selects what happens instead:
#
#   run          (default) regenerate anything stale, then render
#   check        fail fast naming every stale job, regenerate nothing
#   allow-stale  render a draft from whatever is staged, after naming staleness
#   skip         do not invoke the entrypoint at all
#
# One thing this entrypoint cannot regenerate: the two pipeline arms at
# s3a://eurocontrol/opdi/research/adsc/{base,merged}, built by
# benchmarks/run_adsc_day.py at roughly six cluster-hours for three days. The
# jobs read those tables; if they have not been built, the jobs fail rather
# than reporting a zero. See [Reproducing this](#sec-reproduce).
REPO   <- Sys.getenv("OPDI_REPO_DIR",
                     normalizePath(file.path(getwd(), "..", "..", "..", "opdi"),
                                   mustWork = FALSE))
PYTHON <- Sys.getenv("OPDI_PYTHON", file.path(REPO, ".venv310", "bin", "python"))
MODE   <- Sys.getenv("OPDI_RENDER", "run")

regen <- file.path(REPO, "benchmarks", "regenerate_adsc_v1.py")
if (!file.exists(regen) && MODE != "skip") {
  stop("regenerate_adsc_v1.py not found under REPO=", REPO,
       " -- set OPDI_REPO_DIR, or OPDI_RENDER=skip to render without it.")
}

if (MODE != "skip" && file.exists(regen)) {
  flags <- switch(MODE,
                  "run" = character(0),
                  "check" = "--check",
                  "allow-stale" = c("--check", "--allow-stale"),
                  character(0))
  status <- system2(PYTHON, c(shQuote(regen), flags), stdout = "", stderr = "")
  if (!identical(status, 0L)) {
    stop("regenerate_adsc_v1.py exited ", status,
         " -- the figures below would not match the code that is checked out. ",
         "Set OPDI_RENDER=allow-stale to render anyway.")
  }
}
```

```{r setup}
#| include: false
library(knitr)

# Read-only. Every figure and table on this page comes from a committed CSV.
# Nothing here reads S3, a database or a Spark cluster.
D <- function(f) {
  p <- file.path("data", f)
  if (!file.exists(p)) return(NULL)
  read.csv(p, stringsAsFactors = FALSE)
}

census <- D("adsc_census.csv")                      # one row per sampled day
gain   <- D("adsc_gain_2026.csv")                   # one row per arm + extension
holes  <- D("adsc_holes_2026.csv")                  # one row per bridged hole
sync   <- D("adsc_time_sync_2026.csv")              # one row per airframe + ALL
syncp  <- D("adsc_time_sync_pairs_2026.csv")        # one row per bracketed pair
ev     <- D("adsc_events_2026.csv")                 # one row per (arm, milestone)
evap   <- D("adsc_events_by_airport_2026.csv")      # + per aerodrome

# A missing file is reported in place, never silently skipped: a chapter whose
# table vanished reads as a chapter with nothing to say.
missing_note <- function(name) {
  sprintf(paste("*`%s` is not staged. Run",
                "`python benchmarks/regenerate_adsc_v1.py` with cluster",
                "credentials; the [Provenance](#sec-provenance) chapter lists",
                "what is present.*"), name)
}

# One accessor per file, each stopping rather than returning a zero-length
# value: a missing row would otherwise reach a table cell as a blank and read
# as a measured zero.
GAIN <- function(arm, col) {
  v <- gain[gain$arm == arm, col]
  if (length(v) != 1) stop("no single gain row for ", arm, "/", col)
  v
}
CEN <- function(day, col) {
  v <- census[census$day == day, col]
  if (length(v) != 1) stop("no single census row for ", day, "/", col)
  v
}
SYNC <- function(icao24, col) {
  v <- sync[sync$icao24 == icao24, col]
  if (length(v) != 1) stop("no single time-sync row for ", icao24, "/", col)
  v
}
EV <- function(arm, milestone, col) {
  v <- ev[ev$arm == arm & ev$milestone == milestone, col]
  if (length(v) != 1) stop("no single event row for ", arm, "/", milestone)
  v
}

num   <- function(x) formatC(x, format = "d", big.mark = ",")
pct   <- function(x) sprintf("%.1f%%", x)
p1    <- function(x) sprintf("%.1f", x)
signed <- function(x) sprintf("%+.1f", x)

# Configuration, not measurement. Named here so the prose and the parameter
# table read one source -- `AdscConfig` in `src/opdi/config.py`.
SHADOW_BEFORE_S <- 60
SHADOW_AFTER_S  <- 60
TIME_OFFSET_S   <- 0.0
HOLE_SECONDS    <- 300
ISOLATED_GAP_S  <- 20
STUDY_DAYS      <- c("2026-06-05", "2026-06-06", "2026-06-07")
```
````

- [ ] **Step 2: Write the source chapter**

Append:

````markdown
# What the feed is

```{r census-table}
#| echo: false
if (is.null(census)) {
  cat(missing_note("adsc_census.csv"))
} else {
  cols <- c("day", "hour_partitions", "rows", "rows_in_box",
            "airframes_in_box", "median_cadence_s", "in_box_above_fl300",
            "in_box_below_10000ft", "in_box_west_of_10w", "in_box_on_ground",
            "exact_duplicate_rows")
  kable(census[, cols], row.names = FALSE,
        col.names = c("Day", "Hours read", "Rows", "In box",
                      "Airframes in box", "Median cadence (s)", "> FL300",
                      "< 10,000 ft", "West of 10°W", "On ground",
                      "Exact duplicates"),
        caption = paste("The raw ADS-C topic, by sampled day. Every band after",
                        "`In box` is counted **inside** OPDI's published",
                        "European coverage box -- that is the population the",
                        "pipeline would ingest. `Hours read` matters: a day",
                        "with 6 of 24 partitions present has a cadence that",
                        "reflects the gap, not the feed."))
}
```

```{r census-fill}
#| echo: false
if (is.null(census)) {
  cat(missing_note("adsc_census.csv"))
} else {
  fill_cols <- grep("^fill_", names(census), value = TRUE)
  worst <- data.frame(
    column = sub("^fill_", "", fill_cols),
    max_fill = sapply(fill_cols, function(c) max(census[[c]], na.rm = TRUE)))
  worst <- worst[order(-worst$max_fill), ]
  kable(worst, row.names = FALSE,
        col.names = c("Column", "Best fill rate across all sampled days (%)"),
        caption = paste("Every column of the feed, by the best fill rate it",
                        "reached on any sampled day. A column at 0.0 here was",
                        "empty on every day sampled -- which is the evidence",
                        "for not carrying it."))
}
```

Write the prose of this chapter from those two tables. It must state, with the
numbers beside each claim:

1. How many rows a day the feed delivers worldwide and inside the box, and how
   many airframes that is.
2. The median cadence, **against ADS-B's 5 s ingested resolution**, as a ratio.
3. That the feed is a cruise feed: the share above FL300, the share below
   10,000 ft, the share west of 10°W, and that `on_ground` is never true.
4. Which columns are populated and which are empty on every day sampled —
   naming `ias`, `tas`, `mach`, `oat`/`tat`, `wd`/`ws`, `nav_altitude_mcp`,
   `registration`, `ac_type` and `alt_geom` explicitly, because each of them is
   a field a reader would reasonably expect ADS-C to carry and would otherwise
   assume OPDI discarded.
5. That `nic`, `rc`, `seen`, `seen_pos` and `rssi` are constant placeholders
   rather than measurements, and are therefore not carried as if they were.
6. That the archive runs from 2023-07-07 with 24 gaps longer than a day, and
   that the feed averages 22.3 of 24 hourly partitions a day in 2026 — so a
   missing hour is normal and the ingest tolerates it by design.
````

- [ ] **Step 3: Write the method chapter**

Append:

````markdown
# How it is merged

| Decision | Value | Where |
|---|---|---|
| Merge point | Before segmentation | `TrackProcessor._merge_adsc` |
| Default | Off (`adsc.enabled = False`) | `AdscConfig` |
| ADS-C callsign | `NULL` | `osn_adsc.to_opdi_schema` |
| Shadow window | −`r SHADOW_BEFORE_S` s … +`r SHADOW_AFTER_S` s | `AdscConfig.shadow_{before,after}_seconds` |
| Global time correction | `r TIME_OFFSET_S` s (off) | `AdscConfig.time_offset_seconds` |
| Thinning applied to ADS-C | None | `AdscIngestion.ingest_from_s3` |
| Isolated-point rule | Exempt for ADS-C | `CleaningConfig.sparse_sources` |
| Stale-broadcast rule | Exempt for ADS-C | `CleaningConfig.sparse_sources` |
| Dedup tie-break | ADS-B wins | `drop_duplicate_statevectors` |
| Segment split | Unchanged, `r HOLE_SECONDS` s | `CleaningConfig.segment_gap_seconds` |

Write the prose from that table. Four things must be argued, not asserted:

1. **Why before segmentation.** The hypothesis is about coverage and track
   continuity, both of which are properties of `track_id`; merging afterwards
   could only attach ADS-C to tracks ADS-B had already drawn.
2. **Why the callsign is dropped.** The feed's `flight` field carries the IATA
   number (`LH133`) and ADS-B broadcasts the ICAO callsign (`DLH133`). The
   production A9 arm breaks a track when the last non-blank callsign changes,
   so carrying it would split exactly the tracks the merge exists to join. A9
   bridges NULLs with `F.last(..., ignorenulls=True)`, so NULL removes a false
   signal rather than losing a true one. Say plainly that an IATA→ICAO mapping
   would recover the field and was not attempted here.
3. **Why the cleaner had to change.** `isolated_max_gap_seconds` is
   `r ISOLATED_GAP_S` s and the feed's median cadence is six hundred. State
   that without the exemption step 02a NULLs the position, altitude and speed
   of **every** ADS-C sample — so an unexempted run would have measured a
   null result for a reason having nothing to do with the hypothesis.
4. **Why `track_id` changes, and what that costs.** This is the fourth
   `track_id` boundary after legacy → A8 → A9, it is silent to a joiner (an
   empty join, not an error), and it is confined to the research warehouse
   precisely because of that. Nothing published moves.
````

- [ ] **Step 4: Write the trajectory-effect chapter**

Append:

````markdown
# What the merge did to the trajectories

```{r gain-table}
#| echo: false
if (is.null(gain)) {
  cat(missing_note("adsc_gain_2026.csv"))
} else {
  arms <- gain[gain$arm %in% c("base", "merged"), ]
  cols <- c("arm", "rows", "rows_adsc", "tracks", "tracks_with_adsc",
            "segments", "track_hours", "median_samples_per_track",
            "tracks_with_two_callsigns")
  kable(arms[, cols], row.names = FALSE,
        col.names = c("Arm", "Rows", "ADS-C rows", "Tracks",
                      "Tracks with ADS-C", "Segments", "Track hours",
                      "Median samples/track", "Two-callsign tracks"),
        caption = paste("Both arms over", paste(STUDY_DAYS, collapse = ", "),
                        "after cleaning (`osn_tracks_clean`)."))
}
```

```{r extension-table}
#| echo: false
if (is.null(gain) || !("extension" %in% gain$arm)) {
  cat(missing_note("adsc_gain_2026.csv (extension row)"))
} else {
  e <- gain[gain$arm == "extension", ]
  kable(e[, c("airframe_days", "first_shift_median_s", "last_shift_median_s",
              "seen_earlier", "seen_later")],
        row.names = FALSE,
        col.names = c("Airframe-days matched", "Median first-sample shift (s)",
                      "Median last-sample shift (s)", "Seen earlier",
                      "Seen later"),
        caption = paste("Track ends, merged against baseline, keyed on",
                        "`icao24` and day -- `track_id` cannot be the key",
                        "because the merge changes it."))
}
```

```{r holes-table}
#| echo: false
if (is.null(holes)) {
  cat(missing_note("adsc_holes_2026.csv"))
} else if (nrow(holes) == 0) {
  cat(paste("**No reception hole was closed.** Every surviving ADS-C sample",
            "sat within", HOLE_SECONDS, "s of an ADS-B sample on at least one",
            "side, so none of them added coverage the ADS-B feed did not",
            "already have. This is a result, not a missing table."))
} else {
  g <- pmin(holes$gap_before_s, holes$gap_after_s) / 60
  br <- c(0, 5, 10, 20, 60, Inf)
  tab <- table(cut(g, breaks = br, right = FALSE))
  kable(data.frame(band = names(tab), holes = as.integer(tab)),
        row.names = FALSE,
        col.names = c("Smaller side of the hole (minutes)", "ADS-C samples"),
        caption = paste(num(nrow(holes)), "ADS-C samples sat more than",
                        HOLE_SECONDS, "s from an ADS-B sample on both sides.",
                        "An infinite side means the sample extended the track",
                        "past its last ADS-B report."))
}
```

Write the prose from those three tables. It must answer, in order:

1. **Did anything arrive?** ADS-C rows as an absolute number and as a share of
   all rows; tracks carrying at least one, as a number and as a share.
2. **Did track count fall?** Fewer tracks for more rows is gap bridging. More
   tracks would mean the merge is splitting — if that is what the table shows,
   say so and stop drawing conclusions from the rest.
3. **Did it arrive where coverage was missing?** The hole distribution, and the
   count of samples that extended a track past its last ADS-B report.
4. **Did it do harm?** The two-callsign count in both arms. If merged exceeds
   base, name the number and say that the merge is fusing flights.
5. The honest summary sentence: what share of *flights* in the study period saw
   any ADS-C at all.
````

- [ ] **Step 5b: Write the time-synchronisation chapter**

Append, **before** the flight-events chapter — the milestone result cannot be
read without it:

````markdown
# Whether the two clocks agree

ADS-B and ADS-C arrive over different links and neither states a position age
that can be used. ADS-B's `last_pos_update` is real but small. ADS-C's `seen`
and `seen_pos` are constant zero — the feed *asserts* that every report is
current — and the readsb record does not expose the ADS-C report's own
time-of-position, so there is nothing in band to correct from. The skew was
therefore measured.

For each ADS-C report with an ADS-B sample of the same airframe within
`r MAX_GAP_S` s on **both** sides, the ADS-B position is interpolated to the
ADS-C timestamp and the offset decomposed against the direction the ADS-B pair
actually moved. **Cross-track** is what the two feeds disagree about that no
timing explanation can absorb; **along-track divided by ground speed** is the
implied time offset. A position error is isotropic and lands in both; a timing
error lands only along track.

```{r sync-overall}
#| echo: false
if (is.null(sync)) {
  cat(missing_note("adsc_time_sync_2026.csv"))
} else {
  a <- sync[sync$icao24 == "ALL", ]
  kable(data.frame(
    quantity = c("Bracketed pairs", "Median |cross-track|",
                 "Median |along-track|", "Along:cross ratio",
                 "Median implied offset", "sigma of implied offset"),
    value = c(num(a$n), paste0(p1(a$median_abs_cross_m), " m"),
              paste0(p1(a$median_abs_along_m), " m"),
              paste0(p1(a$median_abs_along_m /
                          max(a$median_abs_cross_m, 1e-9)), "x"),
              paste0(signed(a$median_implied_dt_s), " s"),
              paste0(p1(a$std_implied_dt_s), " s"))),
    row.names = FALSE, col.names = c("", "Measured"),
    caption = paste("ADS-C against interpolated ADS-B over",
                    paste(STUDY_DAYS, collapse = ", "), "."))
}
```

```{r sync-alternative}
#| echo: false
if (is.null(syncp)) {
  cat(missing_note("adsc_time_sync_pairs_2026.csv"))
} else {
  # The alternative explanation, checkable rather than argued away: the OSN
  # archive carries positions forward, which would bias the result in exactly
  # the direction measured.
  ok <- !is.na(syncp$adsb_pos_age_s) & !is.na(syncp$implied_dt_s)
  kable(data.frame(
    quantity = c("Median ADS-B position age", "Max ADS-B position age",
                 "corr(implied offset, ADS-B position age)"),
    value = c(paste0(p1(median(syncp$adsb_pos_age_s[ok])), " s"),
              paste0(p1(max(syncp$adsb_pos_age_s[ok])), " s"),
              sprintf("%.2f", cor(syncp$implied_dt_s[ok],
                                  syncp$adsb_pos_age_s[ok])))),
    row.names = FALSE, col.names = c("", "Measured"),
    caption = paste("Could the offset be ADS-B's own staleness rather than",
                    "ADS-C's skew? The archive carries positions forward, so",
                    "the question has to be answered from the data."))
}
```

```{r sync-per-airframe}
#| echo: false
if (is.null(sync)) {
  cat(missing_note("adsc_time_sync_2026.csv"))
} else {
  pa <- sync[sync$icao24 != "ALL", ]
  pa <- pa[order(pa$median_implied_dt_s), ]
  kable(pa[, c("icao24", "n", "median_implied_dt_s", "std_implied_dt_s",
               "median_abs_cross_m")],
        row.names = FALSE, digits = 2,
        col.names = c("Airframe", "Pairs", "Median implied offset (s)",
                      "sigma (s)", "Median |cross| (m)"),
        caption = paste("Per airframe. The question this table decides:",
                        "is the skew a property of the feed, which one",
                        "constant could correct, or of the individual",
                        "aircraft, which it could not?"))
}
```

Write the prose of this chapter from those three tables. It must settle four
things, in order, and each conclusion must be refutable by a number on the page:

1. **Is it timing or position?** The along-to-cross ratio. State that a
   position error is isotropic, so a large ratio cannot be one.
2. **Is it ADS-B's carry-forward?** The position-age table. The prototype
   measured a median age of 0.22 s, a maximum of 0.92 s and a correlation of
   0.23 — too small to explain an offset of a few hundred metres.
3. **Is it one number or many?** Compare within-airframe sigma against the
   spread of per-airframe medians. The prototype found sigma 0.10–0.31 s within
   airframes against a +0.01 s to +2.09 s spread between them, plus one
   airframe persistently at −12.9 s. If that holds, say plainly that
   `time_offset_seconds` ships at zero **because** a constant would damage the
   aircraft that are already synchronised, and that a per-airframe correction
   is the real fix and is not attempted here.
4. **Does it matter?** Convert the offset to metres at cruise speed, and
   compare it against the alternative — a straight line interpolated across a
   ten-minute reception hole, whose own error is tens of kilometres. Say which
   is larger. If the skew is small against the alternative, the honest
   conclusion is that it is a correction worth reporting and not a reason to
   withhold the merge; say so in those words rather than leaving the reader to
   infer it.

Also state what the skew does **not** reach: the derivative-spike filter.
`latlon_d1_max_deg_s` is 0.01 deg/s, about 1,100 m/s of apparent ground speed;
a few hundred metres of displacement across a 600 s gap is three orders of
magnitude below that. The filter cannot see this error, so it neither corrects
it nor is damaged by it — which is also why no source guard was added to it.
````

Add to the `setup` chunk, beside the other configuration constants:

```r
MAX_GAP_S <- 60   # adsc_time_sync.py --max-gap-s
```

- [ ] **Step 5c: Write the flight-events chapter**

Append:

````markdown
# What it did to the flight events

```{r events-table}
#| echo: false
if (is.null(ev)) {
  cat(missing_note("adsc_events_2026.csv"))
} else {
  wide <- merge(
    ev[ev$arm == "base", c("milestone", "n_truth", "n_detected", "bias_s",
                           "mad_s")],
    ev[ev$arm == "merged", c("milestone", "n_detected", "bias_s", "mad_s")],
    by = "milestone", suffixes = c("_base", "_merged"))
  wide$delta_detected <- wide$n_detected_merged - wide$n_detected_base
  kable(wide, row.names = FALSE, caption = paste(
    "Both arms against APDF 202606, twenty study aerodromes,",
    paste(STUDY_DAYS, collapse = ", "),
    ". `n_truth` is shared: one truth frame scores both arms."))
}
```

```{r events-by-airport}
#| echo: false
if (is.null(evap)) {
  cat(missing_note("adsc_events_by_airport_2026.csv"))
} else {
  b <- evap[evap$arm == "base", ]
  m <- evap[evap$arm == "merged", ]
  j <- merge(b, m, by = c("airport", "milestone"), suffixes = c("_b", "_m"))
  j$delta <- j$n_detected_m - j$n_detected_b
  moved <- j[j$delta != 0, c("airport", "milestone", "n_detected_b",
                             "n_detected_m", "delta")]
  if (nrow(moved) == 0) {
    cat(paste("**No (aerodrome, milestone) cell changed.** Across twenty",
              "aerodromes and every scored milestone, the two arms detected",
              "the same movements. Read this beside the trajectory chapter:",
              "it is a null result about *milestones*, not about coverage."))
  } else {
    kable(moved[order(-abs(moved$delta)), ], row.names = FALSE,
          col.names = c("Aerodrome", "Milestone", "Base", "Merged", "Delta"),
          caption = "Every (aerodrome, milestone) cell where the arms differ.")
  }
}
```

Write the prose from those two tables. It must state:

1. The headline: whether any milestone's detection count or error distribution
   moved, and by how much.
2. **Why the A-CDM runway family could not move**, with the numbers from the
   source chapter: no `on_ground`, 48 in-box rows a day below 10,000 ft, a
   600 s cadence against detectors that interpolate a threshold crossing
   between adjacent samples. A reader must not be able to mistake this null for
   a detector failure.
3. Where a change was physically possible — `first_seen`, `last_seen`, the FL
   crossings, and ADEP/ADES through the flight list — and what the tables show
   there.
3b. For any event that *did* move, whether it could have been interpolated
   across an ADS-B/ADS-C pair, and therefore whether it inherits the clock
   offset from the previous chapter. Nothing in the pipeline prevents that
   interpolation (`segment_id` is read by no detector), so a moved event is not
   automatically a better event.
4. If nothing moved anywhere: say that the hypothesis as stated is **not
   supported for this period and this coverage box**, and distinguish that
   from "ADS-C is useless" — the trajectory chapter's hole count is the
   coverage result and it stands on its own.
````

- [ ] **Step 6: Write the limitations, reproduction and provenance chapters**

Append:

````markdown
# What this does not measure {#sec-limits}

Each of these is a measurement that was not made, not a result:

- **Only Europe.** Scoring is inside `EUROPE_BBOX`, and three quarters of the
  in-box ADS-C rows sit in its Atlantic corner. The feed is global; a worldwide
  arm would see far more of it, and APDF could not score it.
- **Only June 2026, three days.** The flight-events-v4 period, chosen for
  comparability. ADS-C volumes in 2023–2024 are not scored.
- **No shadow-window sweep.** −`r SHADOW_BEFORE_S`/+`r SHADOW_AFTER_S` s is a
  reasoned default, not a tuned one. A wider window drops more ADS-C; a
  narrower one admits near-duplicate positions.
- **No per-airframe time correction, and no stability study of the skew.**
  The time-synchronisation chapter measures the offset on one day and 25
  airframes. Whether an airframe's offset is stable across days -- the
  precondition for correcting it -- is not measured here.
- **Nothing stops an event being interpolated across a source boundary.**
  `segment_id` splits at 300 s coverage holes and is read by no detector
  (`grep -rn segment_id src/opdi/pipeline/` finds two comments). An event
  derived between an ADS-B and an ADS-C sample therefore inherits the
  along-track displacement. This is pre-existing -- the same detectors
  already interpolate across 30-minute ADS-B holes -- and making
  `segment_id` load-bearing would change published detector behaviour, so
  it is named here as a follow-up rather than changed on this branch.
- **No IATA→ICAO callsign recovery.** The feed's `flight` field is discarded
  rather than mapped, so ADS-C contributes no callsign evidence to segmentation
  or to the flight list.
- **The MLAT topic is untouched.** `readsb.aircraft.mlat.avro` is a second
  unused OpenSky source, denser than ADS-C (374 objects a day) but starting
  only at 2026-05-18. It is multilateration, not ADS-C, and is out of scope.
- **No measurement of ADS-C positional accuracy.** `nic` and `rc` are constant
  zero in the feed, so the usual integrity containment bounds are unavailable;
  nothing here establishes how accurate an ADS-C position is, only what
  including it changes.

# Reproducing this {#sec-reproduce}

```bash
# 1. The two arms. ~6 cluster-hours. Not regenerated by the entrypoint.
export PYSPARK_DRIVER_PYTHON=$PWD/.venv310/bin/python
unset PYSPARK_PYTHON OPDI_WAREHOUSE
for d in 2026-06-05 2026-06-06 2026-06-07; do
  .venv310/bin/python -u benchmarks/run_adsc_day.py --day $d --arm both
done

# 2. Every figure on this page.
.venv310/bin/python -u benchmarks/regenerate_adsc_v1.py

# 3. This page.
cd ../opdi-portal/papers && quarto render adsc-fusion-v1/index.qmd
```

# Provenance {#sec-provenance}

```{r provenance}
#| echo: false
mf <- file.path("data", "_manifest.json")
if (!file.exists(mf)) {
  cat(paste("**No manifest.** Every figure on this page is unverified: nothing",
            "records which script, arguments or commit produced it."))
} else {
  j <- jsonlite::fromJSON(mf, simplifyVector = FALSE)
  rows <- do.call(rbind, lapply(names(j), function(k) data.frame(
    output = k,
    script = j[[k]]$script %||% "",
    sha = j[[k]]$git_sha %||% "",
    dirty = isTRUE(j[[k]]$git_dirty),
    stringsAsFactors = FALSE)))
  kable(rows, row.names = FALSE,
        col.names = c("Output", "Script", "Commit", "Dirty tree"),
        caption = paste("Each staged output, and the command that produced",
                        "it. A `dirty` row was produced from uncommitted",
                        "code: the number is real and cannot be reproduced",
                        "from a commit."))
}
```

```{r provenance-missing}
#| echo: false
expected <- c("adsc_census.csv", "adsc_gain_2026.csv", "adsc_holes_2026.csv",
              "adsc_time_sync_2026.csv", "adsc_time_sync_pairs_2026.csv",
              "adsc_events_2026.csv", "adsc_events_by_airport_2026.csv")
absent <- expected[!file.exists(file.path("data", expected))]
if (length(absent)) {
  cat(paste0("**Not staged:** `", paste(absent, collapse = "`, `"), "`. ",
             "The chapters above say so in place; this list is the index."))
} else {
  cat("All seven expected outputs are staged.")
}
```
````

Add `%||%` near the top of the `setup` chunk, because the provenance chunk uses
it:

```r
`%||%` <- function(a, b) if (is.null(a)) b else a
```

- [ ] **Step 7: Render it**

Run:
```bash
cd ../opdi-portal/papers
OPDI_RENDER=allow-stale quarto render adsc-fusion-v1/index.qmd --to html
```
Expected: an HTML page with every table either populated or carrying its
"not staged" note. Then render for real:
```bash
quarto render adsc-fusion-v1/index.qmd
```
Expected: HTML and PDF, with `adsc-fusion-v1.pdf` saved beside the source by
the project's `_save-pdf.sh` post-render hook.

- [ ] **Step 8: Add the publication card**

In `papers/index.qmd`, insert **above** the flight-events-v4 card (newest
first):

```markdown
:::: {.publication-card}
::: {.publication-info}
**[ADS-C in the OPDI trajectory — version 1](adsc-fusion-v1/index.qmd)**
&nbsp;·&nbsp; [PDF](adsc-fusion-v1/adsc-fusion-v1.pdf)

Tests whether merging OpenSky's ADS-C feed into the ADS-B trajectories improves
flight-event extraction. ADS-C is merged before segmentation, so `track_id`
sees both sources, and the trajectory cleaner is made source-aware — its
twenty-second isolated-point rule would otherwise delete a feed whose median
cadence is ten minutes. Both arms are rebuilt from the same state vectors and
scored against APDF over the twenty aerodromes of flight events version 4.

Reports what the feed actually is: ~5,000 rows a day inside OPDI's European
box, 97.6% of them above FL300, three quarters west of 10°W, no `on_ground`
flag, and every speed, temperature, wind and selected-altitude field empty on
every day sampled. The A-CDM runway family therefore cannot move, and that null
is reported as measured rather than omitted.

Also measures what the two feeds disagree about. They agree on where the track
is to within about nine metres across track and disagree by hundreds of metres
along it — a timing offset, not a position error — and that offset is a
property of the individual airframe, not of the feed. One aircraft in
twenty-five is persistently about thirteen seconds out while another is
synchronised to a hundredth of a second, which is why no global correction is
applied.
:::
::::
```

- [ ] **Step 9: Update the workspace `CLAUDE.md`**

In `opdi-workspace/CLAUDE.md`, under "Conventions that matter", add a bullet
after the track-identity one:

```markdown
- **ADS-C is a second source, off by default.** `AdscConfig.enabled`
  (`src/opdi/config.py`) unions OpenSky's ADS-C topic
  (`s3a://readsb-aircraft-states/topics/readsb.aircraft.adsc.avro`, hourly
  `hour=<unix>` partitions, snappy parquet despite the `.avro` name) into the
  state-vector frame **before** segmentation, via step 01b and
  `TrackProcessor._merge_adsc`. Enabling it is a fourth `track_id` boundary
  after legacy → A8 → A9 and is silent to a joiner, so the study arms live at
  `s3a://eurocontrol/opdi/research/adsc/{base,merged}` and nothing published
  moves. Three facts drive the design and are easy to get wrong: the feed's
  `flight` field is the **IATA** number (`LH133`), not the ICAO callsign ADS-B
  broadcasts (`DLH133`), so it is dropped — carried through, the A9
  callsign-change break would split every merged track; the 5 s thinning rules
  are not applied, because `modulo` would delete a feed whose instants are
  arbitrary doubles; and `CleaningConfig.sparse_sources` exempts ADS-C from the
  isolated-point and stale-broadcast stages, without which step 02a NULLs every
  ADS-C value (20 s rule, 600 s cadence). The `source` column exists **only**
  in a run with the flag on — a column present in new partitions of a
  production table and absent from old ones breaks `spark.read.parquet` schema
  inference silently. **The two feeds do not share a clock:** measured against
  ADS-B interpolated to the same instant, ADS-C reports sit a median +1.48 s
  ahead along track (9 m cross-track against 377 m along-track, a 43x ratio, so
  it is timing and not position), and the offset is a property of the
  *individual airframe* -- tight within one (sigma 0.10-0.31 s), spread +0.01 s
  to +2.09 s between them, with one airframe persistently at -12.9 s. So
  `time_offset_seconds` ships at 0.0: a constant would damage the aircraft
  already synchronised. `benchmarks/adsc_time_sync.py` is the instrument. See
  `opdi-portal/papers/adsc-fusion-v1/`.
```

- [ ] **Step 10: Commit, submodule-first**

```bash
# in opdi-portal
git add papers/adsc-fusion-v1 papers/index.qmd _site
git commit -m "papers: ADS-C in the OPDI trajectory, version 1"

# in the meta-repo
cd ..
git add CLAUDE.md docs/superpowers opdi opdi-benchmarker opdi-portal
git commit -m "adsc: ADS-C/ADS-B fusion -- pipeline, benchmarker, paper"
```

---

## Self-review

**Spec coverage.** Every row of the spec's "Decisions taken" table maps to a
task: arm isolation → 8; merge point → 6; default off → 2, 6; schema → 3;
`source` only when enabled → 6, 11; callsign NULL → 3; no extras struct → 1
(the census is the standing evidence); paper scope → 10, 13; benchmark period →
8, 10; both arms rebuilt → 8. The three deliverables map to tasks 1–10
(`opdi`), 11 (`opdi-benchmarker`) and 12–13 (`opdi-portal`). Every non-goal is
restated in the paper's limitations chapter (Task 13, Step 6).

**Time synchronisation.** Every row of the spec's "Time synchronisation —
measured" decision table maps to a task: measure it → 9b; no global correction
→ 2 (`time_offset_seconds = 0.0`, with a test) and 3 (applied in the ingest, a
no-op at zero); asymmetric shadow window → 2 and 4; derivative filter
unchanged → asserted nowhere, because nothing was changed, and argued in Task
9b's module docstring and the paper's sync chapter; per-airframe correction out
of scope → the paper's limitations; `last_pos_update` as an assertion → Task 3's
docstring and Task 9b's `adsb_pos_age_s` column, which is what makes the
alternative explanation checkable. Task 7's `segment_id` test comment was
corrected: it marks the discontinuity and protects nothing, because no detector
reads it.

**Known gap, stated rather than hidden.** The spec's "measure first, then
decide" instinct is honoured only for the extras struct (Task 1, Step 8 is an
explicit stop-and-report gate). Everything else is built before any cluster
number exists, because the cluster numbers cannot be obtained without the
pipeline change. If Task 1's census contradicts the 2026-06-01 measurement, the
schema decision in Task 3 has to be revisited with the user before Task 8
spends six cluster-hours.

**Type consistency.** `source` is a `STRING` with values `"ADS-B"` / `"ADS-C"`
everywhere: `ADSB_SOURCE`/`ADSC_SOURCE` (Task 3) are the only spellings, read
by `CleaningConfig.sparse_sources` (Task 2), `_sparse_mask` (Task 7),
`arm_summary`/`bridged_holes` (Task 9), `DEFAULT_SOURCE` and `SOURCE_STYLE`
(Task 11). `shadow_filter(adsc, adsb, before_seconds, after_seconds)` takes **timestamp**
`event_time` on both sides and is called only from `_merge_adsc`, after the
cast — `to_opdi_schema` hands back a double and `ingest_from_s3` casts before
writing, so the table the merge reads is already a timestamp. `arm_path(arm,
table)` and `arm_tables(prefix, arm)` take their arguments in opposite orders;
both are tested.

**Ordering.** Tasks 1–7 are laptop-only and must pass `pytest` before Task 8
spends cluster time. Task 8's Step 6 gate (step 01b writing zero rows) is the
cheapest place to catch a broken ingest. Task 9 must be read before Task 10's
result is interpreted — the plan says so in both places.

## Execution Handoff

Plan complete and saved to
`docs/superpowers/plans/2026-10-16-adsc-fusion.md`, with its spec at
`docs/superpowers/specs/2026-10-16-adsc-fusion.md`. Two execution options:

**1. Subagent-Driven (recommended)** — a fresh subagent per task, reviewed
between tasks, fast iteration. Tasks 1–7 and 9–12 are laptop-testable and suit
this well; tasks 8 and 13's cluster steps need a human at the terminal.

**2. Inline Execution** — execute tasks in this session using
`superpowers:executing-plans`, batching with checkpoints for review.
