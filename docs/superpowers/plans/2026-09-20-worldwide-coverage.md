# Worldwide Coverage Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let one configuration value switch the whole OPDI pipeline between the
published European bounding box (the default, unchanged) and worldwide coverage,
build the worldwide reference data a global run needs, and let `opdi-benchmarker`
read and cache whichever coverage the pipeline produced.

**Architecture:** The ingestion bounding box is currently hardcoded in **six
places** that do not know about each other. This plan replaces all six with a
single `CoverageConfig` carrying `bbox: Optional[tuple]`, where `None` means
worldwide. Every geographic filter in the pipeline asks that one object. A
worldwide run additionally writes to its own warehouse prefix, so the published
European tables and their `track_id` contract are untouched, and a coverage
marker in the warehouse makes mixing the two coverages in one prefix a loud
failure rather than a silent one.

**Tech Stack:** PySpark (native column expressions only — no `applyInPandas`, no
`pandas_udf`, no `traffic`), pytest with the local `spark` fixture in
`tests/conftest.py`, pyosmium for the OSM planet extract, h3-py 4.5.0.

**Spec:** This document. It is self-contained: the requirements come from the
user's request (a single `worldwide` / `bbox=None` parameter; reference data for
all medium and large airports worldwide; the European box stays the default) plus
the four decisions recorded under *Fixed decisions* below, which the user chose
explicitly before this plan was written.

---

## Context

OPDI ingests OpenSky state vectors, filters them to a European bounding box,
splits them into tracks, derives a flight list, and extracts flight events. The
box is `(-25.86653, 26.74617, 49.65699, 70.25976)` — `(min_lon, min_lat, max_lon,
max_lat)`.

**That box is written out six times, in five files, with no link between them.**
Four are `LAT_MIN`/`LAT_MAX`/`LON_MIN`/`LON_MAX` class constants that are
*textually identical* and separately maintained:

| # | Location | What it gates |
|---|---|---|
| 1 | `src/opdi/ingestion/osn_statevectors.py:58` `DEFAULT_BBOX` | Which state vectors are kept at ingest |
| 2 | `src/opdi/reference/h3_airport_zones.py:298-301` | Which airports get detection zones (step 00a) |
| 3 | `src/opdi/reference/h3_airport_zones.py:738-742` | A **second** filter, on H3 cell *centres*, inside `prepare_for_flight_list_spark` |
| 4 | `src/opdi/reference/h3_airport_layouts.py:385-389` | Which airports get ground layouts (step 00b) |
| 5 | `src/opdi/reference/h3_runway_grid.py:371-374` | Which airports get a runway/approach grid (step 00f) |
| 6 | `src/opdi/pipeline/flights.py:53` `BBOX` | Whether a track endpoint "left the observed area" (`OOA`) |

Copy 3 matters more than it looks: it filters the *cells*, not the airports, so
even if copies 2 and 4 were widened, every cell outside the European box would
still be dropped on the way to the table and the widening would appear to have
done nothing.

**Measured scale.** OurAirports (fetched 2026-09-20, 86,106 rows) lists **5,280**
large or medium aerodromes worldwide against **1,357** inside the current box plus
its 3° offset — a **3.89×** reference build. By continent: Asia 1,541, North
America 1,418, Europe 991, Africa 534, South America 449, Oceania 341,
Antarctica 6.

**Ingestion read cost does not change.** `ingest_from_s3` reads whole hourly
partitions from `s3a://opensky-hdfs-backup/tables_v4/state_vectors` and applies
the bounding box *afterwards*, in `_apply_filters`. A worldwide run therefore
reads exactly the same bytes from S3 as a European run does today. What grows is
everything downstream of the filter: the rows written, the tracks built, and the
events extracted.

---

## Global Constraints

Copied from `CLAUDE.md` and the codebase's own conventions. Every task's
requirements implicitly include this section.

* **Everything is native Spark.** Column expressions and window functions. No
  `traffic`, no `applyInPandas`, no `pandas_udf`.
* **Storage is SI; human-facing values are aviation.** Scale the *comparison*,
  never the stored column. Reuse `FT_PER_M = 3.28084`, `KT_PER_MPS = 1.94384`,
  `FTMIN_PER_MPS = 196.850394`.
* **New config fields carry their unit in the name** (`_deg`, `_nm`, `_ft`,
  `_seconds`). `tests/test_detection_config.py` and `tests/test_event_config.py`
  property-test this.
* **The European default must not move.** After every task,
  `OPDIConfig.for_environment("opensky").coverage.bbox` equals
  `(-25.86653, 26.74617, 49.65699, 70.25976)` exactly, and a European run
  produces byte-identical filters to today. This is the single most important
  invariant in the plan: the box is a published contract through `track_id` and
  through every study that tuned against it.
* **Never mutate a published `version` string.** Nothing in this plan changes an
  algorithm, so no version string changes. If one appears to need changing, stop
  — something has gone wrong.
* **Do not convert the storage layer to aviation units**, and do not touch
  `track_gap_low_altitude_meters` or the `osn_tracks` DDL comments.
* **A worldwide run never writes to a European table.** Enforced by the warehouse
  prefix (Task 1) and by the coverage marker (Task 6), not by discipline.
* **One distributed Spark job at a time from this pod.** The driver runs
  client-mode inside the JupyterLab container (16 GiB cgroup) and the
  `jupyterlab` Service routes exactly two ports. A second concurrent job fails in
  a way that reads like a quota problem.

### Fixed decisions

These four were chosen by the user on 2026-09-20 and are not open.

| Decision | Value | Consequence |
|---|---|---|
| Output destination | **Separate table set** | Worldwide resolves its own `warehouse_path`. European tables and their `track_id` continuity are untouched. |
| Airport scope | **large + medium, as today** | 5,280 worldwide vs 1,357 now. All three reference tables stay keyed to the same airport set, so none of them joins to the others with new gaps. |
| OSM geometry source | **`planet.osm.pbf`, then the existing aeroway pre-filter** | 94.86 GB download (measured); Europe's 34.9 GB filters to 16 MB, so the planet should land in the tens of MB. One file, one code path. |
| Out-of-area (`OOA`) | **Disabled worldwide** | With `bbox=None` there is no edge to have crossed. An undetected endpoint is `NULL`, not `OOA`. Documented as a behaviour difference, not hidden. |

### Decided in this plan, stated so it can be overridden

* **Step 00c (airspaces) does not run worldwide.** Its sources are PRU Atlas
  ANSP/FIR/country parquets, which cover Europe only, and `CLAUDE.md` records
  that `opdi_h3_airspace_ref` is **read by nothing** — no airspace or FIR events
  exist. Running it worldwide would produce a European table inside a worldwide
  warehouse and cost cluster time for a product with no consumer. Task 7 skips it
  and prints that it did.
* **The worldwide default warehouse is `s3a://eurocontrol/opdi-world`**, with
  `OPDI_WAREHOUSE_WORLD` overriding it. `OPDI_WAREHOUSE` is deliberately *not*
  consulted in worldwide mode — see Task 1 Step 4 for why.

---

## File Structure

**Created:**

| File | Responsibility |
|---|---|
| `opdi/src/opdi/coverage.py` | `EUROPE_BBOX`, `CoverageConfig`, and the two filter builders (`spark_filter`, `pandas_mask`). A leaf module: `config.py` imports it, and every filter site imports it without importing `config`, so no cycles. |
| `opdi/tests/test_coverage_config.py` | The European default, the worldwide switch, the warehouse split. |
| `opdi/tests/test_coverage_filters.py` | That every one of the six sites asks the config and none keeps a private copy. |
| `opdi/tests/test_zone_geometry_global.py` | Antimeridian and polar behaviour of the zone build. |
| `opdi/tests/test_coverage_marker.py` | The warehouse guard. |

**Modified:**

| File | Change |
|---|---|
| `src/opdi/config.py` | `OPDIConfig.coverage`; `for_environment(env, worldwide=False)`. |
| `src/opdi/ingestion/osn_statevectors.py` | `DEFAULT_BBOX` deleted; the box comes from `config.coverage`. An explicit `bbox=` argument still wins, including an explicit `None`. |
| `src/opdi/pipeline/flights.py` | `BBOX` deleted; `at_border` takes the box and returns `lit(False)` when there is none. |
| `src/opdi/reference/h3_airport_zones.py` | Both bbox copies deleted; longitude normalisation in `generate_circle_polygon`; polar exclusion; partition count derived from the airport count. |
| `src/opdi/reference/h3_airport_layouts.py` | Bbox copy deleted. |
| `src/opdi/reference/h3_runway_grid.py` | Bbox copy deleted. |
| `src/opdi/reference/pbf_source.py` | `airport_boxes` longitude wrap at the antimeridian. |
| `src/opdi/utils/storage.py` | `write_coverage_marker` / `read_coverage_marker`. |
| `src/opdi/runner.py` | 00c skipped worldwide; the marker written after step 00; `worldwide` threaded through `run_pipeline`. |
| `src/opdi/periodrun.py` | The marker checked in `preflight`; `worldwide` threaded through `run_period`. |
| `src/opdi/cli.py` | `--worldwide`. |

**Modified in `opdi-benchmarker/` (Task 11):**

| File | Change |
|---|---|
| `src/opdi_benchmarker/config.py` | `COVERAGE`, `set_coverage`, `is_worldwide`; `CACHE_BASE` and `SOURCE_BASE` derived from one rule. `REFERENCE_BASE` deliberately unchanged. |
| `src/opdi_benchmarker/build/cli.py` | `--worldwide`, applied before any path is resolved; the source warehouse's coverage marker checked. |
| `src/opdi_benchmarker/app/Home.py`, `app/pages/3_Movements.py` | Show the coverage, and the standing caveat that `opdi_only` means something different worldwide. |
| `src/opdi_benchmarker/provenance.py` | Stamp `coverage` and `source_base` into every manifest. |
| `DATASETS.md` | The worldwide prefixes, and what does not move. |

`coverage.py` is its own module rather than more dataclasses in `config.py`
because `flights.py`, `h3_airport_zones.py` and `osn_statevectors.py` all need the
filter builders, and two of those are imported *by* config-consuming code. A leaf
module is the shape `elevation.py` and `crossings.py` already established.

---

### Task 1: `CoverageConfig` — one bounding box, and the worldwide switch

Every later task deletes a private copy of the box and asks this object instead,
so this comes first and nothing after it touches `config.py` again.

**Files:**
- Create: `opdi/src/opdi/coverage.py`
- Modify: `opdi/src/opdi/config.py` (`OPDIConfig`, ~line 1560; `for_environment`)
- Test: `opdi/tests/test_coverage_config.py`

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `coverage.EUROPE_BBOX: tuple[float, float, float, float]`
  - `coverage.CoverageConfig` with fields `bbox`, `bbox_offset_deg`, `airport_types`
  - `CoverageConfig.is_worldwide -> bool`
  - `CoverageConfig.spark_filter(lat_col, lon_col, *, offset: bool) -> Column`
  - `CoverageConfig.pandas_mask(lat_series, lon_series, *, offset: bool) -> Series`
  - `CoverageConfig.label -> str` (`"europe"` / `"worldwide"` / `"custom"`)
  - `OPDIConfig.coverage`
  - `OPDIConfig.for_environment(env, worldwide=False)`

- [ ] **Step 1: Create the branch**

```bash
cd /home/jupyter/work/opdi-workspace/opdi
git checkout main && git pull --ff-only
git checkout -b feature/worldwide-coverage
```

- [ ] **Step 2: Write the failing tests**

Create `opdi/tests/test_coverage_config.py`:

```python
"""The coverage switch: one box, two modes, and a default that must not move."""
import pytest
from pyspark.sql import functions as F

from opdi.config import OPDIConfig
from opdi.coverage import EUROPE_BBOX, CoverageConfig

#: The published box, written out here on purpose. If someone edits the
#: constant this test fails, which is the entire point: the box reaches
#: published data through track_id and through every study tuned against it.
PUBLISHED = (-25.86653, 26.74617, 49.65699, 70.25976)


def test_the_european_box_is_the_published_one():
    assert EUROPE_BBOX == PUBLISHED


def test_every_environment_still_defaults_to_europe():
    """A coverage that varied by environment would let dev publish rows local
    runs cannot reproduce, with nothing in the data saying why."""
    for env in ("opensky", "local", "dev", "live"):
        assert OPDIConfig.for_environment(env).coverage.bbox == PUBLISHED


def test_worldwide_means_no_box():
    c = OPDIConfig.for_environment("opensky", worldwide=True).coverage
    assert c.bbox is None
    assert c.is_worldwide is True
    assert c.label == "worldwide"


def test_europe_is_not_worldwide():
    c = OPDIConfig.for_environment("opensky").coverage
    assert c.is_worldwide is False
    assert c.label == "europe"


def test_a_custom_box_is_neither():
    """A regional run is a legitimate third case and must not claim to be
    either of the two named ones -- the label ends up in the warehouse marker."""
    assert CoverageConfig(bbox=(0.0, 40.0, 10.0, 50.0)).label == "custom"


def test_worldwide_writes_to_its_own_warehouse():
    """The decision that keeps the published European tables safe. If these two
    are ever equal, a worldwide run appends planet-wide rows to the table the
    European track_id contract lives in."""
    eu = OPDIConfig.for_environment("opensky")
    world = OPDIConfig.for_environment("opensky", worldwide=True)
    assert world.project.warehouse_path != eu.project.warehouse_path
    assert world.project.warehouse_path.endswith("opdi-world")


def test_opdi_warehouse_does_not_leak_into_a_worldwide_run(monkeypatch):
    """OPDI_WAREHOUSE is how production points at the European prefix. If it
    also steered worldwide runs, an operator with it exported -- which is the
    normal state -- would silently write planet-wide rows into opdi-prod."""
    monkeypatch.setenv("OPDI_WAREHOUSE", "s3a://eurocontrol/opdi-prod")
    world = OPDIConfig.for_environment("opensky", worldwide=True)
    assert "opdi-prod" not in world.project.warehouse_path


def test_the_worldwide_warehouse_is_overridable(monkeypatch):
    monkeypatch.setenv("OPDI_WAREHOUSE_WORLD", "s3a://eurocontrol/scratch-world")
    world = OPDIConfig.for_environment("opensky", worldwide=True)
    assert world.project.warehouse_path == "s3a://eurocontrol/scratch-world"


def test_the_spark_filter_is_a_no_op_worldwide(spark):
    """`lit(True)`, not a box of +/-180/90. A literal-true predicate is free for
    the optimiser to remove; a full-globe comparison is four column operations
    Spark must evaluate on every one of ~1e9 rows a day."""
    c = CoverageConfig(bbox=None)
    df = spark.createDataFrame([(0.0, 0.0), (-89.0, 179.9)], "lat double, lon double")
    got = df.filter(c.spark_filter(F.col("lat"), F.col("lon"), offset=False))
    assert got.count() == 2


def test_the_spark_filter_keeps_europe_and_drops_the_rest(spark):
    c = CoverageConfig()
    df = spark.createDataFrame(
        [("EBBR", 50.9, 4.48), ("KJFK", 40.6, -73.8), ("NZAA", -37.0, 174.8)],
        "ident string, lat double, lon double",
    )
    kept = {r.ident for r in
            df.filter(c.spark_filter(F.col("lat"), F.col("lon"), offset=False)).collect()}
    assert kept == {"EBBR"}


def test_the_offset_widens_by_exactly_three_degrees(spark):
    """The reference generators use the box plus an offset so an aerodrome just
    outside it still gets zones. 3 degrees is what all four copies used."""
    c = CoverageConfig()
    assert c.bbox_offset_deg == 3.0
    just_outside = spark.createDataFrame([(72.0, 4.0)], "lat double, lon double")
    assert just_outside.filter(
        c.spark_filter(F.col("lat"), F.col("lon"), offset=False)).count() == 0
    assert just_outside.filter(
        c.spark_filter(F.col("lat"), F.col("lon"), offset=True)).count() == 1


def test_the_pandas_mask_agrees_with_the_spark_filter(spark):
    """Two implementations of one rule. `h3_airport_layouts` filters in pandas
    and `h3_airport_zones` in Spark, so a disagreement means the layout table
    and the zone table cover different airports -- and they join to each other."""
    import pandas as pd
    c = CoverageConfig()
    pdf = pd.DataFrame({
        "lat": [50.9, 40.6, -37.0, 72.0, 26.0],
        "lon": [4.48, -73.8, 174.8, 4.0, 0.0],
    })
    mask = c.pandas_mask(pdf["lat"], pdf["lon"], offset=True)
    sdf = spark.createDataFrame(pdf)
    spark_keep = sdf.filter(
        c.spark_filter(F.col("lat"), F.col("lon"), offset=True)).count()
    assert int(mask.sum()) == spark_keep


def test_the_airport_types_are_the_ones_every_reference_table_uses():
    """Three reference tables keyed to different airport sets would join to
    each other with gaps nothing reports -- the failure h3_airport_zones'
    AIRPORT_TYPES comment records."""
    assert CoverageConfig().airport_types == ("large_airport", "medium_airport")
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `cd /home/jupyter/work/opdi-workspace/opdi && .venv310/bin/python -m pytest tests/test_coverage_config.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'opdi.coverage'`

- [ ] **Step 4: Write `coverage.py`**

Create `opdi/src/opdi/coverage.py`:

```python
"""Where OPDI looks. One bounding box, asked for by everything that filters.

The box used to be written out six times in five files -- four of them
textually identical ``LAT_MIN``/``LAT_MAX``/``LON_MIN``/``LON_MAX`` class
constants with no link between them. Widening coverage therefore meant finding
all six, and missing one produced no error: the ingest would keep worldwide
rows while the detection zones covered Europe, so every flight outside Europe
got a track and no aerodrome. That is a silent wrong answer, which is the
failure mode this module exists to make impossible.

``bbox = None`` means worldwide. It is not a box of +/-180/90 because those are
not the same thing: a full-globe box still compares four columns on every row,
and at the antimeridian it is actively wrong -- a longitude range cannot
express "everywhere" once the coordinate wraps. ``None`` compiles to
``lit(True)``, which the optimiser deletes.
"""

from dataclasses import dataclass
from typing import Optional, Tuple

from pyspark.sql import Column
from pyspark.sql import functions as F

__all__ = ["EUROPE_BBOX", "CoverageConfig"]

#: The published OPDI coverage box, ``(min_lon, min_lat, max_lon, max_lat)``.
#:
#: **Do not edit.** It reaches published data through ``track_id`` -- a track
#: is the run of samples that survived this filter -- and every detection study
#: OPDI has published was tuned inside it. Changing coverage is done by
#: choosing a different ``CoverageConfig``, never by editing this tuple.
EUROPE_BBOX: Tuple[float, float, float, float] = (
    -25.86653, 26.74617, 49.65699, 70.25976
)


@dataclass
class CoverageConfig:
    """The geographic scope of a run.

    ``bbox`` is ``(min_lon, min_lat, max_lon, max_lat)`` in degrees, or ``None``
    for worldwide.
    """

    bbox: Optional[Tuple[float, float, float, float]] = EUROPE_BBOX
    """``None`` means worldwide. Anything else is a box in degrees."""

    bbox_offset_deg: float = 3.0
    """How far outside the box reference data is still built, in degrees.

    An aerodrome sitting just beyond the edge still receives traffic that is
    inside it, so its zones and layout must exist or those flights get a track
    and no ADEP. 3 degrees is the value all four reference generators used
    independently before this module existed. Ignored worldwide, where there
    is no edge to be just beyond.
    """

    airport_types: Tuple[str, ...] = ("large_airport", "medium_airport")
    """Which aerodromes get reference data.

    All three reference tables -- detection zones, ground layouts, runway grid
    -- must use one set, or they join to each other with gaps nothing reports.
    Widening this once changed the ADEP/ADES candidate set tenfold against the
    set every detection study tuned on, and OOM'd every executor in the
    namespace; see ``h3_airport_zones.AIRPORT_TYPES``.
    """

    @property
    def is_worldwide(self) -> bool:
        return self.bbox is None

    @property
    def label(self) -> str:
        """``europe``, ``worldwide``, or ``custom``.

        Stamped into the warehouse coverage marker, so a third value is not
        cosmetic: a regional run must not be able to claim it is one of the two
        named coverages and land in that coverage's warehouse.
        """
        if self.bbox is None:
            return "worldwide"
        return "europe" if tuple(self.bbox) == EUROPE_BBOX else "custom"

    def _bounds(self, offset: bool):
        min_lon, min_lat, max_lon, max_lat = self.bbox
        d = self.bbox_offset_deg if offset else 0.0
        return min_lon - d, min_lat - d, max_lon + d, max_lat + d

    def spark_filter(self, lat_col: Column, lon_col: Column, *, offset: bool) -> Column:
        """A predicate selecting positions inside this coverage.

        ``offset`` is for the reference generators, which build slightly
        outside the box; ingest and the flight list pass ``False``.
        """
        if self.bbox is None:
            return F.lit(True)
        min_lon, min_lat, max_lon, max_lat = self._bounds(offset)
        return (
            (lat_col >= F.lit(min_lat)) & (lat_col <= F.lit(max_lat))
            & (lon_col >= F.lit(min_lon)) & (lon_col <= F.lit(max_lon))
        )

    def pandas_mask(self, lat, lon, *, offset: bool):
        """The same rule for a pandas frame. ``h3_airport_layouts`` filters its
        airport loop list in pandas, and the two must not be able to disagree.
        """
        import numpy as np

        if self.bbox is None:
            return np.ones(len(lat), dtype=bool)
        min_lon, min_lat, max_lon, max_lat = self._bounds(offset)
        return (
            lat.between(min_lat, max_lat) & lon.between(min_lon, max_lon)
        ).to_numpy()
```

- [ ] **Step 5: Wire it into `OPDIConfig`**

In `opdi/src/opdi/config.py`, import and add the field:

```python
from opdi.coverage import EUROPE_BBOX, CoverageConfig
```

```python
    coverage: CoverageConfig = field(default_factory=CoverageConfig)
```

Change `for_environment`'s signature and add the worldwide handling at the end,
after the per-environment config has been built:

```python
    @classmethod
    def for_environment(cls, env: str = "dev", worldwide: bool = False) -> "OPDIConfig":
        """Create configuration for a specific environment.

        Args:
            env: Environment name (``"dev"``, ``"live"``, ``"local"``,
                ``"opensky"``).
            worldwide: Drop the coverage bounding box and route output to the
                worldwide warehouse. **Off by default, and that is the point**
                -- the European box is a published contract and every existing
                caller must keep getting it without saying so.
        """
```

At the end of the method, before returning, apply the switch in one place
rather than in each environment branch:

```python
        config = ...  # whatever the branch above built

        if worldwide:
            config.coverage = CoverageConfig(bbox=None)
            config.project = replace(
                config.project,
                warehouse_path=_worldwide_warehouse(config.project.warehouse_path),
            )
        return config
```

with the helper beside it:

```python
def _worldwide_warehouse(europe_path: str) -> str:
    """Where a worldwide run writes.

    ``OPDI_WAREHOUSE_WORLD`` wins; otherwise the European path with its last
    segment suffixed ``-world``.

    **``OPDI_WAREHOUSE`` is deliberately not consulted.** That variable is how
    production points the pipeline at the European prefix, so it is exported in
    the shell an operator would most likely run a worldwide experiment from. If
    it steered worldwide runs too, ``--worldwide`` would silently append
    planet-wide rows to ``opdi-prod`` -- the exact outcome the separate table
    set exists to prevent. Task 6's coverage marker is the second line of
    defence behind this one.
    """
    override = os.environ.get("OPDI_WAREHOUSE_WORLD")
    if override:
        return override
    return europe_path.rstrip("/") + "-world"
```

Add `from dataclasses import replace` to the imports if it is not already there.

- [ ] **Step 6: Run the tests to verify they pass**

Run: `.venv310/bin/python -m pytest tests/test_coverage_config.py -v`
Expected: PASS, all thirteen.

- [ ] **Step 7: Run the whole suite**

Run: `.venv310/bin/python -m pytest tests/ -q`
Expected: PASS. Nothing reads `config.coverage` yet, so this task cannot change
behaviour; a failure here means `for_environment` was altered in a way that
touched an existing environment, which it must not.

> **Note on the suite:** running the full ~950-test suite in one process has been
> observed to exhaust Spark resources and produce ~200 failures that all pass in
> isolation (see `EVENTS_RUN_LOG`, the A9 promotion). If you see a large failure
> count spanning files this task cannot touch, re-run the affected files alone
> before treating it as a regression.

- [ ] **Step 8: Commit**

```bash
git add src/opdi/coverage.py src/opdi/config.py tests/test_coverage_config.py
git commit -m "feat(config): one coverage box, with a worldwide switch"
```

---

### Task 2: Ingestion asks the configuration for its box

**Files:**
- Modify: `opdi/src/opdi/ingestion/osn_statevectors.py:58` (`DEFAULT_BBOX`), `:66-95` (`__init__`), `:141-147` (`_apply_filters`)
- Test: `opdi/tests/test_coverage_filters.py`

**Interfaces:**
- Consumes: `config.coverage` (Task 1).
- Produces: `StateVectorIngestion(..., bbox=_UNSET)` where the default follows
  the configuration, an explicit tuple overrides it, and an explicit `None`
  means worldwide.

**Why a sentinel.** Today `self.bbox = bbox if bbox is not None else self.DEFAULT_BBOX`
makes `None` mean "use the default". Worldwide needs `None` to mean "no box", so
the two meanings collide. A module-level `_UNSET = object()` separates them, and
the current docstring's claim that a "False-y value" disables the box is wrong
today — `bbox=()` reaches `if self.bbox:` and disables it, but `bbox=None` does
not. Fix the docstring with the code.

- [ ] **Step 1: Write the failing tests**

Create `opdi/tests/test_coverage_filters.py`:

```python
"""Every geographic filter in the pipeline asks one object for its box.

Six sites used to keep private copies. These tests are the reason they cannot
drift back apart: each asserts that changing `config.coverage` changes what the
site does, which a private copy would not.
"""
import pytest
from pyspark.sql import functions as F

from opdi.config import OPDIConfig
from opdi.coverage import EUROPE_BBOX
from opdi.ingestion.osn_statevectors import StateVectorIngestion

#: (lat, lon, inside-Europe) triples. NZAA and KJFK are the two that matter:
#: one is beyond the box in longitude, the other in both.
PLACES = [
    (50.90, 4.48, True),    # EBBR
    (40.64, -73.78, False),  # KJFK
    (-37.01, 174.79, False), # NZAA
    (1.36, 103.99, False),   # WSSS
]


def _sv(spark):
    return spark.createDataFrame(
        [(lat, lon, 1700000000) for lat, lon, _ in PLACES],
        "lat double, lon double, event_time long",
    )


def test_the_european_ingest_keeps_only_european_rows(spark):
    ing = StateVectorIngestion(spark, OPDIConfig.for_environment("opensky"),
                               time_interval=1)
    assert ing.bbox == EUROPE_BBOX
    assert ing._apply_filters(_sv(spark)).count() == 1


def test_the_worldwide_ingest_keeps_every_row(spark):
    ing = StateVectorIngestion(
        spark, OPDIConfig.for_environment("opensky", worldwide=True),
        time_interval=1)
    assert ing.bbox is None
    assert ing._apply_filters(_sv(spark)).count() == len(PLACES)


def test_an_explicit_box_still_overrides_the_configuration(spark):
    """The decimation and segmentation benchmarks pass their own box. That must
    keep working, and it must beat the configuration rather than be beaten."""
    ing = StateVectorIngestion(
        spark, OPDIConfig.for_environment("opensky", worldwide=True),
        bbox=(-10.0, 45.0, 10.0, 55.0), time_interval=1)
    assert ing._apply_filters(_sv(spark)).count() == 1


def test_an_explicit_none_means_worldwide_not_the_default(spark):
    """The collision this fix exists for: `None` used to mean 'use the default
    European box', so there was no way to ask for worldwide by argument."""
    ing = StateVectorIngestion(
        spark, OPDIConfig.for_environment("opensky"), bbox=None, time_interval=1)
    assert ing.bbox is None
    assert ing._apply_filters(_sv(spark)).count() == len(PLACES)
```

- [ ] **Step 2: Run to verify failure**

Run: `.venv310/bin/python -m pytest tests/test_coverage_filters.py -v`
Expected: FAIL — `test_the_worldwide_ingest_keeps_every_row` fails because
`bbox=None` still resolves to `DEFAULT_BBOX`.

- [ ] **Step 3: Implement**

Delete `DEFAULT_BBOX` and add above the class:

```python
#: Distinguishes "the caller said nothing" from "the caller said worldwide".
#: ``None`` used to mean the first; worldwide needs it to mean the second.
_UNSET = object()
```

In `__init__`, change the parameter default to `_UNSET` and the assignment to:

```python
        self.bbox = config.coverage.bbox if bbox is _UNSET else bbox
```

Update the argument docstring:

```
            bbox: Bounding box as ``(min_lon, min_lat, max_lon, max_lat)``.
                  Omit it to follow ``config.coverage.bbox`` -- the European
                  box by default, ``None`` under ``worldwide=True``. Pass
                  ``None`` explicitly to force worldwide regardless of the
                  configuration; pass a tuple to force a specific box.
```

`_apply_filters` needs no change — `if self.bbox:` is already correct once
`None` reaches it.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `.venv310/bin/python -m pytest tests/test_coverage_filters.py tests/ -q -k "statevector or ingest or decimation or coverage"`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/opdi/ingestion/osn_statevectors.py tests/test_coverage_filters.py
git commit -m "feat(ingest): take the coverage box from configuration"
```

---

### Task 3: Out-of-area follows the box, and there is none worldwide

`at_border` is what produces the `OOA` aerodrome label. Worldwide it has no edge
to test, so it must return false everywhere — not test the old European edge,
which would mean `OOA` silently reads "left Europe" in a planet-wide dataset.

**Files:**
- Modify: `opdi/src/opdi/pipeline/flights.py:51-53` (`BBOX`), `:245-266` (`at_border`), `:661`, `:838`
- Test: `opdi/tests/test_flight_detection.py`

**Interfaces:**
- Consumes: `config.coverage` (Task 1).
- Produces: `at_border(lat, lon, margin_nm=BORDER_MARGIN_NM, bbox=EUROPE_BBOX)`.
  The `bbox` parameter is last and defaults to the European box, so the two
  existing tests that call `at_border(lat, lon)` keep passing unchanged.

- [ ] **Step 1: Write the failing tests**

Append to `opdi/tests/test_flight_detection.py`:

```python
def test_at_border_is_false_everywhere_when_there_is_no_box(spark):
    """Worldwide there is no edge, so nothing can be at it.

    Returning the old European edge instead would label a flight crossing 26.7N
    over the Sahara as having left the observed area, in a dataset that
    observes the Sahara.
    """
    df = spark.createDataFrame(
        [(26.8, 0.0), (0.0, 0.0), (-89.9, 179.9), (70.2, 49.6)],
        "lat double, lon double",
    )
    got = [r.b for r in df.select(
        at_border(F.col("lat"), F.col("lon"), bbox=None).alias("b")).collect()]
    assert got == [False, False, False, False]


def test_at_border_uses_the_box_it_is_given(spark):
    """A point inside the European box can be at the border of a smaller one.
    If this ever stops holding, `at_border` has gone back to a module constant.
    """
    df = spark.createDataFrame([(50.9, 4.48)], "lat double, lon double")
    inside_europe = df.select(
        at_border(F.col("lat"), F.col("lon")).alias("b")).collect()[0].b
    at_edge_of_small = df.select(
        at_border(F.col("lat"), F.col("lon"),
                  bbox=(4.0, 50.5, 5.0, 51.5)).alias("b")).collect()[0].b
    assert inside_europe is False
    assert at_edge_of_small is True


def test_a_worldwide_flight_list_assigns_no_out_of_area_label(spark, tmp_path):
    """The user-visible consequence, asserted at the level a consumer sees.

    An undetected endpoint under worldwide coverage is NULL -- an honest 'no
    aerodrome found' -- not OOA, which asserts something the run cannot know.
    """
    from opdi.pipeline.flights import FlightListProcessor
    cfg = OPDIConfig.for_environment("opensky", worldwide=True)
    proc = FlightListProcessor(spark, cfg, log_file_path=str(tmp_path / "l.parquet"))
    sv = _two_tracks_ending_far_from_any_aerodrome(spark)   # helper, below
    out = proc._endpoint_candidates_frame(sv)               # name per current code
    assert out.filter(F.col("adep") == OOA).count() == 0
    assert out.filter(F.col("ades") == OOA).count() == 0
```

Write `_two_tracks_ending_far_from_any_aerodrome` as a module-level helper
following the existing builders in this file, with a docstring saying why its
geometry is what it is. **Before writing it, read the current
`FlightListProcessor` to confirm the method name the last assertion calls** —
`flights.py:838` and `flights.py:1110` are the two places `at_border` and `OOA`
meet, and the seam that is cheapest to test may be either. If no seam is
testable without cluster data, drop that third test and say so in the commit
message rather than writing one that asserts nothing.

- [ ] **Step 2: Run to verify failure**

Run: `.venv310/bin/python -m pytest tests/test_flight_detection.py -v -k border`
Expected: FAIL with `TypeError: at_border() got an unexpected keyword argument 'bbox'`

- [ ] **Step 3: Implement**

Replace the module constant:

```python
from opdi.coverage import EUROPE_BBOX

#: Default box for :func:`at_border`, kept so the two existing callers in the
#: tests read unchanged. Production passes ``config.coverage.bbox`` explicitly;
#: this default exists for the test seam, not for the pipeline.
BBOX = EUROPE_BBOX
```

and the function:

```python
def at_border(lat, lon, margin_nm: float = BORDER_MARGIN_NM, bbox=BBOX):
    """True where a position sits within *margin_nm* of the coverage edge.

    ``bbox=None`` means worldwide, and returns ``lit(False)`` everywhere: there
    is no edge, so nothing can be near it. That is not the same as a box of
    +/-180/90, which would make every flight crossing the antimeridian look
    like it had left the observed area.

    The longitude margin is scaled by 1/cos(latitude): a degree of longitude
    shrinks toward the pole, so an unscaled margin would be twice as strict in
    northern Norway as in the Canaries.
    """
    if bbox is None:
        return lit(False)
    min_lon, min_lat, max_lon, max_lat = bbox
    ...  # body unchanged
```

At both call sites (`flights.py:661` and `:838`) pass the configured box:

```python
        sv = sv.withColumn(
            "_border",
            at_border(col("lat"), col("lon"), bbox=self.config.coverage.bbox),
        )
```

Leave `OOA` and the precedence at `:723-733` and `:1110-1112` alone. With
`at_border` false everywhere, the `when(border, OOA)` arms simply never fire and
the existing `.otherwise` produces the NULL / `"undetermined"` result — no new
branch, and the European behaviour is bit-identical.

- [ ] **Step 4: Run the tests**

Run: `.venv310/bin/python -m pytest tests/test_flight_detection.py tests/test_flights_labelling.py -v`
Expected: PASS, including the two pre-existing `at_border` tests unchanged.

- [ ] **Step 5: Commit**

```bash
git add src/opdi/pipeline/flights.py tests/test_flight_detection.py
git commit -m "feat(flights): out-of-area follows the coverage box, and worldwide has none"
```

---

### Task 4: The three reference generators share one box

Four copies die here, including the one on cell centres that would have made the
other three look ineffective.

**Files:**
- Modify: `opdi/src/opdi/reference/h3_airport_zones.py:298-301`, `:390-400`, `:413-430`, `:655-665`, `:738-742`
- Modify: `opdi/src/opdi/reference/h3_airport_layouts.py:385-389`, `:468-505`
- Modify: `opdi/src/opdi/reference/h3_runway_grid.py:371-374`, `:416-442`
- Test: `opdi/tests/test_coverage_filters.py` (append)

**Interfaces:**
- Consumes: `config.coverage` (Task 1).
- Produces: no signature changes. `LAT_MIN`/`LAT_MAX`/`LON_MIN`/`LON_MAX`/
  `BBOX_OFFSET` are removed from all three classes; each reads
  `self.config.coverage` instead. `AIRPORT_TYPES` on `AirportDetectionZoneGenerator`
  becomes `self.config.coverage.airport_types`, and the two generators that
  default `airport_types` to a literal list do the same.

- [ ] **Step 1: Write the failing tests**

Append to `opdi/tests/test_coverage_filters.py`:

```python
def test_the_zone_generator_takes_its_box_from_configuration(spark):
    """Not `LAT_MIN`. A class constant cannot follow a worldwide switch, and a
    generator that kept one would build European zones inside a worldwide
    warehouse -- producing tracks with no ADEP for every flight outside Europe,
    with nothing reporting why."""
    from opdi.reference.h3_airport_zones import AirportDetectionZoneGenerator
    for cls in (AirportDetectionZoneGenerator,):
        assert not hasattr(cls, "LAT_MIN"), f"{cls.__name__} kept a private box"


def test_no_reference_generator_keeps_a_private_box():
    from opdi.reference.h3_airport_zones import AirportDetectionZoneGenerator
    from opdi.reference.h3_airport_layouts import AirportLayoutGenerator
    from opdi.reference.h3_runway_grid import RunwayGridGenerator
    for cls in (AirportDetectionZoneGenerator, AirportLayoutGenerator,
                RunwayGridGenerator):
        for attr in ("LAT_MIN", "LAT_MAX", "LON_MIN", "LON_MAX", "BBOX_OFFSET"):
            assert not hasattr(cls, attr), f"{cls.__name__}.{attr} still exists"


@pytest.mark.parametrize("worldwide,expect", [(False, {"EBBR"}),
                                              (True, {"EBBR", "KJFK", "NZAA", "WSSS"})])
def test_the_zone_airport_filter_follows_coverage(spark, worldwide, expect):
    from opdi.reference.h3_airport_zones import AirportDetectionZoneGenerator
    cfg = OPDIConfig.for_environment("opensky", worldwide=worldwide)
    gen = AirportDetectionZoneGenerator(spark, cfg)
    apt = spark.createDataFrame(
        [("EBBR", "large_airport", 50.90, 4.48),
         ("KJFK", "large_airport", 40.64, -73.78),
         ("NZAA", "large_airport", -37.01, 174.79),
         ("WSSS", "large_airport", 1.36, 103.99),
         ("EBCI", "small_airport", 50.46, 4.45)],
        "ident string, type string, latitude_deg double, longitude_deg double",
    )
    got = {r.ident for r in gen.filter_airports(apt).collect()}
    assert got == expect, "small_airport must be excluded in both modes"


def test_the_cell_centre_filter_follows_coverage_too(spark):
    """The sixth copy, and the one that would have made the other five look
    ineffective: `prepare_for_flight_list_spark` filters the H3 cells, so a
    European filter there drops every worldwide cell no matter how many
    airports the generator was given."""
    from opdi.reference.h3_airport_zones import AirportDetectionZoneGenerator
    gen = AirportDetectionZoneGenerator(
        spark, OPDIConfig.for_environment("opensky", worldwide=True))
    cells = spark.createDataFrame(
        [(50.9, 4.48), (-37.0, 174.8)], "lat double, lon double")
    assert cells.filter(gen.cell_filter(F.col("lat"), F.col("lon"))).count() == 2
```

- [ ] **Step 2: Run to verify failure**

Run: `.venv310/bin/python -m pytest tests/test_coverage_filters.py -v`
Expected: FAIL — `AirportDetectionZoneGenerator.LAT_MIN` still exists.

- [ ] **Step 3: Extract the filters into named methods, then delete the constants**

In `h3_airport_zones.py`, add two small methods to
`AirportDetectionZoneGenerator` and route every existing filter through them.
Named methods rather than inline expressions because the tests above need a
seam, and because `generate()`, `prepare_for_flight_list_spark()` and
`filter_loaded_airports()` each built the same predicate by copy-paste:

```python
    def filter_airports(self, sdf):
        """Aerodromes this coverage builds zones for.

        Type first, then geography. Both filters must be here: the type filter
        alone once widened the candidate set tenfold and OOM'd every executor,
        and the geography filter alone is what this task exists to make follow
        configuration.
        """
        cov = self.config.coverage
        return sdf.filter(
            F.col("type").isin(list(cov.airport_types))
            & cov.spark_filter(
                F.col("latitude_deg").cast("double"),
                F.col("longitude_deg").cast("double"),
                offset=True,
            )
        )

    def cell_filter(self, lat_col, lon_col):
        """Which generated cells are kept, by their centre.

        Uses the **same offset** as ``filter_airports``. It must: an aerodrome
        admitted by the offset whose cells were then clipped to the bare box
        would get a lopsided zone, dense on the inside edge and empty on the
        outside, and the asymmetry would be invisible in the table.
        """
        return self.config.coverage.spark_filter(lat_col, lon_col, offset=True)
```

Then:
* delete `BBOX_OFFSET`, `LAT_MIN`, `LAT_MAX`, `LON_MIN`, `LON_MAX` and
  `AIRPORT_TYPES` from the class, moving `AIRPORT_TYPES`' long provenance
  comment onto `CoverageConfig.airport_types` (Task 1 already carries a short
  version — replace it with the full one rather than losing the history);
* replace the filter at `:390-400` and at `:413-430` with `filter_airports`;
* replace the numpy bbox filter at `:660-661` with
  `self.config.coverage.pandas_mask(df.lat, df.lon, offset=True)`;
* replace the cell-centre filter at `:738-742` with `cell_filter`.

In `h3_airport_layouts.py` and `h3_runway_grid.py`, delete the same five
constants and replace both `fetch_airport_list` filters with
`config.coverage.spark_filter(..., offset=True)` plus
`F.col("type").isin(list(config.coverage.airport_types))`. The pandas branch of
`h3_airport_layouts.fetch_airport_list` (the `airports_url` override) uses
`config.coverage.pandas_mask`.

Update the print in all three from `"(within OPDI bbox)"` to
`f"(coverage: {self.config.coverage.label})"` — the old wording would say "within
OPDI bbox" during a worldwide build, which is the kind of log line that costs an
hour when a build looks wrong.

- [ ] **Step 4: Run the tests**

Run: `.venv310/bin/python -m pytest tests/test_coverage_filters.py tests/test_h3_airport_zones_types.py tests/test_h3_zones_v4.py tests/test_h3_airport_layouts.py tests/test_h3_runway_grid.py tests/test_h3_degenerate_ring.py -v`
Expected: PASS. If a pre-existing test pins `LAT_MIN` directly, rewrite it to
assert the same *behaviour* through `filter_airports` rather than deleting it.

- [ ] **Step 5: Commit**

```bash
git add src/opdi/reference/ src/opdi/coverage.py tests/test_coverage_filters.py
git commit -m "refactor(reference): one coverage filter for zones, layouts and the runway grid"
```

---

### Task 5: The antimeridian and the poles

Inside the European box, longitude arithmetic is safe. Worldwide it is not, and
the failure is **silent** — no exception, just missing cells.

**Measured, on h3-py 4.5.0, with the current `generate_circle_polygon`:**

| Aerodrome longitude | Ring radius | Cells produced | Cells expected | Loss |
|---|---|---|---|---|
| 179.34 (NFNL Labasa) | 30 NM | 2,089 | 2,089 | none |
| 179.34 | 110 NM | 20,213 | 27,998 | **28%** |
| 179.90 | 30 NM | 1,279 | 2,054 | **38%** |
| 179.90 | 110 NM | 14,986 | 27,694 | **46%** |
| -179.50 | 110 NM | 18,014 | 27,383 | **34%** |

"Expected" is the same call with longitudes normalised to [-180, 180]; a control
circle at longitude 4.0 gives 1,975 / 26,530, so the normalised numbers are right
and the raw ones are simply short. The cause is `calculate_point` returning
`math.degrees(lon_new_rad)` **unnormalised** — 180.035 for the first row above —
while `utils/geospatial.destination_point:230` already normalises. H3's own
transmeridian handling then works correctly on normalised input and cannot see
unnormalised input at all.

**Eight large or medium aerodromes are affected:** NFFN Nadi, NFNA Nausori,
NFNL Labasa, NGFU Funafuti, NZGS Gisborne, NZWO Wairoa, PMDY Henderson Field,
UHMA Ugolny. All eight are outside the European box, so this bug has never been
reachable and fixing it cannot change a published number.

**The poles.** OurAirports lists `NZSP` (Amundsen–Scott, **latitude -90.0**) as a
medium airport. A circle around a pole is a latitude ring whose vertices sweep
the full longitude range, which H3's polygon fill cannot represent as a cap. Five
aerodromes sit above 75°N (BGQQ, BGTL, CYLT, ENSB, UODN) and are fine — a 110 NM
ring from 82.5°N reaches 84.3°N. Only `|lat| > 85` is degenerate.

**Files:**
- Modify: `opdi/src/opdi/reference/h3_airport_zones.py:211-231` (`calculate_point`), and `filter_airports` from Task 4
- Modify: `opdi/src/opdi/reference/pbf_source.py:306-330` (`airport_boxes`)
- Test: `opdi/tests/test_zone_geometry_global.py`

**Interfaces:**
- Consumes: `filter_airports` (Task 4).
- Produces: `generate_circle_polygon` returns longitudes in [-180, 180];
  `POLAR_LIMIT_DEG = 85.0` on `AirportDetectionZoneGenerator`.

- [ ] **Step 1: Write the failing tests**

Create `opdi/tests/test_zone_geometry_global.py`:

```python
"""Zone geometry where longitude wraps and latitude runs out.

Every number here was measured on h3-py 4.5.0 before the fix, so each assertion
is a fact about the defect rather than a hope about the repair.
"""
import json

import h3
import pytest

from opdi.reference.h3_airport_zones import generate_circle_polygon


def _cells(lon, lat, radius_nm, res=7):
    gj = json.loads(generate_circle_polygon(lon, lat, radius_nm))
    return h3.polygon_to_cells(
        h3.LatLngPoly([(p[1], p[0]) for p in gj["coordinates"][0]]), res)


def test_every_vertex_is_a_valid_longitude():
    """Unnormalised longitudes are the whole defect. 179.34 + a 40 NM radius
    produced a vertex at 180.035, which H3 reads as a coordinate it has no
    handling for rather than as 179.965W."""
    gj = json.loads(generate_circle_polygon(179.34, -16.47, 110))
    lons = [p[0] for p in gj["coordinates"][0]]
    assert max(lons) <= 180.0
    assert min(lons) >= -180.0


def test_a_ring_on_the_dateline_is_not_a_third_short():
    """Measured before the fix: 14,986 cells against a control of 26,530 at the
    same latitude away from the dateline -- 44% missing, no error raised. The
    control is the right comparison because ring size varies with latitude, so
    an absolute count would only pin this one test machine."""
    control = len(_cells(4.0, -16.47, 110))
    dateline = len(_cells(179.9, -16.47, 110))
    assert dateline == pytest.approx(control, rel=0.10)


def test_the_western_side_of_the_dateline_works_too():
    """179.5W, not 179.5E. The two sides fail differently -- one overshoots
    +180 and the other undershoots -180 -- so one case does not cover both.
    Measured before the fix: 18,014 cells against a control of 26,530."""
    control = len(_cells(4.0, -16.47, 110))
    assert len(_cells(-179.5, -16.47, 110)) == pytest.approx(control, rel=0.10)


def test_a_small_ring_clear_of_the_dateline_is_unchanged():
    """The regression guard. NFFN at 177.44E/17.76S never crosses 180 even at
    110 NM, so the fix must not move it: 28,438 cells, measured on the unfixed
    code at exactly these coordinates."""
    assert len(_cells(177.44, -17.76, 110)) == pytest.approx(28438, rel=0.02)


def test_the_european_geometry_is_untouched():
    """Every published zone came from this path.

    Pinned to an absolute count measured on the *unfixed* code -- 2,185 cells
    for a 30 NM ring at EBBR, res 7 -- because the whole risk of this task is
    that a longitude change reaches inside the box. A relative comparison
    against another call of the same function would move with it and prove
    nothing.
    """
    assert len(_cells(4.48, 50.90, 30)) == 2185
    gj = json.loads(generate_circle_polygon(4.48, 50.90, 30))
    lons = [p[0] for p in gj["coordinates"][0]]
    assert min(lons) == pytest.approx(3.6877, abs=1e-3)
    assert max(lons) == pytest.approx(5.2723, abs=1e-3)


def test_a_pole_aerodrome_is_excluded_rather_than_built_wrong(spark):
    """NZSP is at latitude -90.0 and OurAirports calls it a medium airport.

    A circle around a pole is a latitude ring sweeping every longitude, which
    H3's polygon fill cannot express as a cap -- it produces a band, or
    nothing, depending on vertex order. Excluding it is honest; building it is
    a zone that claims to be somewhere it is not.
    """
    from opdi.config import OPDIConfig
    from opdi.reference.h3_airport_zones import AirportDetectionZoneGenerator
    from pyspark.sql import functions as F

    gen = AirportDetectionZoneGenerator(
        spark, OPDIConfig.for_environment("opensky", worldwide=True))
    apt = spark.createDataFrame(
        [("NZSP", "medium_airport", -90.0, 0.0),
         ("CYLT", "medium_airport", 82.52, -62.28),
         ("EBBR", "large_airport", 50.90, 4.48)],
        "ident string, type string, latitude_deg double, longitude_deg double",
    )
    got = {r.ident for r in gen.filter_airports(apt).collect()}
    assert got == {"CYLT", "EBBR"}, "85 deg is the limit; CYLT at 82.5 is fine"
```

- [ ] **Step 2: Run to verify failure**

Run: `.venv310/bin/python -m pytest tests/test_zone_geometry_global.py -v`
Expected: FAIL — `test_every_vertex_is_a_valid_longitude` (max lon 180.035),
`test_a_ring_on_the_dateline_is_not_a_third_short` (14,986 vs 26,530), and
`test_a_pole_aerodrome_is_excluded_rather_than_built_wrong` (NZSP kept).

- [ ] **Step 3: Normalise the longitude**

In `generate_circle_polygon`'s `calculate_point`, replace the return with:

```python
        # Normalised to [-180, 180], the same as
        # `utils.geospatial.destination_point` already does. Without this a
        # circle near the dateline returns vertices past 180, which H3 does not
        # read as wrapped longitudes -- it silently drops the cells beyond the
        # meridian. Measured at longitude 179.9: 14,986 cells instead of
        # 27,694, no error raised. H3's own transmeridian handling is correct
        # on normalised input, so nothing else needs to change.
        lon_new = ((math.degrees(lon_new_rad) + 180.0) % 360.0) - 180.0
        return [lon_new, math.degrees(lat_new_rad)]
```

- [ ] **Step 4: Exclude the poles**

Add to `AirportDetectionZoneGenerator`:

```python
    POLAR_LIMIT_DEG = 85.0
    """Beyond this latitude a circle is a cap, and H3 polygon fill has no cap.

    One aerodrome is affected in the whole OurAirports large+medium set: NZSP
    (Amundsen-Scott, latitude -90.0). The five above 75N are all fine -- a
    110 NM ring from 82.5N reaches only 84.3N. Excluded rather than built,
    because a cap built as a band is a zone claiming to cover somewhere it does
    not, and no join would report it.
    """
```

and extend Task 4's `filter_airports` — replacing its single `return` with the
polar split, so the whole method reads:

```python
    def filter_airports(self, sdf):
        """Aerodromes this coverage builds zones for.

        Type first, then geography, then latitude. All three must be here: the
        type filter alone once widened the candidate set tenfold and OOM'd
        every executor, the geography filter is what follows configuration, and
        the latitude filter is what keeps a pole out of a polygon fill that
        cannot express one.
        """
        cov = self.config.coverage
        kept = sdf.filter(
            F.col("type").isin(list(cov.airport_types))
            & cov.spark_filter(
                F.col("latitude_deg").cast("double"),
                F.col("longitude_deg").cast("double"),
                offset=True,
            )
        )
        polar = F.abs(F.col("latitude_deg")) > F.lit(self.POLAR_LIMIT_DEG)
        # Counted and named, never silent. An aerodrome that vanishes from a
        # reference table with no log line is indistinguishable from one OSM
        # never mapped, and the two need completely different responses.
        excluded = [r.ident for r in kept.filter(polar).select("ident").collect()]
        if excluded:
            print(f"  Excluded {len(excluded)} aerodrome(s) beyond "
                  f"{self.POLAR_LIMIT_DEG:g} deg latitude: {', '.join(excluded)}")
        return kept.filter(~polar)
```

The `collect()` is safe at any scale this can reach: it collects only the
aerodromes *beyond* 85 degrees, which is one row in the entire worldwide set.

- [ ] **Step 5: Fix the aerodrome boxes in `pbf_source`**

`airport_boxes` builds `lat_min`/`lat_max`/`lon_min`/`lon_max` per aerodrome
from its runway extent and pads them, then tests `lon.between(lon_min, lon_max)`
at `:439-440`. For an aerodrome straddling the dateline that interval is
inverted and matches nothing. Only NFNL, NGFU and UHMA are close enough for the
pad to reach 180, and none has runways crossing it — so the correct fix is the
cheap one: detect the inversion and widen to the full range, with a printed
count.

```python
    # An aerodrome whose padded box crosses the antimeridian gets an inverted
    # longitude interval, and `between` on an inverted interval matches nothing
    # -- the aerodrome silently gets no geometry, which looks exactly like an
    # aerodrome OSM never mapped. No large or medium aerodrome has runways
    # actually crossing 180, so widening to the full range costs a handful of
    # extra candidate features and cannot lose any.
    wrapped = out["lon_min"] > out["lon_max"]
    if wrapped.any():
        print(f"  {int(wrapped.sum())} aerodrome box(es) cross the antimeridian; "
              "widening their longitude range to the full circle.")
        out.loc[wrapped, "lon_min"] = -180.0
        out.loc[wrapped, "lon_max"] = 180.0
```

- [ ] **Step 6: Run the tests**

Run: `.venv310/bin/python -m pytest tests/test_zone_geometry_global.py tests/test_h3_zones_v4.py tests/test_h3_degenerate_ring.py tests/test_pbf_source.py -v`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add src/opdi/reference/h3_airport_zones.py src/opdi/reference/pbf_source.py \
        tests/test_zone_geometry_global.py
git commit -m "fix(zones): normalise longitude at the antimeridian, exclude the poles

Measured before: a 110 NM ring at longitude 179.9 produced 14,986 cells against
a control of 27,694 -- 46% missing, no error raised. Unreachable inside the
European box, so no published zone changes."
```

---

### Task 6: A coverage marker, so the two coverages cannot share a warehouse

The warehouse split (Task 1) is the first line of defence and is bypassable — an
operator can set `OPDI_WAREHOUSE_WORLD` to anything, including the European
prefix. This is the second line, and it is the one that catches the mistake after
it has already been made.

Without it, the failure is silent and expensive: worldwide rows appended to
`osn_statevectors_v2` under the European prefix are indistinguishable from
European rows, the tracks built from them get `track_id`s under the published
contract, and the only way to find out is to notice a flight in Auckland.

**Files:**
- Modify: `opdi/src/opdi/utils/storage.py`
- Modify: `opdi/src/opdi/runner.py` (`_step_00_reference_data`), `opdi/src/opdi/periodrun.py` (`preflight`)
- Test: `opdi/tests/test_coverage_marker.py`

**Interfaces:**
- Consumes: `config.coverage` (Task 1).
- Produces:
  - `StorageManager.write_coverage_marker() -> None`
  - `StorageManager.read_coverage_marker() -> Optional[dict]`
  - `StorageManager.check_coverage() -> Optional[str]` — a human-readable
    problem, or `None`

- [ ] **Step 1: Write the failing tests**

Create `opdi/tests/test_coverage_marker.py`:

```python
"""A warehouse remembers which coverage wrote it.

The marker is not for the pipeline -- nothing reads it at query time. It exists
so that pointing a worldwide run at the European warehouse fails on the first
step instead of on the day somebody notices a flight in Auckland inside a
dataset whose track_id contract says Europe.
"""
import pytest

from opdi.config import OPDIConfig
from opdi.utils.storage import StorageManager


def _mgr(spark, tmp_path, worldwide=False):
    cfg = OPDIConfig.for_environment("local")
    cfg.project.warehouse_path = str(tmp_path / "wh")
    if worldwide:
        cfg.coverage = OPDIConfig.for_environment("opensky", worldwide=True).coverage
    return StorageManager(spark, cfg)


def test_an_empty_warehouse_has_no_complaint(spark, tmp_path):
    """A first run must not be blocked by the absence of its own marker."""
    assert _mgr(spark, tmp_path).check_coverage() is None


def test_a_marker_round_trips(spark, tmp_path):
    m = _mgr(spark, tmp_path)
    m.write_coverage_marker()
    got = m.read_coverage_marker()
    assert got["coverage"] == "europe"
    assert got["bbox"] == list(m.config.coverage.bbox)


def test_matching_coverage_passes(spark, tmp_path):
    m = _mgr(spark, tmp_path)
    m.write_coverage_marker()
    assert _mgr(spark, tmp_path).check_coverage() is None


def test_worldwide_into_a_european_warehouse_is_refused(spark, tmp_path):
    """The failure this exists for, in the direction that does the damage."""
    _mgr(spark, tmp_path).write_coverage_marker()
    problem = _mgr(spark, tmp_path, worldwide=True).check_coverage()
    assert problem is not None
    assert "europe" in problem and "worldwide" in problem


def test_european_into_a_worldwide_warehouse_is_refused_too(spark, tmp_path):
    """Less damaging but equally wrong: the European rows would be a subset
    nobody can identify afterwards."""
    _mgr(spark, tmp_path, worldwide=True).write_coverage_marker()
    assert _mgr(spark, tmp_path).check_coverage() is not None


def test_the_problem_names_the_warehouse(spark, tmp_path):
    """An operator reading this message has two warehouse paths in play and
    needs to be told which one they hit."""
    _mgr(spark, tmp_path).write_coverage_marker()
    problem = _mgr(spark, tmp_path, worldwide=True).check_coverage()
    assert str(tmp_path) in problem
```

- [ ] **Step 2: Run to verify failure**

Run: `.venv310/bin/python -m pytest tests/test_coverage_marker.py -v`
Expected: FAIL with `AttributeError: 'StorageManager' object has no attribute 'check_coverage'`

- [ ] **Step 3: Implement**

Add to `StorageManager`. Written as a one-row parquet table through the existing
`write_table`, so it lands wherever the warehouse is and needs no new I/O path:

```python
    COVERAGE_MARKER = "_coverage_marker"
    """One row, recording which coverage wrote this warehouse.

    A leading underscore because it is not a data product: nothing reads it at
    query time, and a consumer enumerating tables should be able to tell at a
    glance that it is bookkeeping.
    """

    def write_coverage_marker(self) -> None:
        """Record which coverage this warehouse holds. Idempotent.

        Overwrite rather than append: this is a property of the warehouse, not
        a history of runs against it, and an append would leave two rows whose
        disagreement nothing resolves.
        """
        cov = self.config.coverage
        df = self.spark.createDataFrame(
            [(cov.label,
              [float(x) for x in cov.bbox] if cov.bbox else None,
              list(cov.airport_types))],
            "coverage string, bbox array<double>, airport_types array<string>",
        )
        self.write_table(df, self.COVERAGE_MARKER, mode="overwrite")

    def read_coverage_marker(self) -> Optional[dict]:
        """The marker, or ``None`` if this warehouse has never been stamped.

        Every failure reads as unstamped -- absent, unreadable, or empty. A
        marker that cannot be read must not block a run: the guard exists to
        catch a specific mistake, and a guard that fails closed on its own
        malfunction would make every warehouse unusable the first time the
        parquet read hiccuped.
        """
        try:
            rows = self.read_table(self.COVERAGE_MARKER).limit(1).collect()
        except Exception:
            return None
        if not rows:
            return None
        r = rows[0]
        return {
            "coverage": r["coverage"],
            "bbox": list(r["bbox"]) if r["bbox"] is not None else None,
            "airport_types": list(r["airport_types"]),
        }

    def check_coverage(self) -> Optional[str]:
        """A problem describing a coverage mismatch, or ``None``.

        An unstamped warehouse is **not** a problem: it is a first run, or a
        warehouse written before markers existed. Refusing those would make
        every existing prefix unusable, which is a bigger break than the one
        being prevented.
        """
        marker = self.read_coverage_marker()
        if marker is None:
            return None
        cov = self.config.coverage
        if marker["coverage"] == cov.label and marker["bbox"] == (
            [float(x) for x in cov.bbox] if cov.bbox else None
        ):
            return None
        return (
            f"coverage mismatch at {self.base_path}: the warehouse was written "
            f"with coverage {marker['coverage']!r} (bbox {marker['bbox']}) and "
            f"this run is {cov.label!r} (bbox "
            f"{list(cov.bbox) if cov.bbox else None}). Mixing two coverages in "
            "one warehouse is silent -- the rows are indistinguishable "
            "afterwards and the track_id contract only holds for one of them. "
            "Point this run at a different warehouse, or delete the existing "
            "tables if they are no longer wanted."
        )
```

Note the message names both coverages, both boxes, and the warehouse path: an
operator hitting this has two warehouse paths in play and needs to be told which
one they reached.

In `runner._step_00_reference_data`, call `check_coverage()` first and raise
`RuntimeError(problem)` on a non-`None` result, then `write_coverage_marker()`
after the substeps complete — after, so a failed reference build does not stamp a
warehouse it did not populate.

In `periodrun.preflight`, prepend the same check to the returned problem list so
a period run refuses before creating a session.

- [ ] **Step 4: Run the tests**

Run: `.venv310/bin/python -m pytest tests/test_coverage_marker.py tests/test_storage_partitions.py tests/test_storage_write_mode.py tests/test_periodrun.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/opdi/utils/storage.py src/opdi/runner.py src/opdi/periodrun.py \
        tests/test_coverage_marker.py
git commit -m "feat(storage): stamp coverage on a warehouse and refuse to mix"
```

---

### Task 7: Plumb `--worldwide` through the runners, and scale the zone build

**Files:**
- Modify: `opdi/src/opdi/cli.py:33-100`
- Modify: `opdi/src/opdi/runner.py` (`run_pipeline` signature; `_step_00c_airspaces`; `REFERENCE_SUBSTEPS` dispatch)
- Modify: `opdi/src/opdi/periodrun.py` (`run_period` signature)
- Modify: `opdi/src/opdi/reference/h3_airport_zones.py` (`ZONE_BUILD_PARTITIONS`)
- Test: `opdi/tests/test_coverage_config.py` (append), `opdi/tests/test_periodrun.py` (append)

**Interfaces:**
- Consumes: everything above.
- Produces: `opdi run --worldwide`; `run_pipeline(..., worldwide=False)`;
  `run_period(..., worldwide=False)`;
  `AirportDetectionZoneGenerator.zone_build_partitions(n_airports) -> int`.

**Partition scaling.** `ZONE_BUILD_PARTITIONS = 2000` was chosen for 21,712 rows
(16 rings × 1,357 aerodromes) to put ~11 rows in a task, because each row carries
an array of up to ~98,000 cell strings and 2,400 rows in a task measured ~7.7 GB —
every executor OOMKilled, exit 137. Worldwide is 16 × 5,280 = 84,480 rows, so a
fixed 2,000 puts 42 rows in a task: four times the memory that already killed
executors at 2,400. **Derive the count from the same ratio rather than hardcoding
a second number**, so the two can never disagree.

- [ ] **Step 1: Write the failing tests**

Append to `opdi/tests/test_coverage_config.py`:

```python
def test_the_zone_partition_count_scales_with_the_airport_count():
    """2,000 partitions was tuned for 1,357 aerodromes -- ~11 rows per task,
    because 2,400 rows in a task measured 7.7 GB and OOMKilled every executor.
    Worldwide is 3.89x the aerodromes, so a fixed 2,000 restores exactly the
    memory profile that number exists to avoid."""
    from opdi.reference.h3_airport_zones import AirportDetectionZoneGenerator as G
    assert G.zone_build_partitions(1357) == 2000        # Europe, unchanged
    assert G.zone_build_partitions(5280) >= 7000        # worldwide
    assert G.zone_build_partitions(50) == 2000          # floor, never fewer


def test_the_cli_accepts_worldwide():
    from opdi.cli import main
    import pytest
    with pytest.raises(SystemExit):
        main(["run", "--help"])   # smoke: the parser builds


def test_run_pipeline_takes_worldwide():
    import inspect
    from opdi.runner import run_pipeline
    sig = inspect.signature(run_pipeline)
    assert sig.parameters["worldwide"].default is False


def test_run_period_takes_worldwide():
    import inspect
    from opdi.periodrun import run_period
    assert inspect.signature(run_period).parameters["worldwide"].default is False
```

Append to `opdi/tests/test_periodrun.py`:

```python
def test_airspaces_are_skipped_worldwide():
    """00c's sources are PRU Atlas ANSP/FIR parquets, which cover Europe only,
    and opdi_h3_airspace_ref is read by nothing. Running it worldwide would
    write a European table into a worldwide warehouse and cost cluster time for
    a product with no consumer."""
    from opdi.runner import reference_substeps_for_coverage
    from opdi.config import OPDIConfig
    world = reference_substeps_for_coverage(
        OPDIConfig.for_environment("opensky", worldwide=True))
    assert world["00c"] is False
    europe = reference_substeps_for_coverage(OPDIConfig.for_environment("opensky"))
    assert europe["00c"] is True
```

- [ ] **Step 2: Run to verify failure**

Run: `.venv310/bin/python -m pytest tests/test_coverage_config.py tests/test_periodrun.py -v -k "worldwide or partition"`
Expected: FAIL.

- [ ] **Step 3: Implement the partition scaling**

Replace the constant with a classmethod, keeping its whole existing comment as
the docstring and adding the derivation:

```python
    #: Rows per partition the 2,000-partition figure implies: 21,712 / 2,000.
    #: Kept as the derived quantity so the two numbers cannot disagree.
    ZONE_ROWS_PER_TASK = 21712 / 2000    # ~10.9

    ZONE_BUILD_PARTITIONS_FLOOR = 2000

    @classmethod
    def zone_build_partitions(cls, n_airports: int, n_rings: int = 16) -> int:
        """Partitions for the ring polyfill, scaled to the build's size.

        [existing ZONE_BUILD_PARTITIONS comment, verbatim]

        Row count is a terrible proxy for cost here -- each row carries an
        *array* of cells and the outermost ring is ~98,250 of them -- so the
        figure that matters is rows per task, not partitions. 2,000 is the
        floor rather than the answer: it was right for 1,357 aerodromes, and
        the worldwide build has 5,280.
        """
        want = math.ceil(n_airports * n_rings / cls.ZONE_ROWS_PER_TASK)
        return max(cls.ZONE_BUILD_PARTITIONS_FLOOR, want)
```

`h3_airport_zones.py` already imports `math` (`generate_circle_polygon` uses it),
so no new import is needed. Call the method with the actual airport count and
`len(self.radii_nm)` at the existing `repartition` site, and print the chosen
value — a build that OOMs is much easier to diagnose when the log says how many
partitions it asked for.

- [ ] **Step 4: Implement the plumbing**

* `cli.py`: add `--worldwide` (`action="store_true"`, default `False`) with help
  text naming both consequences — no bounding box, and a separate warehouse.
  Pass it to `run_pipeline`.
* `runner.run_pipeline`: add `worldwide: bool = False`, pass it to
  `OPDIConfig.for_environment(env, worldwide=worldwide)`, and add
  `Coverage : {config.coverage.label}` and `Warehouse : {config.project.warehouse_path}`
  to the banner it already prints. Both lines matter: an operator who cannot see
  which warehouse a run is writing to has no way to catch the mistake Task 6
  exists to refuse.
* `runner`: add `reference_substeps_for_coverage(config) -> dict` returning the
  per-substep run flags with `00c` forced off when `config.coverage.is_worldwide`,
  and have `_step_00_reference_data` intersect it with the caller's `run_reference`.
  Print `Skipping 00c (airspaces): sources are European and nothing reads the table`
  so the skip is visible.
* `periodrun.run_period`: add `worldwide: bool = False` and thread it the same
  way. Its state file must distinguish coverages — add the coverage label to
  `RunState.matches` so a European run's progress cannot be mistaken for a
  worldwide one's. **This has bitten before:** a stale `weekrun_2026-06-01_1d.json`
  once made a run report five completed units and skip steps 02-04 entirely.

**The seventh bbox-adjacent leak, and it is in `periodrun`.** `run_period`
carries its own `warehouse: str = DEFAULT_WAREHOUSE` (`"s3a://eurocontrol/opdi-prod"`)
and at `periodrun.py:692` does `config.project.warehouse_path = warehouse`,
**overwriting whatever `for_environment` chose**. A worldwide `run_period` would
therefore write planet-wide rows straight into `opdi-prod` — the exact outcome
Task 1 Step 5's guard exists to prevent, arriving by a route that guard cannot
see. Fix it in `run_period`, before the assignment:

```python
    # `warehouse` defaults to the European prefix, so a worldwide run that did
    # not name one explicitly would land there. The default must follow the
    # coverage; an explicitly-passed warehouse still wins, because a caller who
    # named a prefix means it.
    if worldwide and warehouse == DEFAULT_WAREHOUSE:
        warehouse = config.project.warehouse_path   # set by for_environment
    config.project.warehouse_path = warehouse
```

Add a test for it in `tests/test_periodrun.py`:

```python
def test_a_worldwide_period_run_does_not_default_to_the_european_prefix():
    """periodrun overwrites config.project.warehouse_path with its own default,
    which is the European prefix. Without this the --worldwide flag would be
    silently discarded at the one entry point production actually uses."""
    import inspect
    from opdi.periodrun import run_period, DEFAULT_WAREHOUSE
    assert DEFAULT_WAREHOUSE == "s3a://eurocontrol/opdi-prod"
    assert inspect.signature(run_period).parameters["worldwide"].default is False
    # Behavioural half: resolve the warehouse the way run_period does.
    from opdi.config import OPDIConfig
    world = OPDIConfig.for_environment("opensky", worldwide=True)
    assert world.project.warehouse_path != DEFAULT_WAREHOUSE
```

**`reference_substeps_to_run` also needs the coverage filter.** It derives its
per-substep flags from `REFERENCE_OUTPUTS`, which includes
`"00c": ("opdi_h3_airspace_ref",)`. In a fresh worldwide warehouse that table is
absent, so it would return `{"00c": True}` and build the European airspace layer
anyway — the skip in `_step_00_reference_data` is not enough on its own. Compose
the two:

```python
    flags = reference_substeps_to_run(storage, force=force)
    for substep, run in reference_substeps_for_coverage(config).items():
        if not run:
            flags[substep] = False
```

- [ ] **Step 5: Run the tests**

Run: `.venv310/bin/python -m pytest tests/ -q`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/opdi/cli.py src/opdi/runner.py src/opdi/periodrun.py \
        src/opdi/reference/h3_airport_zones.py tests/
git commit -m "feat(cli): --worldwide, and a zone partition count that scales with it"
```

---

### Task 8: Build the worldwide OSM extract

Operational, not code. No cluster, no credentials, one long download. Do it
early so it overlaps with the cluster tasks rather than blocking them.

**Files:** none. Produces `/home/jupyter/work/osm/aeroway-planet.osm.pbf`.

- [ ] **Step 1: Check there is room**

```bash
df -h /home/jupyter | tail -1
```
Expected: at least 120 GB available. Measured 2026-09-20: 378 GB free against a
94.86 GB download — comfortable.

- [ ] **Step 2: Download the planet**

```bash
cd /home/jupyter/work/osm
curl -L --retry 5 --retry-delay 30 -C - \
  -o planet-latest.osm.pbf \
  https://planet.openstreetmap.org/pbf/planet-latest.osm.pbf
```
`-C -` resumes a partial file, which matters at ~84 GB. Expect a few hours.
**Measured 2026-09-20: the snapshot is `planet-260914.osm.pbf`, 94,857,311,258
bytes (94.86 GB / 88.3 GiB).** Verify the finished file matches that size before
filtering — a truncated download produces an osmium error that reads like a
corrupt file. The controller started this download at 19:28 on 2026-09-20 at
~70 MB/s; check whether it already completed before starting another.

- [ ] **Step 3: Filter it to aeroway geometry**

```bash
cd /home/jupyter/work/opdi-workspace/opdi
.venv310/bin/python -c "
from opdi.reference.pbf_filter import filter_aeroway_pbf
s = filter_aeroway_pbf('/home/jupyter/work/osm/planet-latest.osm.pbf',
                       '/home/jupyter/work/osm/aeroway-planet.osm.pbf')
print(s)
"
```
Expected: completes without OOM and writes a file in the tens of MB. **Measured
2026-09-20: 66.5 MB** (nodes 6,177,529 / ways 721,571 / relations 3,973), 64 min,
peak RSS 5.9 GB. Europe's 34.9 GB filtered to 16.1 MB, so the planet is 4.1x
Europe — consistent with 5,280 vs 1,357 aerodromes. Plausible range 40-100 MB.

**Do not judge success by size.** A first attempt was OOM-killed mid-write and
left a 22.9 MB file — comfortably "plausible" — containing 2.64 M nodes and
**zero ways, zero relations**. A PBF is laid out nodes-then-ways-then-relations,
so a truncated file looks right and carries none of the geometry. Assert
ways > 0 AND relations > 0 before any downstream use. **A result under 20 MB or over 500 MB means something went
wrong** — check the node/way/relation counts in the returned stats against
Europe's before proceeding.

The filter is multi-pass over the whole file at roughly 74 MB/s, so budget
45-90 minutes for 94.86 GB. It runs in this 16 GB container by design: it never calls
`with_areas()` on the big file, which is what made the raw Europe extract fatal.

- [ ] **Step 4: Confirm a non-European aerodrome is present**

```bash
cd /home/jupyter/work/opdi-workspace/opdi
.venv310/bin/python -c "
from opdi.reference.pbf_source import PbfLayoutSource
src = PbfLayoutSource('/home/jupyter/work/osm/aeroway-planet.osm.pbf', None)
for icao in ('EBBR', 'KJFK', 'WSSS', 'NZAA', 'FAOR'):
    print(icao, len(src.features_for(icao)))
"
```
Expected: non-zero for all five. **This is the step that proves the download was
worth it**: EBBR alone proves nothing, because the Europe extract already had it.
If KJFK is zero, the filter dropped the wrong things and Task 9 would build a
worldwide layout table that is quietly Europe-only.

- [ ] **Step 5: Point the configuration at it**

```bash
export OPDI_OSM_PBF=/home/jupyter/work/osm/aeroway-planet.osm.pbf
```
Record this in the run log. `periodrun.preflight_fresh_build` already refuses to
start without a readable extract, so a typo here fails fast rather than falling
back to Overpass — which resolves airport *names* through Nominatim, returns no
data rather than an error, and refuses outright at this scale.

- [ ] **Step 6: Note it in the run log**

Append to `opdi/benchmarks/EVENTS_RUN_LOG.md`, continuing its numbering: the
planet extract's source URL, its download date, its filtered size, and the
five-aerodrome check's result.

---

### Task 9: Build the worldwide reference data

The first cluster task. It is also the first time any of Tasks 1-7 runs against
real data, so treat a surprise here as a bug in those tasks rather than a
configuration problem.

**Files:** none. Produces `oa_airports`, `oa_runways`, `aircraft_db`,
`h3_airport_detection_zones`, `hexaero_airport_layouts`, `h3_runway_zones`
under the worldwide warehouse.

- [ ] **Step 1: Dry-run the preflight, with no cluster**

```bash
cd /home/jupyter/work/opdi-workspace/opdi
.venv310/bin/python -c "
from opdi.config import OPDIConfig
from opdi.periodrun import preflight_fresh_build
c = OPDIConfig.for_environment('opensky', worldwide=True)
print('coverage :', c.coverage.label, c.coverage.bbox)
print('warehouse:', c.project.warehouse_path)
print('problems :', preflight_fresh_build(c))
"
```
Expected: `worldwide None`, a warehouse path ending `-world`, and no problems.
**If the warehouse path contains `opdi-prod`, stop** — `OPDI_WAREHOUSE` has
leaked and Task 1 Step 5's guard is not doing its job.

- [ ] **Step 2: Build the reference set**

```bash
cd /home/jupyter/work/opdi-workspace/opdi
OPDI_OSM_PBF=/home/jupyter/work/osm/aeroway-planet.osm.pbf \
PYTHONUNBUFFERED=1 \
  .venv310/bin/python -m opdi.cli run --env opensky --worldwide --step 00 \
  --start 2026-06-01 --end 2026-06-02 2>&1 | tee /tmp/world-ref.log
```

`PYTHONUNBUFFERED=1` is not optional when redirecting: without it Python blocks
stdout and the step markers do not appear until the process exits, which makes a
multi-hour build indistinguishable from a hung one.

Expected, in order: 00d ingests OurAirports; 00e the aircraft database; 00a
prints ~5,280 aerodromes and the polar exclusion line naming NZSP; 00b processes
~5,280 with a non-trivial failure count (aerodromes OSM has not mapped — expected,
and printed, not an error); 00f processes ~5,280; **00c is skipped with its
reason printed**.

- [ ] **Step 3: Verify the tables, and that they agree with each other**

```bash
cd /home/jupyter/work/opdi-workspace/opdi
.venv310/bin/python - <<'PY'
from opdi.config import OPDIConfig
from opdi.utils.storage import StorageManager
from opdi.pipeline.segmentation import *   # session helper per current code
# build a session the same way benchmarks do, then:
for t in ("oa_airports", "h3_airport_detection_zones",
          "hexaero_airport_layouts", "h3_runway_zones"):
    df = storage.read_table(t)
    print(f"{t:35s} {df.count():>12,}")
# The check that matters: the three H3 tables must cover the same aerodromes.
z = {r[0] for r in storage.read_table("h3_airport_detection_zones")
     .select("apt_ident").distinct().collect()}
g = {r[0] for r in storage.read_table("h3_runway_zones")
     .select("apt_icao").distinct().collect()}   # note: apt_icao here, apt_ident in zones
print("zones only:", len(z - g), " grid only:", len(g - z))
PY
```

Expected: `h3_airport_detection_zones` on the order of 125 M cells — Europe's is
~32 M at 110 NM reach and the build is 3.89× — and a few GB. **`zones only` and
`grid only` should be small and explainable** (the grid needs runway geometry,
which not every medium aerodrome has). A large asymmetry means Task 4 left one
generator on a different airport set, which is the failure `AIRPORT_TYPES`
documents.

- [ ] **Step 4: Confirm coverage actually widened**

```bash
# Any of these being zero means the widening did not reach that table.
for t in h3_airport_detection_zones h3_runway_zones hexaero_airport_layouts; do
  echo "$t: KJFK / WSSS / NZAA present?"
done
```
Check each table for `KJFK`, `WSSS`, `NZAA` and `SBGR`. **Also check `NFNL`** —
it is the dateline aerodrome, and its zone cell count should be comparable to a
same-latitude aerodrome away from the meridian rather than a third short. This is
Task 5 verified on real data rather than in a unit test.

- [ ] **Step 5: Record what it cost**

Append to `EVENTS_RUN_LOG.md`: wall clock per substep, executor count, the
aerodrome counts 00a/00b/00f each reported, 00b's failure count, and the final
table sizes. Task 10 needs these to size its own run.

---

### Task 10: Process one worldwide day

**Files:** none. Produces one day under the worldwide warehouse.

**MEASURED 2026-09-20: the worldwide/Europe row ratio is 2.73x** (three hours of
2026-06-01 at 06Z/12Z/18Z: 1.95x, 2.50x, 3.86x; 108,419,585 vs 39,700,657 rows).
Step 1 below is therefore already done. The projection for one worldwide day is
~24 GB across all tables. The rest of this section is kept because its reasoning
is what produced the right prediction.

**The number nobody knows yet is the volume.** Measured on the European runs:
~0.98 GB/day of state vectors and ~3.90 GB/day of tracks (10.77 GB and 42.93 GB
over 11 days). Worldwide is **not** simply 3.89× that — the airport ratio does
not predict the traffic ratio, and OpenSky's receiver network is densest over
Europe, so the global row count is likely 2.5-4× rather than proportional to
airports. Step 1 below measures it before anything is written, because the
bucket has a hard quota and hitting it mid-run produces
`XMinioAdminBucketQuotaExceeded` halfway through a day.

- [ ] **Step 1: Measure the day before writing it**

```bash
cd /home/jupyter/work/opdi-workspace/opdi
.venv310/bin/python - <<'PY'
# Read one hour of the OSN archive, count rows with and without the European
# box, and multiply. One hour is enough for the ratio and costs minutes.
from datetime import datetime
from opdi.config import OPDIConfig
from opdi.coverage import EUROPE_BBOX
from pyspark.sql import functions as F
# build session, then:
hour = int(datetime(2026, 6, 1, 12).timestamp())
df = spark.read.parquet(f"s3a://opensky-hdfs-backup/tables_v4/state_vectors/hour={hour}")
total = df.count()
eu = df.filter(
    (F.col("lon") >= EUROPE_BBOX[0]) & (F.col("lon") <= EUROPE_BBOX[2])
    & (F.col("lat") >= EUROPE_BBOX[1]) & (F.col("lat") <= EUROPE_BBOX[3])
).count()
print(f"worldwide {total:,}  europe {eu:,}  ratio {total/eu:.2f}x")
PY
```

Record the ratio. Multiply the European per-day figures by it to project the
day's footprint, then check it against the bucket's remaining headroom **before
Step 3**. If the projection does not fit, say so and stop — scaling the run down
is the user's call, not this plan's.

- [ ] **Step 2: Confirm the reference set is complete**

```bash
.venv310/bin/python -c "
from opdi.config import OPDIConfig
from opdi.periodrun import preflight, REQUIRED_REFERENCE_TABLES
# ... build storage against the worldwide warehouse
print('missing:', preflight(storage))
"
```
Expected: `[]`. `preflight` treats a present-but-empty table as missing, which is
the case that matters: a stage whose executors all die still commits a zero-row
part file and a `_SUCCESS` marker, and the table then reads cleanly and contains
nothing.

- [ ] **Step 3: Run the day**

```bash
cd /home/jupyter/work/opdi-workspace/opdi
OPDI_OSM_PBF=/home/jupyter/work/osm/aeroway-planet.osm.pbf \
PYTHONUNBUFFERED=1 \
  .venv310/bin/python -m opdi.cli run --env opensky --worldwide \
  --start 2026-06-01 --end 2026-06-02 \
  --no-airport-zones --no-airport-layouts --no-ourairports \
  --no-aircraft-db 2>&1 | tee /tmp/world-day.log
```

Steps 01, 02, 02a, 03, 04, 04b, with reference generation skipped — Task 9 built
it. **Start with 8 executors, not 13.** Thirteen sits exactly at the namespace
CPU ceiling and produced `ApiCallTimeoutException` hot-prefix contention against
the OpenSky S3 endpoint on the European runs; eight ran clean.

While it runs, judge progress by **shuffle bytes and failed-task count over
time**, not by the stage counter — wave completion makes the counter freeze for
minutes and then jump (23→55 tasks in one 45 s window was observed after 90 s of
apparent stall).

- [ ] **Step 4: Check the output is worldwide, not Europe wearing a new path**

```bash
.venv310/bin/python - <<'PY'
fl = storage.read_table("opdi_flight_list")
print("flights:", fl.count())
fl.groupBy("adep").count().orderBy(F.desc("count")).show(25, False)
print("OOA rows (must be 0):", fl.filter(F.col("adep") == "OOA").count())
PY
```

Expected, and each of these is falsifiable — if one fails, report what failed
rather than the numbers that passed:

* The busiest ADEPs include non-European aerodromes. If the top 25 are all
  European, the ingest box did not widen and Task 2 is not doing its job.
* **Zero rows carry `OOA`.** That is Task 3's user-visible contract.
* The flight count is materially above a European day's. If it is roughly equal,
  the day was filtered somewhere the plan did not find — search for a seventh
  bbox copy before concluding anything else.
* The event table contains `off-block`/`on-block` at non-European aerodromes,
  which is Task 8's planet extract proving itself end to end.

- [ ] **Step 5: Write it down**

Append to `EVENTS_RUN_LOG.md`: the measured worldwide/Europe row ratio, wall
clock per step, executor count, the day's footprint by table, and the four
checks above with their actual values.

---

### Task 11: `opdi-benchmarker` follows the coverage

The benchmarker reads the pipeline's tables and caches its own derived ones. It
has **three** S3 bases, and exactly two of them must follow coverage:

| Base | Default | Follows coverage? |
|---|---|---|
| `SOURCE_BASE` | `s3a://eurocontrol/opdi-prod` | **Yes** — it is the pipeline warehouse, and Task 1 gave worldwide its own |
| `CACHE_BASE` | `s3a://eurocontrol/opdi/research/benchmarker/v1` | **Yes** — see below, this is the one that bites |
| `REFERENCE_BASE` | `s3a://eurocontrol/opdi/research/reference` | **No** — APDF is European whatever the pipeline covered |

**`CACHE_BASE` is the subtle one and the reason this is a task rather than a
line.** The cache is partitioned by `dof=`, so a worldwide build of 2026-06-01
writes to *the same prefix* as the European build of 2026-06-01 and replaces it.
Nothing errors. The Streamlit app then renders worldwide numbers under a
provenance stamp that says Europe, and the only way to notice is to spot Auckland
in a movements table. This is the same failure the warehouse split and the
coverage marker exist to prevent, arriving through the benchmarker's own cache.

**What does *not* need changing, verified by reading the code.** The benchmarker
already has a no-truth path: `config.apdf_month_for(day)` returns `None` outside
`APDF_MONTHS`, and `build/cli.py:106` builds those days with every detection
labelled `opdi_only`. Nothing filters to European aerodromes; the Europe mentions
in `queries.py:296` and `movements.py:37` are explanatory comments and EBBR
calibration anecdotes, not predicates. So worldwide is a *path* problem here,
plus one interpretation problem (Step 6).

**Files:**
- Modify: `opdi-benchmarker/src/opdi_benchmarker/config.py:16-45`
- Modify: `opdi-benchmarker/src/opdi_benchmarker/build/cli.py:196-210` (`main`), and the build entry
- Modify: `opdi-benchmarker/src/opdi_benchmarker/app/Home.py:20-31`
- Modify: `opdi-benchmarker/src/opdi_benchmarker/provenance.py:75-80`
- Modify: `opdi-benchmarker/DATASETS.md:8`, `:32-35`
- Test: `opdi-benchmarker/tests/test_config.py`, `opdi-benchmarker/tests/test_cli.py`

**Interfaces:**
- Consumes: the worldwide warehouse path from Task 1, and the
  `_coverage_marker` table from Task 6.
- Produces:
  - `config.COVERAGE: str` — `"europe"` or `"worldwide"`, from
    `OPDI_BENCHMARKER_COVERAGE`, defaulting to `"europe"`
  - `config.set_coverage(label: str) -> None` — rebinds `CACHE_BASE` and
    `SOURCE_BASE`; raises on an unknown label
  - `config.is_worldwide() -> bool`
  - `opdi-benchmarker build --worldwide`

**This repo has its own venv:** `/home/jupyter/work/opdi-workspace/opdi-benchmarker/.venv`.
Run its tests with `.venv/bin/python -m pytest tests/ -q` from the
`opdi-benchmarker` directory. Do **not** use `opdi/.venv310` — different project,
different dependencies.

**Rebinding module globals is safe here, and that was checked.** Every consumer
reads `config.CACHE_BASE` / `config.SOURCE_BASE` as a module attribute at call
time (`read.py:79`, `Home.py:23`, `config.table_path`, `config.source_table`).
The only `from ... config import` forms in the tree pull `S3_ENDPOINT`,
`EVENT_VERSION_ORDER` and `newest_version`, none of which is coverage-dependent.
If that ever changes, `set_coverage` becomes a silent no-op for the module that
captured the value — so the test below asserts the rebind is visible through
`table_path()`, not just through the global.

- [ ] **Step 1: Write the failing tests**

Append to `opdi-benchmarker/tests/test_config.py`:

```python
import importlib


def _reload(monkeypatch, **env):
    """Re-import config under a given environment.

    The bases are computed at import time, so an env var set afterwards has no
    effect -- which is exactly the bug this helper exists to catch if someone
    later moves the computation.
    """
    for k, v in env.items():
        if v is None:
            monkeypatch.delenv(k, raising=False)
        else:
            monkeypatch.setenv(k, v)
    import opdi_benchmarker.config as c
    return importlib.reload(c)


def test_the_default_coverage_is_europe(monkeypatch):
    c = _reload(monkeypatch, OPDI_BENCHMARKER_COVERAGE=None,
                OPDI_WAREHOUSE=None, OPDI_BENCHMARKER_CACHE=None)
    assert c.COVERAGE == "europe"
    assert c.is_worldwide() is False
    assert c.SOURCE_BASE == "s3a://eurocontrol/opdi-prod"
    assert c.table_path("trajectory") == (
        "s3a://eurocontrol/opdi/research/benchmarker/v1/trajectory")


def test_worldwide_moves_both_the_source_and_the_cache(monkeypatch):
    """Both, not just the source.

    The cache is partitioned by `dof=`, so a worldwide 2026-06-01 written to
    the European cache prefix REPLACES the European 2026-06-01 and the app
    renders worldwide numbers under a European provenance stamp.
    """
    c = _reload(monkeypatch, OPDI_BENCHMARKER_COVERAGE="worldwide",
                OPDI_WAREHOUSE=None, OPDI_BENCHMARKER_CACHE=None)
    assert c.is_worldwide() is True
    assert c.SOURCE_BASE.endswith("opdi-world")
    assert "world" in c.table_path("trajectory")
    assert c.table_path("trajectory") != (
        "s3a://eurocontrol/opdi/research/benchmarker/v1/trajectory")


def test_the_reference_base_does_not_move(monkeypatch):
    """APDF is European whatever the pipeline covered. Moving it would point a
    worldwide build at a prefix holding no extracts at all, and every day would
    silently build as OPDI-only -- which looks exactly like a day outside
    APDF_MONTHS."""
    eu = _reload(monkeypatch, OPDI_BENCHMARKER_COVERAGE=None).REFERENCE_BASE
    w = _reload(monkeypatch, OPDI_BENCHMARKER_COVERAGE="worldwide").REFERENCE_BASE
    assert eu == w == "s3a://eurocontrol/opdi/research/reference"


def test_opdi_warehouse_does_not_steer_a_worldwide_build(monkeypatch):
    """Same precedence rule as the pipeline's `_worldwide_warehouse`.

    Two different rules for one variable across two repos is a trap: an
    operator with OPDI_WAREHOUSE exported -- the normal state -- would get
    coverage-correct paths from the pipeline and European ones here.
    """
    c = _reload(monkeypatch, OPDI_BENCHMARKER_COVERAGE="worldwide",
                OPDI_WAREHOUSE="s3a://eurocontrol/opdi-prod")
    assert "opdi-prod" not in c.SOURCE_BASE


def test_an_explicit_worldwide_warehouse_still_wins(monkeypatch):
    c = _reload(monkeypatch, OPDI_BENCHMARKER_COVERAGE="worldwide",
                OPDI_WAREHOUSE_WORLD="s3a://eurocontrol/scratch-world")
    assert c.SOURCE_BASE == "s3a://eurocontrol/scratch-world"


def test_set_coverage_is_visible_through_the_accessors(monkeypatch):
    """Not just through the globals. If a module ever captures a base with
    `from config import CACHE_BASE`, set_coverage becomes a silent no-op for
    it, and this is the assertion that would still catch the rebind failing
    where it matters."""
    c = _reload(monkeypatch, OPDI_BENCHMARKER_COVERAGE=None,
                OPDI_WAREHOUSE=None, OPDI_BENCHMARKER_CACHE=None)
    before = c.table_path("trajectory")
    c.set_coverage("worldwide")
    assert c.table_path("trajectory") != before
    assert c.source_table("osn_tracks").endswith("opdi-world/osn_tracks")


def test_set_coverage_rejects_an_unknown_label(monkeypatch):
    """A typo must not fall through to the European paths -- that would write
    worldwide data into the published cache, which is the whole risk."""
    c = _reload(monkeypatch, OPDI_BENCHMARKER_COVERAGE=None)
    with pytest.raises(ValueError, match="coverage"):
        c.set_coverage("global")


def test_an_unknown_coverage_env_value_raises_at_import(monkeypatch):
    with pytest.raises(ValueError, match="coverage"):
        _reload(monkeypatch, OPDI_BENCHMARKER_COVERAGE="Worldwide!")
```

Append to `opdi-benchmarker/tests/test_cli.py`:

```python
def test_build_accepts_worldwide_and_sets_the_coverage(monkeypatch):
    """The flag must take effect before any table path is resolved, or the
    build reads the European warehouse and writes the European cache while
    reporting that it ran worldwide."""
    from opdi_benchmarker import config
    from opdi_benchmarker.build import cli

    seen = {}
    monkeypatch.setattr(config, "set_coverage",
                        lambda label: seen.setdefault("label", label))
    monkeypatch.setattr(cli, "_build", lambda *a, **k: 0)   # name per current code
    cli.main(["build", "--from", "2026-06-01", "--to", "2026-06-01", "--worldwide"])
    assert seen["label"] == "worldwide"


def test_build_without_the_flag_leaves_coverage_alone(monkeypatch):
    from opdi_benchmarker import config
    from opdi_benchmarker.build import cli

    calls = []
    monkeypatch.setattr(config, "set_coverage", lambda label: calls.append(label))
    monkeypatch.setattr(cli, "_build", lambda *a, **k: 0)
    cli.main(["build", "--from", "2026-06-01", "--to", "2026-06-01"])
    assert calls == []
```

**Before writing the second file's tests, read `build/cli.py` to confirm the
name of the function `main` dispatches to** — the stub above calls it `_build`.
If the structure does not admit a clean monkeypatch seam, assert on
`config.COVERAGE` after `main` returns instead, and say so in the report.

- [ ] **Step 2: Run to verify failure**

Run: `cd /home/jupyter/work/opdi-workspace/opdi-benchmarker && .venv/bin/python -m pytest tests/test_config.py tests/test_cli.py -v`
Expected: FAIL with `AttributeError: module 'opdi_benchmarker.config' has no attribute 'COVERAGE'`

- [ ] **Step 3: Make the bases coverage-aware**

Rewrite the base computation in `config.py` as a single function so the three
bases cannot be derived by three different rules:

```python
#: Which pipeline coverage this benchmarker reads.
#:
#: ``europe`` (default) or ``worldwide``. Set by
#: ``OPDI_BENCHMARKER_COVERAGE`` -- an environment variable rather than a flag
#: alone, because the Streamlit app has no argv and must be able to point at a
#: worldwide cache too.
COVERAGE = os.environ.get("OPDI_BENCHMARKER_COVERAGE", "europe")

_COVERAGES = ("europe", "worldwide")


def _bases(coverage: str):
    """``(cache_base, source_base)`` for a coverage. The only rule.

    ``REFERENCE_BASE`` is deliberately absent: APDF is European whatever the
    pipeline covered, and moving it would point a worldwide build at a prefix
    holding no extracts, so every day would build as OPDI-only -- which is
    indistinguishable from a day outside ``APDF_MONTHS``.

    ``OPDI_WAREHOUSE`` is ignored under worldwide, matching
    ``opdi.config._worldwide_warehouse``. One variable must not mean two
    different things in two repos.
    """
    if coverage not in _COVERAGES:
        raise ValueError(
            f"unknown coverage {coverage!r}; expected one of {_COVERAGES}. "
            "Falling back to the European paths would write worldwide data "
            "into the published cache."
        )
    if coverage == "worldwide":
        cache = os.environ.get(
            "OPDI_BENCHMARKER_CACHE_WORLD",
            "s3a://eurocontrol/opdi-world/research/benchmarker")
        source = os.environ.get(
            "OPDI_WAREHOUSE_WORLD", "s3a://eurocontrol/opdi-world")
    else:
        cache = os.environ.get(
            "OPDI_BENCHMARKER_CACHE",
            "s3a://eurocontrol/opdi/research/benchmarker")
        source = os.environ.get("OPDI_WAREHOUSE", "s3a://eurocontrol/opdi-prod")
    return cache.rstrip("/") + "/" + CACHE_VERSION, source.rstrip("/")


CACHE_BASE, SOURCE_BASE = _bases(COVERAGE)


def is_worldwide() -> bool:
    return COVERAGE == "worldwide"


def set_coverage(label: str) -> None:
    """Switch coverage at runtime, for the ``--worldwide`` flag.

    Must be called before any table path is resolved. Rebinding the module
    globals is sound because every consumer reads them as module attributes at
    call time; a module that captured one with ``from config import
    CACHE_BASE`` would not see this, which is why
    ``test_set_coverage_is_visible_through_the_accessors`` asserts through
    ``table_path()`` rather than through the global.
    """
    global COVERAGE, CACHE_BASE, SOURCE_BASE
    CACHE_BASE, SOURCE_BASE = _bases(label)   # raises before any rebind
    COVERAGE = label
```

Note the ordering in `set_coverage`: `_bases` is called first, so an invalid
label raises with every global still consistent. Assigning `COVERAGE` first
would leave the module claiming a coverage its paths do not implement.

- [ ] **Step 4: Add the flag**

In `build/cli.py`'s `main`, add to the `build` subparser:

```python
    b.add_argument(
        "--worldwide", action="store_true", default=False,
        help="Read the worldwide pipeline warehouse and write the worldwide "
             "cache. Off by default. Equivalent to "
             "OPDI_BENCHMARKER_COVERAGE=worldwide, which is how the Streamlit "
             "app is switched (it has no argv).",
    )
```

and, **before anything resolves a path**:

```python
    if args.worldwide:
        config.set_coverage("worldwide")
```

Print the resolved coverage, cache base and source base in the build's opening
log lines. An operator who cannot see which prefixes a build is reading and
writing has no way to catch the mistake this task exists to prevent.

- [ ] **Step 5: Refuse a warehouse whose coverage disagrees**

Task 6 stamps `_coverage_marker` into every warehouse the pipeline writes. The
benchmarker reads that warehouse, so it can check it — and it is the cheapest
possible guard against the remaining hole, which is an operator pointing
`OPDI_WAREHOUSE_WORLD` at the European prefix.

In the build path, after the Spark session exists and before any table is read:

```python
def check_source_coverage(spark) -> Optional[str]:
    """A problem if the warehouse's stamped coverage is not ours, else None.

    An unstamped warehouse is not a problem -- it predates the marker. Only a
    stamp that positively disagrees blocks the build.
    """
```

Read `f"{config.SOURCE_BASE}/_coverage_marker"` as parquet, catching every
exception as "unstamped". Raise a `RuntimeError` naming both coverages and the
prefix when it disagrees.

- [ ] **Step 6: Say what `opdi_only` means worldwide**

`metrics.py:86` counts `n_extra` as rows with `match_side = 'opdi_only'`, and
`pages/3_Movements.py:55-56` already explains that APDF is a *reporting* dataset
covering roughly ninety European aerodromes while OPDI produces flights for the
whole box. **Worldwide that caveat stops being a footnote and becomes the
headline**: every movement outside those ninety aerodromes is `opdi_only`, so
`n_extra` no longer estimates ghost tracks — it mostly counts flights APDF was
never going to report. A reader comparing a worldwide `n_extra` against the
European 1.42x/1.01x figures in `movements.py:37` would draw a conclusion the
data does not support.

In `app/Home.py`, next to the existing `config.CACHE_BASE` line, show the
coverage, and when `config.is_worldwide()` render a standing caveat:

> Worldwide coverage. APDF reports roughly ninety European aerodromes, so
> every movement elsewhere is `opdi_only` by construction. The `n_extra` and
> movement-ratio figures are **not** comparable with the European ones and do
> not estimate ghost tracks here.

Add the same sentence to `pages/3_Movements.py` under its existing note, and
gate both on `is_worldwide()` so the European app is untouched.

- [ ] **Step 7: Stamp coverage into provenance**

`provenance.py:77-79` already records `argv`, `git_sha` and `git_dirty`. Add
`"coverage": config.COVERAGE` and `"source_base": config.SOURCE_BASE`. A cached
table whose manifest does not say which warehouse produced it cannot be
attributed afterwards, and the whole point of the manifest — per `CLAUDE.md`,
after three staged CSVs were found to derive from tables written days earlier by
different parameters — is that an output carries what produced it.

Importing `config` into `provenance` must not create a cycle: `provenance`
currently imports `S3_ENDPOINT` *from* `config`, and `config` imports nothing
from `provenance`, so `from opdi_benchmarker import config` is safe. Confirm by
running the suite, not by reading.

- [ ] **Step 8: Update `DATASETS.md`**

Add a worldwide row to the source table (`:32-35`) and a line at `:8` stating
that the base prefix shown is the European one, that
`OPDI_BENCHMARKER_COVERAGE=worldwide` moves both the cache and the source, and
that the reference prefix does not move. Include the Step 6 caveat about
`opdi_only`, because `DATASETS.md` is where someone goes to find out what a
column means.

- [ ] **Step 9: Run the tests**

Run: `cd /home/jupyter/work/opdi-workspace/opdi-benchmarker && .venv/bin/python -m pytest tests/ -q`
Expected: PASS, the pre-existing suite plus ten new tests. The pre-existing
`test_table_path_is_versioned_and_on_s3` must pass **unchanged** — it pins the
European default, which is the guarantee this task must not break.

- [ ] **Step 10: Commit**

```bash
cd /home/jupyter/work/opdi-workspace/opdi-benchmarker
git add src/opdi_benchmarker/config.py src/opdi_benchmarker/build/cli.py \
        src/opdi_benchmarker/app/Home.py src/opdi_benchmarker/app/pages/3_Movements.py \
        src/opdi_benchmarker/provenance.py DATASETS.md tests/
git commit -m "feat(coverage): follow the pipeline's coverage, cache included"
```

---

### Task 12: Documentation, and the submodule pointer

- [ ] **Step 1: `opdi/readme.md`**

Add a worldwide section: the one flag, the separate warehouse, the planet
extract, that 00c does not run and why, and that `OOA` is not assigned. State
plainly that worldwide output is **not** comparable to published European data —
different warehouse, different reference tables, and no benchmark against ground
truth, because `eurocontrol`'s PRISME extracts are European.

- [ ] **Step 2: `opdi-workspace/CLAUDE.md`**

Under *Conventions that matter*, add: coverage is one `CoverageConfig`, the
European box is the default and is a published contract, `bbox=None` is
worldwide, and worldwide writes to its own warehouse with a marker that refuses
to mix. Under *Known inconsistencies*, delete nothing — but note that the
six-copy bbox is resolved.

- [ ] **Step 3: `EVENTS_RUN_LOG.md`**

One numbered decision covering the four fixed decisions and their reasons, plus
the antimeridian measurement from Task 5.

- [ ] **Step 4: `opdi-benchmarker/README.md`**

How to point the benchmarker at a worldwide build (`--worldwide` for the CLI,
`OPDI_BENCHMARKER_COVERAGE=worldwide` for the Streamlit app), that the cache
moves with it, that the reference prefix does not, and the `opdi_only`
interpretation caveat from Task 11 Step 6.

- [ ] **Step 5: Push and open a draft PR**

```bash
cd /home/jupyter/work/opdi-workspace/opdi
git push -u origin feature/worldwide-coverage
gh pr create --draft --title "Worldwide coverage behind one flag" --body-file -
```

- [ ] **Step 6: Commit both submodule pointers**

**Two submodules changed**, not one: `opdi/` and `opdi-benchmarker/`. Commit
inside each first, then both pointers in the meta-repo. That order is the
documented convention, and skipping it leaves the meta-repo pointing at commits
that do not exist on any remote.

---

## Verification

**Unit, no cluster, no credentials:**

```bash
cd /home/jupyter/work/opdi-workspace/opdi
.venv310/bin/python -m pytest tests/ -q
```
Expected: the pre-existing suite plus ~35 new tests, all passing. A large failure
count spanning unrelated files is the known Spark-resource artefact of running
the whole suite in one process — re-run the affected files alone before treating
it as a regression.

```bash
cd /home/jupyter/work/opdi-workspace/opdi-benchmarker
.venv/bin/python -m pytest tests/ -q
```
Expected: the pre-existing suite plus ~10 new tests. **Its own venv** — the
benchmarker is a separate project from `opdi` and `opdi/.venv310` does not carry
its dependencies.

**The contract that must hold, and is cheap to check:**

```bash
.venv310/bin/python -c "
from opdi.config import OPDIConfig
from opdi.coverage import EUROPE_BBOX
eu, w = OPDIConfig.for_environment('opensky'), OPDIConfig.for_environment('opensky', worldwide=True)
assert eu.coverage.bbox == EUROPE_BBOX == (-25.86653, 26.74617, 49.65699, 70.25976)
assert w.coverage.bbox is None
assert w.project.warehouse_path != eu.project.warehouse_path
print('coverage contract ok')
"
```

**End to end, needs the cluster:** Tasks 9 and 10 in order.

**What 'working' looks like — falsifiable predictions.** If these do not hold,
something is wrong and the run log must say what rather than reporting the
numbers as successes:

* A European run after this plan produces **the same `track_id`s** as one before
  it. Nothing in Tasks 1-7 changes segmentation, and if ids move, a filter that
  should have been coverage-neutral was not.
* Step 00a reports **~5,280 aerodromes** worldwide against ~1,357 European, and
  names `NZSP` in its polar-exclusion line.
* `h3_airport_detection_zones` worldwide is **on the order of 125 M cells** —
  Europe's ~32 M times 3.89. An order of magnitude out in either direction means
  the ring reach or the airport set is not what it should be.
* `NFNL`'s zone cell count is **comparable to a same-latitude aerodrome away from
  the dateline**, not a third short.
* The worldwide flight list contains **zero `OOA` rows** and non-European
  aerodromes in its busiest 25.
* `opdi-world` and the European warehouse are **disjoint prefixes**, and pointing
  a worldwide run at the European one fails on step 00 with a message naming both
  coverages.
* `opdi-benchmarker build --worldwide` writes to a cache prefix containing
  `world`, and a European build afterwards still finds its own cached days
  intact. If a European day disappears or changes, the cache did not move with
  the coverage and Task 11's central point was missed.

---

## Open items, stated rather than assumed

* **The worldwide/Europe row ratio is unmeasured.** Task 10 Step 1 measures it on
  one hour before committing to a day. Every volume estimate in this plan is a
  projection from the airport ratio, which is the wrong denominator — OpenSky's
  receiver network is Europe-heavy, so the true traffic ratio is probably lower
  than 3.89×. Nothing downstream should quote a figure until Step 1 has run.
* **The bucket quota is the live constraint, not the code.** The European
  campaign hit `XMinioAdminBucketQuotaExceeded` at 105 GB. A worldwide day is
  projected at 10-20 GB for state vectors plus tracks, before events. How many
  worldwide days fit is a question for the quota owner, and this plan does not
  assume an answer.
* **Nothing worldwide can be benchmarked.** Every OPDI accuracy figure comes from
  `eurocontrol`'s PRISME extracts, which are European. Worldwide ADEP/ADES and
  milestone quality are therefore **unmeasured**, and any paper using worldwide
  output must say so rather than carrying European accuracy figures across. The
  nearest available alternative would be a different reference source entirely
  (FlightRadar24, a scheduled-flights database) and is out of scope here.
* **`opdi_h3_airspace_ref` stays European.** If airspace events are ever built,
  the worldwide equivalent needs a global FIR source, which PRU Atlas is not.
* **OurAirports is the only airport authority.** Its `large_airport` /
  `medium_airport` classification is uneven outside Europe and North America, so
  the worldwide aerodrome set is a proxy for "aerodromes worth detecting" rather
  than a measured one. Nothing in this plan changes that; it is worth measuring
  once there is a worldwide day to measure against.
* **The benchmarker's headline metrics lose their meaning worldwide.** APDF
  reports roughly ninety European aerodromes, so `n_extra` and the movement
  ratios stop estimating ghost tracks and start counting flights APDF was never
  going to report. Task 11 Step 6 surfaces this in the UI rather than fixing it;
  a worldwide-appropriate ghost-track estimator would need a different reference
  source and is out of scope.
* **Step 02's H3 indexing at `res_12` is untested at worldwide volumes.** It is
  pure column expressions and should scale linearly, but "should" is not
  "measured" and Task 10 is where it would first show.
