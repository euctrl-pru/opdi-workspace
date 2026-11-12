# OPDI release v2.0.0: handover notes

Written 2026-11-11. This page lists what was built, what the owner still has to
decide or supply, and the commands to free the old data in the bucket.

## Where the work is

| Repository | Branch | Contents |
|---|---|---|
| `opdi` | `industrialisation` | Daily publishing step, reader, Trino table and view definitions, Airflow DAGs, code clean-up and documentation |
| `opdi-portal` | `feature/new-topics` | All earlier branches merged; new Data and Methods sections; 2026 PRC Data Challenge; not merged to `main` |
| `opdi-workspace` | `industrialisation` | Design, the three plans, this page, `CLAUDE.md` |

## What the owner still has to supply or decide

1. **Bucket space.** One worldwide day is about 0.42 GB of published files.
   The first backfill (January 2025 to November 2026) needs about 290 GB for
   the published files alone. The quota is 200 GB. Raise the quota or publish
   to a larger warehouse before starting.
2. **Trino names.** The catalog `minio` and schema `opdi` in
   `opdi/src/opdi/sql/trino/` and in the portal configuration are placeholders.
3. **Addresses.** The Trino file-browser address and the public download
   address are empty configuration values (`R/portal_config.R` in the portal,
   `PUBLISHED_BASE_URL` in `opdi/src/opdi/published.py`).
4. **Airflow.** Create the pool `opdi` with one slot and set the Variables
   listed in `opdi/airflow/README.md`. How the Airflow workers reach the
   pipeline is set by one Variable, `opdi_command_prefix`.
5. **Take-off and landing times.** The pipeline publishes the time found on
   the runway first, and the time found from the climb or descent otherwise.
   The validation study found more times within 10 seconds with the opposite
   order. Changing the order changes published times, so it was left as it is.
6. **Portal.** The site has not been rendered since the restructure (Quarto
   and R are not on the pod). `PUBLISHING_CHECKLIST.md` in the portal lists the
   steps and the facts to confirm.

## Freeing the old data in the bucket

Inventory on 2026-11-11 (bucket `eurocontrol`, 192.7 GB of 200 GB used):

| Prefix | Size | Note |
|---|---|---|
| `opdi-world/` | 101.7 GB | Earlier worldwide runs and study tables |
| `opdi/osn_symposium_paper_2026/` | 80.7 GB | Data of the symposium paper. Check before deleting. |
| `opdi/` (everything else) | 10.3 GB | Earlier European tables and study tables |
| `opdi-prod/` | does not exist | Nothing to delete |

Before deleting, know what goes with it:

- **Reference tables** (airport zones, airport layouts, runway zones, airport
  and aircraft lists, FIR cells) live in `opdi-world/`. The new release
  rebuilds them with `python -m opdi.orchestration reference`. Copying them
  first (step 1 below) saves that time.
- **Study tables** under `opdi/research/` and `opdi-world/research/` are the
  inputs of the method studies on the portal. Without them a study can no
  longer be re-run from scratch; it still renders from its committed data with
  `OPDI_RENDER=allow-stale`.

The commands use the MinIO client `mc`. Set the alias once:

```bash
mc alias set osn https://s3.opensky-network.org "$AWS_ACCESS_KEY_ID" "$AWS_SECRET_ACCESS_KEY"
```

Step 1 (optional), keep the reference tables for the new release:

```bash
for t in h3_airport_detection_zones h3_runway_zones hexaero_airport_layouts oa_airports oa_runways osn_aircraft_db; do
  mc cp --recursive "osn/eurocontrol/opdi-world/$t/" "osn/eurocontrol/opdi/v2.0.0/$t/"
done
mc cp --recursive osn/eurocontrol/opdi-world/research/airspace_events/ref/opdi_h3_airspace_ref/ \
                  osn/eurocontrol/opdi/v2.0.0/opdi_h3_airspace_ref/
```

Do not copy `coverage_marker`: the first run writes it. After copying, run
`python -m opdi.orchestration reference`; it builds only what is missing.

Step 2, look before deleting:

```bash
mc du --depth 2 osn/eurocontrol/
```

Step 3, delete the old worldwide warehouse (101.7 GB):

```bash
mc rm --recursive --force osn/eurocontrol/opdi-world/
```

Step 4, delete the old contents of `opdi/`, keeping the new release folder:

```bash
# Everything directly under opdi/ except v2.0.0/ and the symposium folder (10.3 GB)
mc ls osn/eurocontrol/opdi/ | awk '{print $NF}' | grep -v -e '^v2.0.0/$' -e '^osn_symposium_paper_2026/$' |
  while read -r p; do mc rm --recursive --force "osn/eurocontrol/opdi/$p"; done

# The symposium paper data (80.7 GB): only when it is no longer needed
mc rm --recursive --force osn/eurocontrol/opdi/osn_symposium_paper_2026/
```

`opdi-prod/` does not exist, so there is nothing to run for it.

## Plausibility of milestones (added 2026-11-12)

Plan: `docs/superpowers/plans/2026-11-12-milestone-plausibility.md`. Study note
and data: `opdi-portal/papers/milestone-plausibility-v1/`. Portal page:
`content/methods/plausibility-checks.qmd`.

- **Block times are read from the ground.** An off-block time is the start of a
  movement from a standstill before the take-off of its own leg; an in-block
  time is the end of the last movement after the landing.
- **Crossings over missing values** are placed between the last known value and
  the current one, and are not given when those are more than 30 minutes apart.
- **Five checks** remove an event only when known facts contradict it: no time;
  a ground event while clearly flying; a take-off or landing time more than
  1,500 ft above the airport; an off-block time not before its take-off; an
  in-block time not after its landing. Removed events go to the working table
  `opdi_flight_events_rejected` and are not published.
- **The v0.0.2 settings are unchanged**: the three switches are off there.

Measured on one worldwide day (14 June 2026) against airport records:

| | Before | After |
|---|---|---|
| Off-block times placed in flight | 4,470 | 27 |
| In-block times placed in flight | 2,934 | 106 |
| Off-block times confirmed within 60 s | 2,167 | 2,172 |
| In-block times confirmed within 60 s | 2,204 | 2,319 |
| Take-off times confirmed within 60 s | 13,737 | 13,768 |
| Landing times confirmed within 60 s | 15,746 | 15,750 |

The checks removed 312 events that day and none that a record confirms. Five
block times that matched a record before are no longer published; each was
read individually and was placed while the aircraft was flying or on the
take-off roll, so the match was by chance.

Limits: one day and about 90 European airports; the height limit for take-off
and landing times removed nothing on that day; landing events at the runway
threshold above 500 ft are left as they are.

## Processing time (measured, one worldwide day, eight executors)

Step 03 takes 29 minutes and step 04 about 69 to 87 minutes, of which holding,
point merge and FIR crossings are about 51 minutes. Steps 01, 02 and 02a were
not timed on a full day. A whole day is roughly three to four hours, so the
2025-2026 backfill run one day at a time is about three months. Speeding it up
without changing results has been discussed and is waiting for the owner's
go-ahead; nothing was started.
