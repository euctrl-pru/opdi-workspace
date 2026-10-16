# ADS-B / ADS-C clock-skew prototype

The hand-run measurement behind the spec's "Time synchronisation — measured"
section, kept because every number in that section comes from it and a
reviewer should be able to re-derive them without cluster access.

- `prototype.py` — reads the ADS-C day already staged locally plus three hours
  of `opensky-hdfs-backup/tables_v4/state_vectors` (all seven row groups per
  hour, nine columns) straight from S3 with `pyarrow.fs.S3FileSystem`, brackets
  each ADS-C report with the nearest ADS-B sample each side within 60 s, and
  decomposes the offset into along-track and cross-track.
- `pairs.parquet` — the 46 bracketed pairs it produced on 2026-06-01
  11:00–14:00 UTC, 25 airframes.

`benchmarks/adsc_time_sync.py` (plan Task 9b) is this measurement in Spark,
over the three study days, staged with provenance. This directory is the
prototype, not the deliverable: it runs on a laptop, it reads one day, and its
sample is too small to support a per-airframe correction — which is exactly
what the spec concludes.
