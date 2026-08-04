# OPDI_WORKSPACE

Workspace for the **Open Performance Data Initiative** (OPDI) — EUROCONTROL Performance Review Unit.

OPDI turns OpenSky Network ADS-B state vectors into a published, versioned dataset of **flight events** (milestones) and **measurements**, for operational performance monitoring.

This is a meta-repo: the working repos are git submodules pinned to specific commits, so the whole workspace is reproducible.

## Setup

```bash
git clone --recurse-submodules <url> OPDI_WORKSPACE
cd OPDI_WORKSPACE
make bootstrap
```

Already cloned without submodules:

```bash
make init
```

## Contents

| Path | Role |
|---|---|
| `opdi/` | PySpark pipeline. The main deliverable. |
| `opdi-portal/` | Quarto website and papers. |
| `eurocontrol/` | R package over PRISME. Ground truth — work laptop only. |
| `traffic/` | Reference algorithms (read-only). |
| `prc-data-challenges/` | 2024 and 2025 PRC Data Challenges: descriptions and winning solutions. |
| `reference/` | Ground-truth extracts (git-lfs). |

See [CLAUDE.md](CLAUDE.md) for the full working guide: pipeline layout, conventions, ground-truth mapping, and the evidence base from the PRC challenges.

## Common tasks

```bash
make status      # commit + branch of every submodule
make sync        # pull each submodule to its tracked branch
make osn-clone   # print the shallow-clone recipe for the OpenSky server
```

## Working on the OpenSky server

Do **not** clone this meta-repo on OSN — `traffic/` and the four PRC repos are reference material and would bloat the checkout for no benefit. Shallow-clone only what runs there:

```bash
make osn-clone
```

## Ground-truth data flow

`eurocontrol/` needs PRISME Oracle access and runs **only on the work laptop**. The flow is one-directional:

```
eurocontrol (R, work laptop)  →  arrow::write_parquet()  →  opdi/reference/ (git-lfs)  →  OSN
```

Papers and benchmarks read the committed parquet. Neither the pipeline nor a paper render ever touches the database.

## Licence

Sub-repos carry their own licences. `opdi` is MIT.
