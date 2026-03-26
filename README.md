# NCHR BPR Raw Data Pipeline

Incremental fetch, parse, and calibration pipeline for multi-year **Bottom Pressure Recorder (BPR)** data from the [Ocean Networks Canada (ONC)](https://oceannetworkscanada.ca) DMAS API — NCHR (North Cascadia) location, 2022-05 to present.

---

## Overview

Raw BPR data is retrieved from the ONC API as ASCII hex strings, one day at a time. Each hex line encodes a PPC timestamp, a housing-temperature A/D count, and two Paroscientific frequency counts (temperature and pressure). The pipeline parses these into frequency periods (µs), stores them in a partitioned Parquet dataset, and calibrates them to physical units (°C, dbar) for analysis.

```
ONC API  →  fetch_raw.py  →  parse_hex.py  →  Parquet dataset  →  Calibrate notebook
```

---

## Repository Structure

```
.
├── GetONCRawData.ipynb           # Driver: fetch → parse → append to Parquet (run to top-up)
├── Calibrate_NCHR_rawData.ipynb  # Analysis: load Parquet → calibrate P & T → plot
├── src/
│   ├── fetch_raw.py              # ONC API wrapper — one day per call
│   ├── parse_hex.py              # Hex-line parser → frequency periods
│   ├── build_parquet.py          # Incremental Parquet builder + loader
│   ├── calibrateBPRData.py       # Paroscientific & Platinum RTD calibration functions
│   ├── parosci.txt               # Paroscientific calibration coefficients (by serial #)
│   └── platinum.txt              # Platinum RTD calibration coefficients (by hex ID)
├── environment.yml               # Conda environment definition (bpr-nchr)
├── .env.example                  # Template for ONC_TOKEN secret
├── .gitattributes                # nbstripout filter — strips notebook outputs from git
├── out/                          # Generated output — gitignored
│   ├── NCHR_BPR_raw.parquet/     # Parquet dataset partitioned by year/month/day
│   └── parse_errors.log          # Hex-line parse failures (appended each run)
├── AGENT.md                      # Developer notes and design decisions
└── .env                          # ONC_TOKEN secret — gitignored
```

---

## Quickstart

### Prerequisites

**1 — Create the conda environment (first time only)**

```csh
conda env create -f environment.yml
conda activate bpr-nchr
```

To update an existing environment after `environment.yml` changes:

```csh
conda env update -f environment.yml --prune
```

**2 — Set your ONC API token**

Copy the template and fill in your token (get it at [data.oceannetworks.ca](https://data.oceannetworks.ca) → My Profile → Web Services API):

```csh
cp .env.example .env
# then edit .env and replace 'your_token_here' with your real token
```

The `.env` file is gitignored — **never commit it**. The notebooks load it automatically via `python-dotenv`.

### 1 — Fetch and build the Parquet dataset

Open **`GetONCRawData.ipynb`** in VS Code, select the `bpr-nchr` kernel, and run all cells.

The notebook will:
- Resume from the last date already stored (or start from `2022-05-23` on a first run)
- Fetch one day at a time from the ONC API with a progress bar
- Parse each hex line into frequency period counts
- Append results to `out/NCHR_BPR_raw.parquet/`, partitioned by year and month

Re-run at any time to top-up the dataset — already-stored days are skipped automatically.

> **First run:** fetching ~3 years of 1 Hz data will take 30–60 minutes depending on API throughput.

### 2 — Calibrate and analyse

Open **`Calibrate_NCHR_rawData.ipynb`**, select `bpr-nchr`, and run all cells.

This notebook:
- Loads the full Parquet dataset (or a date-filtered subset)
- Applies Paroscientific Type-II and Platinum RTD calibration
- Produces a 3-panel time-series plot of pressure (dbar), seawater temperature (°C), and housing temperature (°C)

---

## Sensor Configuration — NCHR Deployment

| Role | ID | Type |
|---|---|---|
| Logger | `0xB9` | Paroscientific BPR |
| Pressure + seawater temp | `93996` | Paroscientific Type-II gauge |
| Housing temperature | `0x98` | Platinum RTD |

---

## Parquet Dataset Schema

Each day is stored as an independent `YYYYMMDD.parquet` file under `out/NCHR_BPR_raw.parquet/year=YYYY/month=MM/`. `year` and `month` are directory names only — not physical columns — to avoid PyArrow schema conflicts.

| Column | Type | Description |
|---|---|---|
| `dmas_time` *(index)* | `datetime64[us]` | ONC server timestamp |
| `ppc_time` | `datetime64[us]` | On-instrument PPC clock |
| `t_housing_counts` | `int32` | Platinum RTD raw A/D counts |
| `xFT` | `int64` | Paroscientific temperature frequency count |
| `xFP` | `int64` | Paroscientific pressure frequency count |
| `X_period_us` | `float64` | Temperature oscillation period (µs) |
| `T_period_us` | `float64` | Pressure oscillation period (µs) |

> Raw frequency periods are stored rather than calibrated values, so the dataset can be recalibrated at any time without re-fetching from the API.

---

## Loading the Dataset Directly (Python)

```python
from src.build_parquet import load_dataset

# Full multi-year dataset
df = load_dataset()

# Date-filtered subset
df = load_dataset(date_from='2024-01-01', date_to='2025-01-01')
```

---

## Parse Error Log

Failed hex lines are recorded in `out/parse_errors.log` (gitignored). Each `PARSE_ERROR` entry includes:

| Field | Description |
|---|---|
| `file=` | Source date (e.g. `2025-01-15`) identifying which day the bad line came from |
| `time=` | `dmas_time` timestamp of the specific reading from the ONC API |
| `reason=` | Exception message explaining why parsing failed |
| `hex=` | The extracted hex block (or the raw API string if hex extraction itself failed) |

A session-start header and a per-day `SUMMARY` line (total / failed / ok counts) are also written automatically by `parse_hex.py`.

Example entry:
```
2025-01-15T03:42:11Z  PARSE_ERROR  file=2025-01-15           time=2025-01-15 03:42:11  reason=expected >=4 chunks, got 2              hex=4599A163B9C5
2025-01-15T03:42:12Z  SUMMARY      file=2025-01-15           total=86400  failed=1  ok=86399
```

---

## References

- ONC Python client docs: <https://oceannetworkscanada.github.io/api-python-client/>
- Paroscientific Type-II calibration equations (U-compensation model)
- Original calibration code: M. Heesemann; modified A. Schlesinger (2020)
