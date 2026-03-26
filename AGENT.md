# AGENT.md – Codebase Overview & Developer Notes

## Project Summary

This project retrieves, parses, and calibrates multi-year Bottom Pressure Recorder (BPR) raw data from the Ocean Networks Canada (ONC) DMAS API, specifically for the **NCHR** (North Cascadia) location. The pipeline produces calibrated pressure and temperature time-series from 2022-05-23 to the present.

---

## Repository Structure

```
.
├── AGENT.md                          # This file
├── README.md                         # User-facing project description
├── environment.yml                   # Conda environment definition (bpr-nchr)
├── .env.example                      # Template for ONC_TOKEN secret
├── .gitattributes                    # nbstripout filter — strips notebook outputs from git
├── GetONCRawData.ipynb               # Notebook: fetch → parse → append to Parquet
├── Calibrate_NCHR_rawData.ipynb      # Notebook: load Parquet → calibrate P & T → plot
├── src/
│   ├── fetch_raw.py                  # ONC API wrapper — one day per call
│   ├── parse_hex.py                  # Hex-line parser → frequency periods + error log
│   ├── build_parquet.py              # Incremental Parquet builder + loader
│   ├── calibrateBPRData.py           # CalibrationCoefficients class (Paro + Platinum RTD)
│   ├── parosci.txt                   # Paroscientific calibration coefficients (by serial #)
│   └── platinum.txt                  # Platinum RTD calibration coefficients (by hex ID)
├── out/                              # Generated output — gitignored
│   ├── NCHR_BPR_raw.parquet/         # Parquet dataset partitioned by year/month/day
│   └── parse_errors.log              # Hex-line parse failures (appended each run)
├── .env                              # ONC_TOKEN secret — gitignored
└── .gitignore
```

---

## Data Flow

```
ONC API  →  fetch_raw.py  →  parse_hex.py  →  build_parquet.py  →  Parquet dataset
                                                                          ↓
                                                          Calibrate_NCHR_rawData.ipynb
```

### 1. Fetch raw data — `src/fetch_raw.py`

- `fetch_day(onc_client, date, location_code, device_category_code) → pd.DataFrame | None`
- Authenticates via the `onc` Python client using `ONC_TOKEN` from the environment.
- Queries `getDirectRawByLocation` with a 1-day UTC window (`allPages=True`).
- Returns a DataFrame with columns `dmas_time` (UTC) and `readings` (raw hex string).
- Returns `None` gracefully on empty days or API errors.

### 2. Parse hex lines — `src/parse_hex.py`

Each `readings` string is a continuous hex block:

```
4599A163  B9 C5BA3B  29D8FC3B  6AACED59  00
  [0]      [1][1]      [2]        [3]    terminator
  PPC time  loggerID+housing-T   paro-T count   paro-P count
```

- `parse_hex_line(reading, source_file) → dict | None`
  - `x[0]` — PPC timestamp (seconds since 1988-01-01)
  - `x[1][2:]` — 3-byte housing temperature A/D count (strips 1-byte logger ID prefix)
  - `x[2]` — Paroscientific temperature frequency count (`xFT`); `0xFFFFFFFF` → `NaN`
  - `x[3]` — Paroscientific pressure frequency count (`xFP`)
  - Computes `X_period_us` and `T_period_us` from raw counts
  - Parse failures are logged to `out/parse_errors.log` with source date, reason, and raw hex
- `parse_day_df(raw_df, source_file) → pd.DataFrame`
  - Applies `parse_hex_line` row-wise; failed rows are dropped and a per-day summary is logged

### 3. Store as Parquet — `src/build_parquet.py`

- `run_incremental_build(onc_client, start_date, ...)` — main entry point
  - Determines resume date from the lexicographically last `YYYYMMDD.parquet` filename (no file I/O needed)
  - Loops day-by-day: fetch → parse → write; skips empty days; logs errors per day
- `_write_day(table, out_dir, date)` — writes `year=YYYY/month=MM/YYYYMMDD.parquet`
  - **O(1) write cost** — no read-back; each day is an independent file
  - Re-running the same day overwrites only that day's file
- `load_dataset(out_dir, date_from, date_to) → pd.DataFrame`
  - Globs all `*.parquet` files, reads with enforced schema, concatenates, sets `dmas_time` as index

### 4. Parquet Schema

| Column | dtype | Notes |
|---|---|---|
| `dmas_time` *(index)* | `timestamp[us]` | ONC server timestamp |
| `ppc_time` | `timestamp[us]` | On-instrument PPC clock |
| `t_housing_counts` | `int32` | Platinum RTD raw A/D counts |
| `xFT` | `int64` | Paroscientific temperature frequency count (`NaN` on error) |
| `xFP` | `int64` | Paroscientific pressure frequency count |
| `X_period_us` | `float64` | Temperature oscillation period (µs) |
| `T_period_us` | `float64` | Pressure oscillation period (µs) |

> `year` and `month` are encoded only in directory names — **not** as physical columns.
> Storing them as columns too causes `ArrowTypeError` when PyArrow reads back with Hive partitioning inference.

### 5. Calibrate — `src/calibrateBPRData.py` + `Calibrate_NCHR_rawData.ipynb`

The `CalibrationCoefficients` class provides:

| Method | Input | Output |
|---|---|---|
| `calibratePlatinum(xT, Coeffs)` | A/D count | Temperature °C (linear: `a·x + b`) |
| `calibrateParoT(xFT, Coeffs)` | freq count | Temperature °C (Type-II Paroscientific) |
| `calibrateParoP(xFP, Coeffs, Temp)` | freq count + °C | Pressure in **dbar** |

Calibration coefficients are loaded from flat text files:
- **`parosci.txt`**: keyed by integer serial number (e.g. `93996`)
- **`platinum.txt`**: keyed by hex device ID (e.g. `0x98`)

---

## Sensor Configuration (NCHR deployment)

| Role | ID | Type |
|---|---|---|
| Logger | `0xB9` | Paroscientific BPR |
| Pressure + seawater temp | `93996` | Paroscientific Type-II gauge |
| Housing temperature | `0x98` | Platinum RTD |

---

## Environment and Secrets

- **Conda environment:** `bpr-nchr` — recreate with `conda env create -f environment.yml`
- **ONC token:** copy `.env.example` → `.env` and fill in `ONC_TOKEN`; loaded automatically by `python-dotenv`
- **Notebook outputs:** stripped from git automatically by `nbstripout` (registered via `.gitattributes`)

---

## Key Design Decisions

1. **Store frequency periods, not calibrated values** — allows recalibration without re-fetching from the API
2. **One Parquet file per day** — O(1) write cost; each day is independent; re-runs overwrite only that day's file
3. **Resume by filename** — `_last_stored_date` reads the last `YYYYMMDD.parquet` stem; no file I/O needed to find the resume point
4. **`0xFFFFFFFF` → `NaN`** — Paroscientific temperature error sentinel stored as `NaN`, not `0`, to preserve data integrity
5. **Fail-safe per day** — parse errors logged to `out/parse_errors.log` and skipped, not fatal
6. **No physical partition columns** — `year`/`month` live only in directory names to avoid `ArrowTypeError` on read
