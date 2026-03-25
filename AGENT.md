# AGENT.md – Codebase Overview & Development Plan

## Project Summary

This project retrieves, parses, and calibrates multi-year Bottom Pressure Recorder (BPR) raw data from the Ocean Networks Canada (ONC) DMAS API, specifically for the **NCHR** (North Cascadia) location. The goal is to produce calibrated pressure and temperature time-series covering 2022-05 to the present.

---

## Repository Structure

```
.
├── AGENT.md                          # This file
├── README.md                         # Brief project description
├── GetONCRawData.ipynb               # Notebook: fetch raw hex data from ONC API day-by-day
├── Calibrate_NCHR_rawData.ipynb      # Notebook: parse hex lines → calibrate P & T
├── test.py                           # Script prototype of the calibration pipeline
├── out/
│   └── NCHR_rawHex_20250101_20250201.csv  # Sample fetched raw data (gitignored)
├── src/
│   ├── calibrateBPRData.py           # Core calibration library (CalibrationCoefficients class)
│   ├── parosci.txt                   # Paroscientific pressure-sensor calibration coefficients (by serial #)
│   └── platinum.txt                  # Platinum RTD temperature-sensor calibration coefficients (by hex ID)
├── .env                              # ONC_TOKEN secret (gitignored)
└── .gitignore                        # Excludes .env, out/, *.csv
```

---

## Data Flow (Current State)

### 1. Fetch raw data – `GetONCRawData.ipynb`

- Authenticates to ONC DMAS via the `onc` Python client using `ONC_TOKEN` from the environment.
- Queries `getDirectRawByLocation` with:
  - `deviceCategoryCode = 'BPR'`
  - `locationCode = 'NCHR'`
  - `dateFrom` / `dateTo` for a 1-day window
- The API returns a JSON object whose `data` key holds a list of records with fields:
  - `times` – ISO-8601 UTC timestamp (DMAS time)
  - `lineTypes` – (mostly empty)
  - `readings` – raw ASCII hex string, e.g. `4599A163B9C5BA3B29D8FC3B6AACED5900`
- Currently the day-loop and API call exist, but the results are **not** accumulated or saved incrementally – each iteration overwrites `out`.

### 2. Parse hex lines – `Calibrate_NCHR_rawData.ipynb` / `test.py`

Each `readings` string is a continuous hex block. The parsing logic (in `test.py` and `Calibrate_NCHR_rawData.ipynb`) does the following:

```
4599A163  B9 C5BA3B  29D8FC3B  6AACED59  00
  [0]      [1][1]     [2]        [3]      terminator
  PPC time  logger ID + housing-temp count   paro-temp count   paro-pressure count
```

1. Extract hex block with regex: `r'[\dA-Fa-f]{26,}'`
2. Split into 8-character (4-byte) chunks: `re.findall(r'[\dA-Fa-f]{8}', block)`
3. **`x[0]`** – PPC timestamp (seconds since 1988-01-01), converted via `calibratePPCTime()`
4. **`x[1][2:]`** – 3-byte housing temperature count (platinum RTD A/D counts), strip 2-char logger ID prefix
5. **`x[2]`** – Paroscientific temperature frequency count (`xFT`); `0xFFFFFFFF` = error → set to 0
6. **`x[3]`** – Paroscientific pressure frequency count (`xFP`)

### 3. Convert counts → physical units – `src/calibrateBPRData.py`

The `CalibrationCoefficients` class provides:

| Method | Input | Output |
|---|---|---|
| `calibratePPCTime(xt)` | 4-byte int | `datetime` (epoch 1988-01-01) |
| `calibratePlatinum(xT, Coeffs)` | A/D count | Temperature °C (linear: `a·x + b`) |
| `calibrateParoT(xFT, Coeffs)` | freq count | Temperature °C (Type-II Paroscientific) |
| `calibrateParoP(xFP, Coeffs, Temp)` | freq count + °C | Pressure in **dbar** |
| `getFrequencyPeriods(xFT, xFP)` | (defined in notebook) | (X, T) period in µs |

Calibration coefficients are loaded from flat text files:
- **`parosci.txt`**: keyed by integer serial number (e.g. `93996`)
- **`platinum.txt`**: keyed by hex device ID (e.g. `0x98`)

### 4. Output

Currently written manually to dated CSV files (`out/NCHR_rawHex_YYYYMMDD_YYYYMMDD.csv`). No persistent multi-year store exists yet.

---

## Sensor Configuration (NCHR deployment)

| Role | ID | Type |
|---|---|---|
| Logger | `0xB9` | Paroscientific BPR |
| Pressure + seawater temp | `93996` | Paroscientific Type-II |
| Housing temp | `0x98` | Platinum RTD |

---

## Proposed Plan: Day-by-Day Raw Data → Multi-Year Parquet Pipeline

### Goal
Build a reproducible pipeline that:
1. Fetches raw hex data from the ONC API **one day at a time** (2022-05-23 → today)
2. Parses each ASCII hex line into integer counts
3. Converts counts to **frequency periods** (µs) — the lossless intermediate representation
4. Appends each day to a single **Parquet** file (or partitioned Parquet dataset)
5. The Parquet file can be reloaded into pandas for calibration and analysis at any time

Keeping raw frequency periods (not calibrated values) in the Parquet file is recommended: calibration coefficients may be revised, and reprocessing from periods is cheap.

---

### Step-by-Step Implementation Plan

#### Step 1 – Create `src/fetch_raw.py`
A standalone module that wraps the ONC API fetch:
- Input: `date` (datetime), `location_code`, `device_category_code`
- Output: `pd.DataFrame` with columns `dmas_time`, `readings`
- Handles pagination (`allPages=True`) and empty-day responses gracefully

#### Step 2 – Create `src/parse_hex.py`
A standalone module for hex-line parsing:
- `parse_hex_line(reading: str) -> dict | None`
  - Returns `{'ppc_time', 't_housing_counts', 'xFT', 'xFP'}` or `None` on parse failure
- `parse_day_df(df: pd.DataFrame) -> pd.DataFrame`
  - Vectorised application of `parse_hex_line` over a day's DataFrame
  - Adds `dmas_time` column from the source DataFrame index
  - Computes frequency periods `T_period_us`, `X_period_us` using `getFrequencyPeriods`

#### Step 3 – Create `src/build_parquet.py`
The incremental writer:
- Checks an existing Parquet file (e.g. `out/NCHR_BPR_raw.parquet`) for the latest date already stored
- Loops day-by-day from `start_date` (or last stored date + 1) to `today`
- For each day: fetch → parse → compute periods → append to Parquet
- Use **PyArrow** or **fastparquet** for efficient append/upsert
- Schema:

| Column | dtype | Notes |
|---|---|---|
| `dmas_time` | `datetime64[us, UTC]` | ONC server timestamp (index) |
| `ppc_time` | `datetime64[us]` | On-instrument clock |
| `t_housing_counts` | `int32` | Platinum RTD A/D counts |
| `xFT` | `int64` | Parosci temperature freq count |
| `xFP` | `int64` | Parosci pressure freq count |
| `T_period_us` | `float64` | Pressure period µs |
| `X_period_us` | `float64` | Temperature period µs |
| `date` | `date32` | Partition column |

- Partition by **year** or **year-month** to keep individual files manageable

#### Step 4 – Update `GetONCRawData.ipynb`
Replace the current manual loop with a call to `build_parquet.run_incremental_build()`, so the notebook becomes a thin driver.

#### Step 5 – Update `Calibrate_NCHR_rawData.ipynb`
Load from the Parquet file instead of a single CSV, then apply calibration functions from `src/calibrateBPRData.py` to produce a calibrated DataFrame.

---

### Recommended Libraries

| Package | Purpose |
|---|---|
| `onc` | ONC API client (already used) |
| `pandas` | DataFrames (already used) |
| `pyarrow` | Parquet read/write with partitioning |
| `tqdm` | Progress bar for multi-year fetch loop |

Install with:
```bash
pip install pyarrow tqdm
```

---

### File Layout After Implementation

```
src/
├── calibrateBPRData.py   # existing – unchanged
├── fetch_raw.py          # NEW – ONC API wrapper
├── parse_hex.py          # NEW – hex-line parser
├── build_parquet.py      # NEW – incremental Parquet builder
├── parosci.txt
└── platinum.txt
out/
└── NCHR_BPR_raw.parquet  # multi-year raw frequency data (gitignored)
GetONCRawData.ipynb       # updated – thin driver notebook
Calibrate_NCHR_rawData.ipynb  # updated – loads from Parquet
```

---

### Key Design Decisions

1. **Store frequency periods, not calibrated values** – allows recalibration without re-fetching
2. **Incremental append** – the pipeline can be re-run daily; already-fetched days are skipped
3. **Parquet partitioned by date** – efficient range queries when loading multi-year data into pandas
4. **Fail-safe per day** – parse errors on individual hex lines are logged and skipped, not fatal
5. **`0xFFFFFFFF` handling** – temperature count errors are stored as `NaN` (not 0) to preserve data integrity
