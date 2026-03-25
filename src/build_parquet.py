"""
build_parquet.py – Incremental day-by-day builder for the multi-year BPR Parquet dataset.

Usage (from a notebook or script):

    from src.build_parquet import run_incremental_build
    from onc import ONC
    import os

    onc = ONC(os.getenv('ONC_TOKEN'))
    run_incremental_build(onc)

The output is a Hive-partitioned Parquet dataset at ``out/NCHR_BPR_raw.parquet/``
partitioned by ``year`` and ``month``, which can be read back with:

    from src.build_parquet import load_dataset
    df = load_dataset()
    # or a date-filtered subset:
    df = load_dataset(date_from='2025-01-01', date_to='2026-01-01')
"""

import logging
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
from tqdm import tqdm

from src.fetch_raw import fetch_day
from src.parse_hex import parse_day_df

logger = logging.getLogger(__name__)

# --------------------------------------------------------------------------- #
# Parquet schema
# --------------------------------------------------------------------------- #
SCHEMA = pa.schema(
    [
        pa.field("dmas_time",          pa.timestamp("us")),
        pa.field("ppc_time",           pa.timestamp("us")),
        pa.field("t_housing_counts",   pa.int32()),
        pa.field("xFT",                pa.int64()),
        pa.field("xFP",                pa.int64()),
        pa.field("X_period_us",        pa.float64()),
        pa.field("T_period_us",        pa.float64()),
    ]
)

DEFAULT_START = datetime(2022, 5, 23, tzinfo=timezone.utc)
DEFAULT_OUT_DIR = Path(__file__).parents[1] / "out" / "NCHR_BPR_raw.parquet"


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #

def _last_stored_date(out_dir: Path) -> datetime | None:
    """Return the last date already stored in the Parquet dataset, or None."""
    if not out_dir.exists():
        return None
    try:
        files = sorted(out_dir.rglob("*.parquet"))
        if not files:
            return None
        # Read only dmas_time from all files, find the maximum
        tables = [pq.read_table(f, columns=["dmas_time"], schema=SCHEMA) for f in files]
        all_times = pa.concat_tables(tables).column("dmas_time").to_pandas()
        max_ts = all_times.max()
        return pd.Timestamp(max_ts).to_pydatetime().replace(tzinfo=timezone.utc)
    except Exception as exc:
        logger.warning("Could not read existing dataset: %s", exc)
        return None


def _df_to_arrow(df: pd.DataFrame, date: datetime) -> pa.Table:
    """Convert a parsed day-DataFrame to a PyArrow table with partition columns."""
    df = df.reset_index()  # bring dmas_time back as a column

    # Ensure nullable int columns are cast to plain int64/int32 for Arrow
    df["t_housing_counts"] = df["t_housing_counts"].astype("Int32")
    df["xFT"] = df["xFT"].astype("Int64")
    df["xFP"] = df["xFP"].astype("Int64")

    # Cast timestamps to plain (non-tz) microsecond timestamps for Arrow
    for col in ("dmas_time", "ppc_time"):
        df[col] = pd.to_datetime(df[col]).dt.tz_localize(None)

    return pa.Table.from_pandas(df, schema=SCHEMA, preserve_index=False)


def _write_day(table: pa.Table, out_dir: Path, date: datetime) -> None:
    """Merge a single day's data into its monthly partition file.

    Reads any existing data for the same year/month partition, concatenates
    the new day, deduplicates on dmas_time, and rewrites as a single
    ``data.parquet`` file.  This keeps one file per month while allowing
    day-by-day incremental builds without filename collisions.
    """
    year, month = date.year, date.month
    partition_dir = out_dir / f"year={year}" / f"month={month}"
    partition_dir.mkdir(parents=True, exist_ok=True)
    partition_file = partition_dir / "data.parquet"

    if partition_file.exists():
        existing = pq.read_table(partition_file, schema=SCHEMA)
        combined = pa.concat_tables([existing, table])
    else:
        combined = table

    # Deduplicate on dmas_time, keep last occurrence, sort chronologically
    df = combined.to_pandas()
    df = df.drop_duplicates(subset="dmas_time", keep="last").sort_values("dmas_time")
    combined = pa.Table.from_pandas(df, schema=SCHEMA, preserve_index=False)

    pq.write_table(combined, partition_file)


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #

def run_incremental_build(
    onc_client,
    start_date: datetime = DEFAULT_START,
    out_dir: Path = DEFAULT_OUT_DIR,
    location_code: str = "NCHR",
    device_category_code: str = "BPR",
    end_date: datetime | None = None,
) -> None:
    """Fetch, parse, and append BPR data day-by-day to the Parquet dataset.

    Already-stored days (detected by the latest timestamp in the dataset)
    are skipped automatically.

    Parameters
    ----------
    onc_client:
        Initialised ``onc.ONC`` instance.
    start_date:
        Earliest date to fetch if no data exists yet (default: 2022-05-23).
    out_dir:
        Root directory of the Parquet dataset.
    location_code:
        ONC location code.
    device_category_code:
        ONC device category code.
    end_date:
        Last day to fetch (exclusive). Defaults to today UTC.
    """
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    if end_date is None:
        end_date = datetime.now(tz=timezone.utc)

    # Resume from the day after the last stored timestamp
    last = _last_stored_date(out_dir)
    if last is not None:
        resume_date = (last + timedelta(days=1)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        logger.info("Resuming from %s (last stored: %s)", resume_date.date(), last.date())
    else:
        resume_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
        logger.info("Starting fresh from %s", resume_date.date())

    # Build list of days to fetch
    days = []
    dt = resume_date
    while dt.date() < end_date.date():
        days.append(dt)
        dt += timedelta(days=1)

    if not days:
        logger.info("Dataset is already up to date.")
        print("Dataset is already up to date.")
        return

    print(f"Fetching {len(days)} day(s) from {days[0].date()} to {days[-1].date()} …")

    n_ok = 0
    n_empty = 0
    n_error = 0

    for day in tqdm(days, unit="day"):
        try:
            raw_df = fetch_day(onc_client, day, location_code, device_category_code)
            if raw_df is None or raw_df.empty:
                n_empty += 1
                continue

            parsed_df = parse_day_df(raw_df, source_file=day.strftime("%Y-%m-%d"))
            if parsed_df.empty:
                logger.warning("All lines failed to parse for %s", day.date())
                n_empty += 1
                continue

            table = _df_to_arrow(parsed_df, day)
            _write_day(table, out_dir, day)
            n_ok += 1

        except Exception as exc:
            logger.error("Error processing %s: %s", day.date(), exc, exc_info=True)
            n_error += 1

    print(
        f"Done. Days written: {n_ok} | empty: {n_empty} | errors: {n_error}\n"
        f"Dataset: {out_dir}"
    )


def load_dataset(
    out_dir: Path = DEFAULT_OUT_DIR,
    date_from: str | None = None,
    date_to: str | None = None,
) -> pd.DataFrame:
    """Load the full (or filtered) Parquet dataset into a pandas DataFrame.

    Parameters
    ----------
    out_dir:
        Root directory of the Parquet dataset.
    date_from:
        Optional ISO date string ``'YYYY-MM-DD'`` to filter from (inclusive).
    date_to:
        Optional ISO date string ``'YYYY-MM-DD'`` to filter to (exclusive).

    Returns
    -------
    pd.DataFrame indexed by ``dmas_time``.
    """
    out_dir = Path(out_dir)
    files = sorted(out_dir.rglob("*.parquet"))
    if not files:
        raise FileNotFoundError(f"No Parquet files found in {out_dir}")

    tables = [pq.read_table(f, schema=SCHEMA) for f in files]
    df = pa.concat_tables(tables).to_pandas()
    df = df.set_index("dmas_time").sort_index()

    if date_from:
        df = df[df.index >= pd.Timestamp(date_from)]
    if date_to:
        df = df[df.index < pd.Timestamp(date_to)]

    return df
