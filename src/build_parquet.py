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

    import pandas as pd
    import pyarrow.dataset as ds

    dataset = ds.dataset('out/NCHR_BPR_raw.parquet', format='parquet', partitioning='hive')
    df = dataset.to_table().to_pandas()
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
        pa.field("year",               pa.int16()),
        pa.field("month",              pa.int8()),
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
        dataset = ds.dataset(str(out_dir), format="parquet", partitioning="hive")
        table = dataset.to_table(columns=["dmas_time"])
        if table.num_rows == 0:
            return None
        max_ts = table.column("dmas_time").to_pandas().max()
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

    df["year"]  = pa.array([date.year]  * len(df), type=pa.int16())
    df["month"] = pa.array([date.month] * len(df), type=pa.int8())

    # Cast timestamps to plain (non-tz) microsecond timestamps for Arrow
    for col in ("dmas_time", "ppc_time"):
        df[col] = pd.to_datetime(df[col]).dt.tz_localize(None)

    return pa.Table.from_pandas(df, schema=SCHEMA, preserve_index=False)


def _write_day(table: pa.Table, out_dir: Path) -> None:
    """Append a single day's Arrow table to the partitioned Parquet dataset."""
    pq.write_to_dataset(
        table,
        root_path=str(out_dir),
        partition_cols=["year", "month"],
        existing_data_behavior="overwrite_or_ignore",
        basename_template="{i}.parquet",
    )


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

            parsed_df = parse_day_df(raw_df)
            if parsed_df.empty:
                logger.warning("All lines failed to parse for %s", day.date())
                n_empty += 1
                continue

            table = _df_to_arrow(parsed_df, day)
            _write_day(table, out_dir)
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
    dataset = ds.dataset(str(out_dir), format="parquet", partitioning="hive")

    filters = []
    if date_from:
        filters.append(ds.field("dmas_time") >= pd.Timestamp(date_from))
    if date_to:
        filters.append(ds.field("dmas_time") < pd.Timestamp(date_to))

    table = dataset.to_table(filter=(filters[0] if len(filters) == 1 else
                                     pa.compute.and_(*filters)) if filters else None)
    df = table.to_pandas()
    df = df.set_index("dmas_time").sort_index()
    return df
