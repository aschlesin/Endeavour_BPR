"""
fetch_raw.py – ONC API wrapper for BPR raw data retrieval.

Fetches one day of raw hex data from the ONC DMAS API for a given
location and device category, returning a tidy pandas DataFrame.
"""

import logging
from datetime import datetime, timedelta, timezone

import pandas as pd

logger = logging.getLogger(__name__)


def fetch_day(
    onc_client,
    date: datetime,
    location_code: str = "NCHR",
    device_category_code: str = "BPR",
) -> pd.DataFrame | None:
    """Fetch one day of raw BPR data from the ONC API.

    Parameters
    ----------
    onc_client:
        An initialised ``onc.ONC`` client instance.
    date:
        The day to fetch (time component is ignored; UTC midnight is used).
    location_code:
        ONC location code, e.g. ``'NCHR'``.
    device_category_code:
        ONC device category code, e.g. ``'BPR'``.

    Returns
    -------
    pd.DataFrame or None
        DataFrame with columns ``dmas_time`` (UTC, tz-naive) and
        ``readings``.  Returns ``None`` if the API returns no data for
        that day.
    """
    day_start = datetime(date.year, date.month, date.day, tzinfo=timezone.utc)
    day_end = day_start + timedelta(days=1)

    params = {
        "deviceCategoryCode": device_category_code,
        "locationCode": location_code,
        "dateFrom": day_start.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
        "dateTo": day_end.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
    }

    logger.debug("Fetching %s for %s", location_code, day_start.date())

    try:
        out = onc_client.getDirectRawByLocation(params, allPages=True)
    except Exception as exc:
        logger.warning("API error for %s on %s: %s", location_code, day_start.date(), exc)
        return None

    data = out.get("data") if out else None
    if not data:
        logger.info("No data for %s on %s", location_code, day_start.date())
        return None

    df = pd.DataFrame(data)

    # Normalise timestamp column to tz-naive UTC DatetimeIndex
    df["dmas_time"] = pd.to_datetime(df["times"], utc=True).dt.tz_localize(None)
    df = df.drop(columns=["times"])

    # Keep only the columns we care about
    cols = ["dmas_time", "readings"]
    extra = [c for c in df.columns if c not in cols]
    if extra:
        logger.debug("Dropping extra API columns: %s", extra)
    df = df[cols].copy()

    return df
