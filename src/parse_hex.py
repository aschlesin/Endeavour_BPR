"""
parse_hex.py – Parse raw BPR ASCII hex lines into integer counts and frequency periods.

Hex-line format (each field is 4 bytes / 8 hex chars):
  [0]  PPC timestamp  – seconds since 1988-01-01
  [1]  logger-ID (1 byte / 2 chars) + housing-temp A/D count (3 bytes)
  [2]  Paroscientific temperature frequency count (xFT); 0xFFFFFFFF = error
  [3]  Paroscientific pressure frequency count (xFP)
  [4]  Terminator ('00')

Frequency-period conversion (from Calibrate_NCHR_rawData.ipynb):
  X_period_us  (temperature) = ((xFT + 2^32) * 4.656612873e-9) / 4   [µs]
  T_period_us  (pressure)    =  (xFP + 2^32) * 4.656612873e-9         [µs]
"""

import datetime
import logging
import re
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# --------------------------------------------------------------------------- #
# Dedicated error log – writes bad lines to  out/parse_errors.log
# --------------------------------------------------------------------------- #
_ERROR_LOG = Path(__file__).parents[1] / "out" / "parse_errors.log"
_ERROR_LOG.parent.mkdir(parents=True, exist_ok=True)

_err_handler = logging.FileHandler(_ERROR_LOG, mode="a", encoding="utf-8")
_err_handler.setFormatter(
    logging.Formatter("%(asctime)s  %(message)s", datefmt="%Y-%m-%dT%H:%M:%SZ")
)

parse_error_logger = logging.getLogger("parse_hex.errors")
parse_error_logger.setLevel(logging.WARNING)
parse_error_logger.addHandler(_err_handler)
parse_error_logger.propagate = False  # don't bubble up to the root logger

# Write a session-start header so each run is clearly delimited in the log
_SEP = "=" * 72
parse_error_logger.warning(_SEP)
parse_error_logger.warning(
    "Session start  %s",
    datetime.datetime.utcnow().isoformat(timespec="seconds") + "Z",
)
parse_error_logger.warning(_SEP)

# Constants
_EPOCH = datetime.datetime(1988, 1, 1)
_SCALE = 4.656612873e-9          # counts → seconds
_OVERFLOW = 4_294_967_296        # 2^32 (unsigned overflow correction)

# Minimum hex block length: 5 × 8 chars = 40, but existing code uses ≥26
_HEX_PATTERN = re.compile(r"[0-9A-Fa-f]{26,}")
_CHUNK_PATTERN = re.compile(r"[0-9A-Fa-f]{8}")


def _counts_to_ppc_time(raw: int) -> datetime.datetime:
    """Convert a 4-byte PPC counter to a Python datetime (epoch 1988-01-01)."""
    return _EPOCH + datetime.timedelta(seconds=int(raw))


def _freq_periods(xFT: int, xFP: int):
    """Convert raw frequency counts to period in microseconds.

    Returns (X_period_us, T_period_us) or (NaN, NaN) on zero/error inputs.
    """
    X = ((xFT + _OVERFLOW) * _SCALE) / 4 if xFT != 0 else np.nan
    T = (xFP + _OVERFLOW) * _SCALE if xFP != 0 else np.nan
    return X, T


def parse_hex_line(reading: str, source_file: str = "") -> Optional[dict]:
    """Parse a single raw ASCII hex reading string.

    Parameters
    ----------
    reading:
        Raw hex string from the ONC API ``readings`` column,
        e.g. ``'4599A163B9C5BA3B29D8FC3B6AACED5900'``.
    source_file:
        Optional label (e.g. date string) included in error log entries.

    Returns
    -------
    dict or None
        Keys: ``ppc_time`` (datetime), ``t_housing_counts`` (int),
        ``xFT`` (int or NaN), ``xFP`` (int), ``X_period_us`` (float),
        ``T_period_us`` (float).
        Returns ``None`` if the line cannot be parsed.
    """
    try:
        hex_block = _HEX_PATTERN.search(reading)
        if hex_block is None:
            raise ValueError("no hex block found")

        chunks = _CHUNK_PATTERN.findall(hex_block.group(0))
        if len(chunks) < 4:
            raise ValueError(f"expected >=4 chunks, got {len(chunks)}")

        # [0] PPC timestamp
        ppc_time = _counts_to_ppc_time(int(chunks[0], 16))

        # [1] strip 1-byte (2-char) logger ID prefix → 3-byte housing-temp count
        t_housing_counts = int(chunks[1][2:], 16)

        # [2] Paroscientific temperature count; 0xFFFFFFFF = error → NaN
        raw_xFT = int(chunks[2], 16)
        xFT = np.nan if chunks[2].upper() == "FFFFFFFF" else raw_xFT

        # [3] Paroscientific pressure count
        xFP = int(chunks[3], 16)

        # Frequency periods
        xFT_for_period = 0 if np.isnan(xFT) else int(xFT)
        X_period_us, T_period_us = _freq_periods(xFT_for_period, xFP)

        return {
            "ppc_time": ppc_time,
            "t_housing_counts": t_housing_counts,
            "xFT": xFT,
            "xFP": xFP,
            "X_period_us": X_period_us,
            "T_period_us": T_period_us,
        }

    except Exception as exc:
        parse_error_logger.warning(
            "PARSE_ERROR  file=%-24s  reason=%-40s  raw=%s",
            source_file or "<unknown>",
            str(exc),
            reading,
        )
        logger.debug("Failed to parse line %r: %s", reading, exc)
        return None


def parse_day_df(raw_df: pd.DataFrame, source_file: str = "") -> pd.DataFrame:
    """Parse a full day's raw DataFrame into a structured DataFrame.

    Parameters
    ----------
    raw_df:
        DataFrame with columns ``dmas_time`` and ``readings``,
        as returned by :func:`fetch_raw.fetch_day`.
    source_file:
        Label (e.g. ``'2025-01-15'``) passed to :func:`parse_hex_line`
        so that error log entries identify which day the bad line came from.

    Returns
    -------
    pd.DataFrame
        One row per successfully parsed reading with columns:
        ``dmas_time``, ``ppc_time``, ``t_housing_counts``,
        ``xFT``, ``xFP``, ``X_period_us``, ``T_period_us``.
        Rows that fail to parse are dropped and recorded in ``out/parse_errors.log``.
    """
    records = []
    n_failed = 0

    for dmas_time, reading in zip(raw_df["dmas_time"], raw_df["readings"]):
        parsed = parse_hex_line(str(reading), source_file=source_file)
        if parsed is None:
            n_failed += 1
            continue
        parsed["dmas_time"] = dmas_time
        records.append(parsed)

    if n_failed:
        logger.warning("Failed to parse %d / %d lines", n_failed, len(raw_df))
        parse_error_logger.warning(
            "SUMMARY      file=%-24s  total=%d  failed=%d  ok=%d",
            source_file or "<unknown>",
            len(raw_df),
            n_failed,
            len(raw_df) - n_failed,
        )

    if not records:
        return pd.DataFrame(
            columns=[
                "dmas_time", "ppc_time", "t_housing_counts",
                "xFT", "xFP", "X_period_us", "T_period_us",
            ]
        )

    df = pd.DataFrame(records)[
        ["dmas_time", "ppc_time", "t_housing_counts",
         "xFT", "xFP", "X_period_us", "T_period_us"]
    ]

    df["dmas_time"] = pd.to_datetime(df["dmas_time"])
    df["ppc_time"] = pd.to_datetime(df["ppc_time"])
    df["t_housing_counts"] = df["t_housing_counts"].astype("Int32")
    df["xFT"] = pd.array(df["xFT"], dtype="Int64")
    df["xFP"] = df["xFP"].astype("Int64")

    return df.set_index("dmas_time")
