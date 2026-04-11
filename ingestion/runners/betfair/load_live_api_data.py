"""
Betfair runner — football prematch odds.

Fetches live pre-match odds from the Betfair Exchange API and returns a
bronze-schema DataFrame. Called by the feature pipeline as part of the
daily run.

Competition IDs to ingest are configured in feature_pipeline/config.py.
"""

from datetime import datetime

import pandas as pd

from data_sources.api_betfair import fetch_prematch_odds
from ingestion.bronze_ingest import BRONZE_COLUMNS
from ingestion.canonicalise import pivot_betfair_odds


def from_betfair_api(
    from_time: datetime,
    to_time: datetime,
    competition_ids: list[str] | None = None,
) -> pd.DataFrame:
    """
    Fetch live pre-match odds from Betfair and return a bronze-schema DataFrame.

    Args:
        from_time:       Window start (datetime, UTC).
        to_time:         Window end (datetime, UTC).
        competition_ids: Optional list of Betfair competition IDs to filter.

    Returns:
        DataFrame conforming to BRONZE_COLUMNS. result column will be NaN.
    """
    raw = fetch_prematch_odds(from_time, to_time, competition_ids)

    if raw.empty:
        return pd.DataFrame(columns=BRONZE_COLUMNS)

    return pivot_betfair_odds(raw)
