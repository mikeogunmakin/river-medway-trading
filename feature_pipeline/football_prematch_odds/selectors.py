"""
Selectors — football prematch odds.

Filters the bronze table to define what rows are passed through to the
feature store. As the bronze table grows to cover multiple sports and
leagues, selectors control exactly what each pipeline processes.

Add or adjust filters here to change the scope of what gets written
to the feature store.
"""

import pandas as pd

# Leagues to include in the feature store.
# Add league names here as you expand coverage.
LEAGUES = [
    "Premier League",
]


def select_historical(df: pd.DataFrame) -> pd.DataFrame:
    """
    Filter bronze to completed historical matches for the configured leagues.
    Used by the historical backfill runner.
    """
    mask = df["result"].notna() & df["league"].isin(LEAGUES)
    return df[mask].copy()


def select_upcoming(df: pd.DataFrame) -> pd.DataFrame:
    """
    Filter bronze to upcoming fixtures (no result yet) for the configured leagues.
    Used by the live daily runner.
    """
    mask = df["result"].isna() & df["league"].isin(LEAGUES)
    return df[mask].copy()
