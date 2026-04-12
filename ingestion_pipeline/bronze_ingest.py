"""
Bronze storage — football prematch odds.

Handles reading, writing, and upserting the bronze parquet table.
"""

from pathlib import Path

import pandas as pd

_REPO_ROOT = Path(__file__).resolve().parents[1]
BRONZE_PATH = _REPO_ROOT / ".database" / "bronze" / "football_prematch_odds.parquet"

BRONZE_COLUMNS = [
    "date",
    "time",
    "league",
    "home_team",
    "away_team",
    "home_win_odds",
    "draw_odds",
    "away_odds",
    "result",
    "source",
]


def load_bronze() -> pd.DataFrame:
    """Load the existing bronze table, or return an empty DataFrame if none exists."""
    if not BRONZE_PATH.exists():
        return pd.DataFrame(columns=BRONZE_COLUMNS)
    return pd.read_parquet(BRONZE_PATH)


def save_bronze(df: pd.DataFrame) -> None:
    """Save the full bronze table to parquet."""
    BRONZE_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(BRONZE_PATH, index=False)


def upsert_bronze(new_df: pd.DataFrame) -> None:
    """
    Append new rows to the bronze table and deduplicate.

    Deduplication key: (date, home_team, away_team, league).
    Existing rows are preserved; new rows win on conflict (keep="last").
    """
    existing = load_bronze()
    combined = pd.concat([existing, new_df], ignore_index=True)
    combined = combined.drop_duplicates(subset=["date", "home_team", "away_team", "league"], keep="last")
    combined = combined.sort_values("date").reset_index(drop=True)
    save_bronze(combined)
    print(f"Bronze table updated: {len(combined)} total rows.")
