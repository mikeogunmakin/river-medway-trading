"""
Bronze: Football Pre-Match Odds Table.

Builds and maintains the bronze-layer odds table from two sources:
  - Historical CSVs downloaded from football-data.co.uk  (backfill path)
  - Live Betfair Exchange API                            (incremental path)

Bronze schema (one row per match):
    date           | datetime (UTC)
    league         | str
    home_team      | str
    away_team      | str
    home_win_odds  | float   (BFEH → AvgH fallback)
    draw_odds      | float   (BFED → AvgD fallback)
    away_odds      | float   (BFEA → AvgA fallback)
    result         | str     (H / D / A — NaN for future fixtures)
    source         | str     ("historical" | "betfair_api")
"""

from pathlib import Path

import pandas as pd

from ingestion_pipeline.ingestion_utils import fetch_prematch_odds
from ingestion_pipeline.config import get_league_name

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

_REPO_ROOT = Path(__file__).resolve().parents[1]
BRONZE_PATH = _REPO_ROOT / ".data" / "bronze" / "football_prematch_odds.parquet"

# ---------------------------------------------------------------------------
# Bronze schema columns (enforced on every write)
# ---------------------------------------------------------------------------

BRONZE_COLUMNS = [
    "date",
    "league",
    "home_team",
    "away_team",
    "home_win_odds",
    "draw_odds",
    "away_odds",
    "result",
    "source",
]

# ---------------------------------------------------------------------------
# football-data.co.uk CSV column mappings
# ---------------------------------------------------------------------------

# Preferred odds columns (Betfair Exchange)
_BFE_ODDS = {"home_win_odds": "BFEH", "draw_odds": "BFED", "away_odds": "BFEA"}

# Fallback odds columns (market average)
_AVG_ODDS = {"home_win_odds": "AvgH", "draw_odds": "AvgD", "away_odds": "AvgA"}

# Actual column names in football-data.co.uk files (no spaces)
_BFE_COLS = {"BFE H": "BFEH", "BFE D": "BFED", "BFE A": "BFEA"}


def _pick_odds_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Select BFEH/BFED/BFEA if present and non-null, otherwise fall back to
    AvgH/AvgD/AvgA. Returns df with standardised home_win_odds/draw_odds/away_odds.
    """
    bfe_available = all(col in df.columns for col in ["BFEH", "BFED", "BFEA"])
    bfe_has_data = bfe_available and df[["BFEH", "BFED", "BFEA"]].notna().any().all()

    if bfe_has_data:
        df["home_win_odds"] = df["BFEH"]
        df["draw_odds"] = df["BFED"]
        df["away_odds"] = df["BFEA"]
    else:
        df["home_win_odds"] = df["AvgH"]
        df["draw_odds"] = df["AvgD"]
        df["away_odds"] = df["AvgA"]

    return df


# ---------------------------------------------------------------------------
# Source 1: Historical CSV (football-data.co.uk)
# ---------------------------------------------------------------------------


def from_csv(csv_path: str) -> pd.DataFrame:
    """
    Read a football-data.co.uk season CSV and return a bronze-schema DataFrame.

    League name is derived from the 'Div' column in the CSV using LEAGUE_CODES
    in league_config.py. Add any missing codes there before running.

    Args:
        csv_path: Path to the CSV file (e.g. .data/data_lake/E0_2324.csv).

    Returns:
        DataFrame conforming to BRONZE_COLUMNS.
    """
    raw = pd.read_csv(csv_path, dayfirst=True, parse_dates=["Date"])

    df = _pick_odds_columns(raw)

    df = df.rename(columns={
        "Date":     "date",
        "HomeTeam": "home_team",
        "AwayTeam": "away_team",
        "FTR":      "result",
    })

    df["league"] = df["Div"].map(get_league_name)
    df["source"] = "historical"
    df["date"] = pd.to_datetime(df["date"], utc=True)

    return df[BRONZE_COLUMNS].dropna(subset=["home_win_odds", "draw_odds", "away_odds"])


# ---------------------------------------------------------------------------
# Source 2: Betfair Exchange API (live / incremental)
# ---------------------------------------------------------------------------


def _pivot_betfair_odds(raw: pd.DataFrame) -> pd.DataFrame:
    """
    Pivot runner-level Betfair data into one row per match.

    Betfair MATCH_ODDS markets have three runners: home team, away team, Draw.
    The event_name is 'Home Team v Away Team', which we use to identify sides.
    """
    rows = []

    for (market_id, event_name, competition_name, market_start_time), group in raw.groupby(
        ["market_id", "event_name", "competition_name", "market_start_time"]
    ):
        # event_name format: "Man City v Arsenal"
        parts = str(event_name).split(" v ", maxsplit=1)
        if len(parts) != 2:
            continue

        home_name, away_name = parts[0].strip(), parts[1].strip()

        odds = {}
        for _, runner in group.iterrows():
            name = str(runner["runner_name"]).strip()
            price = runner["best_back_price"]

            if name == "The Draw":
                odds["draw_odds"] = price
            elif name == home_name:
                odds["home_win_odds"] = price
            elif name == away_name:
                odds["away_odds"] = price

        if len(odds) < 3:
            continue

        rows.append({
            "date":          market_start_time,
            "league":        competition_name,
            "home_team":     home_name,
            "away_team":     away_name,
            "home_win_odds": odds["home_win_odds"],
            "draw_odds":     odds["draw_odds"],
            "away_odds":     odds["away_odds"],
            "result":        None,
            "source":        "betfair_api",
        })

    return pd.DataFrame(rows, columns=BRONZE_COLUMNS)


def from_betfair_api(from_time, to_time, competition_ids=None) -> pd.DataFrame:
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

    return _pivot_betfair_odds(raw)


# ---------------------------------------------------------------------------
# Bronze storage (local parquet — swap read/write here when moving to GCS)
# ---------------------------------------------------------------------------


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

    Deduplication key: (date, home_team, away_team).
    Existing rows are preserved; new rows are appended only if not already present.
    """
    existing = load_bronze()
    combined = pd.concat([existing, new_df], ignore_index=True)
    combined = combined.drop_duplicates(subset=["date", "home_team", "away_team"], keep="last")
    combined = combined.sort_values("date").reset_index(drop=True)
    save_bronze(combined)
    print(f"Bronze table updated: {len(combined)} total rows.")
