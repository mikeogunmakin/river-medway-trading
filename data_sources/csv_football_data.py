"""
football-data.co.uk CSV source — football prematch odds.

Handles reading and parsing season CSV files downloaded from
https://www.football-data.co.uk/data.php

To add a new league:
  1. Find the league code from the URL above
  2. Add an entry to LEAGUE_CODES below: "CODE": "League Name"
"""

import pandas as pd

# ---------------------------------------------------------------------------
# League code mappings
# ---------------------------------------------------------------------------

LEAGUE_CODES = {
    # England
    "E0": "Premier League",
    "E1": "Championship",
    "E2": "League One",
    "E3": "League Two",

    # Germany
    "D1": "Bundesliga",
    "D2": "2. Bundesliga",

    # Spain
    "SP1": "La Liga",
    "SP2": "La Liga 2",

    # Italy
    "I1": "Serie A",
    "I2": "Serie B",

    # France
    "F1": "Ligue 1",
    "F2": "Ligue 2",

    # Netherlands
    "N1": "Eredivisie",

    # Portugal
    "P1": "Primeira Liga",

    # Scotland
    "SC0": "Scottish Premiership",

    # Belgium
    "B1": "First Division A",

    # Turkey
    "T1": "Süper Lig",

    # Greece
    "G1": "Super League Greece",
}

# Odds column priority — tried in order until one has data
_ODDS_PRIORITY = [
    ("BFEH", "BFED", "BFEA"),  # Betfair Exchange (preferred)
    ("AvgH", "AvgD", "AvgA"),  # Market average
    ("PSH",  "PSD",  "PSA"),   # Pinnacle (secondary fallback)
]

# ---------------------------------------------------------------------------
# League lookup
# ---------------------------------------------------------------------------


def get_league_name(code: str) -> str:
    """
    Return the league name for a given football-data.co.uk code.

    Raises:
        KeyError: if the code is not in LEAGUE_CODES — add it to source/csv_football_data.py.
    """
    if code not in LEAGUE_CODES:
        raise KeyError(
            f"Unknown league code '{code}'. Add it to source/csv_football_data.py."
        )
    return LEAGUE_CODES[code]


# ---------------------------------------------------------------------------
# CSV parsing
# ---------------------------------------------------------------------------


def pick_odds_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Select odds columns in priority order: BFEH → AvgH → PSH.
    Uses the first set where all three columns exist and have non-null data.

    Raises:
        ValueError: if no recognised odds columns are found in the CSV.
    """
    for h_col, d_col, a_col in _ODDS_PRIORITY:
        cols = [h_col, d_col, a_col]
        if all(c in df.columns for c in cols) and df[cols].notna().any().all():
            df = df.copy()
            df["home_win_odds"] = df[h_col]
            df["draw_odds"] = df[d_col]
            df["away_odds"] = df[a_col]
            print(f"  Using odds columns: {h_col} / {d_col} / {a_col}")
            return df

    raise ValueError(
        "No recognised odds columns found in CSV. "
        "Expected one of: BFEH/BFED/BFEA, AvgH/AvgD/AvgA, PSH/PSD/PSA."
    )


def from_csv(csv_path: str) -> pd.DataFrame:
    """
    Read a football-data.co.uk season CSV and return a bronze-schema DataFrame.

    League name is derived from the 'Div' column using LEAGUE_CODES above.
    Add any missing codes there before running.

    Args:
        csv_path: Path to the CSV file (e.g. .database/data_lake/E0_2324.csv).

    Returns:
        DataFrame with columns: date, league, home_team, away_team,
        home_win_odds, draw_odds, away_odds, result, source.
    """
    raw = pd.read_csv(csv_path)
    raw["Date"] = pd.to_datetime(raw["Date"], dayfirst=True)

    df = pick_odds_columns(raw)

    df = df.rename(columns={
        "Date":     "date",
        "HomeTeam": "home_team",
        "AwayTeam": "away_team",
        "FTR":      "result",
    })

    df["league"] = df["Div"].map(get_league_name)
    df["source"] = "historical"
    df["date"] = pd.to_datetime(df["date"], utc=True)

    bronze_columns = ["date", "league", "home_team", "away_team",
                      "home_win_odds", "draw_odds", "away_odds", "result", "source"]

    return df[bronze_columns].dropna(subset=["home_win_odds", "draw_odds", "away_odds"])
