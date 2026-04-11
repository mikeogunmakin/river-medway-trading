"""
Canonicalisation — maps raw source data to the bronze schema.

Each function takes source-specific raw data and returns a DataFrame
conforming to BRONZE_COLUMNS.
"""

import pandas as pd

from ingestion_pipeline.bronze_ingest import BRONZE_COLUMNS


def pivot_betfair_odds(raw: pd.DataFrame) -> pd.DataFrame:
    """
    Pivot runner-level Betfair data into one row per match.

    Betfair MATCH_ODDS markets have three runners: home team, away team, Draw.
    The event_name is 'Home Team v Away Team', which we use to identify sides.
    """
    rows = []

    for (market_id, event_name, competition_name, market_start_time), group in raw.groupby(
        ["market_id", "event_name", "competition_name", "market_start_time"]
    ):
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
