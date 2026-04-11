"""
Historical ingestion pipeline — football prematch odds.

Run manually (~every 6 months) when new season CSVs are downloaded from
football-data.co.uk into .data/data_lake/.

Reads each unprocessed CSV, maps to the bronze schema, and upserts into
the bronze parquet table. Already-processed files are skipped via a log.
"""

from pathlib import Path

import pandas as pd

from ingestion_pipeline.utils import (
    BRONZE_COLUMNS,
    get_league_name,
    load_processed_log,
    pick_odds_columns,
    save_processed_log,
    upsert_bronze,
)

_REPO_ROOT = Path(__file__).resolve().parents[1]


def from_csv(csv_path: str) -> pd.DataFrame:
    """
    Read a football-data.co.uk season CSV and return a bronze-schema DataFrame.

    League name is derived from the 'Div' column using LEAGUE_CODES in config.py.
    Add any missing codes there before running.

    Args:
        csv_path: Path to the CSV file (e.g. .data/data_lake/E0_2324.csv).

    Returns:
        DataFrame conforming to BRONZE_COLUMNS.
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

    return df[BRONZE_COLUMNS].dropna(subset=["home_win_odds", "draw_odds", "away_odds"])


def main() -> None:
    """
    Loop all CSVs in .data/data_lake/, skip already-processed files,
    and append new data into the bronze table.
    """
    data_lake_path = _REPO_ROOT / ".data" / "data_lake"
    csv_files = sorted(data_lake_path.glob("*.csv"))

    if not csv_files:
        print("No CSV files found in .data/data_lake/")
        return

    processed = load_processed_log()
    new_files = [f for f in csv_files if f.name not in processed]

    if not new_files:
        print("All CSV files already processed. Bronze table is up to date.")
        return

    print(f"Found {len(new_files)} new file(s) to process (skipping {len(processed)} already done).")

    for csv_path in new_files:
        print(f"\nProcessing: {csv_path.name}")
        try:
            df = from_csv(str(csv_path))
            upsert_bronze(df)
            processed.add(csv_path.name)
            save_processed_log(processed)
            print(f"  OK: {csv_path.name} done ({len(df)} rows)")
        except Exception as e:
            print(f"  FAILED: {csv_path.name}: {e}")


if __name__ == "__main__":
    main()
