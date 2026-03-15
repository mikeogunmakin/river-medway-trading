"""
Incremental feature pipeline — football prematch odds.

Fetches live pre-match odds from Betfair, writes to bronze, computes features,
and writes the resulting feature group to the feature store for inference.
"""

from datetime import datetime, timedelta, timezone

import pandas as pd

from ingestion_pipeline.bronze_football_prematch_odds import from_betfair_api, upsert_bronze, load_bronze
from ingestion_pipeline.ingestion_utils import COMPETITION_IDS
from feature_pipeline.feature_engineering.prematch_odds import build_features


def write_to_feature_store(df: pd.DataFrame) -> None:
    # TODO: replace with Hopsworks or Feast feature store write
    raise NotImplementedError("Feature store write not yet configured.")


def main() -> None:
    now = datetime.now(timezone.utc)
    from_time = now + timedelta(minutes=60)
    to_time = from_time + timedelta(hours=24)

    df = from_betfair_api(from_time, to_time, competition_ids=COMPETITION_IDS)

    if df.empty:
        print("No upcoming fixtures found.")
        return

    upsert_bronze(df)

    bronze = load_bronze()
    upcoming = bronze[bronze["result"].isna()].copy()  # future fixtures only
    features = build_features(upcoming)
    write_to_feature_store(features)
    print(f"Incremental update complete. {len(features)} rows written to feature store.")


if __name__ == "__main__":
    main()
