"""
Incremental feature pipeline — football prematch odds.

Fetches live pre-match odds from Betfair, writes to bronze, computes features,
and writes the resulting feature group to the feature store for inference.
"""

import os
from datetime import datetime, timedelta, timezone

import hopsworks
import pandas as pd
from dotenv import load_dotenv

from ingestion_pipeline.ing_pipeline_football_prematch_odds import from_betfair_api, upsert_bronze, load_bronze
from ingestion_pipeline.ingestion_utils import COMPETITION_IDS
from feature_pipeline.feature_engineering.prematch_odds import build_features

FEATURE_GROUP_NAME = "football_prematch_odds"
FEATURE_GROUP_VERSION = 2


def get_feature_store():
    load_dotenv()
    project = hopsworks.login(
        project=os.getenv("HOPSWORKS_PROJECT"),
        api_key_value=os.getenv("HOPSWORKS_API_KEY"),
    )
    return project.get_feature_store()


def write_to_feature_store(df: pd.DataFrame) -> None:
    fs = get_feature_store()

    fg = fs.get_or_create_feature_group(
        name=FEATURE_GROUP_NAME,
        version=FEATURE_GROUP_VERSION,
        primary_key=["date", "league", "home_team", "away_team"],
        description="Pre-match odds features for football fixtures",
        online_enabled=False,
    )

    fg.insert(df, write_options={"wait_for_job": True})
    print(f"  Written to feature group '{FEATURE_GROUP_NAME}' v{FEATURE_GROUP_VERSION}")


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
