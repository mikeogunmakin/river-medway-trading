"""
Backfill feature pipeline — football prematch odds.

Reads the bronze odds table, computes features via feature_engineering.prematch_odds,
and writes the resulting feature group to Hopsworks.
"""

import os

import hopsworks
import pandas as pd
from dotenv import load_dotenv

from ingestion_pipeline.ing_pipeline_football_prematch_odds import load_bronze
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
    df = load_bronze()
    df = df[df["result"].notna()].copy()  # historical matches only
    features = build_features(df)
    write_to_feature_store(features)
    print(f"Backfill complete. {len(features)} rows written to feature store.")


if __name__ == "__main__":
    main()
