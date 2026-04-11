"""
Hopsworks feature store — connection and write utilities.

Shared by all feature pipelines. Add new feature group writers here as
additional sports/markets are onboarded.
"""

import os

import hopsworks
import pandas as pd
from dotenv import load_dotenv

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
