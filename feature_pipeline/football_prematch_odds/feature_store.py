"""
Feature store — football prematch odds feature group.

Defines the feature group schema and handles writes to Hopsworks.
Inherits the Hopsworks connection from common/feature_store_base.py.
"""

import pandas as pd

from feature_pipeline.common.feature_store_base import get_feature_store

FEATURE_GROUP_NAME = "football_prematch_odds"
FEATURE_GROUP_VERSION = 5


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
