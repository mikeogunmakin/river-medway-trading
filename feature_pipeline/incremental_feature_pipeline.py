"""
Incremental feature pipeline.

Fetches the latest fixture data from the API, computes features via feature_engineering,
and writes the resulting feature DataFrame to the feature store for inference.
"""

import os

import pandas as pd
from dotenv import load_dotenv

from ingestion_pipeline.api_football.fixture_stats_api import fetch_fixture_stats
from feature_pipeline.feature_engineering import (
    compute_form,
    compute_rolling_goal_avg,
    compute_shot_conversion_rate,
)


def configure():
    load_dotenv()


def fetch_latest_fixtures(fixture_ids: list[int]) -> pd.DataFrame:
    api_key = os.getenv("api_key")
    return fetch_fixture_stats(fixture_ids, api_key)


def build_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values("date").reset_index(drop=True)

    df["home_rolling_goals"] = compute_rolling_goal_avg(df, "home_team_id", "home_goals")
    df["away_rolling_goals"] = compute_rolling_goal_avg(df, "away_team_id", "away_goals")
    df["home_form"] = compute_form(df, "home_team_id", "home_result")
    df["away_form"] = compute_form(df, "away_team_id", "away_result")
    df["home_shot_conversion"] = compute_shot_conversion_rate(df, "home_goals", "home_shots_on_goal")
    df["away_shot_conversion"] = compute_shot_conversion_rate(df, "away_goals", "away_shots_on_goal")

    return df


def write_to_feature_store(df: pd.DataFrame) -> None:
    # TODO: replace with Hopsworks or Feast feature store write
    raise NotImplementedError("Feature store write not yet configured.")


def main(fixture_ids: list[int]) -> None:
    configure()
    df = fetch_latest_fixtures(fixture_ids)
    features = build_features(df)
    write_to_feature_store(features)
    print(f"Incremental update complete. {len(features)} rows written to feature store.")


if __name__ == "__main__":
    main([867946])
