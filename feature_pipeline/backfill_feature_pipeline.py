"""
Backfill feature pipeline.

Reads historical fixture data from CSV, computes features via feature_engineering,
and writes the resulting feature DataFrame to the feature store.
"""

import pandas as pd

from feature_pipeline.feature_engineering import (
    compute_form,
    compute_rolling_goal_avg,
    compute_shot_conversion_rate,
)


def load_historical_data(csv_path: str) -> pd.DataFrame:
    df = pd.read_csv(csv_path, parse_dates=["date"])
    return df


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


def main(csv_path: str) -> None:
    df = load_historical_data(csv_path)
    features = build_features(df)
    write_to_feature_store(features)
    print(f"Backfill complete. {len(features)} rows written to feature store.")


if __name__ == "__main__":
    main("fixture_stats.csv")
