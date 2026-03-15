"""
Backfill feature pipeline — football prematch odds.

Reads the bronze odds table, computes features via feature_engineering.prematch_odds,
and writes the resulting feature group to the feature store.
"""

import pandas as pd

from ingestion_pipeline.bronze_football_prematch_odds import load_bronze
from feature_pipeline.feature_engineering.prematch_odds import build_features


def write_to_feature_store(df: pd.DataFrame) -> None:
    # TODO: replace with Hopsworks or Feast feature store write
    raise NotImplementedError("Feature store write not yet configured.")


def main() -> None:
    df = load_bronze()
    df = df[df["result"].notna()].copy()  # historical matches only
    features = build_features(df)
    write_to_feature_store(features)
    print(f"Backfill complete. {len(features)} rows written to feature store.")


if __name__ == "__main__":
    main()
