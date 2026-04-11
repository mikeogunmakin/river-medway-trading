"""
Historical feature pipeline — football prematch odds.

Run manually when a new batch of historical CSV data is available.

Reads the bronze odds table, computes features via features.build_features,
and writes the resulting feature group to Hopsworks.
"""

from ingestion.bronze_ingest import load_bronze
from feature_pipeline.features import build_features
from feature_pipeline.feature_store_football_prematch_odds import write_to_feature_store


def main() -> None:
    df = load_bronze()
    df = df[df["result"].notna()].copy()  # historical matches only
    features = build_features(df)
    write_to_feature_store(features)
    print(f"Backfill complete. {len(features)} rows written to feature store.")


if __name__ == "__main__":
    main()
