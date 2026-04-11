"""
Historical feature runner — football prematch odds.

Run manually (~every 12 months) when new historical CSV data has been
loaded into bronze via ingestion/runners/historical/load_historical_csvs.py.

Reads bronze, selects completed historical matches, computes features,
and writes to the Hopsworks feature store.
"""

from ingestion_pipeline.bronze_ingest import load_bronze
from feature_pipeline.football_prematch_odds.features import build_features
from feature_pipeline.football_prematch_odds.selectors import select_historical
from feature_pipeline.football_prematch_odds.feature_store import write_to_feature_store


def main() -> None:
    df = load_bronze()
    df = select_historical(df)
    features = build_features(df)
    write_to_feature_store(features)
    print(f"Backfill complete. {len(features)} rows written to feature store.")


if __name__ == "__main__":
    main()
