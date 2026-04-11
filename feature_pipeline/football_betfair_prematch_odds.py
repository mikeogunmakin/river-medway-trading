"""
Betfair feature pipeline — football prematch odds.

Run daily. Fetches live pre-match odds from the Betfair API for upcoming fixtures,
writes to bronze, computes features, and writes to the feature store for inference.

Competition IDs to ingest are configured in config.py.
"""

from datetime import datetime, timedelta, timezone

from dotenv import load_dotenv

from ingestion.runners.betfair.load_live_api_data import from_betfair_api
from ingestion.bronze_ingest import load_bronze, upsert_bronze
from feature_pipeline.config import COMPETITION_IDS
from feature_pipeline.features import build_features
from feature_pipeline.feature_store_football_prematch_odds import write_to_feature_store


def main() -> None:
    load_dotenv()
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
    print(f"Daily update complete. {len(features)} rows written to feature store.")


if __name__ == "__main__":
    main()
