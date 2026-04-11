"""
Live feature runner — football prematch odds.

Run daily. Fetches upcoming fixture odds from Betfair, writes to bronze,
computes features, and writes to the Hopsworks feature store for inference.

Competition IDs are configured in ingestion/runners/betfair/selector.py.
Leagues written to the feature store are configured in selectors.py.
"""

from datetime import datetime, timedelta, timezone

from dotenv import load_dotenv

from ingestion.runners.betfair.load_live_api_data_to_database import from_betfair_api
from ingestion.runners.betfair.selector import COMPETITION_IDS
from ingestion.bronze_ingest import load_bronze, upsert_bronze
from feature_pipeline.football_prematch_odds.features import build_features
from feature_pipeline.football_prematch_odds.selectors import select_upcoming
from feature_pipeline.football_prematch_odds.feature_store import write_to_feature_store


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
    upcoming = select_upcoming(bronze)
    features = build_features(upcoming)
    write_to_feature_store(features)
    print(f"Daily update complete. {len(features)} rows written to feature store.")


if __name__ == "__main__":
    main()
