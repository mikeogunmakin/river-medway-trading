"""
Historical runner — football prematch odds.

Run manually (~every 6 months) when new season CSVs are downloaded from
football-data.co.uk into .database/data_lake/.

Reads each unprocessed CSV, maps to the bronze schema, and upserts into
the bronze parquet table. Already-processed files are skipped via the log.
"""

from pathlib import Path

from data_sources.csv_football_data import from_csv
from ingestion.bronze_ingest import upsert_bronze
from ingestion.processed_log import load_processed_log, save_processed_log

_REPO_ROOT = Path(__file__).resolve().parents[3]


def main() -> None:
    """
    Loop all CSVs in .database/data_lake/, skip already-processed files,
    and append new data into the bronze table.
    """
    data_lake_path = _REPO_ROOT / ".database" / "data_lake"
    csv_files = sorted(data_lake_path.glob("*.csv"))

    if not csv_files:
        print("No CSV files found in .database/data_lake/")
        return

    processed = load_processed_log()
    new_files = [f for f in csv_files if f.name not in processed]

    if not new_files:
        print("All CSV files already processed. Bronze table is up to date.")
        return

    print(f"Found {len(new_files)} new file(s) to process (skipping {len(processed)} already done).")

    for csv_path in new_files:
        print(f"\nProcessing: {csv_path.name}")
        try:
            df = from_csv(str(csv_path))
            upsert_bronze(df)
            processed.add(csv_path.name)
            save_processed_log(processed)
            print(f"  OK: {csv_path.name} done ({len(df)} rows)")
        except Exception as e:
            print(f"  FAILED: {csv_path.name}: {e}")


if __name__ == "__main__":
    main()
