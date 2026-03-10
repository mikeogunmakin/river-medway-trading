import os

import pandas as pd
import requests
from dotenv import load_dotenv

base_url = 'https://v3.football.api-sports.io'

league_ids = [39]
season = 2024


def configure():
    load_dotenv()


def get_fixtures_for_league(league_id: int, season: int) -> list[dict]:
    """Fetch fixture_id and date for a given league and season."""
    url = f"{base_url}/fixtures"
    headers = {
        "x-apisports-key": os.getenv('api_key')
    }

    params = {
        "league": league_id,
        "season": season
    }

    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()

    data = response.json()

    if data.get("errors"):
        print(f"  [!] API error for league {league_id}: {data['errors']}")
        return []

    fixtures = data.get("response", [])
    return [
        {
            "league_id": league_id,
            "fixture_id": f["fixture"]["id"],
            "date": f["fixture"]["date"]
        }
        for f in fixtures
    ]


def get_all_fixtures(league_ids: list[int], season: int) -> pd.DataFrame:
    """Fetch fixtures for all leagues and return a combined DataFrame."""
    all_rows = []

    for league_id in league_ids:
        print(f"Fetching fixtures for league {league_id}, season {season}...")
        rows = get_fixtures_for_league(league_id, season)
        all_rows.extend(rows)
        print(f"  → Found {len(rows)} fixtures")

    df = pd.DataFrame(all_rows, columns=["league_id", "fixture_id", "date"])
    df["date"] = pd.to_datetime(df["date"], utc=True)

    return df


def main():
    configure()
    df = get_all_fixtures(league_ids, season)
    print(df.head(10).to_string(index=False))
    print(f"\nTotal fixtures: {len(df)}")
    print(f"Columns: {list(df.columns)}")

    # Save to CSV
    output_file = "fixture_ids.csv"
    df.to_csv(output_file, index=False)
    print(f"\n✓ Saved to {output_file}")

    return df


if __name__ == "__main__":
    df = main()