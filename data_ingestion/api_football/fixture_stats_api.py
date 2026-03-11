import os
import re

import pandas as pd
import requests
from dotenv import load_dotenv


def configure():
    load_dotenv()


def clean_stat_name(name: str) -> str:
    return re.sub(r"[^a-z0-9_]", "", name.lower().replace(" ", "_"))


def clean_value(value):
    if value is None:
        return 0
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return 0
        if s.endswith("%"):
            try:
                return float(s[:-1])
            except ValueError:
                return 0
        try:
            return float(s) if "." in s else int(s)
        except ValueError:
            return s
    return value


def fetch_fixture_stats(fixture_ids, api_key: str) -> pd.DataFrame:
    """
    Fetch home/away stats for one or many fixture IDs.
    Returns one row per fixture with home_ and away_ prefixed stat columns.
    """
    if isinstance(fixture_ids, int):
        fixture_ids = [fixture_ids]

    headers = {"x-apisports-key": os.getenv('api_key')}
    rows = []

    for fixture_id in fixture_ids:
        row = {"fixture_id": fixture_id}

        # --- Fixture metadata ---
        fixture_resp = requests.get(
            f"{os.getenv('base_url')}/fixtures",
            headers=headers,
            params={"id": fixture_id}
        ).json()

        fixture_data = (fixture_resp.get("response") or [{}])[0]
        if not fixture_data:
            rows.append({**row, "error": "fixture_not_found"})
            continue

        f = fixture_data.get("fixture", {})
        league = fixture_data.get("league", {})
        teams = fixture_data.get("teams", {})

        home_team_id = teams.get("home", {}).get("id")
        away_team_id = teams.get("away", {}).get("id")

        row.update({
            "date":           f.get("date"),
            "referee":        f.get("referee"),
            "venue_name":     f.get("venue", {}).get("name"),
            "league_id":      league.get("id"),
            "league_name":    league.get("name"),
            "season":         league.get("season"),
            "round":          league.get("round"),
            "home_team_id":   home_team_id,
            "home_team_name": teams.get("home", {}).get("name"),
            "away_team_id":   away_team_id,
            "away_team_name": teams.get("away", {}).get("name"),
        })

        # --- Statistics ---
        stats_resp = requests.get(
            f"{os.getenv('base_url')}/fixtures/statistics",
            headers=headers,
            params={"fixture": fixture_id}
        ).json()

        stat_blocks = stats_resp.get("response") or []
        if not stat_blocks:
            rows.append({**row, "error": "stats_not_found"})
            continue

        for i, block in enumerate(stat_blocks):
            team_id = block.get("team", {}).get("id")
            prefix = "home_" if team_id == home_team_id else "away_"

            for stat in block.get("statistics", []):
                col = clean_stat_name(stat.get("type", ""))
                row[f"{prefix}{col}"] = clean_value(stat.get("value"))

        rows.append(row)

    df = pd.DataFrame(rows)

    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce", utc=True)

    return df.sort_values("fixture_id").reset_index(drop=True)


def main():
    configure()
    df = fetch_fixture_stats(867946, os.getenv('api_key'))
    print(df.head(10).to_string(index=False))
    print(f"\nTotal fixtures: {len(df)}")
    print(f"Columns: {list(df.columns)}")

    # Save to CSV
    output_file = "fixture_stats.csv"
    df.to_csv(output_file, index=False)
    print(f"\n✓ Saved to {output_file}")

    return df


if __name__ == "__main__":
    df = main()
