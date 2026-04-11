"""
Ingestion pipeline utilities — football prematch odds.

Houses all shared functions:
  - League code lookup
  - Bronze storage (load, save, upsert, processed-files log)
  - CSV odds column selection and Betfair odds pivoting
  - Betfair Exchange API (auth, market discovery, odds fetching)
"""

import json
import os
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv

from ingestion_pipeline.config import (
    BETFAIR_API_URL,
    LEAGUE_CODES,
    MATCH_ODDS_MARKET_TYPE,
    SOCCER_EVENT_TYPE_ID,
)

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

_REPO_ROOT = Path(__file__).resolve().parents[1]
BRONZE_PATH = _REPO_ROOT / ".data" / "bronze" / "football_prematch_odds.parquet"
PROCESSED_LOG_PATH = _REPO_ROOT / ".data" / "bronze" / "processed_files.json"

# ---------------------------------------------------------------------------
# Bronze schema columns (enforced on every write)
# ---------------------------------------------------------------------------

BRONZE_COLUMNS = [
    "date",
    "league",
    "home_team",
    "away_team",
    "home_win_odds",
    "draw_odds",
    "away_odds",
    "result",
    "source",
]

# Odds column priority — tried in order until one has data
_ODDS_PRIORITY = [
    ("BFEH", "BFED", "BFEA"),  # Betfair Exchange (preferred)
    ("AvgH", "AvgD", "AvgA"),  # Market average
    ("PSH",  "PSD",  "PSA"),   # Pinnacle (secondary fallback)
]

# ---------------------------------------------------------------------------
# League lookup
# ---------------------------------------------------------------------------


def get_league_name(code: str) -> str:
    """
    Return the league name for a given football-data.co.uk code.

    Raises:
        KeyError: if the code is not in LEAGUE_CODES — add it to config.py.
    """
    if code not in LEAGUE_CODES:
        raise KeyError(
            f"Unknown league code '{code}'. Add it to ingestion_pipeline/config.py."
        )
    return LEAGUE_CODES[code]


# ---------------------------------------------------------------------------
# CSV odds column selection
# ---------------------------------------------------------------------------


def pick_odds_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Select odds columns in priority order: BFEH → AvgH → PSH.
    Uses the first set where all three columns exist and have non-null data.

    Raises:
        ValueError: if no recognised odds columns are found in the CSV.
    """
    for h_col, d_col, a_col in _ODDS_PRIORITY:
        cols = [h_col, d_col, a_col]
        if all(c in df.columns for c in cols) and df[cols].notna().any().all():
            df = df.copy()
            df["home_win_odds"] = df[h_col]
            df["draw_odds"] = df[d_col]
            df["away_odds"] = df[a_col]
            print(f"  Using odds columns: {h_col} / {d_col} / {a_col}")
            return df

    raise ValueError(
        "No recognised odds columns found in CSV. "
        "Expected one of: BFEH/BFED/BFEA, AvgH/AvgD/AvgA, PSH/PSD/PSA."
    )


# ---------------------------------------------------------------------------
# Betfair odds pivoting
# ---------------------------------------------------------------------------


def pivot_betfair_odds(raw: pd.DataFrame) -> pd.DataFrame:
    """
    Pivot runner-level Betfair data into one row per match.

    Betfair MATCH_ODDS markets have three runners: home team, away team, Draw.
    The event_name is 'Home Team v Away Team', which we use to identify sides.
    """
    rows = []

    for (market_id, event_name, competition_name, market_start_time), group in raw.groupby(
        ["market_id", "event_name", "competition_name", "market_start_time"]
    ):
        parts = str(event_name).split(" v ", maxsplit=1)
        if len(parts) != 2:
            continue

        home_name, away_name = parts[0].strip(), parts[1].strip()

        odds = {}
        for _, runner in group.iterrows():
            name = str(runner["runner_name"]).strip()
            price = runner["best_back_price"]

            if name == "The Draw":
                odds["draw_odds"] = price
            elif name == home_name:
                odds["home_win_odds"] = price
            elif name == away_name:
                odds["away_odds"] = price

        if len(odds) < 3:
            continue

        rows.append({
            "date":          market_start_time,
            "league":        competition_name,
            "home_team":     home_name,
            "away_team":     away_name,
            "home_win_odds": odds["home_win_odds"],
            "draw_odds":     odds["draw_odds"],
            "away_odds":     odds["away_odds"],
            "result":        None,
            "source":        "betfair_api",
        })

    return pd.DataFrame(rows, columns=BRONZE_COLUMNS)


# ---------------------------------------------------------------------------
# Bronze storage
# ---------------------------------------------------------------------------


def load_bronze() -> pd.DataFrame:
    """Load the existing bronze table, or return an empty DataFrame if none exists."""
    if not BRONZE_PATH.exists():
        return pd.DataFrame(columns=BRONZE_COLUMNS)
    return pd.read_parquet(BRONZE_PATH)


def save_bronze(df: pd.DataFrame) -> None:
    """Save the full bronze table to parquet."""
    BRONZE_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(BRONZE_PATH, index=False)


def upsert_bronze(new_df: pd.DataFrame) -> None:
    """
    Append new rows to the bronze table and deduplicate.

    Deduplication key: (date, home_team, away_team).
    Existing rows are preserved; new rows are appended only if not already present.
    """
    existing = load_bronze()
    combined = pd.concat([existing, new_df], ignore_index=True)
    combined = combined.drop_duplicates(subset=["date", "home_team", "away_team", "league"], keep="last")
    combined = combined.sort_values("date").reset_index(drop=True)
    save_bronze(combined)
    print(f"Bronze table updated: {len(combined)} total rows.")


# ---------------------------------------------------------------------------
# Processed files log
# ---------------------------------------------------------------------------


def load_processed_log() -> set:
    """Return the set of CSV filenames already loaded into bronze."""
    if not PROCESSED_LOG_PATH.exists():
        return set()
    return set(json.loads(PROCESSED_LOG_PATH.read_text()))


def save_processed_log(processed: set) -> None:
    PROCESSED_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    PROCESSED_LOG_PATH.write_text(json.dumps(sorted(processed), indent=2))


# ---------------------------------------------------------------------------
# Betfair API — authentication
# ---------------------------------------------------------------------------


def get_betfair_session_token() -> str:
    """Authenticate with Betfair using certificate-based login."""
    load_dotenv()
    url = "https://identitysso-cert.betfair.com/api/certlogin"
    payload = {
        "username": os.getenv("BETFAIR_USERNAME"),
        "password": os.getenv("BETFAIR_PASSWORD"),
    }
    headers = {
        "X-Application": os.getenv("BETFAIR_APP_KEY"),
        "Content-Type": "application/x-www-form-urlencoded",
    }
    cert = (
        os.getenv("BETFAIR_CERT_PATH"),
        os.getenv("BETFAIR_KEY_PATH"),
    )

    response = requests.post(url, data=payload, headers=headers, cert=cert)
    response.raise_for_status()

    data = response.json()
    if data.get("loginStatus") != "SUCCESS":
        raise RuntimeError(f"Betfair login failed: {data.get('loginStatus')}")

    print("✓ Betfair authentication successful")
    return data["sessionToken"]


def get_headers(session_token: str) -> dict:
    load_dotenv()
    return {
        "X-Application": os.getenv("BETFAIR_APP_KEY"),
        "X-Authentication": session_token,
        "Content-Type": "application/json",
    }


# ---------------------------------------------------------------------------
# Betfair API — market discovery
# ---------------------------------------------------------------------------


def list_competitions(session_token: str) -> pd.DataFrame:
    """Fetch all available soccer competitions from Betfair — useful for finding IDs."""
    url = f"{BETFAIR_API_URL}/listCompetitions/"
    body = {"filter": {"eventTypeIds": [SOCCER_EVENT_TYPE_ID]}}

    response = requests.post(url, json=body, headers=get_headers(session_token))
    response.raise_for_status()

    data = response.json()
    rows = [
        {
            "competition_id": c["competition"]["id"],
            "competition_name": c["competition"]["name"],
            "market_count": c.get("marketCount"),
            "region": c.get("competitionRegion"),
        }
        for c in data
    ]
    return pd.DataFrame(rows).sort_values("competition_name")


def get_match_odds_markets(
    session_token: str,
    from_time: datetime,
    to_time: datetime,
    competition_ids: list[str] | None = None,
) -> list[dict]:
    """Fetch MATCH_ODDS markets within a time window, optionally filtered by competition."""
    url = f"{BETFAIR_API_URL}/listMarketCatalogue/"

    market_filter = {
        "eventTypeIds": [SOCCER_EVENT_TYPE_ID],
        "marketTypeCodes": [MATCH_ODDS_MARKET_TYPE],
        "marketStartTime": {
            "from": from_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "to": to_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
        },
    }

    if competition_ids:
        market_filter["competitionIds"] = competition_ids

    body = {
        "filter": market_filter,
        "marketProjection": ["EVENT", "COMPETITION", "MARKET_START_TIME", "RUNNER_DESCRIPTION"],
        "maxResults": 200,
    }

    response = requests.post(url, json=body, headers=get_headers(session_token))
    response.raise_for_status()

    markets = response.json()
    print(f"  → Found {len(markets)} MATCH_ODDS markets")
    return markets


# ---------------------------------------------------------------------------
# Betfair API — odds fetching
# ---------------------------------------------------------------------------


def get_back_odds_for_markets(
    session_token: str,
    market_ids: list[str],
) -> list[dict]:
    """Fetch best available back odds for a list of market IDs."""
    url = f"{BETFAIR_API_URL}/listMarketBook/"

    body = {
        "marketIds": market_ids,
        "priceProjection": {
            "priceData": ["EX_BEST_OFFERS"],
            "exBestOffersOverrides": {
                "bestPricesDepth": 1,
                "rollupModel": "STAKE",
                "rollupLimit": 0,
            },
        },
    }

    response = requests.post(url, json=body, headers=get_headers(session_token))
    response.raise_for_status()

    return response.json()


def parse_odds(markets: list[dict], market_books: list[dict]) -> pd.DataFrame:
    """Combine market catalogue and book data into a flat DataFrame."""
    catalogue_map = {m["marketId"]: m for m in markets}

    rows = []
    for book in market_books:
        market_id = book["marketId"]
        catalogue = catalogue_map.get(market_id, {})
        event = catalogue.get("event", {})
        competition = catalogue.get("competition", {})
        market_start = catalogue.get("marketStartTime")

        for runner in book.get("runners", []):
            runner_name = next(
                (
                    r["runnerName"]
                    for r in catalogue.get("runners", [])
                    if r["selectionId"] == runner["selectionId"]
                ),
                str(runner["selectionId"]),
            )

            best_back = None
            best_back_size = None
            ex = runner.get("ex", {})
            available_to_back = ex.get("availableToBack", [])
            if available_to_back:
                best_back = available_to_back[0].get("price")
                best_back_size = available_to_back[0].get("size")

            rows.append({
                "market_id": market_id,
                "competition_id": competition.get("id"),
                "competition_name": competition.get("name"),
                "event_id": event.get("id"),
                "event_name": event.get("name"),
                "market_start_time": market_start,
                "runner_id": runner["selectionId"],
                "runner_name": runner_name,
                "status": runner.get("status"),
                "best_back_price": best_back,
                "best_back_size": best_back_size,
                "snapshot_time": datetime.now(timezone.utc).isoformat(),
            })

    df = pd.DataFrame(rows)
    if not df.empty:
        df["market_start_time"] = pd.to_datetime(df["market_start_time"], utc=True)

    return df


def fetch_prematch_odds(
    from_time: datetime,
    to_time: datetime,
    competition_ids: list[str] | None = None,
) -> pd.DataFrame:
    """Main entry point: fetch back odds for all matches in the given window."""
    session_token = get_betfair_session_token()

    print(f"Fetching markets starting between {from_time} and {to_time}...")
    if competition_ids:
        print(f"  Filtering by competition IDs: {competition_ids}")

    markets = get_match_odds_markets(session_token, from_time, to_time, competition_ids)

    if not markets:
        print("No markets found.")
        return pd.DataFrame()

    market_ids = [m["marketId"] for m in markets]

    batch_size = 200
    all_books = []
    for i in range(0, len(market_ids), batch_size):
        batch = market_ids[i: i + batch_size]
        print(f"  Fetching odds for markets {i + 1}–{i + len(batch)}...")
        books = get_back_odds_for_markets(session_token, batch)
        all_books.extend(books)

    df = parse_odds(markets, all_books)
    print(f"  → Parsed {len(df)} runner rows across {len(markets)} markets")
    return df
