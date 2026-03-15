"""
Betfair Exchange API utilities.

Handles authentication, market discovery, and odds fetching from the
Betfair Exchange REST API. Used by bronze_football_prematch_odds.py.
"""

import os
from datetime import datetime, timezone

import pandas as pd
import requests
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

BETFAIR_API_URL = "https://api.betfair.com/exchange/betting/rest/v1.0"
MATCH_ODDS_MARKET_TYPE = "MATCH_ODDS"
SOCCER_EVENT_TYPE_ID = "1"
SNAPSHOT_MINUTES_BEFORE = 60


def configure():
    load_dotenv()


# ---------------------------------------------------------------------------
# Authentication
# ---------------------------------------------------------------------------


def get_betfair_session_token() -> str:
    """Authenticate with Betfair using certificate-based login."""
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
    return {
        "X-Application": os.getenv("BETFAIR_APP_KEY"),
        "X-Authentication": session_token,
        "Content-Type": "application/json",
    }


# ---------------------------------------------------------------------------
# Market discovery
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
# Odds fetching
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
