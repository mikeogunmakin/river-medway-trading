import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, Iterable, List, Optional, Union

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_URL = "https://v3.football.api-sports.io"


def _clean_stat_name(stat_name: str) -> str:
    """Convert a stat name like 'Shots on Goal' to 'shots_on_goal'."""
    return re.sub(r"[^a-z0-9_]", "", stat_name.lower().replace(" ", "_"))


def _clean_value(value: Any) -> Any:
    """
    Normalise API values:
    - None -> 0
    - '55%' -> 55.0
    - '12' -> 12
    - '3.5' -> 3.5
    - otherwise return original string/object
    """
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


def _build_session(api_key: str, timeout: int = 30) -> requests.Session:
    """
    Build a requests session with retries configured.
    """
    session = requests.Session()
    session.headers.update({"x-apisports-key": api_key})

    retry = Retry(
        total=3,
        connect=3,
        read=3,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    # Store timeout on session for convenience
    session.request_timeout = timeout  # type: ignore[attr-defined]
    return session


def _api_get(session: requests.Session, path: str, params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Perform a GET request and return parsed JSON.
    """
    response = session.get(
        f"{BASE_URL}{path}",
        params=params,
        timeout=getattr(session, "request_timeout", 30),
    )
    response.raise_for_status()
    payload = response.json()

    if not isinstance(payload, dict):
        raise ValueError(f"Unexpected API response type for {path}: {type(payload).__name__}")

    return payload


def _extract_fixture_meta(fixture_payload: Dict[str, Any], fixture_id: int) -> Dict[str, Any]:
    """
    Flatten fixture metadata into a single dict.
    """
    fixture = fixture_payload.get("fixture") or {}
    league = fixture_payload.get("league") or {}
    venue = fixture.get("venue") or {}
    status = fixture.get("status") or {}
    teams = fixture_payload.get("teams") or {}

    return {
        "fixture_id": fixture_id,
        "date": fixture.get("date"),
        "timestamp": fixture.get("timestamp"),
        "referee": fixture.get("referee"),
        "timezone": fixture.get("timezone"),
        "venue_id": venue.get("id"),
        "venue_name": venue.get("name"),
        "venue_city": venue.get("city"),
        "status_long": status.get("long"),
        "status_short": status.get("short"),
        "elapsed": status.get("elapsed"),
        "league_id": league.get("id"),
        "league_name": league.get("name"),
        "country": league.get("country"),
        "season": league.get("season"),
        "round": league.get("round"),
        "home_team_id": (teams.get("home") or {}).get("id"),
        "home_team_name": (teams.get("home") or {}).get("name"),
        "away_team_id": (teams.get("away") or {}).get("id"),
        "away_team_name": (teams.get("away") or {}).get("name"),
    }


def _add_team_stats(
    row: Dict[str, Any],
    team_block: Dict[str, Any],
    prefix: str,
) -> None:
    """
    Add one team's statistics into the row using the provided prefix.
    """
    team = team_block.get("team") or {}
    row[f"{prefix}team_id"] = team.get("id")
    row[f"{prefix}team_name"] = team.get("name")

    for stat in team_block.get("statistics", []):
        stat_type = stat.get("type")
        if not stat_type:
            continue
        col = _clean_stat_name(stat_type)
        row[f"{prefix}{col}"] = _clean_value(stat.get("value"))


def _assign_prefix(
    team_id: Optional[int],
    home_team_id: Optional[int],
    away_team_id: Optional[int],
    home_taken: bool,
) -> str:
    """
    Decide whether a stats block belongs to home_ or away_.
    """
    if home_team_id is not None and team_id == home_team_id:
        return "home_"
    if away_team_id is not None and team_id == away_team_id:
        return "away_"

    # Fallback when fixture metadata is unavailable or team ids don't match
    return "away_" if home_taken else "home_"


def _fetch_single_fixture(
    session: requests.Session,
    fixture_id: int,
    include_fixture_meta: bool,
) -> Dict[str, Any]:
    """
    Fetch one fixture's metadata and statistics and flatten to a single row dict.
    """
    fixture_id = int(fixture_id)
    row: Dict[str, Any] = {"fixture_id": fixture_id}

    home_team_id: Optional[int] = None
    away_team_id: Optional[int] = None

    if include_fixture_meta:
        fixture_resp = _api_get(session, "/fixtures", {"id": fixture_id})
        fixture_items = fixture_resp.get("response") or []

        if not fixture_items:
            return {"fixture_id": fixture_id, "error": "fixture_not_found"}

        meta = _extract_fixture_meta(fixture_items[0], fixture_id)
        row.update(meta)
        home_team_id = meta.get("home_team_id")
        away_team_id = meta.get("away_team_id")

    stats_resp = _api_get(session, "/fixtures/statistics", {"fixture": fixture_id})
    stats_items = stats_resp.get("response") or []

    if not stats_items:
        row["error"] = "stats_not_found"
        return row

    home_taken = False
    away_taken = False

    for team_block in stats_items:
        team = team_block.get("team") or {}
        team_id = team.get("id")

        prefix = _assign_prefix(team_id, home_team_id, away_team_id, home_taken)

        if prefix == "home_":
            home_taken = True
        else:
            away_taken = True

        _add_team_stats(row, team_block, prefix)

    return row


def fetch_fixture_stats_home_away(
    fixture_ids: Union[int, Iterable[int]],
    api_key: str,
    include_fixture_meta: bool = True,
    max_workers: int = 1,
    timeout: int = 30,
) -> pd.DataFrame:
    """
    Fetch API-Football statistics for one or many fixtures.

    Returns one row per fixture with:
    - optional fixture metadata
    - home_* stats
    - away_* stats

    Parameters
    ----------
    fixture_ids:
        A single fixture id or an iterable of ids.
    api_key:
        API-Football API key.
    include_fixture_meta:
        Whether to fetch /fixtures metadata for each fixture.
    max_workers:
        Number of threads to use. Use >1 to improve throughput for many fixtures.
    timeout:
        Request timeout in seconds.

    Returns
    -------
    pd.DataFrame
    """
    if isinstance(fixture_ids, int):
        fixture_id_list = [fixture_ids]
    else:
        fixture_id_list = [int(fx) for fx in fixture_ids]

    if not fixture_id_list:
        return pd.DataFrame()

    rows: List[Dict[str, Any]] = []

    # Sequential mode
    if max_workers <= 1:
        with _build_session(api_key=api_key, timeout=timeout) as session:
            for fixture_id in fixture_id_list:
                try:
                    rows.append(
                        _fetch_single_fixture(
                            session=session,
                            fixture_id=fixture_id,
                            include_fixture_meta=include_fixture_meta,
                        )
                    )
                except requests.RequestException as exc:
                    rows.append({"fixture_id": fixture_id, "error": f"http_error: {exc}"})
                except Exception as exc:
                    rows.append({"fixture_id": fixture_id, "error": f"unexpected_error: {exc}"})

    # Parallel mode
    else:
        def worker(fixture_id: int) -> Dict[str, Any]:
            with _build_session(api_key=api_key, timeout=timeout) as session:
                return _fetch_single_fixture(
                    session=session,
                    fixture_id=fixture_id,
                    include_fixture_meta=include_fixture_meta,
                )

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(worker, fixture_id): fixture_id for fixture_id in fixture_id_list}
            for future in as_completed(futures):
                fixture_id = futures[future]
                try:
                    rows.append(future.result())
                except requests.RequestException as exc:
                    rows.append({"fixture_id": fixture_id, "error": f"http_error: {exc}"})
                except Exception as exc:
                    rows.append({"fixture_id": fixture_id, "error": f"unexpected_error: {exc}"})

    df = pd.DataFrame(rows)

    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce", utc=True)

    if "fixture_id" in df.columns:
        df = df.sort_values("fixture_id").reset_index(drop=True)

    return df
