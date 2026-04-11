"""
Configuration constants for the ingestion pipeline.

- LEAGUE_CODES: football-data.co.uk division codes → league names.
  Add new codes from https://www.football-data.co.uk/data.php as needed.

- Betfair API constants: endpoint, market type, and event type identifiers.
"""

# ---------------------------------------------------------------------------
# football-data.co.uk league codes
# ---------------------------------------------------------------------------

LEAGUE_CODES = {
    # England
    "E0": "Premier League",
    "E1": "Championship",
    "E2": "League One",
    "E3": "League Two",

    # Germany
    "D1": "Bundesliga",
    "D2": "2. Bundesliga",

    # Spain
    "SP1": "La Liga",
    "SP2": "La Liga 2",

    # Italy
    "I1": "Serie A",
    "I2": "Serie B",

    # France
    "F1": "Ligue 1",
    "F2": "Ligue 2",

    # Netherlands
    "N1": "Eredivisie",

    # Portugal
    "P1": "Primeira Liga",

    # Scotland
    "SC0": "Scottish Premiership",

    # Belgium
    "B1": "First Division A",

    # Turkey
    "T1": "Süper Lig",

    # Greece
    "G1": "Super League Greece",
}

# ---------------------------------------------------------------------------
# Betfair API
# ---------------------------------------------------------------------------

BETFAIR_API_URL = "https://api.betfair.com/exchange/betting/rest/v1.0"
MATCH_ODDS_MARKET_TYPE = "MATCH_ODDS"
SOCCER_EVENT_TYPE_ID = "1"
SNAPSHOT_MINUTES_BEFORE = 60
