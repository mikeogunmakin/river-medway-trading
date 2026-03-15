"""
League code mappings for football-data.co.uk CSV files.

To add a new league:
  1. Find the league code from https://www.football-data.co.uk/data.php
  2. Add an entry below: "CODE": "League Name"
"""

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


def get_league_name(code: str) -> str:
    """
    Return the league name for a given football-data.co.uk code.

    Raises:
        KeyError: if the code is not in LEAGUE_CODES — add it to league_config.py.
    """
    if code not in LEAGUE_CODES:
        raise KeyError(
            f"Unknown league code '{code}'. Add it to ingestion_pipeline/league_config.py."
        )
    return LEAGUE_CODES[code]
