"""
Selectors — football prematch odds.

Filters the bronze table to define what rows are passed through to the
feature store. As the bronze table grows to cover multiple sports and
leagues, selectors control exactly what each pipeline processes.

Add or adjust filters here to change the scope of what gets written
to the feature store.
"""

import pandas as pd

# Leagues to include in the feature store.
# Add league names here as you expand coverage.
LEAGUES = [
    # England
    "English Premier League",
    "Championship",
    "League One",
    "League Two",
    "English Sky Bet Championship",
    "English Sky Bet League 1",
    "English Sky Bet League 2",
    "English National League",
    "English FA Trophy",
    # Germany
    "Bundesliga",
    "2. Bundesliga",
    "German Bundesliga",
    "German Bundesliga 2",
    "German Cup",
    # Spain
    "La Liga",
    "La Liga 2",
    "Spanish La Liga",
    "Spanish Segunda Division",
    # Italy
    "Serie A",
    "Serie B",
    "Italian Serie B",
    "Italian Serie C",
    "Italian Coppa Italia",
    # France
    "Ligue 1",
    "Ligue 2",
    "French Ligue 1",
    "French Cup",
    # Netherlands
    "Eredivisie",
    "Dutch Eredivisie",
    "Dutch Playoffs",
    # Portugal
    "Primeira Liga",
    "Portuguese Primera Liga",
    # Scotland
    "Scottish Premiership",
    "Scottish Championship",
    "Scottish League One",
    "Scottish League Two",
    "Scottish Playoffs",
    "Scottish FA Cup",
    # Belgium
    "First Division A",
    "Belgian Pro League",
    # Turkey
    "Süper Lig",
    "Turkish Super League",
    "Turkish Cup",
    "Turkish 1 Lig",
    # Greece
    "Super League Greece",
    # Nordic
    "Norwegian Eliteserien",
    "Norwegian 1st Division",
    "Swedish Allsvenskan",
    "Swedish Superettan",
    "Finnish Veikkausliiga",
    "Finnish Ykkonen",
    "Latvian Virsliga",
    # Eastern Europe
    "Romanian Liga I",
    "Romanian Liga II",
    "Ukrainian Premier League",
    "Croatian HNL",
    "Bulgarian A League",
    "Lithuanian A Lyga",
    "Georgian Umaglesi Liga",
    "Azerbaijani 1st Division",
    "Kazakhstan Premier League",
    "Polish Ekstraklasa",
    "Slovakian Super League",
    "Czech 2 Liga",
    "Czech U19",
    # Iberia / Small European nations
    "Cypriot 1st Division",
    "Israeli Premier League",
    "Austrian Landesliga",
    "Maltese Premier League",
    "San Marino Campionato",
    "Estonian Cup",
    "Irish Premier Division",
    # European competitions
    "UEFA Champions League",
    "UEFA Europa Conference League",
    # Middle East
    "UAE Arabian Gulf League",
    "Saudi National League",
    "Saudi Professional League",
    "Qatari Emir Cup",
    "Bahraini Premier",
    "Omani Professional League",
    # Africa
    "Ethiopian Premier League",
    "South African Premier Division",
    "Egyptian Premier League",
    "Tanzanian Premier League",
    "Botswana Premier League",
    "Nigerian Premier League",
    "Rwandan National Football League",
    # Central Asia
    "Uzbekistan 1st Division",
    # Asia / Pacific
    "Japanese J. League 100 Year Vision",
    "Japanese J. League 2/3 100 Year Vision",
    "Chinese Super League",
    "Chinese League 2",
    "South Korean K1 League",
    "Indian Super League",
    "Indian 1-League",
    "Indonesian Super League",
    "Singapore Premier League",
    "Bhutan Premier League",
    "Australian A-League Men",
    "Australian A-League Women",
    # South America
    "Uruguayan Primera Division",
    "Uruguayan Reserves",
    "Argentinian Primera Division Reserves",
    "Brazilian Serie A",
    "Brazilian Serie B",
    "Brazilian Cup",
    "Colombian Primera A",
    "Peruvian Primera Division",
    "Ecuadorian Serie A",
    # North / Central America
    "US MLS",
    "US United Soccer League",
    "US National Women Soccer League",
    "Mexican Liga MX",
    "Honduras Liga Nacional",
    "Panamanian Premier League",
    # Continental / International
    "CONMEBOL Copa Libertadores",
    "CONMEBOL Copa Sudamericana",
    "CONCACAF Champions League",
    "FIFA World Cup",
]


def select_historical(df: pd.DataFrame) -> pd.DataFrame:
    """
    Filter bronze to completed historical matches for the configured leagues.
    Used by the historical backfill runner.
    """
    mask = df["result"].notna() & df["league"].isin(LEAGUES)
    return df[mask].copy()


def select_upcoming(df: pd.DataFrame) -> pd.DataFrame:
    """
    Filter bronze to upcoming fixtures (no result yet) for the configured leagues.
    Used by the live daily runner.
    """
    mask = df["result"].isna() & df["league"].isin(LEAGUES)
    return df[mask].copy()
