"""
Feature engineering — football prematch odds feature group.

Pure functions for computing odds-based features, plus build_features()
which applies all of them to a bronze-schema DataFrame.
"""

import numpy as np
import pandas as pd


# ---------------------------------------------------------------------------
# Odds-based features
# ---------------------------------------------------------------------------


def normalised_prob(
    home_odds: pd.Series,
    draw_odds: pd.Series,
    away_odds: pd.Series,
) -> tuple[pd.Series, pd.Series, pd.Series]:
    """
    Convert raw decimal odds to normalised probabilities that sum to exactly 1,
    removing the bookmaker's overround (margin).

    Used internally by normalised_odds().

    Returns:
        (p_home, p_draw, p_away) — three Series of normalised probabilities,
        each in [0, 1], summing to 1 per row.
    """
    p_home = 1 / home_odds
    p_draw = 1 / draw_odds
    p_away = 1 / away_odds
    total = p_home + p_draw + p_away
    return p_home / total, p_draw / total, p_away / total


def normalised_odds(
    home_odds: pd.Series,
    draw_odds: pd.Series,
    away_odds: pd.Series,
) -> tuple[pd.Series, pd.Series, pd.Series]:
    """
    Convert raw decimal odds to fair odds by removing the bookmaker's overround.

    Computes normalised probabilities via normalised_prob(), then converts back
    to decimal odds (1 / p_norm), giving margin-free 'true' odds for each outcome.

    Returns:
        (fair_home, fair_draw, fair_away) — three Series of fair decimal odds.
    """
    p_home, p_draw, p_away = normalised_prob(home_odds, draw_odds, away_odds)
    return 1 / p_home, 1 / p_draw, 1 / p_away


def implied_probability(odds: pd.Series) -> pd.Series:
    """
    Convert decimal odds to raw implied probability: p = 1 / odds.

    The implied probability reflects the market's assessment of the likelihood
    of an outcome before the overround is removed. Useful as a direct feature
    or as an intermediate step before normalisation. Higher overround signals
    lower-liquidity markets where the book takes a larger edge.

    Args:
        odds: Decimal odds for a single outcome (home, draw, or away).

    Returns:
        Series of implied probabilities in (0, 1].
    """
    return 1 / odds


def home_vs_away(home_odds: pd.Series, away_odds: pd.Series) -> pd.Series:
    """
    Compute the signed difference between home and away win odds (home - away).

    Positive values indicate the away side is favoured; negative values indicate
    the home side is favoured. Magnitude reflects how one-sided the market is.

    Args:
        home_odds: Decimal odds for the home win.
        away_odds: Decimal odds for the away win.

    Returns:
        Series of signed spread values.
    """
    return home_odds - away_odds


def draw_vs_home(draw_odds: pd.Series, home_odds: pd.Series) -> pd.Series:
    """
    Compute draw odds minus home win odds.

    Positive values mean the draw is longer than the home win — the home side
    is favoured. Negative values mean the draw is priced shorter than the home
    win, indicating the market sees a stalemate as more likely than a home victory.

    Returns:
        Series of signed differences (draw - home).
    """
    return draw_odds - home_odds


def draw_vs_away(draw_odds: pd.Series, away_odds: pd.Series) -> pd.Series:
    """
    Compute draw odds minus away win odds.

    Positive values mean the draw is longer than the away win — the away side
    is favoured. Negative values mean the draw is priced shorter than the away
    win, indicating the market sees a stalemate as more likely than an away victory.

    Returns:
        Series of signed differences (draw - away).
    """
    return draw_odds - away_odds


def home_fav(home_odds: pd.Series, away_odds: pd.Series) -> pd.Series:
    """
    Binary flag: 1 if the home side is the market favourite, else 0.

    The home side is considered favourite when their win odds are shorter than
    the away win odds. Useful as a categorical signal for home/away asymmetry.

    Returns:
        Series of 0/1 integers.
    """
    return (home_odds < away_odds).astype(int)


def log_odds(odds: pd.Series) -> pd.Series:
    """
    Apply a natural log transform to decimal odds.

    Compresses the scale of large odds values and makes the distribution more
    symmetric. Particularly useful for away or draw odds, which can be heavily
    right-skewed.

    Args:
        odds: Decimal odds for a single outcome.

    Returns:
        Series of log-transformed odds values.
    """
    return np.log(odds)


# ---------------------------------------------------------------------------
# Pipeline entry point
# ---------------------------------------------------------------------------


def build_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply all prematch odds features to a bronze-schema DataFrame.

    Expects columns: home_win_odds, draw_odds, away_odds.
    Returns the input DataFrame with feature columns added.
    """
    df = df.copy()

    # Fair odds — raw odds with bookmaker margin removed
    fair_home, fair_draw, fair_away = normalised_odds(
        df["home_win_odds"], df["draw_odds"], df["away_odds"]
    )
    df["fair_home_odds"] = fair_home
    df["fair_draw_odds"] = fair_draw
    df["fair_away_odds"] = fair_away

    # Implied probabilities of fair odds
    df["implied_prob_home_win"] = implied_probability(fair_home)
    df["implied_prob_draw"]     = implied_probability(fair_draw)
    df["implied_prob_away_win"] = implied_probability(fair_away)

    df["home_vs_away"] = home_vs_away(fair_home, fair_away)
    df["draw_vs_home"] = draw_vs_home(fair_draw, fair_home)
    df["draw_vs_away"] = draw_vs_away(fair_draw, fair_away)
    df["home_fav"]     = home_fav(fair_home, fair_away)

    df["log_home_odds"] = log_odds(fair_home)
    df["log_draw_odds"] = log_odds(fair_draw)
    df["log_away_odds"] = log_odds(fair_away)

    return df
