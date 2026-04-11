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

    Used internally by normalised_odds() and directly by market_entropy().

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


def odds_spread(home_odds: pd.Series, away_odds: pd.Series) -> pd.Series:
    """
    Compute the absolute difference between home and away win odds.

    A large spread indicates a heavily one-sided market — one team is a clear
    favourite. A small spread signals a competitive, evenly-matched fixture.
    This feature captures market-implied competitive balance and can help
    distinguish close matches from mismatches, which often have different
    score-line distributions.

    Args:
        home_odds: Decimal odds for the home win.
        away_odds: Decimal odds for the away win.

    Returns:
        Series of non-negative spread values.
    """
    return (home_odds - away_odds).abs()


def draw_margin(
    home_odds: pd.Series,
    draw_odds: pd.Series,
    away_odds: pd.Series,
) -> pd.Series:
    """
    Compute draw odds minus the minimum of home and away odds.

    Measures how much more expensive the draw is relative to the stronger
    side's win price. A high draw margin suggests the market prices a decisive
    result as more likely — common in fixtures with a heavy favourite. A low
    or negative margin implies the draw is competitively priced, often seen in
    evenly-matched games or in leagues with high draw rates.

    Returns:
        Series where positive values indicate draw is priced above the
        favourite's win odds.
    """
    min_win_odds = pd.concat([home_odds, away_odds], axis=1).min(axis=1)
    return draw_odds - min_win_odds


def favourite_odds(
    home_odds: pd.Series,
    draw_odds: pd.Series,
    away_odds: pd.Series,
) -> pd.Series:
    """
    Return the lowest (shortest) odds across all three outcomes per row.

    The shortest price in the market is the outcome the book considers most
    likely. In trading, the favourite's odds directly proxy market confidence
    and liquidity concentration. Very short favourites (<1.3) often compress
    variance but also compress value — useful for staking and model
    calibration.

    Returns:
        Series of minimum odds values (the market favourite price).
    """
    return pd.concat([home_odds, draw_odds, away_odds], axis=1).min(axis=1)


def odds_range(
    home_odds: pd.Series,
    draw_odds: pd.Series,
    away_odds: pd.Series,
) -> pd.Series:
    """
    Compute the range (max - min) of the three outcome odds.

    A wide range reflects a polarised market with a strong favourite and a
    heavy underdog. A narrow range indicates uncertainty across all outcomes.
    This is a simple measure of market spread that complements entropy — a
    high range can coexist with low entropy if one outcome dominates, while a
    low range with moderate entropy suggests genuine three-way uncertainty.

    Returns:
        Series of odds range values (>=0).
    """
    all_odds = pd.concat([home_odds, draw_odds, away_odds], axis=1)
    return all_odds.max(axis=1) - all_odds.min(axis=1)


def home_vs_draw(home_odds: pd.Series, draw_odds: pd.Series) -> pd.Series:
    """
    Compute home win odds minus draw odds.

    A positive value means the draw is priced shorter than the home win —
    the market considers a stalemate more likely than a home victory. A
    negative value (more common) signals the home side is favoured over the
    draw. This ratio is particularly informative in defensive leagues or
    away-heavy fixtures where draws are systematically underpriced.

    Returns:
        Series; negative values indicate home is favoured over the draw.
    """
    return home_odds - draw_odds


def away_vs_draw(away_odds: pd.Series, draw_odds: pd.Series) -> pd.Series:
    """
    Compute away win odds minus draw odds.

    Analogous to home_vs_draw but from the away perspective. When this value
    is negative, the away side is priced closer to the draw — unusual and
    potentially indicative of a strong travelling side or a weak home team.
    Useful for identifying fixtures where the away win is undervalued relative
    to the draw.

    Returns:
        Series; negative values indicate away is favoured over the draw.
    """
    return away_odds - draw_odds


def odds_std(
    home_odds: pd.Series,
    draw_odds: pd.Series,
    away_odds: pd.Series,
) -> pd.Series:
    """
    Compute the standard deviation of the three outcome odds per row.

    Standard deviation captures dispersion in the odds distribution. Low std
    implies the market sees all outcomes as roughly equiprobable (high
    uncertainty), while high std indicates one or more outcomes are priced
    very differently — typically when there is a dominant favourite or a very
    long-shot outcome. Complements odds_range with a sensitivity to outliers.

    Returns:
        Series of per-row standard deviations.
    """
    return pd.concat([home_odds, draw_odds, away_odds], axis=1).std(axis=1)


def market_entropy(
    p_home_norm: pd.Series,
    p_draw_norm: pd.Series,
    p_away_norm: pd.Series,
) -> pd.Series:
    """
    Compute Shannon entropy of the normalised outcome probability distribution.

    H = -(p_home * log(p_home) + p_draw * log(p_draw) + p_away * log(p_away))

    Entropy measures how evenly spread the market's probability mass is across
    the three outcomes. Maximum entropy (~1.099 nats for three outcomes) occurs
    when all three are equally likely — the market has no strong view. Minimum
    entropy (0) occurs when one outcome is near-certain. High entropy fixtures
    are harder to predict and carry more risk; low entropy fixtures have a
    clear market narrative. Useful as a match-difficulty or uncertainty signal
    for bet sizing and model confidence.

    Args:
        p_home_norm: Normalised home win probability (output of normalised_odds).
        p_draw_norm: Normalised draw probability.
        p_away_norm: Normalised away win probability.

    Returns:
        Series of entropy values in [0, ln(3)] nats.
    """
    return -(
        p_home_norm * np.log(p_home_norm)
        + p_draw_norm * np.log(p_draw_norm)
        + p_away_norm * np.log(p_away_norm)
    )


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

    # Normalised probabilities — stored as features and used for implied probabilities and entropy
    p_home, p_draw, p_away = normalised_prob(
        df["home_win_odds"], df["draw_odds"], df["away_odds"]
    )
    df["p_home_norm"] = p_home
    df["p_draw_norm"] = p_draw
    df["p_away_norm"] = p_away

    df["implied_prob_home"] = implied_probability(p_home)
    df["implied_prob_draw"] = implied_probability(p_draw)
    df["implied_prob_away"] = implied_probability(p_away)

    df["odds_spread"]    = odds_spread(fair_home, fair_away)
    df["draw_margin"]    = draw_margin(fair_home, fair_draw, fair_away)
    df["favourite_odds"] = favourite_odds(fair_home, fair_draw, fair_away)
    df["odds_range"]     = odds_range(fair_home, fair_draw, fair_away)
    df["home_vs_draw"]   = home_vs_draw(fair_home, fair_draw)
    df["away_vs_draw"]   = away_vs_draw(fair_away, fair_draw)
    df["odds_std"]       = odds_std(fair_home, fair_draw, fair_away)
    df["market_entropy"] = market_entropy(p_home, p_draw, p_away)

    return df
