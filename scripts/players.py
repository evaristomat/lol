# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from scipy import stats

# ==============================
# Utilitários de probabilidade
# ==============================


def implied_prob(odds: float) -> float:
    """Probabilidade implícita bruta (com vig)."""
    p = 1.0 / max(float(odds), 1e-12)
    return float(np.clip(p, 1e-6, 1 - 1e-6))


def remove_vig_pair(p_over: float, p_under: float) -> Tuple[float, float]:
    """Normaliza Over/Under para remover vig (soma = 1)."""
    s = p_over + p_under
    if s <= 0:
        return p_over, p_under
    return (
        float(np.clip(p_over / s, 1e-6, 1 - 1e-6)),
        float(np.clip(p_under / s, 1e-6, 1 - 1e-6)),
    )


def posterior(p_prior: float, p_like: float, w_prior: float = 0.5) -> float:
    """Combinação Bayesiana simples (média ponderada)."""
    p_prior = float(np.clip(p_prior, 1e-6, 1 - 1e-6))
    p_like = float(np.clip(p_like, 1e-6, 1 - 1e-6))
    return w_prior * p_prior + (1.0 - w_prior) * p_like


def fair_from_p(p: float) -> float:
    """Odds justas a partir de uma probabilidade."""
    p = float(np.clip(p, 1e-6, 1 - 1e-6))
    return 1.0 / p


def ev_from(p_real: float, odds: float) -> float:
    """EV (em fração). Para % multiplique por 100 depois."""
    p_real = float(np.clip(p_real, 1e-6, 1 - 1e-6))
    return (p_real * float(odds)) - 1.0


# ==============================
# Parsing
# ==============================


def extract_side(selection_name: str) -> Optional[str]:
    s = str(selection_name).lower()
    if "over" in s:
        return "over"
    if "under" in s:
        return "under"
    return None


def extract_player_patched(selection_name: str, candidates: List[str]) -> Optional[str]:
    """Busca por nome do jogador no selection_name (robusto a formatos)."""
    low = str(selection_name).lower()
    # 1) word boundary
    for c in sorted(candidates, key=len, reverse=True):
        if f" {c.lower()} " in f" {low} ":
            return c
    # 2) substring
    for c in sorted(candidates, key=len, reverse=True):
        if c.lower() in low:
            return c
    return None


# ==============================
# Estatísticas a partir do histórico
# ==============================


def get_player_values(
    df_hist: pd.DataFrame, player: str, team: Optional[str], stat: str, n: int = 50
) -> np.ndarray:
    """Retorna vetor dos últimos n valores (ordenado por data desc).
    Se team não bater, usa o time com mais entradas para o player."""
    sub = df_hist[df_hist["playername"] == player]
    if team is not None:
        sub_team = sub[sub["teamname"] == team]
    else:
        sub_team = pd.DataFrame(columns=sub.columns)
    if sub_team.empty:
        if not sub.empty:
            counts = sub.groupby("teamname").size().sort_values(ascending=False)
            if len(counts) > 0:
                fallback = counts.index[0]
                sub_team = sub[sub["teamname"] == fallback]
            else:
                sub_team = sub
    sub_team = sub_team.dropna(subset=[stat]).sort_values("date", ascending=False)
    return sub_team[stat].astype(float).values[:n]


def calc_window_stats(values: np.ndarray, handicap: float, side: str) -> Optional[dict]:
    """Métricas de janela (l10/l20): mean, median, std, cv, hit_rate, trend, CI."""
    if values is None or len(values) == 0:
        return None
    mean = float(np.mean(values))
    median = float(np.median(values))
    std = float(np.std(values, ddof=0))
    cv = float((std / mean * 100.0) if mean > 0 else 0.0)
    if side == "over":
        hit_rate = float(np.mean(values > handicap))
    else:
        hit_rate = float(np.mean(values < handicap))
    # IC 95%
    if len(values) > 1:
        ci = stats.t.interval(0.95, len(values) - 1, loc=mean, scale=stats.sem(values))
        ci_low, ci_up = float(ci[0]), float(ci[1])
    else:
        ci_low = ci_up = mean
    # Tendência
    trend = 0.0
    if len(values) >= 20:
        recent = float(np.mean(values[:10]))
        older = float(np.mean(values[10:20]))
        trend = float(((recent - older) / older * 100.0) if older > 0 else 0.0)
    return dict(
        mean=mean,
        median=median,
        std=std,
        cv=cv,
        hit_rate=hit_rate,
        ci_lower=ci_low,
        ci_upper=ci_up,
        trend=trend,
    )


def like_from_cdf(stat_l10: dict, stat_l20: dict, handicap: float, side: str) -> float:
    """Likelihood via Mediana + CDF (ponderado 60/40 entre L10/L20)."""

    def prob_from(median, std):
        if std <= 0:
            return (
                1.0
                if ((median > handicap) if side == "over" else (median < handicap))
                else 0.0
            )
        z = (handicap - median) / std
        return (1.0 - stats.norm.cdf(z)) if side == "over" else stats.norm.cdf(z)

    p10 = prob_from(stat_l10["median"], stat_l10["std"])
    p20 = prob_from(stat_l20["median"], stat_l20["std"])
    return 0.6 * p10 + 0.4 * p20


# ==============================
# Núcleo: avaliação por evento
# ==============================

STAT_MAP = {
    "Map 1 - Player Total Kills": "kills",
    "Map 1 - Player Total Deaths": "deaths",
    "Map 1 - Player Total Assists": "assists",
}


def _build_priors(valid_odds: pd.DataFrame, use_clean_prior: bool) -> pd.DataFrame:
    """
    Anexa colunas:
      - p_prior_raw (sempre)
      - p_prior_clean (se for possível normalizar o par Over/Under)
    E retorna o DataFrame com a coluna 'p_prior' escolhida (raw ou clean, conforme flag).
    """
    frame = valid_odds.copy()
    frame["p_prior_raw"] = frame["odds_value"].apply(implied_prob)

    out = []
    for (player, stat, line), grp in frame.groupby(
        ["player", "stat", "handicap"], dropna=False
    ):
        grp = grp.copy()
        if grp["side"].nunique() == 2 and len(grp) >= 2:
            try:
                p_over = float(grp.loc[grp["side"] == "over", "p_prior_raw"].iloc[0])
                p_under = float(grp.loc[grp["side"] == "under", "p_prior_raw"].iloc[0])
                cov, cuv = remove_vig_pair(p_over, p_under)
                grp.loc[grp["side"] == "over", "p_prior_clean"] = cov
                grp.loc[grp["side"] == "under", "p_prior_clean"] = cuv
            except Exception:
                grp["p_prior_clean"] = grp["p_prior_raw"]
        else:
            grp["p_prior_clean"] = grp["p_prior_raw"]
        out.append(grp)

    frame = pd.concat(out, ignore_index=True) if out else frame
    frame["p_prior"] = (
        frame["p_prior_clean"] if use_clean_prior else frame["p_prior_raw"]
    )
    return frame


def _choose_team_for_player(
    df_hist: pd.DataFrame, player: str, home_team: str, away_team: str
) -> Optional[str]:
    """Heurística simples para escolher time do jogador neste evento."""
    v_home = get_player_values(df_hist, player, home_team, "kills", 1)
    v_away = get_player_values(df_hist, player, away_team, "kills", 1)
    return home_team if len(v_home) >= len(v_away) else away_team


def evaluate_event_three_methods(
    odds_df: pd.DataFrame,
    history_df: pd.DataFrame,
    home_team: str,
    away_team: str,
    weight_prior: float = 0.5,
    min_roi: Optional[float] = None,
) -> Tuple[pd.DataFrame, Dict[str, pd.DataFrame]]:
    """
    Calcula 3 abordagens para cada (player, stat, line, side, odds):
      - original  : prior bruto (com vig) + likelihood hit-rate L10/L20
      - bayes_hit : prior sem vig + likelihood hit-rate L10/L20
      - bayes_cdf : prior sem vig + likelihood via mediana + CDF (L10/L20)
    Retorna (df_combined, {"original": df1, "bayes_hit": df2, "bayes_cdf": df3}).
    """
    # ----------------------------
    # Pré-processar odds
    # ----------------------------
    df = odds_df.copy()
    df["handicap"] = pd.to_numeric(df["handicap"], errors="coerce")
    df["odds_value"] = pd.to_numeric(df["odds_value"], errors="coerce")
    df["side"] = df["selection_name"].apply(extract_side)
    df["stat"] = df["market_name"].map(STAT_MAP)

    candidates = history_df["playername"].dropna().unique().tolist()
    df["player"] = df["selection_name"].apply(
        lambda s: extract_player_patched(s, candidates)
    )

    valid = df[
        df["handicap"].notna()
        & df["odds_value"].notna()
        & df["side"].notna()
        & df["stat"].notna()
        & df["player"].notna()
    ].copy()

    if valid.empty:
        empty = pd.DataFrame(
            columns=[
                "method",
                "player",
                "stat",
                "line",
                "side",
                "odds",
                "p_real",
                "p_prior",
                "p_like",
                "fair",
                "roi",
                "ev",
                "l10_pct",
                "l20_pct",
                "mean_l10",
                "median_l10",
                "std_l10",
                "cv_l10",
                "trend",
            ]
        )
        return empty, {
            "original": empty.copy(),
            "bayes_hit": empty.copy(),
            "bayes_cdf": empty.copy(),
        }

    # ----------------------------
    # Priors para cada abordagem
    # ----------------------------
    # original: prior bruto (com vig)
    frame_orig = _build_priors(valid, use_clean_prior=False)
    # bayes: prior sem vig (limpo)
    frame_clean = _build_priors(valid, use_clean_prior=True)

    # ----------------------------
    # Avaliação linha a linha
    # ----------------------------
    rows_original, rows_bhit, rows_bcdf = [], [], []

    for _, r in frame_clean.iterrows():
        player = r["player"]
        stat = r["stat"]
        side = r["side"]
        line = float(r["handicap"])
        odds = float(r["odds_value"])

        # dados históricos
        team_guess = _choose_team_for_player(history_df, player, home_team, away_team)
        values = get_player_values(history_df, player, team_guess, stat, n=50)
        if len(values) < 20:
            continue

        l10 = values[:10]
        l20 = values[:20]
        st10 = calc_window_stats(l10, line, side)
        st20 = calc_window_stats(l20, line, side)
        if st10 is None or st20 is None:
            continue

        # ------------------------
        # ORIGINAL (prior bruto) + hit rate
        # ------------------------
        p_prior_o = float(
            frame_orig.loc[
                (frame_orig["player"] == player)
                & (frame_orig["stat"] == stat)
                & (frame_orig["handicap"] == line)
                & (frame_orig["side"] == side),
                "p_prior",
            ].iloc[0]
        )
        p_like_mean = 0.6 * st10["hit_rate"] + 0.4 * st20["hit_rate"]
        p_real_o = posterior(p_prior_o, p_like_mean, weight_prior)
        fair_o = fair_from_p(p_real_o)
        roi_o = (odds / fair_o - 1.0) * 100.0
        ev_o = ev_from(p_real_o, odds) * 100.0

        rows_original.append(
            dict(
                method="original",
                player=player,
                stat=stat.upper(),
                line=line,
                side=side,
                odds=odds,
                p_real=p_real_o * 100.0,
                p_prior=p_prior_o * 100.0,
                p_like=p_like_mean * 100.0,
                fair=fair_o,
                roi=roi_o,
                ev=ev_o,
                l10_pct=st10["hit_rate"] * 100.0,
                l20_pct=st20["hit_rate"] * 100.0,
                mean_l10=st10["mean"],
                median_l10=st10["median"],
                std_l10=st10["std"],
                cv_l10=st10["cv"],
                trend=st10["trend"],
            )
        )

        # ------------------------
        # BAYES_HIT (prior limpo) + hit rate
        # ------------------------
        p_prior_h = float(r["p_prior"])  # limpo
        p_real_h = posterior(p_prior_h, p_like_mean, weight_prior)
        fair_h = fair_from_p(p_real_h)
        roi_h = (odds / fair_h - 1.0) * 100.0
        ev_h = ev_from(p_real_h, odds) * 100.0

        rows_bhit.append(
            dict(
                method="bayes_hit",
                player=player,
                stat=stat.upper(),
                line=line,
                side=side,
                odds=odds,
                p_real=p_real_h * 100.0,
                p_prior=p_prior_h * 100.0,
                p_like=p_like_mean * 100.0,
                fair=fair_h,
                roi=roi_h,
                ev=ev_h,
                l10_pct=st10["hit_rate"] * 100.0,
                l20_pct=st20["hit_rate"] * 100.0,
                mean_l10=st10["mean"],
                median_l10=st10["median"],
                std_l10=st10["std"],
                cv_l10=st10["cv"],
                trend=st10["trend"],
            )
        )

        # ------------------------
        # BAYES_CDF (prior limpo) + CDF (mediana/std)
        # ------------------------
        p_like_cdf = like_from_cdf(st10, st20, line, side)
        p_real_c = posterior(p_prior_h, p_like_cdf, weight_prior)
        fair_c = fair_from_p(p_real_c)
        roi_c = (odds / fair_c - 1.0) * 100.0
        ev_c = ev_from(p_real_c, odds) * 100.0

        rows_bcdf.append(
            dict(
                method="bayes_cdf",
                player=player,
                stat=stat.upper(),
                line=line,
                side=side,
                odds=odds,
                p_real=p_real_c * 100.0,
                p_prior=p_prior_h * 100.0,
                p_like=p_like_cdf * 100.0,
                fair=fair_c,
                roi=roi_c,
                ev=ev_c,
                l10_pct=st10["hit_rate"] * 100.0,
                l20_pct=st20["hit_rate"] * 100.0,
                mean_l10=st10["mean"],
                median_l10=st10["median"],
                std_l10=st10["std"],
                cv_l10=st10["cv"],
                trend=st10["trend"],
            )
        )

    df_original = pd.DataFrame(rows_original)
    df_bhit = pd.DataFrame(rows_bhit)
    df_bcdf = pd.DataFrame(rows_bcdf)

    # Filtro min_roi (opcional)
    if min_roi is not None:
        for df_ in (df_original, df_bhit, df_bcdf):
            if not df_.empty:
                df_.query("roi >= @min_roi", inplace=True)

    # DF combinado
    df_all = pd.concat([df_original, df_bhit, df_bcdf], ignore_index=True)
    return df_all, {"original": df_original, "bayes_hit": df_bhit, "bayes_cdf": df_bcdf}
