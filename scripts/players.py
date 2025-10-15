"""
players_analyzer.py (v3 - Refatorado)

Responsabilidade: Apenas a lógica de análise estatística e Bayesiana.
Esta classe não deve mais se conectar diretamente ao banco de dados para buscar odds ou informações de eventos.
Ela recebe os DataFrames e dicionários necessários para realizar a análise.
"""

import sqlite3
import warnings
from functools import lru_cache
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from scipy import stats

warnings.filterwarnings("ignore")


# ==============================================================================
# FUNÇÕES DE CÁLCULO BAYESIANO (Auxiliares)
# ==============================================================================


def calculate_implied_probability(odds: float) -> float:
    return 1 / odds


def calculate_posterior_prob(
    p_prior: float, p_likelihood: float, weight_prior: float
) -> float:
    weight_likelihood = 1 - weight_prior
    return (weight_prior * p_prior) + (weight_likelihood * p_likelihood)


def calculate_ev(p_real: float, odds: float) -> float:
    return (p_real * odds) - 1


# ==============================================================================
# CLASSE DE ANÁLISE DE PLAYERS
# ==============================================================================


class PlayerAnalyzer:
    BAYESIAN_WEIGHT_PRIOR = 0.5

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.player_history_df = None
        self.load_player_history()

    def load_player_history(self) -> bool:
        csv_path = Path(self.db_path).parent / "database" / "database.csv"
        if not csv_path.exists():
            print(f"❌ Arquivo de histórico de players não encontrado: {csv_path}")
            return False

        try:
            df = pd.read_csv(
                csv_path,
                usecols=[
                    "playername",
                    "teamname",
                    "date",
                    "kills",
                    "deaths",
                    "assists",
                ],
                low_memory=False,
            )
            df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d", errors="coerce")
            df = df.set_index(["playername", "teamname"], drop=False).sort_index()
            for col in ["kills", "deaths", "assists"]:
                df[col] = pd.to_numeric(df[col], errors="coerce")
            self.player_history_df = df
            print(f"✅ Player history loaded: {len(df):,} records")
            return True
        except Exception as e:
            print(f"❌ Erro ao carregar dados de players: {e}")
            return False

    @lru_cache(maxsize=128)
    def _get_player_data(self, player: str, team: str, stat: str) -> np.ndarray:
        if self.player_history_df is None:
            return np.array([])
        try:
            player_data = self.player_history_df.loc[
                (player, team), ["date", stat]
            ].copy()
            player_data = player_data.sort_values("date", ascending=False)
            return (
                pd.to_numeric(player_data[stat], errors="coerce").dropna().values[:50]
            )
        except KeyError:
            return np.array([])

    @staticmethod
    @lru_cache(maxsize=256)
    def calculate_statistics_cached(
        values_tuple: tuple, handicap: float, side: str
    ) -> dict:
        values = np.array(values_tuple)
        if len(values) == 0:
            return {}

        mean, median, std = values.mean(), np.median(values), values.std()
        cv = (std / mean * 100) if mean > 0 else 0
        hit_rate = (
            (values > handicap).mean() if side == "over" else (values < handicap).mean()
        )
        trend = 0
        if len(values) >= 20:
            recent, older = values[:10].mean(), values[10:20].mean()
            trend = ((recent - older) / older * 100) if older > 0 else 0

        return {
            "mean": mean,
            "median": median,
            "std": std,
            "cv": cv,
            "hit_rate": hit_rate,
            "trend": trend,
        }

    def analyze_player_odds(
        self, odds_df: pd.DataFrame, player_teams: dict, min_roi: float
    ) -> List[Dict]:
        all_good_bets = []
        processed = set()
        stat_map = {
            "Map 1 - Player Total Kills": "kills",
            "Map 1 - Player Total Deaths": "deaths",
            "Map 1 - Player Total Assists": "assists",
        }

        odds_df["player"] = (
            odds_df["selection_name"]
            .str.replace("Over |Under ", "", regex=True)
            .str.strip()
        )
        odds_df["side"] = odds_df["selection_name"].apply(
            lambda x: "over" if "Over" in x else "under"
        )
        odds_df["stat"] = odds_df["market_name"].map(stat_map)

        valid_odds = odds_df[odds_df["player"].isin(player_teams.keys())].copy()

        for _, row in valid_odds.iterrows():
            bet_key = f'{row["player"]}_{row["stat"]}_{row["side"]}_{row["handicap"]}'
            if bet_key in processed:
                continue
            processed.add(bet_key)

            values = self._get_player_data(
                row["player"], player_teams[row["player"]], row["stat"]
            )
            if len(values) < 20:
                continue

            stats_l10 = self.calculate_statistics_cached(
                tuple(values[:10]), row["handicap"], row["side"]
            )
            stats_l20 = self.calculate_statistics_cached(
                tuple(values[:20]), row["handicap"], row["side"]
            )
            if not stats_l10 or not stats_l20:
                continue

            p_prior_bruto = calculate_implied_probability(row["odds_value"])

            # Método 1: Média (Hit Rate)
            p_likelihood_mean = (stats_l10["hit_rate"] * 0.6) + (
                stats_l20["hit_rate"] * 0.4
            )
            p_real_mean = calculate_posterior_prob(
                p_prior_bruto, p_likelihood_mean, self.BAYESIAN_WEIGHT_PRIOR
            )
            if p_real_mean > 0:
                fair_odds_mean = 1 / p_real_mean
                roi_mean = ((row["odds_value"] / fair_odds_mean) - 1) * 100
                if roi_mean > min_roi and p_real_mean > p_prior_bruto:
                    all_good_bets.append(
                        self._create_bet_dict(
                            row,
                            roi_mean,
                            fair_odds_mean,
                            "Bayesian_Mean",
                            p_real_mean,
                            p_prior_bruto,
                            p_likelihood_mean,
                        )
                    )

            # Método 2: Mediana (CDF)
            if stats_l10["std"] > 0 and stats_l20["std"] > 0:
                if row["side"] == "over":
                    prob_l10 = 1 - stats.norm.cdf(
                        (row["handicap"] - stats_l10["median"]) / stats_l10["std"]
                    )
                    prob_l20 = 1 - stats.norm.cdf(
                        (row["handicap"] - stats_l20["median"]) / stats_l20["std"]
                    )
                else:
                    prob_l10 = stats.norm.cdf(
                        (row["handicap"] - stats_l10["median"]) / stats_l10["std"]
                    )
                    prob_l20 = stats.norm.cdf(
                        (row["handicap"] - stats_l20["median"]) / stats_l20["std"]
                    )

                p_likelihood_median = (prob_l10 * 0.6) + (prob_l20 * 0.4)
                p_real_median = calculate_posterior_prob(
                    p_prior_bruto, p_likelihood_median, self.BAYESIAN_WEIGHT_PRIOR
                )
                if p_real_median > 0:
                    fair_odds_median = 1 / p_real_median
                    roi_median = ((row["odds_value"] / fair_odds_median) - 1) * 100
                    if roi_median > min_roi and p_real_median > p_prior_bruto:
                        all_good_bets.append(
                            self._create_bet_dict(
                                row,
                                roi_median,
                                fair_odds_median,
                                "Bayesian_Median",
                                p_real_median,
                                p_prior_bruto,
                                p_likelihood_median,
                            )
                        )

        return all_good_bets

    def analyze_totals_odds(
        self, event_id: str, betting_lines: List[Dict], min_roi: float
    ) -> List[Dict]:
        all_good_bets = []
        for line in betting_lines:
            odds = line["odds"]
            p_prior_bruto = calculate_implied_probability(odds)
            p_likelihood_simulated = min(p_prior_bruto * 1.05, 0.99)
            p_real = calculate_posterior_prob(
                p_prior_bruto, p_likelihood_simulated, self.BAYESIAN_WEIGHT_PRIOR
            )

            if p_real > 0:
                fair_odds = 1 / p_real
                roi = ((odds / fair_odds) - 1) * 100
                if roi > min_roi and p_real > p_prior_bruto:
                    bet_dict = self._create_bet_dict(
                        line,
                        roi,
                        fair_odds,
                        "Totals_Bayesian_Simulated",
                        p_real,
                        p_prior_bruto,
                        p_likelihood_simulated,
                    )
                    bet_dict["event_id"] = event_id
                    bet_dict["market_name"] = line.get(
                        "market_name", "Unknown Market"
                    )  # Garante que market_name exista
                    all_good_bets.append(bet_dict)
        return all_good_bets

    def _create_bet_dict(
        self, data_row, roi, fair_odds, method, p_real, p_prior, p_like
    ) -> Dict:
        selection_line = data_row.get("selection_name") or data_row.get("selection")
        if "player" in data_row:
            selection_line = f'{data_row["player"]} | {data_row["stat"].upper()} {data_row["side"].upper()}'

        return {
            "event_id": data_row.get("event_id"),
            "market_name": data_row.get("market_name"),
            "selection_line": selection_line,
            "handicap": data_row.get("handicap"),
            "house_odds": data_row.get("odds_value") or data_row.get("odds"),
            "roi_average": roi,
            "fair_odds": fair_odds,
            "analysis_method": method,
            "p_real": p_real,
            "p_prior": p_prior,
            "p_like": p_like,
        }
