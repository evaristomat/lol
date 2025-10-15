import sqlite3
import warnings
from datetime import datetime
from functools import lru_cache
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd
from scipy import stats

warnings.filterwarnings("ignore")


# ==============================================================================
# FUNÇÕES DE CÁLCULO BAYESIANO
# ==============================================================================


def calculate_implied_probability(odds: float) -> float:
    """Converte odds decimais em probabilidade implícita bruta."""
    return 1 / odds


def remove_vig(probabilities: list[float]) -> list[float]:
    """
    Remove a margem (vig) das probabilidades implícitas.
    Assume que a lista de probabilidades cobre todos os resultados possíveis (ex: V, E, D).
    """
    vig = sum(probabilities) - 1
    if vig <= 0:
        return probabilities

    # Normaliza as probabilidades para remover o vig
    normalized_probabilities = [p / (1 + vig) for p in probabilities]
    return normalized_probabilities


def calculate_posterior_prob(
    p_prior: float, p_likelihood: float, weight_prior: float
) -> float:
    """
    Calcula a Probabilidade Posterior (P_real) usando média ponderada.
    P_real = (weight_prior * P_prior) + ((1 - weight_prior) * P_likelihood)
    """
    weight_likelihood = 1 - weight_prior
    p_real = (weight_prior * p_prior) + (weight_likelihood * p_likelihood)
    return p_real


def calculate_ev(p_real: float, odds: float) -> float:
    """Calcula o Valor Esperado (EV) de uma aposta."""
    return (p_real * odds) - 1


# ==============================================================================
# CLASSE PRINCIPAL REFATORADA PARA COMPARAÇÃO
# ==============================================================================


class PlayerAnalyzer:
    """Analisador otimizado com cache e processamento vetorizado"""

    # Parâmetro chave para o método Bayesiano: Peso do Prior (Odds da Casa)
    BAYESIAN_WEIGHT_PRIOR = 0.5

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.player_history_df = None
        self._stats_cache = {}

    @lru_cache(maxsize=128)
    def _get_player_data(self, player: str, team: str, stat: str) -> np.ndarray:
        """Cache de dados do jogador para evitar filtros repetidos"""
        if self.player_history_df is None:
            return np.array([])

        # Otimização: O índice já está em playername e teamname
        try:
            player_data = self.player_history_df.loc[
                (player, team), ["date", stat]
            ].copy()
        except KeyError:
            return np.array([])

        player_data = player_data.sort_values("date", ascending=False)

        values = pd.to_numeric(player_data[stat], errors="coerce").dropna().values
        return values[:50]  # Limitar a 50 jogos mais recentes

    def load_player_history(self) -> bool:
        """Carrega e otimiza o DataFrame de histórico"""
        csv_path = Path(__file__).parent / "data" / "database" / "database.csv"

        if not csv_path.exists():
            print(f"❌ Arquivo não encontrado: {csv_path}")
            return False

        # Carregar apenas colunas necessárias
        needed_cols = ["playername", "teamname", "date", "kills", "deaths", "assists"]

        try:
            df = pd.read_csv(csv_path, usecols=needed_cols, low_memory=False)
            df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d", errors="coerce")

            # Criar índice para acelerar buscas
            df = df.set_index(["playername", "teamname"], drop=False).sort_index()

            # Converter colunas numéricas de uma vez
            for col in ["kills", "deaths", "assists"]:
                df[col] = pd.to_numeric(df[col], errors="coerce")

            self.player_history_df = df
            print(f"✅ Carregados {len(df):,} registros")
            return True

        except Exception as e:
            print(f"❌ Erro ao carregar dados: {e}")
            return False

    @staticmethod
    @lru_cache(maxsize=256)
    def calculate_statistics_cached(
        values_tuple: tuple, handicap: float, side: str
    ) -> dict:
        """Versão cacheável do cálculo de estatísticas"""
        values = np.array(values_tuple)

        if len(values) == 0:
            return {}

        # Cálculos vetorizados
        mean = values.mean()
        median = np.median(values)
        std = values.std()
        cv = (std / mean * 100) if mean > 0 else 0

        # Hit rate (Likelihood para o Método Média)
        if side == "over":
            hit_rate = (values > handicap).mean()
        else:
            hit_rate = (values < handicap).mean()

        # Intervalo de confiança
        if len(values) > 1:
            ci = stats.t.interval(
                0.95, len(values) - 1, loc=mean, scale=stats.sem(values)
            )
        else:
            ci = (mean, mean)

        # Tendência
        if len(values) >= 20:
            recent = values[:10].mean()
            older = values[10:20].mean()
            trend = ((recent - older) / older * 100) if older > 0 else 0
        else:
            trend = 0

        return {
            "mean": mean,
            "median": median,
            "std": std,
            "cv": cv,
            "hit_rate": hit_rate,
            "ci_lower": ci[0],
            "ci_upper": ci[1],
            "trend": trend,
        }

    def process_odds_batch(
        self, odds_df: pd.DataFrame, player_teams: dict
    ) -> Tuple[list, list]:
        """Processa todas as odds de uma vez usando os dois métodos Bayesianos"""

        stat_map = {
            "Map 1 - Player Total Kills": "kills",
            "Map 1 - Player Total Deaths": "deaths",
            "Map 1 - Player Total Assists": "assists",
        }

        good_bets_mean_bayesian = []
        good_bets_median_bayesian = []
        processed = set()

        # Pré-processar odds_df
        odds_df["player"] = (
            odds_df["selection_name"]
            .str.replace("Over |Under ", "", regex=True)
            .str.strip()
        )
        odds_df["side"] = odds_df["selection_name"].apply(
            lambda x: "over" if "Over" in x else "under" if "Under" in x else None
        )
        odds_df["stat"] = odds_df["market_name"].map(stat_map)

        # Filtrar apenas odds válidas
        valid_odds = odds_df[
            (odds_df["side"].notna())
            & (odds_df["stat"].notna())
            & (odds_df["handicap"].notna())
            & (odds_df["player"].isin(player_teams.keys()))
        ].copy()

        print(
            f"🔍 Analisando {len(valid_odds)} odds válidas com Comparação Bayesiana (Média vs Mediana)..."
        )

        # Processar em lote
        for idx, row in valid_odds.iterrows():
            # Cria uma chave única para evitar processar a mesma aposta (player/stat/handicap/side)
            bet_key = f"{row['player']}_{row['stat']}_{row['side']}_{row['handicap']}"

            if bet_key in processed:
                continue
            processed.add(bet_key)

            # Obter dados do cache
            values = self._get_player_data(
                row["player"], player_teams[row["player"]], row["stat"]
            )

            if len(values) < 20:
                continue

            # Calcular estatísticas (com cache)
            l10 = tuple(values[:10])
            l20 = tuple(values[:20])

            stats_l10 = self.calculate_statistics_cached(
                l10, row["handicap"], row["side"]
            )
            stats_l20 = self.calculate_statistics_cached(
                l20, row["handicap"], row["side"]
            )

            if not stats_l10 or not stats_l20:
                continue

            # P_Prior (Probabilidade Implícita Bruta)
            p_prior_bruto = calculate_implied_probability(row["odds_value"])

            # ==================================================================
            # MÉTODO 1: BAYESIANO COM LIKELIHOOD BASEADO NA MÉDIA (HIT RATE)
            # ==================================================================

            # P_Likelihood (Hit Rate Ponderado)
            p_likelihood_mean = (stats_l10["hit_rate"] * 0.6) + (
                stats_l20["hit_rate"] * 0.4
            )

            # P_Real (Probabilidade Posterior)
            p_real_mean = calculate_posterior_prob(
                p_prior_bruto, p_likelihood_mean, self.BAYESIAN_WEIGHT_PRIOR
            )

            # Calcular EV e ROI
            if p_real_mean > 0:
                fair_odds_mean = 1 / p_real_mean
                roi_mean = ((row["odds_value"] / fair_odds_mean) - 1) * 100
                ev_mean = calculate_ev(p_real_mean, row["odds_value"]) * 100

                # Filtrar EV+ (ROI > 5% e P_Real > P_Implícita Bruta)
                if roi_mean > 5 and p_real_mean > p_prior_bruto:
                    good_bets_mean_bayesian.append(
                        {
                            "player": row["player"],
                            "stat": row["stat"].upper(),
                            "line": row["handicap"],
                            "side": row["side"],
                            "odds": row["odds_value"],
                            "fair": fair_odds_mean,
                            "roi": roi_mean,
                            "ev": ev_mean,
                            "p_real": p_real_mean * 100,
                            "p_prior": p_prior_bruto * 100,
                            "p_like": p_likelihood_mean * 100,
                            "l10_pct": stats_l10["hit_rate"] * 100,
                            "l20_pct": stats_l20["hit_rate"] * 100,
                            "mean_l10": stats_l10["mean"],
                            "cv_l10": stats_l10["cv"],
                            "trend": stats_l10["trend"],
                        }
                    )

            # ==================================================================
            # MÉTODO 2: BAYESIANO COM LIKELIHOOD BASEADO NA MEDIANA (CDF)
            # ==================================================================

            if stats_l10["std"] > 0 and stats_l20["std"] > 0:
                # 1. Calcular a probabilidade (Likelihood) usando a Mediana e a Distribuição Normal
                # A probabilidade é calculada como a área sob a curva (CDF)
                if row["side"] == "over":
                    # Z-score para a linha em relação à Mediana
                    z_l10 = (row["handicap"] - stats_l10["median"]) / stats_l10["std"]
                    z_l20 = (row["handicap"] - stats_l20["median"]) / stats_l20["std"]
                    # Probabilidade de ser > handicap (1 - CDF)
                    prob_l10 = 1 - stats.norm.cdf(z_l10)
                    prob_l20 = 1 - stats.norm.cdf(z_l20)
                else:  # side == "under"
                    # Z-score para a linha em relação à Mediana
                    z_l10 = (row["handicap"] - stats_l10["median"]) / stats_l10["std"]
                    z_l20 = (row["handicap"] - stats_l20["median"]) / stats_l20["std"]
                    # Probabilidade de ser < handicap (CDF)
                    prob_l10 = stats.norm.cdf(z_l10)
                    prob_l20 = stats.norm.cdf(z_l20)

                # P_Likelihood (Probabilidade Ponderada Mediana/CDF)
                p_likelihood_median = (prob_l10 * 0.6) + (prob_l20 * 0.4)

                # P_Real (Probabilidade Posterior)
                p_real_median = calculate_posterior_prob(
                    p_prior_bruto, p_likelihood_median, self.BAYESIAN_WEIGHT_PRIOR
                )

                # Calcular EV e ROI
                if p_real_median > 0:
                    fair_odds_median = 1 / p_real_median
                    roi_median = ((row["odds_value"] / fair_odds_median) - 1) * 100
                    ev_median = calculate_ev(p_real_median, row["odds_value"]) * 100

                    # Filtrar EV+ (ROI > 5% e P_Real > P_Implícita Bruta)
                    if roi_median > 5 and p_real_median > p_prior_bruto:
                        good_bets_median_bayesian.append(
                            {
                                "player": row["player"],
                                "stat": row["stat"].upper(),
                                "line": row["handicap"],
                                "side": row["side"],
                                "odds": row["odds_value"],
                                "fair": fair_odds_median,
                                "roi": roi_median,
                                "ev": ev_median,
                                "p_real": p_real_median * 100,
                                "p_prior": p_prior_bruto * 100,
                                "p_like": p_likelihood_median * 100,
                                "median_l10": stats_l10["median"],
                                "std_l10": stats_l10["std"],
                                "cv_l10": stats_l10["cv"],
                                "trend": stats_l10["trend"],
                            }
                        )

        # Retorna as duas listas para comparação
        return good_bets_mean_bayesian, good_bets_median_bayesian

    def analyze_event(self, event_id: str, min_roi: float = 5):
        """Análise principal do evento"""

        # Carregar dados se necessário
        if self.player_history_df is None:
            if not self.load_player_history():
                return

        # Conectar ao banco
        # A importação do sqlite3 está no topo do arquivo
        with sqlite3.connect(self.db_path) as conn:
            # Buscar informações do evento
            event_query = """
            SELECT e.league_name, e.match_date, ht.name as home_team, at.name as away_team
            FROM events e
            JOIN teams ht ON e.home_team_id = ht.team_id
            JOIN teams at ON e.away_team_id = at.team_id
            WHERE e.event_id = ?
            """

            event_df = pd.read_sql_query(event_query, conn, params=[event_id])

            if event_df.empty:
                print("❌ Evento não encontrado")
                return

            event = event_df.iloc[0]

            # Buscar odds
            odds_query = """
            SELECT market_name, selection_name, handicap, odds_value
            FROM current_odds
            WHERE event_id = ? 
              AND odds_type = 'player' 
              AND market_name IN (
                  'Map 1 - Player Total Kills',
                  'Map 1 - Player Total Deaths', 
                  'Map 1 - Player Total Assists'
              )
            """

            odds_df = pd.read_sql_query(odds_query, conn, params=[event_id])

        if odds_df.empty:
            print("❌ Sem odds de players")
            return

        # Converter handicap
        odds_df["handicap"] = pd.to_numeric(odds_df["handicap"], errors="coerce")
        odds_df = odds_df.dropna(subset=["handicap"])

        # Definir times dos jogadores (MANTIDO DO CÓDIGO ORIGINAL)
        player_teams = {
            "Doran": event["home_team"],
            "Oner": event["home_team"],
            "Faker": event["home_team"],
            "Gumayusi": event["home_team"],
            "Keria": event["home_team"],
            "TheShy": event["away_team"],
            "Wei": event["away_team"],
            "Rookie": event["away_team"],
            "GALA": event["away_team"],
            "Meiko": event["away_team"],
        }

        print(f"\n{'='*80}")
        print(f"🏆 {event['home_team']} vs {event['away_team']}")
        print(f"📅 {event['match_date']} | {event['league_name']}")
        print("=" * 80)

        # Processar odds em lote
        good_bets_mean_bayesian, good_bets_median_bayesian = self.process_odds_batch(
            odds_df, player_teams
        )

        # Exibir resultados - Método Média (Hit Rate)
        if good_bets_mean_bayesian:
            good_bets_mean_bayesian.sort(key=lambda x: x["roi"], reverse=True)
            print(
                f"\n📊 MÉTODO 1: BAYESIANO COM LIKELIHOOD BASEADO NA MÉDIA (HIT RATE)"
            )
            print(f"   Peso do Prior (Casa): {self.BAYESIAN_WEIGHT_PRIOR}")
            print(f"🎯 {len(good_bets_mean_bayesian)} apostas com valor:\n")

            for b in good_bets_mean_bayesian[:10]:  # Top 10
                print(f"✅ {b['player']} | {b['stat']} {b['side'].upper()} {b['line']}")
                print(
                    f"   Odds: {b['odds']:.2f} → Fair: {b['fair']:.2f} | ROI: {b['roi']:+.1f}% | EV: {b['ev']:+.1f}%"
                )
                print(
                    f"   P_Real: {b['p_real']:.1f}% | P_Prior: {b['p_prior']:.1f}% | P_Likelihood (Hit Rate): {b['p_like']:.1f}%"
                )
                print(
                    f"   Média L10: {b['mean_l10']:.1f} | CV: {b['cv_l10']:.1f} | L10 Hit: {b['l10_pct']:.0f}% | L20 Hit: {b['l20_pct']:.0f}% | Trend: {b['trend']:+.1f}%\n"
                )
        else:
            print(f"\n😔 MÉTODO 1: Nenhuma aposta com ROI ≥ {min_roi}%")

        # Exibir resultados - Método Mediana (CDF)
        if good_bets_median_bayesian:
            good_bets_median_bayesian.sort(key=lambda x: x["roi"], reverse=True)
            print(f"\n📊 MÉTODO 2: BAYESIANO COM LIKELIHOOD BASEADO NA MEDIANA (CDF)")
            print(f"   Peso do Prior (Casa): {self.BAYESIAN_WEIGHT_PRIOR}")
            print(f"🎯 {len(good_bets_median_bayesian)} apostas com valor:\n")

            for b in good_bets_median_bayesian[:10]:  # Top 10
                print(f"✅ {b['player']} | {b['stat']} {b['side'].upper()} {b['line']}")
                print(
                    f"   Odds: {b['odds']:.2f} → Fair: {b['fair']:.2f} | ROI: {b['roi']:+.1f}% | EV: {b['ev']:+.1f}%"
                )
                print(
                    f"   P_Real: {b['p_real']:.1f}% | P_Prior: {b['p_prior']:.1f}% | P_Likelihood (Mediana/CDF): {b['p_like']:.1f}%"
                )
                print(
                    f"   Mediana L10: {b['median_l10']:.1f} | Std: {b['std_l10']:.1f} | CV: {b['cv_l10']:.1f} | Trend: {b['trend']:+.1f}%\n"
                )
        else:
            print(f"\n😔 MÉTODO 2: Nenhuma aposta com ROI ≥ {min_roi}%")

        print("=" * 80)


# Função principal para manter compatibilidade
def analyze_event_players(event_id: str, db_path: str, min_roi: float = 5):
    """Wrapper para manter compatibilidade com código anterior"""
    analyzer = PlayerAnalyzer(db_path)
    analyzer.analyze_event(event_id, min_roi)


if __name__ == "__main__":
    # ATENÇÃO: O código original usava um caminho relativo que pode não existir.
    # Para fins de demonstração e refatoração, mantemos a estrutura, mas
    # a execução real dependerá da existência do arquivo 'data/lol_odds.db'.
    DB_PATH = Path(__file__).parent / "data" / "lol_odds.db"
    EVENT_ID = "182052047"

    # Usar a classe otimizada
    analyzer = PlayerAnalyzer(str(DB_PATH))
    analyzer.analyze_event(EVENT_ID, min_roi=5)
