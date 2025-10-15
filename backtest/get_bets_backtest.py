# -*- coding: utf-8 -*-
import itertools
import random
import sqlite3
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from get_roi_backtest import ROIAnalyzer
from scipy import stats


class BacktestScanner:
    def __init__(
        self,
        odds_db_path: str,
        backtest_db_path: str = "db_backtest.db",
        # Exclusão mútua Over/Under por linha
        allow_both_sides: bool = False,
        min_delta_roi_pp: float = 0.0,  # p.p. mínimos para trocar de lado quando ambos passam
        # ---------- PARÂMETROS INICIAIS (serão ajustados pelo otimizador) ----------
        # Bayes (limites de clamp do w_prior e prior Beta)
        bayes_w_prior_floor: float = 0.55,
        bayes_w_prior_cap: float = 0.75,
        bayes_prior_a0: float = 5.0,
        bayes_prior_b0: float = 5.0,
        # Median (gates & posterior gain & filtro de volatilidade)
        med_pvalue_max: float = 0.20,
        med_min_abs_z: float = 1.00,
        med_min_edge: float = 0.06,
        med_min_posterior_gain: float = 0.03,
        med_max_cv_15: float = 60.0,
        # Otimizador
        optimize_params: bool = True,
        optimize_sample_events: int = 120,  # quantos eventos usar no tuning
        optimize_random_seed: int = 42,
    ):
        self.odds_db_path = odds_db_path
        self.backtest_db_path = backtest_db_path
        self.analyzer = ROIAnalyzer(odds_db_path)
        self.setup_database()

        # Controles gerais
        self.allow_both_sides = allow_both_sides
        self.min_delta_roi_pp = float(min_delta_roi_pp)

        # Parâmetros Bayes
        self.bayes_w_prior_floor = float(bayes_w_prior_floor)
        self.bayes_w_prior_cap = float(bayes_w_prior_cap)
        self.bayes_prior_a0 = float(bayes_prior_a0)
        self.bayes_prior_b0 = float(bayes_prior_b0)

        # Parâmetros Median
        self.med_pvalue_max = float(med_pvalue_max)
        self.med_min_abs_z = float(med_min_abs_z)
        self.med_min_edge = float(med_min_edge)
        self.med_min_posterior_gain = float(med_min_posterior_gain)
        self.med_max_cv_15 = float(med_max_cv_15)

        # Otimizador
        self.optimize_params_flag = bool(optimize_params)
        self.optimize_sample_events = int(optimize_sample_events)
        self.optimize_random_seed = int(optimize_random_seed)

    # ======================= DB =======================

    def setup_database(self):
        conn = sqlite3.connect(self.backtest_db_path)
        cursor = conn.cursor()

        cursor.execute(
            """
        CREATE TABLE IF NOT EXISTS backtest_bets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id TEXT NOT NULL,
            method TEXT NOT NULL,
            league_name TEXT,
            match_date TEXT,
            home_team TEXT,
            away_team TEXT,
            market_name TEXT NOT NULL,
            selection_line TEXT NOT NULL,
            handicap REAL NOT NULL,
            house_odds REAL NOT NULL,
            p_real REAL NOT NULL,
            p_prior REAL NOT NULL,
            p_like REAL NOT NULL,
            fair_odds REAL NOT NULL,
            roi REAL NOT NULL,
            ev REAL NOT NULL,
            l10_hit_rate REAL,
            l15_hit_rate REAL,
            mean_l10 REAL,
            median_l10 REAL,
            std_l10 REAL,
            cv_l10 REAL,
            trend REAL,
            w_prior REAL,
            is_normal BOOLEAN,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(event_id, method, market_name, selection_line, handicap)
        )
        """
        )

        cursor.execute("CREATE INDEX IF NOT EXISTS idx_method ON backtest_bets(method)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_roi ON backtest_bets(roi)")
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_event ON backtest_bets(event_id)"
        )

        conn.commit()
        conn.close()

    def get_all_events(self) -> List[str]:
        conn = sqlite3.connect(self.odds_db_path)
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT DISTINCT event_id
            FROM current_odds
            WHERE market_name LIKE '%Totals' AND odds_type IN ('map_1','map_2')
        """
        )
        events = [row[0] for row in cursor.fetchall()]
        conn.close()
        return events

    # ======================= Helpers matemáticos =======================

    @staticmethod
    def _implied_prob(odds: float) -> float:
        return float(np.clip(1.0 / max(odds, 1e-12), 1e-6, 1 - 1e-6))

    @staticmethod
    def _remove_vig_pair(p_over: float, p_under: float) -> Tuple[float, float]:
        s = p_over + p_under
        if s <= 0:
            return p_over, p_under
        return (
            float(np.clip(p_over / s, 1e-6, 1 - 1e-6)),
            float(np.clip(p_under / s, 1e-6, 1 - 1e-6)),
        )

    @staticmethod
    def _posterior(p_prior: float, p_like: float, w_prior: float = 0.5) -> float:
        p_prior = float(np.clip(p_prior, 1e-6, 1 - 1e-6))
        p_like = float(np.clip(p_like, 1e-6, 1 - 1e-6))
        p = w_prior * p_prior + (1.0 - w_prior) * p_like
        return float(np.clip(p, 0.02, 0.98))  # evita fair absurda

    @staticmethod
    def _fair_from_p(p: float) -> float:
        return 1.0 / float(np.clip(p, 1e-6, 1 - 1e-6))

    @staticmethod
    def _ev_percent(p_real: float, odds: float) -> float:
        return (float(np.clip(p_real, 1e-6, 1 - 1e-6)) * float(odds) - 1.0) * 100.0

    # ======================= Identificação de stat =======================

    def _get_stat_type(self, selection: str) -> str:
        s = selection.lower()
        if "dragon" in s:
            return "dragons"
        if "baron" in s:
            return "barons"
        if "kill" in s:
            return "kills"
        if "tower" in s:
            return "towers"
        if "inhibitor" in s:
            return "inhibitors"
        return ""

    def _get_stat_type_from_market_or_selection(
        self, market_name: str, selection: str
    ) -> Optional[str]:
        s = (market_name or "").lower()
        if "kill" in s:
            return "kills"
        if "tower" in s:
            return "towers"
        if "baron" in s:
            return "barons"
        if "dragon" in s:
            return "dragons"
        if "inhibitor" in s:
            return "inhibitors"
        typ = self._get_stat_type(selection)
        return typ if typ else None

    # ======================= Estatística/robustez =======================

    def _test_normality(self, values: np.ndarray) -> bool:
        if len(values) < 3:
            return False
        try:
            _, p = stats.shapiro(values)
            return p > 0.05
        except Exception:
            return False

    # ======================= Stats por time =======================

    def _get_team_stats_from_analyzer(
        self, team: str, selection: str, handicap: float
    ) -> Optional[dict]:
        stat_type = self._get_stat_type(selection)
        if not stat_type:
            return None

        values = self.analyzer.get_team_stats(team, stat_type, limit=50)
        if len(values) < 15:
            return None

        values = np.array(values)
        l10 = values[:10]
        l15 = values[:15]

        side = "over" if "over" in selection.lower() else "under"

        mean_10 = float(np.mean(l10))
        median_10 = float(np.median(l10))
        std_10 = float(np.std(l10, ddof=0))
        cv_10 = float((std_10 / mean_10 * 100.0) if mean_10 > 0 else 0.0)

        mean_15 = float(np.mean(l15))
        median_15 = float(np.median(l15))
        std_15 = float(np.std(l15, ddof=0))

        if side == "over":
            hit_10 = float(np.mean(l10 > handicap))
            hit_15 = float(np.mean(l15 > handicap))
        else:
            hit_10 = float(np.mean(l10 < handicap))
            hit_15 = float(np.mean(l15 < handicap))

        trend = 0.0
        if len(values) >= 15:
            recent = float(np.mean(values[:10]))
            older = float(np.mean(values[10:15]))
            trend = float(((recent - older) / older * 100.0) if older > 0 else 0.0)

        is_normal = self._test_normality(l10)

        return {
            "values_10": l10,
            "values_15": l15,
            "mean_10": mean_10,
            "median_10": median_10,
            "std_10": std_10,
            "cv_10": cv_10,
            "hit_10": hit_10,
            "mean_15": mean_15,
            "median_15": median_15,
            "std_15": std_15,
            "hit_15": hit_15,
            "trend": trend,
            "is_normal": is_normal,
        }

    def _get_team_values_for_stat(
        self, team: str, stat_type: str, limit: int = 50
    ) -> np.ndarray:
        vals = self.analyzer.get_team_stats(team, stat_type, limit=limit)
        if not isinstance(vals, list) or len(vals) == 0:
            return np.array([])
        return np.array(vals, dtype=float)

    # ======================= Métodos: p_like (by team) =======================

    def _p_like_original_by_team(self, stats: dict) -> float:
        return float(0.6 * stats["hit_10"] + 0.4 * stats["hit_15"])

    def _p_like_avg_teams(
        self, stats_over: dict, stats_under: dict
    ) -> Tuple[float, float]:
        p_over_t1 = 0.6 * stats_over["hit_10"] + 0.4 * stats_over["hit_15"]
        p_under_t2 = 0.6 * stats_under["hit_10"] + 0.4 * stats_under["hit_15"]
        p_over_from_t2 = 1.0 - (
            0.6 * stats_under["hit_10"] + 0.4 * stats_under["hit_15"]
        )
        p_under_from_t1 = 1.0 - (
            0.6 * stats_over["hit_10"] + 0.4 * stats_over["hit_15"]
        )
        return float((p_over_t1 + p_over_from_t2) / 2.0), float(
            (p_under_t2 + p_under_from_t1) / 2.0
        )

    # ---------- Bayes (Beta-Binomial) ----------
    def _beta_binomial_post_mean(self, s: int, n: int) -> float:
        a = self.bayes_prior_a0 + s
        b = self.bayes_prior_b0 + (n - s)
        return float(a / max(a + b, 1e-9))

    def _team_hit_counts(
        self, values_10: np.ndarray, values_15: np.ndarray, handicap: float, side: str
    ) -> Tuple[int, int]:
        if side == "over":
            p10 = float(np.mean(values_10 > handicap))
            p15 = float(np.mean(values_15 > handicap))
        else:
            p10 = float(np.mean(values_10 < handicap))
            p15 = float(np.mean(values_15 < handicap))
        eff_trials = 0.6 * len(values_10) + 0.4 * len(values_15)
        eff_success = 0.6 * (p10 * len(values_10)) + 0.4 * (p15 * len(values_15))
        s = int(round(eff_success))
        n = int(round(eff_trials))
        s = max(0, min(s, n))
        return s, n

    def _p_like_bayes_matchup(
        self, stats_over: dict, stats_under: dict, handicap: float
    ) -> Tuple[float, float]:
        s1, n1 = self._team_hit_counts(
            stats_over["values_10"], stats_over["values_15"], handicap, "over"
        )
        p_over_t1 = self._beta_binomial_post_mean(s1, n1)

        s2, n2 = self._team_hit_counts(
            stats_under["values_10"], stats_under["values_15"], handicap, "under"
        )
        p_under_t2 = self._beta_binomial_post_mean(s2, n2)

        # complementares cruzados
        p_over_from_t2 = 1.0 - self._beta_binomial_post_mean(s2, n2)
        s1c, n1c = self._team_hit_counts(
            stats_over["values_10"], stats_over["values_15"], handicap, "under"
        )
        p_under_from_t1 = 1.0 - self._beta_binomial_post_mean(s1c, n1c)

        return float((p_over_t1 + p_over_from_t2) / 2.0), float(
            (p_under_t2 + p_under_from_t1) / 2.0
        )

    # ======================= TOTALS: método baseado em MEDIANA =======================

    def _w15(self) -> np.ndarray:
        w = np.concatenate([np.full(10, 0.6 / 10.0), np.full(5, 0.4 / 5.0)])
        return w / w.sum()

    def _median_test_sum(
        self, home_vals: np.ndarray, away_vals: np.ndarray, handicap: float
    ) -> Tuple[float, float, float, float, float, float, float]:
        """
        Retorna:
          p_like_over, p_like_under, p_value, |z|, med_home, med_away, (cv_home_15, cv_away_15)
        - mediana ponderada (0.6/0.4)
        - MAD -> sigma robusto
        - Normal approx
        """
        if len(home_vals) < 15 or len(away_vals) < 15:
            return 0.5, 0.5, 1.0, 0.0, np.nan, np.nan, (np.inf, np.inf)

        h15 = home_vals[:15]
        a15 = away_vals[:15]
        w = self._w15()

        def wmedian(x, ww):
            idx = np.argsort(x)
            xs = x[idx]
            ws = ww[idx]
            c = np.cumsum(ws)
            return float(xs[np.searchsorted(c, 0.5, side="left")])

        def mad_sigma(x):
            med = np.median(x)
            mad = np.median(np.abs(x - med))
            return max(1.4826 * float(mad), 1e-6)

        med_home = wmedian(h15, w)
        med_away = wmedian(a15, w)
        sig_home = mad_sigma(h15)
        sig_away = mad_sigma(a15)

        # CV(15) aproximado para filtro de volatilidade (usa DP simples como proxy)
        cv_home_15 = (
            float(np.std(h15, ddof=0) / max(np.mean(h15), 1e-6) * 100.0)
            if np.mean(h15) > 0
            else 999.0
        )
        cv_away_15 = (
            float(np.std(a15, ddof=0) / max(np.mean(a15), 1e-6) * 100.0)
            if np.mean(a15) > 0
            else 999.0
        )

        median_total = med_home + med_away
        sd_total = float(np.sqrt(sig_home**2 + sig_away**2))

        z = (handicap - median_total) / sd_total
        p_over = 1.0 - float(stats.norm.cdf(z))
        p_over = float(np.clip(p_over, 1e-3, 1 - 1e-3))
        cdf_abs = float(stats.norm.cdf(abs(z)))
        p_value = float(2.0 * (1.0 - cdf_abs))

        return (
            p_over,
            1.0 - p_over,
            p_value,
            float(abs(z)),
            med_home,
            med_away,
            (cv_home_15, cv_away_15),
        )

    # ---------- Pesos e gates ----------

    def _w_prior_bayes_from_stats(self, stats_over: dict, stats_under: dict) -> float:
        def wf(st):
            base = 0.3 + 0.3 * np.clip(st["cv_10"] / 100.0, 0, 1)
            pen = 0.1 if st["trend"] < -10 else (0.05 if st["trend"] < -5 else 0.0)
            return float(base + pen)

        try:
            w = (wf(stats_over) + wf(stats_under)) / 2.0
        except Exception:
            w = 0.65
        return float(np.clip(w, self.bayes_w_prior_floor, self.bayes_w_prior_cap))

    def _median_gate(
        self,
        p_like_over: float,
        p_like_under: float,
        pval: float,
        z_abs: float,
        cv_home_15: float,
        cv_away_15: float,
    ) -> Tuple[bool, bool]:
        # Filtro de volatilidade
        if (cv_home_15 > self.med_max_cv_15) or (cv_away_15 > self.med_max_cv_15):
            return False, False
        gate_over = (
            (pval <= self.med_pvalue_max)
            and (z_abs >= self.med_min_abs_z)
            and (abs(p_like_over - 0.5) >= self.med_min_edge)
        )
        gate_under = (
            (pval <= self.med_pvalue_max)
            and (z_abs >= self.med_min_abs_z)
            and (abs(p_like_under - 0.5) >= self.med_min_edge)
        )
        return gate_over, gate_under

    def _w_prior_from_pvalue_and_robust(
        self, pvalue: float, robust_ratio: float
    ) -> float:
        # Baseado no pvalue, com ajuste suave por assimetria de escalas
        base_lo, base_hi = 0.50, 0.80  # prior mais conservador (≥0.50)
        base = float(np.clip(base_lo + (base_hi - base_lo) * pvalue, base_lo, base_hi))
        asym = abs(np.log(max(robust_ratio, 1e-6)))
        adj = -min(0.05, 0.02 * asym)  # reduz w_prior no máx 0.05
        return float(np.clip(base + adj, 0.45, 0.85))

    # ======================= Emissão =======================

    def _emit_pick(
        self,
        bucket: List[Dict],
        *,
        keep: bool,
        event_id: str,
        method: str,
        league: str,
        match_date: str,
        team1: str,
        team2: str,
        market: str,
        line_obj: Dict,
        handicap: float,
        p_real: float,
        p_prior: float,
        p_like: float,
        fair: float,
        roi: float,
        ev: float,
        stats_side: dict,
        w_prior: float,
    ):
        if not keep:
            return
        if p_real <= p_prior:
            return
        if (
            p_real - p_prior
        ) < self.med_min_posterior_gain and method == "median_test_sum":
            # ganho mínimo de posterior exigido no método median
            return
        bucket.append(
            {
                "event_id": event_id,
                "method": method,
                "league_name": league,
                "match_date": match_date,
                "home_team": team1,
                "away_team": team2,
                "market_name": market,
                "selection_line": line_obj["selection"],
                "handicap": handicap,
                "house_odds": line_obj["odds"],
                "p_real": p_real * 100,
                "p_prior": p_prior * 100,
                "p_like": p_like * 100,
                "fair_odds": fair,
                "roi": roi,
                "ev": ev,
                "l10_hit_rate": stats_side["hit_10"] * 100,
                "l15_hit_rate": stats_side["hit_15"] * 100,
                "mean_l10": stats_side["mean_10"],
                "median_l10": stats_side["median_10"],
                "std_l10": stats_side["std_10"],
                "cv_l10": stats_side["cv_10"],
                "trend": stats_side["trend"],
                "w_prior": w_prior,
                "is_normal": stats_side["is_normal"],
            }
        )

    # ======================= 4 MÉTODOS =======================

    def analyze_event_four_methods(
        self, event_id: str, min_roi: float = 10
    ) -> Tuple[List[Dict], List[Dict], List[Dict], List[Dict], Dict[str, int]]:
        """
        4 métodos:
          1) avg_team_original  : prior BRUTO; média por time no mesmo lado (sem cruzamento).
          2) avg_teams          : prior LIMPO + cruzamento complementar.
          3) bayes_matchup      : Beta-Binomial por time + prior LIMPO + w_prior adaptativo (clamp tunável).
          4) median_test_sum    : TOTAL (mediana/MAD), Normal approx, prior LIMPO, gates e w_prior robustos.
        Retorna as apostas e um dicionário de contagem de candidatos analisados por método.
        """
        event_info = self.analyzer.get_event_info(event_id)
        if not event_info:
            return (
                [],
                [],
                [],
                [],
                {
                    "avg_team_original": 0,
                    "avg_teams": 0,
                    "bayes_matchup": 0,
                    "median_test_sum": 0,
                },
            )

        team1 = event_info.get("home_team", "Team A")
        team2 = event_info.get("away_team", "Team B")
        league = event_info.get("league_name", "Unknown")
        match_date = event_info.get("match_date", "Unknown")

        bets_orig, bets_avg, bets_bayes, bets_median = [], [], [], []
        counters = {
            "avg_team_original": 0,
            "avg_teams": 0,
            "bayes_matchup": 0,
            "median_test_sum": 0,
        }

        markets = ["Map 1 - Totals", "Map 2 - Totals"]
        for market in markets:
            lines = self.analyzer.get_betting_lines(event_id, market)

            by_h = {}
            for line in lines:
                h = line["handicap"]
                if h not in by_h:
                    by_h[h] = {}
                side = "over" if "over" in line["selection"].lower() else "under"
                by_h[h][side] = line

            for handicap, sides in by_h.items():
                if "over" not in sides or "under" not in sides:
                    continue
                over_line = sides["over"]
                under_line = sides["under"]

                # ---------- Stats por TIME e LADO ----------
                stats_over_t1 = self._get_team_stats_from_analyzer(
                    team1, over_line["selection"], handicap
                )
                stats_over_t2 = self._get_team_stats_from_analyzer(
                    team2, over_line["selection"], handicap
                )
                stats_under_t1 = self._get_team_stats_from_analyzer(
                    team1, under_line["selection"], handicap
                )
                stats_under_t2 = self._get_team_stats_from_analyzer(
                    team2, under_line["selection"], handicap
                )
                if any(
                    s is None
                    for s in [
                        stats_over_t1,
                        stats_over_t2,
                        stats_under_t1,
                        stats_under_t2,
                    ]
                ):
                    continue

                # somente para logs/info
                stats_over = stats_over_t1
                stats_under = stats_under_t2

                # PRIORs
                p_over_raw = self._implied_prob(over_line["odds"])
                p_under_raw = self._implied_prob(under_line["odds"])
                p_over_clean, p_under_clean = self._remove_vig_pair(
                    p_over_raw, p_under_raw
                )

                # ===== 1) avg_team_original =====
                p_like_over_t1 = self._p_like_original_by_team(stats_over_t1)
                p_like_over_t2 = self._p_like_original_by_team(stats_over_t2)
                p_real_over_t1 = self._posterior(
                    p_over_raw, p_like_over_t1, w_prior=0.5
                )
                p_real_over_t2 = self._posterior(
                    p_over_raw, p_like_over_t2, w_prior=0.5
                )
                fair_over_avg = (
                    self._fair_from_p(p_real_over_t1)
                    + self._fair_from_p(p_real_over_t2)
                ) / 2.0
                roi_over_orig = (over_line["odds"] / fair_over_avg - 1.0) * 100.0
                ev_over_orig = self._ev_percent(
                    (p_real_over_t1 + p_real_over_t2) / 2.0, over_line["odds"]
                )

                p_like_under_t1 = self._p_like_original_by_team(stats_under_t1)
                p_like_under_t2 = self._p_like_original_by_team(stats_under_t2)
                p_real_under_t1 = self._posterior(
                    p_under_raw, p_like_under_t1, w_prior=0.5
                )
                p_real_under_t2 = self._posterior(
                    p_under_raw, p_like_under_t2, w_prior=0.5
                )
                fair_under_avg = (
                    self._fair_from_p(p_real_under_t1)
                    + self._fair_from_p(p_real_under_t2)
                ) / 2.0
                roi_under_orig = (under_line["odds"] / fair_under_avg - 1.0) * 100.0
                ev_under_orig = self._ev_percent(
                    (p_real_under_t1 + p_real_under_t2) / 2.0, under_line["odds"]
                )

                counters["avg_team_original"] += 2  # over e under analisados
                pick_over = roi_over_orig > min_roi
                pick_under = roi_under_orig > min_roi
                if not self.allow_both_sides and pick_over and pick_under:
                    if roi_over_orig + 1e-9 >= roi_under_orig + self.min_delta_roi_pp:
                        pick_under = False
                    else:
                        pick_over = False

                self._emit_pick(
                    bets_orig,
                    keep=pick_over,
                    event_id=event_id,
                    method="avg_team_original",
                    league=league,
                    match_date=match_date,
                    team1=team1,
                    team2=team2,
                    market=market,
                    line_obj=over_line,
                    handicap=handicap,
                    p_real=(p_real_over_t1 + p_real_over_t2) / 2.0,
                    p_prior=p_over_raw,
                    p_like=(p_like_over_t1 + p_like_over_t2) / 2.0,
                    fair=fair_over_avg,
                    roi=roi_over_orig,
                    ev=ev_over_orig,
                    stats_side=stats_over_t1,
                    w_prior=0.5,
                )

                self._emit_pick(
                    bets_orig,
                    keep=pick_under,
                    event_id=event_id,
                    method="avg_team_original",
                    league=league,
                    match_date=match_date,
                    team1=team1,
                    team2=team2,
                    market=market,
                    line_obj=under_line,
                    handicap=handicap,
                    p_real=(p_real_under_t1 + p_real_under_t2) / 2.0,
                    p_prior=p_under_raw,
                    p_like=(p_like_under_t1 + p_like_under_t2) / 2.0,
                    fair=fair_under_avg,
                    roi=roi_under_orig,
                    ev=ev_under_orig,
                    stats_side=stats_under_t1,
                    w_prior=0.5,
                )

                # ===== 2) avg_teams (prior LIMPO + cruzamento) =====
                p_like_over_avg, p_like_under_avg = self._p_like_avg_teams(
                    stats_over, stats_under
                )
                p_real_over_avg = self._posterior(
                    p_over_clean, p_like_over_avg, w_prior=0.5
                )
                p_real_under_avg = self._posterior(
                    p_under_clean, p_like_under_avg, w_prior=0.5
                )
                fair_over_avg2 = self._fair_from_p(p_real_over_avg)
                fair_under_avg2 = self._fair_from_p(p_real_under_avg)
                roi_over_avg2 = (over_line["odds"] / fair_over_avg2 - 1.0) * 100.0
                roi_under_avg2 = (under_line["odds"] / fair_under_avg2 - 1.0) * 100.0
                ev_over_avg2 = self._ev_percent(p_real_over_avg, over_line["odds"])
                ev_under_avg2 = self._ev_percent(p_real_under_avg, under_line["odds"])

                counters["avg_teams"] += 2
                pick_over = roi_over_avg2 > min_roi
                pick_under = roi_under_avg2 > min_roi
                if not self.allow_both_sides and pick_over and pick_under:
                    if roi_over_avg2 + 1e-9 >= roi_under_avg2 + self.min_delta_roi_pp:
                        pick_under = False
                    else:
                        pick_over = False

                self._emit_pick(
                    bets_avg,
                    keep=pick_over,
                    event_id=event_id,
                    method="avg_teams",
                    league=league,
                    match_date=match_date,
                    team1=team1,
                    team2=team2,
                    market=market,
                    line_obj=over_line,
                    handicap=handicap,
                    p_real=p_real_over_avg,
                    p_prior=p_over_clean,
                    p_like=p_like_over_avg,
                    fair=fair_over_avg2,
                    roi=roi_over_avg2,
                    ev=ev_over_avg2,
                    stats_side=stats_over,
                    w_prior=0.5,
                )

                self._emit_pick(
                    bets_avg,
                    keep=pick_under,
                    event_id=event_id,
                    method="avg_teams",
                    league=league,
                    match_date=match_date,
                    team1=team1,
                    team2=team2,
                    market=market,
                    line_obj=under_line,
                    handicap=handicap,
                    p_real=p_real_under_avg,
                    p_prior=p_under_clean,
                    p_like=p_like_under_avg,
                    fair=fair_under_avg2,
                    roi=roi_under_avg2,
                    ev=ev_under_avg2,
                    stats_side=stats_under,
                    w_prior=0.5,
                )

                # ===== 3) bayes_matchup =====
                p_like_over_bm, p_like_under_bm = self._p_like_bayes_matchup(
                    stats_over, stats_under, handicap
                )
                w_prior_bayes = self._w_prior_bayes_from_stats(stats_over, stats_under)
                p_real_over_bm = self._posterior(
                    p_over_clean, p_like_over_bm, w_prior=w_prior_bayes
                )
                p_real_under_bm = self._posterior(
                    p_under_clean, p_like_under_bm, w_prior=w_prior_bayes
                )
                fair_over_bm = self._fair_from_p(p_real_over_bm)
                fair_under_bm = self._fair_from_p(p_real_under_bm)
                roi_over_bm = (over_line["odds"] / fair_over_bm - 1.0) * 100.0
                roi_under_bm = (under_line["odds"] / fair_under_bm - 1.0) * 100.0
                ev_over_bm = self._ev_percent(p_real_over_bm, over_line["odds"])
                ev_under_bm = self._ev_percent(p_real_under_bm, under_line["odds"])

                counters["bayes_matchup"] += 2
                pick_over = roi_over_bm > min_roi
                pick_under = roi_under_bm > min_roi
                if not self.allow_both_sides and pick_over and pick_under:
                    if roi_over_bm + 1e-9 >= roi_under_bm + self.min_delta_roi_pp:
                        pick_under = False
                    else:
                        pick_over = False

                self._emit_pick(
                    bets_bayes,
                    keep=pick_over,
                    event_id=event_id,
                    method="bayes_matchup",
                    league=league,
                    match_date=match_date,
                    team1=team1,
                    team2=team2,
                    market=market,
                    line_obj=over_line,
                    handicap=handicap,
                    p_real=p_real_over_bm,
                    p_prior=p_over_clean,
                    p_like=p_like_over_bm,
                    fair=fair_over_bm,
                    roi=roi_over_bm,
                    ev=ev_over_bm,
                    stats_side=stats_over,
                    w_prior=w_prior_bayes,
                )

                self._emit_pick(
                    bets_bayes,
                    keep=pick_under,
                    event_id=event_id,
                    method="bayes_matchup",
                    league=league,
                    match_date=match_date,
                    team1=team1,
                    team2=team2,
                    market=market,
                    line_obj=under_line,
                    handicap=handicap,
                    p_real=p_real_under_bm,
                    p_prior=p_under_clean,
                    p_like=p_like_under_bm,
                    fair=fair_under_bm,
                    roi=roi_under_bm,
                    ev=ev_under_bm,
                    stats_side=stats_under,
                    w_prior=w_prior_bayes,
                )

                # ===== 4) median_test_sum =====
                stat_type = self._get_stat_type_from_market_or_selection(
                    market, over_line["selection"]
                )
                if not stat_type:
                    continue
                home_vals = self._get_team_values_for_stat(team1, stat_type, limit=50)
                away_vals = self._get_team_values_for_stat(team2, stat_type, limit=50)
                if len(home_vals) < 15 or len(away_vals) < 15:
                    continue

                (
                    p_like_over_md,
                    p_like_under_md,
                    pval_md,
                    zabs_md,
                    med_home,
                    med_away,
                    (cvh, cva),
                ) = self._median_test_sum(home_vals, away_vals, handicap)

                gate_over, gate_under = self._median_gate(
                    p_like_over_md, p_like_under_md, pval_md, zabs_md, cvh, cva
                )
                robust_ratio = float(
                    (np.median(np.abs(home_vals[:15] - med_home)) + 1e-6)
                    / (np.median(np.abs(away_vals[:15] - med_away)) + 1e-6)
                )
                w_prior_median = self._w_prior_from_pvalue_and_robust(
                    pval_md, robust_ratio
                )

                p_real_over_md = self._posterior(
                    p_over_clean, p_like_over_md, w_prior=w_prior_median
                )
                p_real_under_md = self._posterior(
                    p_under_clean, p_like_under_md, w_prior=w_prior_median
                )
                fair_over_md = self._fair_from_p(p_real_over_md)
                fair_under_md = self._fair_from_p(p_real_under_md)
                roi_over_md = (over_line["odds"] / fair_over_md - 1.0) * 100.0
                roi_under_md = (under_line["odds"] / fair_under_md - 1.0) * 100.0
                ev_over_md = self._ev_percent(p_real_over_md, over_line["odds"])
                ev_under_md = self._ev_percent(p_real_under_md, under_line["odds"])

                counters["median_test_sum"] += 2
                pick_over = (
                    gate_over
                    and roi_over_md > min_roi
                    and (p_real_over_md - p_over_clean) >= self.med_min_posterior_gain
                )
                pick_under = (
                    gate_under
                    and roi_under_md > min_roi
                    and (p_real_under_md - p_under_clean) >= self.med_min_posterior_gain
                )
                if not self.allow_both_sides and pick_over and pick_under:
                    if roi_over_md + 1e-9 >= roi_under_md + self.min_delta_roi_pp:
                        pick_under = False
                    else:
                        pick_over = False

                self._emit_pick(
                    bets_median,
                    keep=pick_over,
                    event_id=event_id,
                    method="median_test_sum",
                    league=league,
                    match_date=match_date,
                    team1=team1,
                    team2=team2,
                    market=market,
                    line_obj=over_line,
                    handicap=handicap,
                    p_real=p_real_over_md,
                    p_prior=p_over_clean,
                    p_like=p_like_over_md,
                    fair=fair_over_md,
                    roi=roi_over_md,
                    ev=ev_over_md,
                    stats_side=stats_over,
                    w_prior=w_prior_median,
                )

                self._emit_pick(
                    bets_median,
                    keep=pick_under,
                    event_id=event_id,
                    method="median_test_sum",
                    league=league,
                    match_date=match_date,
                    team1=team1,
                    team2=team2,
                    market=market,
                    line_obj=under_line,
                    handicap=handicap,
                    p_real=p_real_under_md,
                    p_prior=p_under_clean,
                    p_like=p_like_under_md,
                    fair=fair_under_md,
                    roi=roi_under_md,
                    ev=ev_under_md,
                    stats_side=stats_under,
                    w_prior=w_prior_median,
                )

        return bets_orig, bets_avg, bets_bayes, bets_median, counters

    # ======================= Persistência =======================

    def save_bets(self, bets: List[Dict]):
        if not bets:
            return
        conn = sqlite3.connect(self.backtest_db_path)
        cursor = conn.cursor()
        for bet in bets:
            try:
                cursor.execute(
                    """
                    INSERT OR IGNORE INTO backtest_bets 
                    (event_id, method, league_name, match_date, home_team, away_team,
                     market_name, selection_line, handicap, house_odds, p_real, p_prior,
                     p_like, fair_odds, roi, ev, l10_hit_rate, l15_hit_rate, mean_l10,
                     median_l10, std_l10, cv_l10, trend, w_prior, is_normal)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                    (
                        bet["event_id"],
                        bet["method"],
                        bet["league_name"],
                        bet["match_date"],
                        bet["home_team"],
                        bet["away_team"],
                        bet["market_name"],
                        bet["selection_line"],
                        bet["handicap"],
                        bet["house_odds"],
                        bet["p_real"],
                        bet["p_prior"],
                        bet["p_like"],
                        bet["fair_odds"],
                        bet["roi"],
                        bet["ev"],
                        bet["l10_hit_rate"],
                        bet["l15_hit_rate"],
                        bet["mean_l10"],
                        bet["median_l10"],
                        bet["std_l10"],
                        bet["cv_l10"],
                        bet["trend"],
                        bet["w_prior"],
                        bet["is_normal"],
                    ),
                )
            except Exception as e:
                print(f"❌ Erro: {e}")
        conn.commit()
        conn.close()

    # ======================= Otimização de parâmetros =======================

    def _score_expected_profit(self, bets: List[Dict]) -> float:
        # stake = 1; ev é % => lucro esperado em unidades = ev/100
        return float(sum(b["ev"] / 100.0 for b in bets))

    def optimize_params(self, min_roi: float, sample_events: List[str]):
        """
        Otimiza parâmetros de Bayes e Median maximizando lucro esperado (EV).
        Ajusta in-place: bayes_w_prior_floor/cap, bayes_prior a0/b0, e gates do Median.
        """
        random.seed(self.optimize_random_seed)

        # Grades de busca (mantidas compactas para ficar ágil; ajuste se quiser explorar mais)
        bayes_floor_grid = [0.50, 0.55, 0.60]
        bayes_cap_grid = [0.70, 0.75, 0.80]
        bayes_prior_grid = [(4.0, 4.0), (5.0, 5.0), (6.0, 6.0)]

        med_p_grid = [0.10, 0.15, 0.20, 0.25]
        med_z_grid = [0.8, 1.0, 1.2]
        med_edge_grid = [0.04, 0.06, 0.08]
        med_gain_grid = [0.02, 0.03, 0.05]
        med_cv_grid = [50.0, 60.0, 70.0]

        best_score = -1e9
        best_cfg = None

        # Amostra os eventos para tuning
        events = sample_events

        # Faz busca por produto cartesiano (simples)
        for bf, bc, (a0, b0), mp, mz, me, mg, mcv in itertools.product(
            bayes_floor_grid,
            bayes_cap_grid,
            bayes_prior_grid,
            med_p_grid,
            med_z_grid,
            med_edge_grid,
            med_gain_grid,
            med_cv_grid,
        ):
            # set temporariamente
            old = (
                self.bayes_w_prior_floor,
                self.bayes_w_prior_cap,
                self.bayes_prior_a0,
                self.bayes_prior_b0,
                self.med_pvalue_max,
                self.med_min_abs_z,
                self.med_min_edge,
                self.med_min_posterior_gain,
                self.med_max_cv_15,
            )

            self.bayes_w_prior_floor = bf
            self.bayes_w_prior_cap = bc
            self.bayes_prior_a0 = a0
            self.bayes_prior_b0 = b0
            self.med_pvalue_max = mp
            self.med_min_abs_z = mz
            self.med_min_edge = me
            self.med_min_posterior_gain = mg
            self.med_max_cv_15 = mcv

            # roda análise (sem salvar no DB) e computa score usando apenas Bayes e Median
            score = 0.0
            picks_cnt = 0
            for ev in events:
                _, _, b_bayes, b_median, _ = self.analyze_event_four_methods(
                    ev, min_roi=min_roi
                )
                score += self._score_expected_profit(b_bayes)
                score += self._score_expected_profit(b_median)
                picks_cnt += len(b_bayes) + len(b_median)

            # Heurística: preferir configurações com picks suficientes (evita overfitting a poucos picks)
            # bônus suave por volume moderado
            volume_bonus = 0.001 * picks_cnt  # 0.1u por 100 picks
            score += volume_bonus

            if score > best_score:
                best_score = score
                best_cfg = (bf, bc, a0, b0, mp, mz, me, mg, mcv)

            # restaura antes do próximo laço
            (
                self.bayes_w_prior_floor,
                self.bayes_w_prior_cap,
                self.bayes_prior_a0,
                self.bayes_prior_b0,
                self.med_pvalue_max,
                self.med_min_abs_z,
                self.med_min_edge,
                self.med_min_posterior_gain,
                self.med_max_cv_15,
            ) = old

        # aplica melhor configuração encontrada
        if best_cfg is not None:
            (bf, bc, a0, b0, mp, mz, me, mg, mcv) = best_cfg
            self.bayes_w_prior_floor = bf
            self.bayes_w_prior_cap = bc
            self.bayes_prior_a0 = a0
            self.bayes_prior_b0 = b0
            self.med_pvalue_max = mp
            self.med_min_abs_z = mz
            self.med_min_edge = me
            self.med_min_posterior_gain = mg
            self.med_max_cv_15 = mcv

            print("\n🧪 MELHOR CONFIG (EV esperado):")
            print(
                f"   Bayes: w_prior∈[{bf:.2f},{bc:.2f}]  |  Beta(a0,b0)=({a0:.1f},{b0:.1f})"
            )
            print(
                f"   Median: p≤{mp:.2f}, |z|≥{mz:.2f}, edge≥{me:.2f}, posterior_gain≥{mg:.2f}, CV15≤{mcv:.0f}"
            )
            print(f"   Score EV (aprox.): {best_score:.3f} (unidades)\n")
        else:
            print(
                "\n⚠️ Otimizador não encontrou configuração melhor (mantendo defaults).\n"
            )

    # ======================= Execução principal =======================

    def run_backtest(self, min_roi: float = 10, optimize_on: bool = True):
        methods_enabled = [
            "avg_team_original",
            "avg_teams",
            "bayes_matchup",
            "median_test_sum",
        ]
        print("🔍 Iniciando backtest com métodos:", ", ".join(methods_enabled))
        print(
            f"⚙️  allow_both_sides={self.allow_both_sides}, min_delta_roi_pp={self.min_delta_roi_pp}"
        )

        events = self.get_all_events()
        total_events = len(events)
        print(f"📋 {total_events} eventos encontrados")

        # Otimização de parâmetros (opcional)
        if optimize_on and self.optimize_params_flag and total_events > 0:
            sample = events.copy()
            random.seed(self.optimize_random_seed)
            random.shuffle(sample)
            sample = sample[: min(self.optimize_sample_events, len(sample))]
            print(f"🧪 Otimizando parâmetros em {len(sample)} eventos (amostra)")
            self.optimize_params(min_roi=min_roi, sample_events=sample)

        print(f"⚙️  Parâmetros após tuning:")
        print(
            f"    Bayes: w_prior∈[{self.bayes_w_prior_floor:.2f},{self.bayes_w_prior_cap:.2f}]  |  Beta(a0,b0)=({self.bayes_prior_a0:.1f},{self.bayes_prior_b0:.1f})"
        )
        print(
            f"    Median: p≤{self.med_pvalue_max:.2f}, |z|≥{self.med_min_abs_z:.2f}, edge≥{self.med_min_edge:.2f}, posterior_gain≥{self.med_min_posterior_gain:.2f}, CV15≤{self.med_max_cv_15:.0f}"
        )

        all_bets = []
        analyzed_counters = {
            "avg_team_original": 0,
            "avg_teams": 0,
            "bayes_matchup": 0,
            "median_test_sum": 0,
        }
        selected_counters = {
            "avg_team_original": 0,
            "avg_teams": 0,
            "bayes_matchup": 0,
            "median_test_sum": 0,
        }

        for i, event_id in enumerate(events, 1):
            print(f"⚡ [{i}/{total_events}] {event_id}")
            b1, b2, b3, b4, ctr = self.analyze_event_four_methods(event_id, min_roi)

            # acumula contadores
            for k in analyzed_counters.keys():
                analyzed_counters[k] += ctr.get(k, 0)

            # acumula picks e selecionados por método
            all_bets.extend(b1)
            selected_counters["avg_team_original"] += len(b1)
            all_bets.extend(b2)
            selected_counters["avg_teams"] += len(b2)
            all_bets.extend(b3)
            selected_counters["bayes_matchup"] += len(b3)
            all_bets.extend(b4)
            selected_counters["median_test_sum"] += len(b4)

        if all_bets:
            self.save_bets(all_bets)
            print(f"\n✅ {len(all_bets)} apostas salvas")
        else:
            print("\n❌ Nenhuma aposta encontrada")

        # ===== Impressão de cobertura (analisadas vs escolhidas) =====
        print("\n📈 COBERTURA POR MÉTODO (analisadas → escolhidas):")
        total_an = 0
        total_es = 0
        for m in ["avg_team_original", "avg_teams", "bayes_matchup", "median_test_sum"]:
            an = analyzed_counters[m]
            es = selected_counters[m]
            pct = (es / an * 100.0) if an > 0 else 0.0
            total_an += an
            total_es += es
            print(f"   {m:17s}: {an:6d} → {es:6d}  ({pct:5.1f}%)")
        pct_total = (total_es / total_an * 100.0) if total_an > 0 else 0.0
        print(
            f"\n🧮 GERAL: analisadas={total_an} | escolhidas={total_es}  ({pct_total:5.1f}%)"
        )

    def get_summary(self) -> pd.DataFrame:
        conn = sqlite3.connect(self.backtest_db_path)
        query = """
        SELECT 
            method,
            COUNT(*) as total_bets,
            AVG(roi) as avg_roi,
            AVG(ev) as avg_ev,
            AVG(house_odds) as avg_odds,
            AVG(fair_odds) as avg_fair,
            AVG(w_prior) as avg_w_prior,
            AVG(CAST(is_normal as FLOAT)) as pct_normal,
            MIN(roi) as min_roi,
            MAX(roi) as max_roi
        FROM backtest_bets
        GROUP BY method
        """
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df


if __name__ == "__main__":
    scanner = BacktestScanner(
        "../data/lol_odds.db",
        backtest_db_path="db_backtest.db",
        allow_both_sides=False,
        min_delta_roi_pp=0.0,
        # params iniciais (serão ajustados pelo otimizador)
        bayes_w_prior_floor=0.55,
        bayes_w_prior_cap=0.75,
        bayes_prior_a0=5.0,
        bayes_prior_b0=5.0,
        med_pvalue_max=0.20,
        med_min_abs_z=1.00,
        med_min_edge=0.06,
        med_min_posterior_gain=0.03,
        med_max_cv_15=60.0,
        # otimização
        optimize_params=True,
        optimize_sample_events=120,
        optimize_random_seed=42,
    )

    # (opcional) limpeza de métodos antigos para comparar execuções frescas:
    # import sqlite3
    # with sqlite3.connect("db_backtest.db") as c:
    #     c.execute("DELETE FROM backtest_bets WHERE method IN ('stat_test_sum','bootstrap_sum');")
    #     c.commit()

    scanner.run_backtest(min_roi=5, optimize_on=True)

    summary = scanner.get_summary()
    print("\n📊 RESUMO POR MÉTODO:")
    print(summary.to_string(index=False))
