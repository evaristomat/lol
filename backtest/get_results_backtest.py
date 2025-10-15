# -*- coding: utf-8 -*-
import logging
import re
import sqlite3
from typing import Dict, List, Optional, Tuple

import pandas as pd

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("backtest_results_updater")


class BacktestResultsUpdater:
    def __init__(self, backtest_db_path: str, esports_db_path: str, odds_db_path: str):
        self.backtest_db_path = backtest_db_path
        self.esports_db_path = esports_db_path
        self.odds_db_path = odds_db_path

    def get_finished_events_from_esports(self) -> List[Dict]:
        """Busca eventos finalizados do banco de esports"""
        conn = sqlite3.connect(self.esports_db_path)

        query = """
        SELECT 
            m.match_id,
            m.bet365_id,
            m.final_score,
            m.time_status,
            m.event_time,
            home.name as home_team,
            away.name as away_team,
            l.name as league_name
        FROM matches m
        JOIN teams home ON m.home_team_id = home.team_id
        JOIN teams away ON m.away_team_id = away.team_id
        JOIN leagues l ON m.league_id = l.league_id
        WHERE m.final_score IS NOT NULL 
        AND m.final_score != ''
        AND m.time_status = 3
        """

        df = pd.read_sql_query(query, conn)
        conn.close()

        finished_events = []
        for _, row in df.iterrows():
            event = {
                "match_id": row["match_id"],
                "event_id": row["bet365_id"],
                "final_score": row["final_score"],
                "event_time": row["event_time"],
                "home_team": row["home_team"],
                "away_team": row["away_team"],
                "league_name": row["league_name"],
            }
            finished_events.append(event)

        logger.info(f"Encontrados {len(finished_events)} eventos finalizados")
        return finished_events

    def get_backtest_bets(self) -> List[Dict]:
        """Busca todas as apostas do backtest (sem filtro de status)"""
        conn = sqlite3.connect(self.backtest_db_path)

        query = """
        SELECT 
            id,
            event_id,
            method,
            market_name,
            selection_line,
            handicap,
            house_odds,
            roi,
            ev,
            home_team,
            away_team,
            league_name,
            match_date
        FROM backtest_bets
        """

        df = pd.read_sql_query(query, conn)
        conn.close()

        bets = []
        for _, row in df.iterrows():
            bet = {
                "bet_id": row["id"],
                "event_id": row["event_id"],
                "method": row["method"],
                "market_name": row["market_name"],
                "selection_line": row["selection_line"],
                "handicap": row["handicap"],
                "house_odds": row["house_odds"],
                "roi": row["roi"],
                "ev": row["ev"],
                "home_team": row["home_team"],
                "away_team": row["away_team"],
                "league_name": row["league_name"],
                "match_date": row["match_date"],
            }
            bets.append(bet)

        logger.info(f"Encontradas {len(bets)} apostas no backtest")
        return bets

    def get_map_statistics(
        self, match_id: int, map_number: int = 1
    ) -> Dict[str, Tuple]:
        """Busca estatísticas do mapa específico de uma partida"""
        conn = sqlite3.connect(self.esports_db_path)

        map_query = """
        SELECT map_id 
        FROM game_maps 
        WHERE match_id = ? AND map_number = ?
        """
        map_cursor = conn.cursor()
        map_cursor.execute(map_query, (match_id, map_number))
        map_result = map_cursor.fetchone()

        if not map_result:
            conn.close()
            return {}

        map_id = map_result[0]

        stats_query = """
        SELECT stat_name, home_value, away_value
        FROM map_statistics
        WHERE map_id = ?
        """

        stats_cursor = conn.cursor()
        stats_cursor.execute(stats_query, (map_id,))
        stats_results = stats_cursor.fetchall()

        conn.close()

        statistics = {}
        for stat_name, home_value, away_value in stats_results:
            statistics[stat_name] = (home_value, away_value)

        return statistics

    def determine_map_number_from_market_name(self, market_name: str) -> int:
        """Determina o número do mapa com base no nome do mercado"""
        if not market_name:
            return 1

        match = re.search(r"Map\s+(\d+)", market_name)
        if match:
            return int(match.group(1))
        else:
            return 1

    def _extract_stat_name(self, selection: str) -> str:
        """Extrai o nome da estatística da seleção da aposta"""
        selection_lower = selection.lower()

        if "baron" in selection_lower:
            return "barons"
        elif "dragon" in selection_lower:
            return "dragons"
        elif "kill" in selection_lower:
            return "kills"
        elif "tower" in selection_lower:
            return "towers"
        elif "inhibitor" in selection_lower:
            return "inhibitors"
        elif "gold" in selection_lower:
            return "gold"
        else:
            return "kills"

    def _get_stat_value(self, map_stats: Dict, stat_name: str) -> Tuple[float, float]:
        """Obtém valores numéricos de uma estatística"""
        if stat_name not in map_stats:
            return (None, None)

        home_value, away_value = map_stats[stat_name]

        try:
            if isinstance(home_value, str) and "k" in home_value.lower():
                home_value = float(home_value.lower().replace("k", "")) * 1000
            else:
                home_value = float(home_value) if home_value else 0

            if isinstance(away_value, str) and "k" in away_value.lower():
                away_value = float(away_value.lower().replace("k", "")) * 1000
            else:
                away_value = float(away_value) if away_value else 0
        except (ValueError, TypeError):
            home_value, away_value = 0, 0

        return (home_value, away_value)

    def determine_bet_result(
        self, bet: Dict, event: Dict, map_stats: Dict, map_number: int
    ) -> Tuple[str, float, Optional[float]]:
        """
        Determina o resultado de uma aposta
        Retorna: (status, profit_loss, actual_value)
        """
        selection = bet["selection_line"]
        handicap = bet["handicap"]
        house_odds = bet.get("house_odds", 1.0)
        stake = 1.0  # Backtest usa stake unitário

        actual_value = None

        if "Over" in selection or "Under" in selection:
            stat_name = self._extract_stat_name(selection)
            home_value, away_value = self._get_stat_value(map_stats, stat_name)

            if home_value is None or away_value is None:
                return ("unknown", 0, None)

            total_value = home_value + away_value
            actual_value = total_value

            if "Over" in selection:
                if total_value > handicap:
                    return ("won", (house_odds - 1) * stake, actual_value)
                else:
                    return ("lost", -stake, actual_value)
            else:  # Under
                if total_value < handicap:
                    return ("won", (house_odds - 1) * stake, actual_value)
                else:
                    return ("lost", -stake, actual_value)

        return ("unknown", 0, None)

    def update_backtest_results(self):
        """Atualiza os resultados de todas as apostas do backtest"""
        try:
            # Adicionar colunas se não existirem
            self._add_result_columns()

            finished_events = self.get_finished_events_from_esports()
            backtest_bets = self.get_backtest_bets()

            if not finished_events or not backtest_bets:
                logger.info("Nada para atualizar")
                return

            events_dict = {event["event_id"]: event for event in finished_events}

            conn = sqlite3.connect(self.backtest_db_path)
            cursor = conn.cursor()

            updated_count = 0
            unknown_count = 0

            for bet in backtest_bets:
                event_id = bet["event_id"]

                if event_id not in events_dict:
                    continue

                event = events_dict[event_id]
                match_id = event["match_id"]

                map_number = self.determine_map_number_from_market_name(
                    bet["market_name"]
                )
                map_stats = self.get_map_statistics(match_id, map_number)

                if not map_stats:
                    continue

                bet_status, profit_loss, actual_value = self.determine_bet_result(
                    bet, event, map_stats, map_number
                )

                if bet_status == "unknown":
                    unknown_count += 1
                    continue

                update_query = """
                UPDATE backtest_bets 
                SET result_status = ?, 
                    profit_loss = ?, 
                    actual_value = ?
                WHERE id = ?
                """

                cursor.execute(
                    update_query,
                    (bet_status, round(profit_loss, 2), actual_value, bet["bet_id"]),
                )
                updated_count += 1

            conn.commit()
            conn.close()

            logger.info(f"✅ {updated_count} apostas atualizadas")
            logger.info(f"❓ {unknown_count} apostas desconhecidas")

        except Exception as e:
            logger.error(f"Erro durante atualização: {str(e)}")
            import traceback

            traceback.print_exc()

    def _add_result_columns(self):
        """Adiciona colunas de resultado se não existirem"""
        conn = sqlite3.connect(self.backtest_db_path)
        cursor = conn.cursor()

        try:
            cursor.execute("ALTER TABLE backtest_bets ADD COLUMN result_status TEXT")
        except sqlite3.OperationalError:
            pass

        try:
            cursor.execute("ALTER TABLE backtest_bets ADD COLUMN profit_loss REAL")
        except sqlite3.OperationalError:
            pass

        try:
            cursor.execute("ALTER TABLE backtest_bets ADD COLUMN actual_value REAL")
        except sqlite3.OperationalError:
            pass

        conn.commit()
        conn.close()

    def get_performance_by_method(self) -> pd.DataFrame:
        """Gera relatório de performance por método"""
        conn = sqlite3.connect(self.backtest_db_path)

        query = """
        SELECT 
            method,
            COUNT(*) as total_bets,
            SUM(CASE WHEN result_status = 'won' THEN 1 ELSE 0 END) as won_bets,
            SUM(CASE WHEN result_status = 'lost' THEN 1 ELSE 0 END) as lost_bets,
            SUM(CASE WHEN result_status IS NULL THEN 1 ELSE 0 END) as pending_bets,
            ROUND(AVG(CASE WHEN result_status IN ('won', 'lost') THEN house_odds END), 2) as avg_odds,
            ROUND(SUM(COALESCE(profit_loss, 0)), 2) as total_profit,
            ROUND(COUNT(*) * 1.0, 2) as total_stake,
            ROUND((SUM(COALESCE(profit_loss, 0)) / (COUNT(*) * 1.0)) * 100, 2) as roi_pct,
            ROUND((SUM(CASE WHEN result_status = 'won' THEN 1.0 ELSE 0 END) / 
                   NULLIF(SUM(CASE WHEN result_status IN ('won', 'lost') THEN 1 ELSE 0 END), 0)) * 100, 2) as win_rate_pct
        FROM backtest_bets
        GROUP BY method
        ORDER BY roi_pct DESC
        """

        df = pd.read_sql_query(query, conn)
        conn.close()

        return df


def main():
    BACKTEST_DB = "db_backtest.db"
    ESPORTS_DB = "../data/lol_esports.db"
    ODDS_DB = "../data/lol_odds.db"

    updater = BacktestResultsUpdater(BACKTEST_DB, ESPORTS_DB, ODDS_DB)

    print("🔄 Atualizando resultados do backtest...")
    updater.update_backtest_results()

    print("\n📊 PERFORMANCE POR MÉTODO:")
    print("=" * 100)

    df = updater.get_performance_by_method()
    print(df.to_string(index=False))

    print("\n" + "=" * 100)
    print("📈 ANÁLISE:")
    for _, row in df.iterrows():
        if row["win_rate_pct"] and row["roi_pct"]:
            print(f"\n🎯 {row['method'].upper()}")
            print(
                f"   Apostas: {int(row['total_bets'])} | Win Rate: {row['win_rate_pct']:.1f}% | ROI: {row['roi_pct']:.2f}%"
            )
            print(
                f"   Lucro: {row['total_profit']:.2f}u | Odds Média: {row['avg_odds']:.2f}"
            )


if __name__ == "__main__":
    main()
