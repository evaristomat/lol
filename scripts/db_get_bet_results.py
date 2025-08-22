import sqlite3
import pandas as pd
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import logging
import re

# Configurar logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("bet_results_updater")


class BetResultsUpdater:
    def __init__(self, bets_db_path: str, esports_db_path: str):
        self.bets_db_path = bets_db_path
        self.esports_db_path = esports_db_path

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

        logger.info(
            f"Encontrados {len(finished_events)} eventos finalizados no lol_esports.db"
        )
        return finished_events

    def get_pending_bets_from_bets_db(self) -> List[Dict]:
        """Busca apostas pendentes do banco de apostas"""
        conn = sqlite3.connect(self.bets_db_path)

        query = """
        SELECT 
            b.id as bet_id,
            b.event_id,
            b.market_name,
            b.selection_line,
            b.handicap,
            b.house_odds,
            b.stake,
            b.potential_win,
            b.actual_value,
            e.home_team,
            e.away_team,
            e.league_name,
            e.match_date
        FROM bets b
        JOIN events e ON b.event_id = e.event_id
        WHERE b.bet_status = 'pending'
        """

        df = pd.read_sql_query(query, conn)
        conn.close()

        pending_bets = []
        for _, row in df.iterrows():
            bet = {
                "bet_id": row["bet_id"],
                "event_id": row["event_id"],
                "market_name": row["market_name"],
                "selection_line": row["selection_line"],
                "handicap": row["handicap"],
                "house_odds": row["house_odds"],
                "stake": row["stake"] or 1.0,
                "potential_win": row["potential_win"] or 0.0,
                "actual_value": row["actual_value"],
                "home_team": row["home_team"],
                "away_team": row["away_team"],
                "league_name": row["league_name"],
                "match_date": row["match_date"],
            }
            pending_bets.append(bet)

        logger.info(f"Encontradas {len(pending_bets)} apostas pendentes no bets.db")
        return pending_bets

    def get_map_statistics(
        self, match_id: int, map_number: int = 1
    ) -> Dict[str, Tuple]:
        """Busca estatísticas do mapa específico de uma partida"""
        conn = sqlite3.connect(self.esports_db_path)

        # Primeiro, encontrar o map_id do mapa específico
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
            logger.warning(
                f"Mapa {map_number} não encontrado para a partida {match_id}"
            )
            return {}

        map_id = map_result[0]

        # Buscar todas as estatísticas deste mapa
        stats_query = """
        SELECT stat_name, home_value, away_value
        FROM map_statistics
        WHERE map_id = ?
        """

        stats_cursor = conn.cursor()
        stats_cursor.execute(stats_query, (map_id,))
        stats_results = stats_cursor.fetchall()

        conn.close()

        # Organizar as estatísticas em um dicionário
        statistics = {}
        for stat_name, home_value, away_value in stats_results:
            statistics[stat_name] = (home_value, away_value)

        return statistics

    def determine_map_number_from_market_name(self, market_name: str) -> int:
        """
        Determina o número do mapa com base no nome do mercado.
        Exemplo: "Map 1 - Totals" → 1, "Map 2 - Totals" → 2
        """
        if not market_name:
            return 1  # Default para Map 1

        # Procura por padrão "Map X" no market_name
        match = re.search(r"Map\s+(\d+)", market_name)
        if match:
            return int(match.group(1))
        else:
            return 1  # Default para Map 1

    def determine_bet_result(
        self, bet: Dict, event: Dict, map_stats: Dict, map_number: int
    ) -> Tuple[str, float, float, str]:
        """
        Determina o resultado de uma aposta com base nas estatísticas do mapa
        Retorna: (status, actual_win, actual_value, debug_info)
        """
        selection = bet["selection_line"]
        handicap = bet["handicap"]
        stake = bet.get("stake", 1.0)
        house_odds = bet.get("house_odds", 1.0)

        # Calcular ganho potencial
        potential_win = (house_odds - 1) * stake

        # Extrair informações do evento
        final_score = event.get("final_score", "0-0")

        # Iniciar informações de debug
        debug_info = f"🗺️  Mapa analisado: {map_number}\n"
        debug_info += f"📊 Aposta: {selection} {handicap} @ {house_odds:.2f}\n"
        debug_info += f"📋 Resultado do jogo: {final_score}\n"

        # Inicializar valor real
        actual_value = None

        # Lógica para diferentes tipos de apostas
        if "Home" in selection and (
            "Win" in selection or "winner" in selection.lower()
        ):
            # Aposta no time da casa vencer
            home_kills, away_kills = self._get_stat_value(map_stats, "kills")
            map_winner = (
                "home"
                if home_kills > away_kills
                else "away"
                if away_kills > home_kills
                else "draw"
            )

            debug_info += f"🎯 Tipo: Vitória do Home | Vencedor real: {map_winner}\n"
            debug_info += f"⚔️ Kills: {home_kills}-{away_kills}\n"

            if map_winner == "home":
                return (
                    "won",
                    potential_win,
                    actual_value,
                    debug_info + "✅ Resultado: WIN",
                )
            else:
                return ("lost", -stake, actual_value, debug_info + "❌ Resultado: LOSS")

        elif "Away" in selection and (
            "Win" in selection or "winner" in selection.lower()
        ):
            # Aposta no time visitante vencer
            home_kills, away_kills = self._get_stat_value(map_stats, "kills")
            map_winner = (
                "home"
                if home_kills > away_kills
                else "away"
                if away_kills > home_kills
                else "draw"
            )

            debug_info += f"🎯 Tipo: Vitória do Away | Vencedor real: {map_winner}\n"
            debug_info += f"⚔️ Kills: {home_kills}-{away_kills}\n"

            if map_winner == "away":
                return (
                    "won",
                    potential_win,
                    actual_value,
                    debug_info + "✅ Resultado: WIN",
                )
            else:
                return ("lost", -stake, actual_value, debug_info + "❌ Resultado: LOSS")

        elif "Over" in selection or "Under" in selection:
            # Apostas do tipo Over/Under
            stat_name = self._extract_stat_name(selection)
            home_value, away_value = self._get_stat_value(map_stats, stat_name)

            if home_value is None or away_value is None:
                debug_info += f"⚠️ Estatística '{stat_name}' não encontrada\n"
                return ("unknown", 0, 0, debug_info + "❓ Resultado: UNKNOWN")

            total_value = home_value + away_value
            actual_value = total_value  # Armazenar o valor real

            debug_info += f"🎯 Tipo: {'Over' if 'Over' in selection else 'Under'} {stat_name.capitalize()}\n"
            debug_info += f"📈 Valor real: {total_value:.1f} vs Handicap: {handicap}\n"
            debug_info += (
                f"🔢 Detalhamento: {home_value:.1f} (Home) + {away_value:.1f} (Away)\n"
            )

            if "Over" in selection:
                if total_value > handicap:
                    return (
                        "won",
                        potential_win,
                        actual_value,
                        debug_info + "✅ Resultado: WIN",
                    )
                else:
                    return (
                        "lost",
                        -stake,
                        actual_value,
                        debug_info + "❌ Resultado: LOSS",
                    )
            else:  # Under
                if total_value < handicap:
                    return (
                        "won",
                        potential_win,
                        actual_value,
                        debug_info + "✅ Resultado: WIN",
                    )
                else:
                    return (
                        "lost",
                        -stake,
                        actual_value,
                        debug_info + "❌ Resultado: LOSS",
                    )

        # Se não reconhecer o tipo de aposta, marca como desconhecido
        debug_info += f"⚠️ Tipo de aposta não reconhecido\n"
        return ("unknown", 0, 0, debug_info + "❓ Resultado: UNKNOWN")

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
            return "kills"  # Default para kills

    def _get_stat_value(self, map_stats: Dict, stat_name: str) -> Tuple[float, float]:
        """Obtém valores numéricos de uma estatística, convertendo se necessário"""
        if stat_name not in map_stats:
            return (None, None)

        home_value, away_value = map_stats[stat_name]

        # Converter para números se possível
        try:
            # Para valores como "49.8k", converter para 49800
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

    def update_bet_results(self):
        """Atualiza os resultados de todas as apostas pendentes"""
        try:
            # Buscar eventos finalizados e apostas pendentes
            finished_events = self.get_finished_events_from_esports()
            pending_bets = self.get_pending_bets_from_bets_db()

            if not finished_events or not pending_bets:
                logger.info("Nada para atualizar")
                return

            # Criar dicionário de eventos para acesso rápido
            events_dict = {event["event_id"]: event for event in finished_events}

            # Conectar ao banco de apostas
            bets_conn = sqlite3.connect(self.bets_db_path)
            bets_cursor = bets_conn.cursor()

            updated_count = 0

            for bet in pending_bets:
                event_id = bet["event_id"]

                if event_id not in events_dict:
                    continue

                event = events_dict[event_id]
                match_id = event["match_id"]

                # Determinar o número do mapa para esta aposta
                map_number = self.determine_map_number_from_market_name(
                    bet["market_name"]
                )

                # Buscar estatísticas do mapa específico
                map_stats = self.get_map_statistics(match_id, map_number)

                if not map_stats:
                    logger.warning(
                        f"Estatísticas não encontradas para o evento {event_id}, mapa {map_number}"
                    )
                    continue

                # Determinar resultado da aposta
                bet_status, actual_win, actual_value, debug_info = (
                    self.determine_bet_result(bet, event, map_stats, map_number)
                )

                if bet_status == "unknown":
                    logger.warning(
                        f"Tipo de aposta desconhecido: {bet['selection_line']}"
                    )
                    continue

                # Formatar o valor com 2 casas decimais
                formatted_win = round(actual_win, 2)

                # Mostrar informações de debug
                print(f"\n🔍 ANALISANDO APOSTA #{bet['bet_id']}")
                print(f"🏆 {event['home_team']} vs {event['away_team']}")
                print(f"📅 {event['event_time']} | {event['league_name']}")
                print(debug_info)
                if actual_value is not None:
                    print(f"📊 Valor real: {actual_value:.1f}")
                print(f"💰 Resultado financeiro: {formatted_win:.2f}")
                print("─" * 50)

                # Atualizar aposta no banco
                update_query = """
                UPDATE bets 
                SET bet_status = ?, actual_win = ?, actual_value = ?, result_verified = TRUE, updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """

                bets_cursor.execute(
                    update_query,
                    (bet_status, formatted_win, actual_value, bet["bet_id"]),
                )
                updated_count += 1

                logger.info(
                    f"Aposta {bet['bet_id']} atualizada: {bet_status} ({formatted_win:.2f})"
                )

            # Atualizar status dos eventos
            for event_id in events_dict:
                update_event_query = """
                UPDATE events 
                SET status = 'finished', updated_at = CURRENT_TIMESTAMP
                WHERE event_id = ?
                """
                bets_cursor.execute(update_event_query, (event_id,))

            bets_conn.commit()
            bets_conn.close()

            logger.info(f"Atualização concluída: {updated_count} apostas atualizadas")

        except Exception as e:
            logger.error(f"Erro durante a atualização: {str(e)}")
            import traceback

            traceback.print_exc()

    def get_performance_report(self) -> Dict:
        """Geras relatório de performance das apostas"""
        try:
            conn = sqlite3.connect(self.bets_db_path)

            query = """
            SELECT 
                COUNT(*) as total_bets,
                SUM(CASE WHEN bet_status = 'won' THEN 1 ELSE 0 END) as won_bets,
                SUM(CASE WHEN bet_status = 'lost' THEN 1 ELSE 0 END) as lost_bets,
                SUM(CASE WHEN bet_status = 'pending' THEN 1 ELSE 0 END) as pending_bets,
                SUM(COALESCE(actual_win, 0)) as total_profit_loss,
                SUM(COALESCE(stake, 0)) as total_stake,
                AVG(CASE WHEN bet_status != 'pending' THEN house_odds ELSE NULL END) as avg_odds
            FROM bets
            """

            df = pd.read_sql_query(query, conn)
            conn.close()

            if df.empty:
                return {}

            stats = df.iloc[0].to_dict()

            # Calcular métricas adicionais
            settled_bets = stats["won_bets"] + stats["lost_bets"]
            if settled_bets > 0:
                stats["win_rate"] = (stats["won_bets"] / settled_bets) * 100
            else:
                stats["win_rate"] = 0

            if stats["total_stake"] > 0:
                stats["roi"] = (stats["total_profit_loss"] / stats["total_stake"]) * 100
            else:
                stats["roi"] = 0

            # Garantir que avg_odds não seja None
            if stats["avg_odds"] is None:
                stats["avg_odds"] = 0

            # Arredondar valores para 2 casas decimais
            stats["total_profit_loss"] = round(stats["total_profit_loss"], 2)
            stats["total_stake"] = round(stats["total_stake"], 2)
            stats["avg_odds"] = round(stats["avg_odds"], 2)

            return stats

        except Exception as e:
            logger.error(f"Erro ao gerar relatório: {str(e)}")
            return {}


# Função principal
def main():
    # Configurar caminhos dos bancos de dados
    BETS_DB_PATH = "../data/bets.db"
    ESPORTS_DB_PATH = "../data/lol_esports.db"

    # Criar instância do atualizador
    updater = BetResultsUpdater(BETS_DB_PATH, ESPORTS_DB_PATH)

    # Executar atualização
    print("🔄 Iniciando atualização de resultados...")
    updater.update_bet_results()

    # Gerar relatório de performance
    print("\n📊 Relatório de Performance:")
    stats = updater.get_performance_report()

    if stats:
        print(f"   Total de Apostas: {int(stats['total_bets'])}")
        print(f"   Apostas Ganhas: {int(stats['won_bets'])}")
        print(f"   Apostas Perdidas: {int(stats['lost_bets'])}")
        print(f"   Apostas Pendentes: {int(stats['pending_bets'])}")
        print(f"   Taxa de Acerto: {stats['win_rate']:.1f}%")
        print(f"   Stake Total: {stats['total_stake']:.2f}")
        print(f"   Lucro/Prejuízo: {stats['total_profit_loss']:.2f}")
        print(f"   ROI: {stats['roi']:.1f}%")
        print(f"   Odds Média: {stats['avg_odds']:.2f}")
    else:
        print("   Nenhum dado disponível para relatório")


if __name__ == "__main__":
    main()