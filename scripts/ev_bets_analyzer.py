import sqlite3
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
import statistics
from team_stats_analyzer import TeamStatsAnalyzer, TeamGameStats


@dataclass
class BetLine:
    """Linha de aposta disponível"""

    market_name: str
    selection_name: str
    odds_value: float
    handicap: Optional[float] = None

    # Campos derivados para análise
    market_type: str = ""  # "kills", "dragons", etc.
    bet_type: str = ""  # "over", "under"
    threshold: float = 0.0  # valor da linha


@dataclass
class MatchupOdds:
    """Odds de um confronto específico"""

    event_id: str
    home_team: str
    away_team: str
    league: str
    match_date: str

    # Odds organizadas por mercado
    total_kills_lines: List[BetLine]
    total_dragons_lines: List[BetLine]
    total_towers_lines: List[BetLine]
    total_inhibitors_lines: List[BetLine]
    total_barons_lines: List[BetLine]


@dataclass
class HistoricalBetResult:
    """Resultado de uma aposta simulada em jogo histórico"""

    game_stats: TeamGameStats
    bet_line: BetLine
    actual_value: float  # valor real que aconteceu no jogo
    won: bool  # True se ganhou a aposta
    profit: float  # lucro/prejuízo (-1 a odds-1)


@dataclass
class SimulatedROI:
    """ROI simulado baseado em jogos históricos"""

    market_type: str
    bet_type: str
    threshold: float
    odds: float

    # Resultados da simulação
    games_analyzed: int
    wins: int
    losses: int
    win_rate: float

    # Financeiro
    total_invested: float  # sempre = games_analyzed (1 unit por jogo)
    total_returned: float  # soma dos retornos
    net_profit: float  # lucro líquido
    roi_percentage: float  # ROI em %

    # Detalhes
    results: List[HistoricalBetResult]


class EVBetAnalyzer:
    """
    Analisa apostas EV+ integrando dados históricos com odds disponíveis
    """

    def __init__(self, odds_db_path="../data/lol_odds.db"):
        self.odds_db_path = Path(odds_db_path)
        self.team_analyzer = TeamStatsAnalyzer()

    def get_future_match(
        self, event_id: str = None
    ) -> Optional[Tuple[str, str, str, str, str]]:
        """
        Busca um jogo futuro específico ou o próximo disponível
        Retorna: (event_id, home_team, away_team, league, match_date)
        """
        try:
            with sqlite3.connect(self.odds_db_path) as conn:
                cursor = conn.cursor()

                if event_id:
                    # Busca jogo específico
                    cursor.execute(
                        """
                        SELECT event_id, home_team, away_team, league_name, match_date
                        FROM events 
                        WHERE event_id = ?
                    """,
                        (event_id,),
                    )
                else:
                    # Busca próximo jogo futuro
                    cursor.execute("""
                        SELECT event_id, home_team, away_team, league_name, match_date
                        FROM events 
                        WHERE match_date > datetime('now')
                        ORDER BY match_date ASC
                        LIMIT 1
                    """)

                result = cursor.fetchone()
                if result:
                    return result
                else:
                    print("❌ Nenhum jogo futuro encontrado")
                    return None

        except Exception as e:
            print(f"❌ Erro ao buscar jogo: {e}")
            return None

    def extract_line_info(
        self, market_name: str, selection_name: str, handicap: Optional[str]
    ) -> Tuple[str, str, float]:
        """
        Extrai informações da linha de aposta baseado na estrutura real do banco
        Retorna: (market_type, bet_type, threshold)
        """
        market_type = ""
        bet_type = ""
        threshold = 0.0

        # Identificar tipo de mercado baseado no market_name
        market_lower = market_name.lower()
        selection_lower = selection_name.lower()

        # Verificar se é um mercado de totals
        if "totals" in market_lower:
            # Identificar o tipo específico pelo selection_name
            if "kill" in selection_lower:
                market_type = "kills"
            elif "dragon" in selection_lower:
                market_type = "dragons"
            elif "tower" in selection_lower or "turret" in selection_lower:
                market_type = "towers"
            elif "inhibitor" in selection_lower:
                market_type = "inhibitors"
            elif "baron" in selection_lower:
                market_type = "barons"
            else:
                return market_type, bet_type, threshold

            # Extrair over/under e threshold do handicap
            if handicap:
                handicap_lower = handicap.lower()
                if handicap_lower.startswith("o "):  # Over
                    bet_type = "over"
                    try:
                        threshold = float(handicap_lower.replace("o ", ""))
                    except:
                        threshold = 0.0
                elif handicap_lower.startswith("u "):  # Under
                    bet_type = "under"
                    try:
                        threshold = float(handicap_lower.replace("u ", ""))
                    except:
                        threshold = 0.0

        return market_type, bet_type, threshold

    def get_match_odds(self, event_id: str) -> Optional[MatchupOdds]:
        """Busca todas as odds relevantes de um confronto baseado na estrutura real"""

        try:
            with sqlite3.connect(self.odds_db_path) as conn:
                cursor = conn.cursor()

                # Buscar informações básicas do evento
                cursor.execute(
                    """
                    SELECT event_id, home_team, away_team, league_name, match_date
                    FROM events 
                    WHERE event_id = ?
                """,
                    (event_id,),
                )

                event_info = cursor.fetchone()
                if not event_info:
                    print(f"❌ Evento {event_id} não encontrado")
                    return None

                event_id, home_team, away_team, league, match_date = event_info

                # Buscar odds dos mercados de totals (tanto Match Totals quanto Map X - Totals)
                cursor.execute(
                    """
                    SELECT market_name, selection_name, odds_value, handicap
                    FROM current_odds 
                    WHERE event_id = ? 
                    AND (
                        market_name LIKE '%Totals' OR
                        market_name = 'Match Totals'
                    )
                    AND (
                        selection_name LIKE '%Kill%' OR
                        selection_name LIKE '%Dragon%' OR
                        selection_name LIKE '%Tower%' OR
                        selection_name LIKE '%Turret%' OR
                        selection_name LIKE '%Inhibitor%' OR
                        selection_name LIKE '%Baron%'
                    )
                    ORDER BY market_name, selection_name
                """,
                    (event_id,),
                )

                odds_data = cursor.fetchall()

                if not odds_data:
                    print(f"⚠️ Nenhuma odd de totals encontrada para evento {event_id}")
                    return None

                # Organizar odds por mercado
                total_kills_lines = []
                total_dragons_lines = []
                total_towers_lines = []
                total_inhibitors_lines = []
                total_barons_lines = []

                print(f"📊 ODDS ENCONTRADAS PARA {home_team} vs {away_team}:")
                print("-" * 60)

                for market_name, selection_name, odds_value, handicap in odds_data:
                    market_type, bet_type, threshold = self.extract_line_info(
                        market_name, selection_name, handicap
                    )

                    if not market_type or not bet_type or threshold == 0.0:
                        continue

                    bet_line = BetLine(
                        market_name=market_name,
                        selection_name=selection_name,
                        odds_value=odds_value,
                        handicap=handicap,
                        market_type=market_type,
                        bet_type=bet_type,
                        threshold=threshold,
                    )

                    # Organizar por mercado
                    if market_type == "kills":
                        total_kills_lines.append(bet_line)
                    elif market_type == "dragons":
                        total_dragons_lines.append(bet_line)
                    elif market_type == "towers":
                        total_towers_lines.append(bet_line)
                    elif market_type == "inhibitors":
                        total_inhibitors_lines.append(bet_line)
                    elif market_type == "barons":
                        total_barons_lines.append(bet_line)

                    print(
                        f"   {market_type.upper()} {bet_type} {threshold} @{odds_value} ({market_name})"
                    )

                return MatchupOdds(
                    event_id=event_id,
                    home_team=home_team,
                    away_team=away_team,
                    league=league,
                    match_date=match_date,
                    total_kills_lines=total_kills_lines,
                    total_dragons_lines=total_dragons_lines,
                    total_towers_lines=total_towers_lines,
                    total_inhibitors_lines=total_inhibitors_lines,
                    total_barons_lines=total_barons_lines,
                )

        except Exception as e:
            print(f"❌ Erro ao buscar odds: {e}")
            return None

    def create_mock_odds(
        self, predictions: Dict[str, float], home_team: str, away_team: str
    ) -> MatchupOdds:
        """Cria odds simuladas para teste baseadas nas previsões"""

        mock_kills_lines = []
        mock_dragons_lines = []
        mock_towers_lines = []

        # Kills - criar linhas próximas à previsão
        kills_pred = predictions["kills"]
        mock_kills_lines.extend(
            [
                BetLine(
                    "Total Kills",
                    f"Over {kills_pred - 5:.1f}",
                    1.90,
                    kills_pred - 5,
                    "kills",
                    "over",
                    kills_pred - 5,
                ),
                BetLine(
                    "Total Kills",
                    f"Under {kills_pred - 5:.1f}",
                    1.90,
                    kills_pred - 5,
                    "kills",
                    "under",
                    kills_pred - 5,
                ),
                BetLine(
                    "Total Kills",
                    f"Over {kills_pred:.1f}",
                    2.00,
                    kills_pred,
                    "kills",
                    "over",
                    kills_pred,
                ),
                BetLine(
                    "Total Kills",
                    f"Under {kills_pred:.1f}",
                    1.80,
                    kills_pred,
                    "kills",
                    "under",
                    kills_pred,
                ),
            ]
        )

        # Dragons
        dragons_pred = predictions["dragons"]
        mock_dragons_lines.extend(
            [
                BetLine(
                    "Total Dragons",
                    f"Over {dragons_pred - 0.5:.1f}",
                    1.85,
                    dragons_pred - 0.5,
                    "dragons",
                    "over",
                    dragons_pred - 0.5,
                ),
                BetLine(
                    "Total Dragons",
                    f"Under {dragons_pred - 0.5:.1f}",
                    1.95,
                    dragons_pred - 0.5,
                    "dragons",
                    "under",
                    dragons_pred - 0.5,
                ),
            ]
        )

        # Torres
        towers_pred = predictions["towers"]
        mock_towers_lines.extend(
            [
                BetLine(
                    "Total Towers",
                    f"Over {towers_pred - 1:.1f}",
                    1.90,
                    towers_pred - 1,
                    "towers",
                    "over",
                    towers_pred - 1,
                ),
                BetLine(
                    "Total Towers",
                    f"Under {towers_pred - 1:.1f}",
                    1.90,
                    towers_pred - 1,
                    "towers",
                    "under",
                    towers_pred - 1,
                ),
            ]
        )

        print(f"🎭 CRIANDO ODDS SIMULADAS PARA TESTE:")
        all_lines = mock_kills_lines + mock_dragons_lines + mock_towers_lines
        for line in all_lines:
            print(
                f"   {line.market_type.upper()} {line.bet_type} {line.threshold:.1f}: @{line.odds_value}"
            )

        return MatchupOdds(
            event_id="mock",
            home_team=home_team,
            away_team=away_team,
            league="Mock League",
            match_date="2025-01-01",
            total_kills_lines=mock_kills_lines,
            total_dragons_lines=mock_dragons_lines,
            total_towers_lines=mock_towers_lines,
            total_inhibitors_lines=[],
            total_barons_lines=[],
        )

    def get_actual_value_from_game(
        self, game_stats: TeamGameStats, market_type: str
    ) -> float:
        """Obtém o valor real que aconteceu no jogo para um mercado específico"""
        if market_type == "kills":
            return game_stats.series_total_kills
        elif market_type == "dragons":
            return game_stats.series_total_dragons
        elif market_type == "towers":
            return game_stats.series_total_towers
        elif market_type == "inhibitors":
            return game_stats.series_total_inhibitors
        elif market_type == "barons":
            return game_stats.series_total_barons
        else:
            return 0.0

    def simulate_bet_on_historical_games(
        self, games: List[TeamGameStats], bet_line: BetLine
    ) -> SimulatedROI:
        """Simula uma aposta nos jogos históricos - como você explicou dos 40% ROI"""

        results = []
        total_invested = 0
        total_returned = 0
        wins = 0

        for game in games:
            # Valor real que aconteceu no jogo
            actual_value = self.get_actual_value_from_game(game, bet_line.market_type)

            # Verifica se a aposta ganhou
            if bet_line.bet_type == "over":
                won = actual_value > bet_line.threshold
            else:  # under
                won = actual_value < bet_line.threshold

            # Calcula lucro/prejuízo
            total_invested += 1  # 1 unidade por jogo
            if won:
                profit = bet_line.odds_value - 1  # odds - stake
                total_returned += bet_line.odds_value
                wins += 1
            else:
                profit = -1  # perde a stake
                total_returned += 0

            result = HistoricalBetResult(
                game_stats=game,
                bet_line=bet_line,
                actual_value=actual_value,
                won=won,
                profit=profit,
            )
            results.append(result)

        # Calcula métricas finais
        games_analyzed = len(games)
        losses = games_analyzed - wins
        win_rate = wins / games_analyzed if games_analyzed > 0 else 0
        net_profit = total_returned - total_invested
        roi_percentage = (
            (net_profit / total_invested * 100) if total_invested > 0 else 0
        )

        return SimulatedROI(
            market_type=bet_line.market_type,
            bet_type=bet_line.bet_type,
            threshold=bet_line.threshold,
            odds=bet_line.odds_value,
            games_analyzed=games_analyzed,
            wins=wins,
            losses=losses,
            win_rate=win_rate,
            total_invested=total_invested,
            total_returned=total_returned,
            net_profit=net_profit,
            roi_percentage=roi_percentage,
            results=results,
        )

    def analyze_matchup_ev(self, event_id: str = None):
        """Análise completa de EV+ para um confronto"""

        print("🔍 INICIANDO ANÁLISE DE EV+")
        print("=" * 70)

        # 1. Buscar jogo futuro
        if event_id:
            match_info = self.get_future_match(event_id)
        else:
            match_info = self.get_future_match()

        if not match_info:
            return None

        event_id, home_team, away_team, league, match_date = match_info

        print(f"🎮 CONFRONTO SELECIONADO:")
        print(f"   Event ID: {event_id}")
        print(f"   Teams: {home_team} vs {away_team}")
        print(f"   Liga: {league}")
        print(f"   Data: {match_date}")

        # 2. Analisar estatísticas dos times
        print(f"\n📊 ANALISANDO ESTATÍSTICAS DOS TIMES...")

        home_stats = self.team_analyzer.get_team_stats(home_team, limit=10)
        away_stats = self.team_analyzer.get_team_stats(away_team, limit=10)

        if home_stats.games_found == 0 or away_stats.games_found == 0:
            print("❌ Dados históricos insuficientes")
            return None

        print(f"\n📈 HOME - {home_team}:")
        print(
            f"   🎯 Últimos {home_stats.games_found} jogos | Win Rate: {home_stats.win_rate:.1%}"
        )
        print(f"   ⚔️  Kills por mapa: {home_stats.avg_map_kills_general:.1f}")
        print(f"   🐉 Dragons por mapa: {home_stats.avg_map_dragons_general:.1f}")
        print(f"   🏗️  Torres por mapa: {home_stats.avg_map_towers_general:.1f}")
        print(f"   🛡️  Inibidores por mapa: {home_stats.avg_map_inhibitors_general:.1f}")
        print(f"   👹 Barões por mapa: {home_stats.avg_map_barons_general:.1f}")

        print(f"\n📈 AWAY - {away_team}:")
        print(
            f"   🎯 Últimos {away_stats.games_found} jogos | Win Rate: {away_stats.win_rate:.1%}"
        )
        print(f"   ⚔️  Kills por mapa: {away_stats.avg_map_kills_general:.1f}")
        print(f"   🐉 Dragons por mapa: {away_stats.avg_map_dragons_general:.1f}")
        print(f"   🏗️  Torres por mapa: {away_stats.avg_map_towers_general:.1f}")
        print(f"   🛡️  Inibidores por mapa: {away_stats.avg_map_inhibitors_general:.1f}")
        print(f"   👹 Barões por mapa: {away_stats.avg_map_barons_general:.1f}")

        # 3. Calcular previsões combinadas
        print(f"\n🎯 PREVISÕES PARA O CONFRONTO:")
        print("-" * 50)

        predicted_kills = (
            home_stats.avg_map_kills_general + away_stats.avg_map_kills_general
        ) / 2
        predicted_dragons = (
            home_stats.avg_map_dragons_general + away_stats.avg_map_dragons_general
        ) / 2
        predicted_towers = (
            home_stats.avg_map_towers_general + away_stats.avg_map_towers_general
        ) / 2
        predicted_inhibitors = (
            home_stats.avg_map_inhibitors_general
            + away_stats.avg_map_inhibitors_general
        ) / 2
        predicted_barons = (
            home_stats.avg_map_barons_general + away_stats.avg_map_barons_general
        ) / 2

        predictions = {
            "kills": predicted_kills,
            "dragons": predicted_dragons,
            "towers": predicted_towers,
            "inhibitors": predicted_inhibitors,
            "barons": predicted_barons,
        }

        for market, value in predictions.items():
            print(f"   {market.upper()}: {value:.1f} por mapa")

        # 4. Buscar odds disponíveis
        print(f"\n🎯 BUSCANDO ODDS DISPONÍVEIS...")
        matchup_odds = self.get_match_odds(event_id)

        # Se não encontrou odds, criar simuladas para teste
        if not matchup_odds or not any(
            [
                matchup_odds.total_kills_lines,
                matchup_odds.total_dragons_lines,
                matchup_odds.total_towers_lines,
                matchup_odds.total_inhibitors_lines,
                matchup_odds.total_barons_lines,
            ]
        ):
            print(
                f"⚠️  Nenhuma odd de totals encontrada, usando odds simuladas para teste..."
            )
            matchup_odds = self.create_mock_odds(predictions, home_team, away_team)

        # 5. Simular ROI histórico
        print(f"\n🎲 SIMULAÇÃO DE ROI HISTÓRICO:")
        print("-" * 50)

        # Combinar jogos históricos dos dois times
        all_historical_games = []
        if home_stats.recent_games:
            all_historical_games.extend(home_stats.recent_games)
        if away_stats.recent_games:
            all_historical_games.extend(away_stats.recent_games)

        # Ordenar por data (mais recentes primeiro) e pegar últimos 20
        all_historical_games.sort(key=lambda x: x.date, reverse=True)
        simulation_games = all_historical_games[:20]

        print(f"📊 Simulando com {len(simulation_games)} jogos históricos combinados")

        # Simular cada linha de aposta
        best_ev_bets = []
        all_simulations = []

        # Testar todas as linhas disponíveis
        all_bet_lines = (
            matchup_odds.total_kills_lines
            + matchup_odds.total_dragons_lines
            + matchup_odds.total_towers_lines
            + matchup_odds.total_inhibitors_lines
            + matchup_odds.total_barons_lines
        )

        for bet_line in all_bet_lines:
            if bet_line.threshold > 0:  # Só simular linhas válidas
                roi_sim = self.simulate_bet_on_historical_games(
                    simulation_games, bet_line
                )
                all_simulations.append(roi_sim)

                # Filtrar apostas EV+ (ROI > 10%)
                if roi_sim.roi_percentage >= 10.0:
                    best_ev_bets.append(roi_sim)

        # Ordenar por ROI e mostrar resultados
        all_simulations.sort(key=lambda x: x.roi_percentage, reverse=True)

        print(f"\n📈 RESULTADOS DA SIMULAÇÃO:")
        print(f"   Total de linhas testadas: {len(all_simulations)}")
        print(f"   Apostas com ROI > 10%: {len(best_ev_bets)}")

        if all_simulations:
            print(f"\n🎯 TOP 10 LINHAS (ordenadas por ROI):")
            for i, sim in enumerate(all_simulations[:10], 1):
                status = "✅" if sim.roi_percentage >= 10.0 else "❌"
                print(
                    f"   {i:2d}. {status} {sim.market_type.upper()} {sim.bet_type} {sim.threshold:.1f} @{sim.odds:.2f}"
                )
                print(
                    f"       {sim.wins}W-{sim.losses}L ({sim.win_rate:.1%}) | ROI: {sim.roi_percentage:+.1f}% | Profit: {sim.net_profit:+.1f}u"
                )

        if best_ev_bets:
            print(f"\n🏆 MELHORES APOSTAS EV+ (ROI ≥ 10%):")
            best_ev_bets.sort(key=lambda x: x.roi_percentage, reverse=True)
            for i, bet in enumerate(best_ev_bets[:5], 1):
                print(
                    f"   {i}. {bet.market_type.upper()} {bet.bet_type} {bet.threshold:.1f} @{bet.odds:.2f}"
                )
                print(
                    f"      ROI: {bet.roi_percentage:+.1f}% | Win Rate: {bet.win_rate:.1%} ({bet.wins}/{bet.games_analyzed})"
                )
                print(
                    f"      Investimento: {bet.total_invested:.0f}u | Retorno: {bet.total_returned:.0f}u | Lucro: {bet.net_profit:+.1f}u"
                )
                print()
        else:
            print(f"   ❌ Nenhuma aposta com EV+ suficiente identificada")

        print(f"\n✅ ANÁLISE EV+ COMPLETA!")

        return {
            "matchup_odds": matchup_odds,
            "home_stats": home_stats,
            "away_stats": away_stats,
            "predictions": predictions,
            "all_simulations": all_simulations,
            "best_ev_bets": best_ev_bets,
        }


# Teste
if __name__ == "__main__":
    analyzer = EVBetAnalyzer()

    # Analisar jogo específico (substitua pelo event_id desejado)
    result = analyzer.analyze_matchup_ev("179715705")

    if result:
        print(f"\n🎯 Dados coletados com sucesso!")
    else:
        print(f"\n❌ Falha na análise")
