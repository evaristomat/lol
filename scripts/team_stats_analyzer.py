import sqlite3
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
import statistics

# Variáveis globais configuráveis
MAX_SERIES = 5  # Número de séries para análise por série
MAX_MAPS = 15  # Número de mapas para análise geral


@dataclass
class TeamGameStats:
    """Estatísticas de um time em um jogo"""

    match_id: int
    team_name: str
    opponent: str
    date: str
    league: str
    result: str  # 'W' ou 'L'
    maps_won: int
    maps_lost: int
    total_maps: int

    # Stats totais da série (soma de todos os mapas)
    series_total_kills: int = 0
    series_total_dragons: int = 0
    series_total_towers: int = 0
    series_total_inhibitors: int = 0
    series_total_barons: int = 0

    # Stats por mapa individual DESTA SÉRIE (média dos mapas da série)
    avg_kills_per_map: float = 0.0
    avg_dragons_per_map: float = 0.0
    avg_towers_per_map: float = 0.0
    avg_inhibitors_per_map: float = 0.0
    avg_barons_per_map: float = 0.0


@dataclass
class TeamStatsAnalysis:
    """Análise estatística de um time"""

    team_name: str
    games_found: int
    wins: int
    losses: int
    win_rate: float

    # Médias por SÉRIE (jogos completos) - últimas MAX_SERIES séries
    avg_series_kills: float = 0.0
    avg_series_dragons: float = 0.0
    avg_series_towers: float = 0.0
    avg_series_inhibitors: float = 0.0
    avg_series_barons: float = 0.0

    # Médias por mapa POR SÉRIE (média das médias de cada série) - últimas MAX_SERIES séries
    avg_map_kills_per_series: float = 0.0
    avg_map_dragons_per_series: float = 0.0
    avg_map_towers_per_series: float = 0.0
    avg_map_inhibitors_per_series: float = 0.0
    avg_map_barons_per_series: float = 0.0

    # Médias por mapa GERAL (últimos MAX_MAPAS mapas jogados)
    avg_map_kills_general: float = 0.0
    avg_map_dragons_general: float = 0.0
    avg_map_towers_general: float = 0.0
    avg_map_inhibitors_general: float = 0.0
    avg_map_barons_general: float = 0.0

    # Últimos jogos
    recent_games: List[TeamGameStats] = None


class TeamStatsAnalyzer:
    """
    Analisa estatísticas históricas dos times usando o banco lol_esports.db
    """

    def __init__(self):
        self.db_path = Path(__file__).parent.parent / "data" / "lol_esports.db"

    def get_team_name_by_id(self, team_id: int) -> str:
        """Obtém o nome do time pelo ID"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()

                cursor.execute("SELECT name FROM teams WHERE team_id = ?", (team_id,))
                team = cursor.fetchone()

                return team["name"] if team else f"Time_{team_id}"

        except Exception as e:
            print(f"❌ Erro ao buscar nome do time {team_id}: {e}")
            return f"Time_{team_id}"

    def get_team_stats_by_id(self, team_id: int) -> TeamStatsAnalysis:
        """Busca estatísticas de um time pelos últimos jogos usando team_id"""
        if not self.db_path.exists():
            print(f"❌ Banco de resultados não encontrado: {self.db_path}")
            return self._create_empty_stats(self.get_team_name_by_id(team_id))

        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()

                # Primeiro, obter o nome do time
                team_name = self.get_team_name_by_id(team_id)
                print(f"✅ Analisando time: {team_name} (ID: {team_id})")

                # Buscar jogos do time (tanto como home quanto away)
                query = """
                    SELECT 
                        m.match_id, m.event_time, m.final_score, m.time_status,
                        ht.team_id as home_team_id, ht.name as home_team_name,
                        at.team_id as away_team_id, at.name as away_team_name,
                        l.name as league_name
                    FROM matches m
                    JOIN teams ht ON m.home_team_id = ht.team_id
                    JOIN teams at ON m.away_team_id = at.team_id
                    JOIN leagues l ON m.league_id = l.league_id
                    WHERE (m.home_team_id = ? OR m.away_team_id = ?)
                    AND m.time_status = 3
                    AND m.final_score IS NOT NULL
                    ORDER BY m.event_time DESC
                """

                cursor.execute(query, (team_id, team_id))
                matches = cursor.fetchall()

                if not matches:
                    print(f"⚠️  Nenhum jogo encontrado para {team_name}")
                    return self._create_empty_stats(team_name)

                # Processar jogos para estatísticas por série (últimas MAX_SERIES séries)
                series_games = []
                for match in matches[:MAX_SERIES]:
                    is_home = team_id == match["home_team_id"]
                    opponent_id = (
                        match["away_team_id"] if is_home else match["home_team_id"]
                    )
                    opponent_name = self.get_team_name_by_id(opponent_id)

                    game_stats = self._process_match_stats(
                        match["match_id"],
                        team_name,
                        opponent_name,
                        match["event_time"],
                        match["league_name"],
                        match["final_score"],
                        is_home,
                        cursor,
                    )

                    if game_stats:
                        series_games.append(game_stats)

                # Processar mapas para estatísticas gerais (últimos MAX_MAPS mapas)
                map_stats = []
                maps_processed = 0

                for match in matches:
                    if maps_processed >= MAX_MAPS:
                        break

                    is_home = team_id == match["home_team_id"]
                    map_data = self._get_map_stats(
                        match["match_id"],
                        is_home,
                        cursor,
                    )

                    for stats in map_data:
                        if maps_processed >= MAX_MAPS:
                            break
                        map_stats.append(stats)
                        maps_processed += 1

                # Calcular estatísticas agregadas
                return self._calculate_team_analysis(team_name, series_games, map_stats)

        except Exception as e:
            print(f"❌ Erro ao buscar estatísticas do time ID {team_id}: {e}")
            import traceback

            traceback.print_exc()
            return self._create_empty_stats(self.get_team_name_by_id(team_id))

    def _process_match_stats(
        self,
        match_id: int,
        team_name: str,
        opponent: str,
        event_time: str,
        league: str,
        final_score: str,
        is_home: bool,
        cursor,
    ) -> Optional[TeamGameStats]:
        """Processa estatísticas de um jogo específico para análise por série"""

        try:
            # Extrair resultado básico do final_score
            if final_score and "-" in final_score:
                scores = final_score.split("-")
                if len(scores) == 2:
                    home_score = int(scores[0])
                    away_score = int(scores[1])

                    # Determinar vitória/derrota
                    if is_home:
                        maps_won = home_score
                        maps_lost = away_score
                        result = "W" if home_score > away_score else "L"
                    else:
                        maps_won = away_score
                        maps_lost = home_score
                        result = "W" if away_score > home_score else "L"

                    total_maps = home_score + away_score
                else:
                    maps_won = maps_lost = total_maps = 0
                    result = "L"
            else:
                maps_won = maps_lost = total_maps = 0
                result = "L"

            # Buscar estatísticas detalhadas por mapa
            cursor.execute(
                """
                SELECT 
                    ms.stat_name,
                    ms.home_value,
                    ms.away_value
                FROM game_maps gm
                JOIN map_statistics ms ON gm.map_id = ms.map_id
                WHERE gm.match_id = ?
                ORDER BY gm.map_number, ms.stat_name
            """,
                (match_id,),
            )

            map_stats = cursor.fetchall()

            # Calcular totais da série
            team_total_kills = 0
            team_total_dragons = 0
            team_total_towers = 0
            team_total_inhibitors = 0
            team_total_barons = 0

            for stat in map_stats:
                stat_name = stat["stat_name"]
                home_value = stat["home_value"]
                away_value = stat["away_value"]

                try:
                    # Converter valores para inteiros
                    home_int = (
                        int(home_value)
                        if home_value and home_value.replace(",", "").isdigit()
                        else 0
                    )
                    away_int = (
                        int(away_value)
                        if away_value and away_value.replace(",", "").isdigit()
                        else 0
                    )

                    # Atribuir valores ao time correto
                    if is_home:
                        team_value = home_int
                    else:
                        team_value = away_int

                    # Acumular estatísticas
                    if stat_name == "kills":
                        team_total_kills += team_value
                    elif stat_name == "dragons":
                        team_total_dragons += team_value
                    elif stat_name == "towers":
                        team_total_towers += team_value
                    elif stat_name == "inhibitors":
                        team_total_inhibitors += team_value
                    elif stat_name == "barons":
                        team_total_barons += team_value

                except (ValueError, TypeError):
                    continue

            # Calcular médias por mapa
            maps_played = total_maps if total_maps > 0 else 1
            avg_kills_per_map = team_total_kills / maps_played
            avg_dragons_per_map = team_total_dragons / maps_played
            avg_towers_per_map = team_total_towers / maps_played
            avg_inhibitors_per_map = team_total_inhibitors / maps_played
            avg_barons_per_map = team_total_barons / maps_played

            return TeamGameStats(
                match_id=match_id,
                team_name=team_name,
                opponent=opponent,
                date=event_time,
                league=league,
                result=result,
                maps_won=maps_won,
                maps_lost=maps_lost,
                total_maps=total_maps,
                series_total_kills=team_total_kills,
                series_total_dragons=team_total_dragons,
                series_total_towers=team_total_towers,
                series_total_inhibitors=team_total_inhibitors,
                series_total_barons=team_total_barons,
                avg_kills_per_map=avg_kills_per_map,
                avg_dragons_per_map=avg_dragons_per_map,
                avg_towers_per_map=avg_towers_per_map,
                avg_inhibitors_per_map=avg_inhibitors_per_map,
                avg_barons_per_map=avg_barons_per_map,
            )

        except Exception as e:
            print(f"⚠️  Erro ao processar jogo {match_id}: {e}")
            return None

    def _get_map_stats(self, match_id: int, is_home: bool, cursor) -> List[Dict]:
        """Obtém estatísticas individuais de cada mapa para análise geral"""
        try:
            cursor.execute(
                """
                SELECT 
                    gm.map_number,
                    ms.stat_name,
                    ms.home_value,
                    ms.away_value
                FROM game_maps gm
                JOIN map_statistics ms ON gm.map_id = ms.map_id
                WHERE gm.match_id = ?
                ORDER BY gm.map_number, ms.stat_name
            """,
                (match_id,),
            )

            map_stats = cursor.fetchall()

            # Organizar estatísticas por mapa
            maps_data = []
            current_map = None
            map_data = {}

            for stat in map_stats:
                map_number = stat["map_number"]
                stat_name = stat["stat_name"]
                home_value = stat["home_value"]
                away_value = stat["away_value"]

                if map_number != current_map:
                    if current_map is not None:
                        maps_data.append(map_data)
                    current_map = map_number
                    map_data = {"map_number": map_number}

                try:
                    home_int = (
                        int(home_value)
                        if home_value and home_value.replace(",", "").isdigit()
                        else 0
                    )
                    away_int = (
                        int(away_value)
                        if away_value and away_value.replace(",", "").isdigit()
                        else 0
                    )

                    if is_home:
                        map_data[stat_name] = home_int
                    else:
                        map_data[stat_name] = away_int
                except (ValueError, TypeError):
                    continue

            if map_data:
                maps_data.append(map_data)

            return maps_data

        except Exception as e:
            print(f"⚠️  Erro ao processar mapas do jogo {match_id}: {e}")
            return []

    def _calculate_team_analysis(
        self, team_name: str, series_games: List[TeamGameStats], map_stats: List[Dict]
    ) -> TeamStatsAnalysis:
        """Calcula análise agregada do time"""
        # Estatísticas por série (últimas MAX_SERIES séries)
        series_stats = self._calculate_series_stats(team_name, series_games)

        # Estatísticas por mapa (últimos MAX_MAPS mapas)
        map_stats_result = self._calculate_map_stats(map_stats)

        return TeamStatsAnalysis(
            team_name=team_name,
            games_found=len(series_games),
            wins=series_stats["wins"],
            losses=series_stats["losses"],
            win_rate=series_stats["win_rate"],
            avg_series_kills=series_stats["avg_kills"],
            avg_series_dragons=series_stats["avg_dragons"],
            avg_series_towers=series_stats["avg_towers"],
            avg_series_inhibitors=series_stats["avg_inhibitors"],
            avg_series_barons=series_stats["avg_barons"],
            avg_map_kills_per_series=series_stats["avg_map_kills"],
            avg_map_dragons_per_series=series_stats["avg_map_dragons"],
            avg_map_towers_per_series=series_stats["avg_map_towers"],
            avg_map_inhibitors_per_series=series_stats["avg_map_inhibitors"],
            avg_map_barons_per_series=series_stats["avg_map_barons"],
            avg_map_kills_general=map_stats_result["avg_kills"],
            avg_map_dragons_general=map_stats_result["avg_dragons"],
            avg_map_towers_general=map_stats_result["avg_towers"],
            avg_map_inhibitors_general=map_stats_result["avg_inhibitors"],
            avg_map_barons_general=map_stats_result["avg_barons"],
            recent_games=series_games,
        )

    def _calculate_series_stats(
        self, team_name: str, games: List[TeamGameStats]
    ) -> Dict:
        """Calcula estatísticas baseadas nas séries"""
        if not games:
            return {
                "wins": 0,
                "losses": 0,
                "win_rate": 0.0,
                "avg_kills": 0.0,
                "avg_dragons": 0.0,
                "avg_towers": 0.0,
                "avg_inhibitors": 0.0,
                "avg_barons": 0.0,
                "avg_map_kills": 0.0,
                "avg_map_dragons": 0.0,
                "avg_map_towers": 0.0,
                "avg_map_inhibitors": 0.0,
                "avg_map_barons": 0.0,
            }

        wins = sum(1 for game in games if game.result == "W")
        losses = len(games) - wins
        win_rate = wins / len(games) if games else 0.0

        # Médias por série
        avg_kills = (
            statistics.mean([game.series_total_kills for game in games])
            if games
            else 0.0
        )
        avg_dragons = (
            statistics.mean([game.series_total_dragons for game in games])
            if games
            else 0.0
        )
        avg_towers = (
            statistics.mean([game.series_total_towers for game in games])
            if games
            else 0.0
        )
        avg_inhibitors = (
            statistics.mean([game.series_total_inhibitors for game in games])
            if games
            else 0.0
        )
        avg_barons = (
            statistics.mean([game.series_total_barons for game in games])
            if games
            else 0.0
        )

        # Médias por mapa (média das médias de cada série)
        avg_map_kills = (
            statistics.mean([game.avg_kills_per_map for game in games])
            if games
            else 0.0
        )
        avg_map_dragons = (
            statistics.mean([game.avg_dragons_per_map for game in games])
            if games
            else 0.0
        )
        avg_map_towers = (
            statistics.mean([game.avg_towers_per_map for game in games])
            if games
            else 0.0
        )
        avg_map_inhibitors = (
            statistics.mean([game.avg_inhibitors_per_map for game in games])
            if games
            else 0.0
        )
        avg_map_barons = (
            statistics.mean([game.avg_barons_per_map for game in games])
            if games
            else 0.0
        )

        return {
            "wins": wins,
            "losses": losses,
            "win_rate": win_rate,
            "avg_kills": avg_kills,
            "avg_dragons": avg_dragons,
            "avg_towers": avg_towers,
            "avg_inhibitors": avg_inhibitors,
            "avg_barons": avg_barons,
            "avg_map_kills": avg_map_kills,
            "avg_map_dragons": avg_map_dragons,
            "avg_map_towers": avg_map_towers,
            "avg_map_inhibitors": avg_map_inhibitors,
            "avg_map_barons": avg_map_barons,
        }

    def _calculate_map_stats(self, map_stats: List[Dict]) -> Dict:
        """Calcula estatísticas baseadas nos mapas individuais"""
        if not map_stats:
            return {
                "avg_kills": 0.0,
                "avg_dragons": 0.0,
                "avg_towers": 0.0,
                "avg_inhibitors": 0.0,
                "avg_barons": 0.0,
            }

        kills = [m.get("kills", 0) for m in map_stats]
        dragons = [m.get("dragons", 0) for m in map_stats]
        towers = [m.get("towers", 0) for m in map_stats]
        inhibitors = [m.get("inhibitors", 0) for m in map_stats]
        barons = [m.get("barons", 0) for m in map_stats]

        return {
            "avg_kills": statistics.mean(kills) if kills else 0.0,
            "avg_dragons": statistics.mean(dragons) if dragons else 0.0,
            "avg_towers": statistics.mean(towers) if towers else 0.0,
            "avg_inhibitors": statistics.mean(inhibitors) if inhibitors else 0.0,
            "avg_barons": statistics.mean(barons) if barons else 0.0,
        }

    def _create_empty_stats(self, team_name: str) -> TeamStatsAnalysis:
        """Cria estatísticas vazias"""
        return TeamStatsAnalysis(
            team_name=team_name,
            games_found=0,
            wins=0,
            losses=0,
            win_rate=0.0,
            recent_games=[],
        )

    def analyze_matchup_by_id(self, home_team_id: int, away_team_id: int) -> Dict:
        """Analisa o confronto específico entre dois times usando seus IDs"""
        home_team_name = self.get_team_name_by_id(home_team_id)
        away_team_name = self.get_team_name_by_id(away_team_id)

        print(f"🔍 ANALISANDO CONFRONTO: {home_team_name} vs {away_team_name}")
        print("=" * 70)
        print(
            f"📊 Configuração: Últimas {MAX_SERIES} séries | Últimos {MAX_MAPS} mapas"
        )
        print("=" * 70)

        # Buscar estatísticas de ambos os times
        print(f"📊 BUSCANDO ESTATÍSTICAS DOS TIMES:")
        print("-" * 50)

        home_stats = self.get_team_stats_by_id(home_team_id)
        away_stats = self.get_team_stats_by_id(away_team_id)

        # Exibir resultados
        self._print_team_analysis(home_stats, "HOME")
        print()
        self._print_team_analysis(away_stats, "AWAY")

        return {
            "home_team": home_team_name,
            "away_team": away_team_name,
            "home_stats": home_stats,
            "away_stats": away_stats,
        }

    def _print_team_analysis(self, stats: TeamStatsAnalysis, position: str):
        """Imprime análise de um time"""
        print(f"📈 {position} - {stats.team_name}")
        print(f"   📊 Jogos analisados: {stats.games_found}")

        if stats.games_found == 0:
            print(f"   ❌ Nenhum dado histórico encontrado")
            return

        print(
            f"   🏆 Vitórias/Derrotas: {stats.wins}W - {stats.losses}L ({stats.win_rate:.1%})"
        )

        print(f"   📊 MÉDIAS POR SÉRIE (últimas {MAX_SERIES} séries):")
        print(f"      ⚔️  Kills por série: {stats.avg_series_kills:.1f}")
        print(f"      🐉 Dragons por série: {stats.avg_series_dragons:.1f}")
        print(f"      🏗️  Torres por série: {stats.avg_series_towers:.1f}")
        print(f"      🛡️  Inibidores por série: {stats.avg_series_inhibitors:.1f}")
        print(f"      👹 Barões por série: {stats.avg_series_barons:.1f}")

        print(f"   🎯 MÉDIAS POR MAPA POR SÉRIE (média das {MAX_SERIES} séries):")
        print(f"      ⚔️  Kills por mapa: {stats.avg_map_kills_per_series:.1f}")
        print(f"      🐉 Dragons por mapa: {stats.avg_map_dragons_per_series:.1f}")
        print(f"      🏗️  Torres por mapa: {stats.avg_map_towers_per_series:.1f}")
        print(
            f"      🛡️  Inibidores por mapa: {stats.avg_map_inhibitors_per_series:.1f}"
        )
        print(f"      👹 Barões por mapa: {stats.avg_map_barons_per_series:.1f}")

        print(f"   🗺️  MÉDIAS POR MAPA GERAL (últimos {MAX_MAPS} mapas):")
        print(f"      ⚔️  Kills por mapa: {stats.avg_map_kills_general:.1f}")
        print(f"      🐉 Dragons por mapa: {stats.avg_map_dragons_general:.1f}")
        print(f"      🏗️  Torres por mapa: {stats.avg_map_towers_general:.1f}")
        print(f"      🛡️  Inibidores por mapa: {stats.avg_map_inhibitors_general:.1f}")
        print(f"      👹 Barões por mapa: {stats.avg_map_barons_general:.1f}")

        # Últimos jogos
        if stats.recent_games:
            print(f"   🎯 ÚLTIMOS {len(stats.recent_games)} JOGOS:")
            for i, game in enumerate(stats.recent_games):
                result_icon = "✅" if game.result == "W" else "❌"
                print(
                    f"      {i + 1:2d}. {result_icon} vs {game.opponent} ({game.maps_won}-{game.maps_lost}) - {game.league}"
                )


# Teste
if __name__ == "__main__":
    analyzer = TeamStatsAnalyzer()

    # Analisar um confronto específico usando team_ids
    result = analyzer.analyze_matchup_by_id(10361241, 10361255)

    if result:
        print(f"\n🎯 ANÁLISE CONCLUÍDA!")
        print(f"✅ Dados coletados para {result['home_team']} vs {result['away_team']}")
    else:
        print(f"\n❌ Não foi possível analisar o confronto")
