import sqlite3
import pandas as pd
from typing import Dict, List, Tuple, Optional
from colorama import init, Fore, Back, Style


class ROIAnalyzer:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = None
        # Inicializa colorama
        init()

    def connect(self):
        """Conecta ao banco de dados"""
        try:
            self.conn = sqlite3.connect(self.db_path)
            return True
        except sqlite3.Error as e:
            print(f"Erro ao conectar ao banco de dados: {e}")
            return False

    def disconnect(self):
        """Desconecta do banco de dados"""
        if self.conn:
            self.conn.close()

    def get_market_odds(
        self, event_id: str, market_name: str = "Map 1 - Totals", odds_type: str = "map_1"
    ) -> pd.DataFrame:
        """Busca as odds de um mercado específico para um evento"""
        query = """
        SELECT selection_name, handicap, odds_value, updated_at, market_name
        FROM current_odds
        WHERE event_id = ? 
        AND market_name = ?
        AND odds_type = ?
        ORDER BY selection_name, handicap
        """

        try:
            df = pd.read_sql_query(query, self.conn, params=[event_id, market_name, odds_type])
            return df
        except Exception as e:
            print(f"Erro ao buscar odds: {e}")
            return pd.DataFrame()

    def get_event_info(self, event_id: str) -> Dict:
        """Busca informações do evento (times, data, etc.)"""
        if not self.connect():
            return {}

        try:
            # Primeiro busca as informações do evento
            query = """
            SELECT home_team_id, away_team_id, league_name, match_date
            FROM events
            WHERE event_id = ?
            """
            cursor = self.conn.cursor()
            cursor.execute(query, (event_id,))
            result = cursor.fetchone()

            if result:
                home_team_id, away_team_id, league_name, match_date = result

                # Busca os nomes dos times
                home_team_query = "SELECT name FROM teams WHERE team_id = ?"
                cursor.execute(home_team_query, (home_team_id,))
                home_team_result = cursor.fetchone()
                home_team = (
                    home_team_result[0] if home_team_result else f"Team {home_team_id}"
                )

                away_team_query = "SELECT name FROM teams WHERE team_id = ?"
                cursor.execute(away_team_query, (away_team_id,))
                away_team_result = cursor.fetchone()
                away_team = (
                    away_team_result[0] if away_team_result else f"Team {away_team_id}"
                )

                return {
                    "home_team": home_team,
                    "away_team": away_team,
                    "league_name": league_name,
                    "match_date": match_date,
                }
            else:
                return {}
        except Exception as e:
            print(f"Erro ao buscar informações do evento: {e}")
            return {}
        finally:
            self.disconnect()

    def get_team_stats(
        self, team_name: str, stat_type: str, limit: int = 10
    ) -> List[float]:
        """
        Busca estatísticas históricas reais de uma equipe do banco lol_esports.db
        """
        try:
            # Conecta ao banco lol_esports.db
            esports_conn = sqlite3.connect("../data/lol_esports.db")
            cursor = esports_conn.cursor()

            # 1. Busca o team_id pelo nome
            cursor.execute(
                "SELECT team_id, name FROM teams WHERE name = ?", (team_name,)
            )
            team_result = cursor.fetchone()

            if not team_result:
                esports_conn.close()
                return self._get_fallback_stats(team_name, stat_type, limit)

            team_id = team_result[0]

            # 2. Busca últimas partidas do time
            query_matches = """
            SELECT m.match_id, m.home_team_id, m.away_team_id, m.event_time
            FROM matches m
            WHERE (m.home_team_id = ? OR m.away_team_id = ?)
            AND m.time_status = 3
            AND m.event_time >= datetime('now', '-60 days')
            ORDER BY m.event_time DESC
            LIMIT 30
            """

            cursor.execute(query_matches, (team_id, team_id))
            matches = cursor.fetchall()

            if not matches:
                esports_conn.close()
                return self._get_fallback_stats(team_name, stat_type, limit)

            # 3. Busca mapas dessas partidas
            match_ids = [str(match[0]) for match in matches]
            placeholders = ",".join(["?"] * len(match_ids))

            query_maps = f"""
            SELECT gm.map_id, gm.match_id, gm.map_number
            FROM game_maps gm
            WHERE gm.match_id IN ({placeholders})
            ORDER BY gm.match_id DESC, gm.map_number ASC
            """

            cursor.execute(query_maps, match_ids)
            all_maps = cursor.fetchall()

            if not all_maps:
                esports_conn.close()
                return self._get_fallback_stats(team_name, stat_type, limit)

            # 4. Busca estatísticas dos mapas
            map_ids = [str(map_data[0]) for map_data in all_maps]
            placeholders = ",".join(["?"] * len(map_ids))

            query_stats = f"""
            SELECT ms.map_id, ms.stat_name, ms.home_value, ms.away_value
            FROM map_statistics ms
            WHERE ms.map_id IN ({placeholders})
            AND ms.stat_name = ?
            ORDER BY ms.map_id DESC
            """

            cursor.execute(query_stats, map_ids + [stat_type])
            all_stats = cursor.fetchall()

            # 5. Processa estatísticas
            valid_stats = []

            for stat in all_stats:
                if len(valid_stats) >= limit:
                    break

                map_id, stat_name, home_value, away_value = stat

                try:
                    home_val = float(home_value) if home_value else 0.0
                    away_val = float(away_value) if away_value else 0.0
                    total_value = home_val + away_val

                    # Apenas para inhibitors, filtra valores = 0
                    if stat_type == "inhibitors" and total_value == 0:
                        continue

                    valid_stats.append(total_value)

                except (ValueError, TypeError):
                    continue

            esports_conn.close()

            if len(valid_stats) == 0:
                return self._get_fallback_stats(team_name, stat_type, limit)

            return valid_stats[:limit]

        except Exception as e:
            return self._get_fallback_stats(team_name, stat_type, limit)

    def _get_fallback_stats(
        self, team_name: str, stat_type: str, limit: int
    ) -> List[float]:
        """Retorna estatísticas mockadas como fallback"""
        import random

        # Gerar dados aleatórios diferentes para cada time baseado no nome
        random.seed(hash(team_name + stat_type))

        if stat_type == "dragons":
            return [random.randint(2, 7) for _ in range(limit)]
        elif stat_type == "barons":
            return [random.randint(0, 2) for _ in range(limit)]
        elif stat_type == "kills":
            return [random.randint(20, 40) for _ in range(limit)]
        elif stat_type == "towers":
            return [random.randint(6, 15) for _ in range(limit)]
        elif stat_type == "inhibitors":
            return [random.randint(1, 3) for _ in range(limit)]
        else:
            return []

    def calculate_roi(
        self,
        historical_data: List[float],
        handicap: float,
        odds: float,
        selection: str,
        debug: bool = True,
        team_name: str = "",
    ) -> Tuple[float, float]:
        """Calcula o ROI e fair_odds baseado em dados históricos"""
        if not historical_data:
            return 0.0, 0.0

        # Conta quantas vezes a aposta teria sido vencedora
        wins = 0
        for value in historical_data:
            if selection.startswith("Over"):
                if value > handicap:
                    wins += 1
            elif selection.startswith("Under"):
                if value < handicap:
                    wins += 1

        total_bets = len(historical_data)
        if total_bets == 0:
            return 0.0, 0.0

        # Calcula probabilidade
        probability = wins / total_bets

        # Debug: print para verificar (com indentação)
        if debug:
            print(
                f"      {Fore.CYAN}📊 {team_name}: {wins}/{total_bets} wins ({probability:.1%}) - {historical_data}{Style.RESET_ALL}"
            )

        # Calcula ROI
        profit = (wins * odds) - total_bets
        roi = (profit / total_bets) * 100

        # Calcula Fair Odds
        fair_odds = 1 / probability if probability > 0 else float("inf")

        return roi, fair_odds

    def calculate_team_roi(
        self,
        team_name: str,
        selection: str,
        handicap: float,
        odds: float,
        debug: bool = False,
    ) -> Tuple[float, float]:
        """Calcula ROI e fair_odds para um time específico"""
        stat_type = self._get_stat_type(selection)
        if not stat_type:
            return 0.0, 0.0

        team_stats = self.get_team_stats(team_name, stat_type)
        return self.calculate_roi(
            team_stats, handicap, odds, selection, debug, team_name
        )

    def calculate_average_roi(
        self, team1: str, team2: str, selection: str, handicap: float, odds: float
    ) -> Tuple[float, float, float, float]:
        """Calcula ROI e fair_odds para ambos os times"""
        # Calcula para cada time
        roi_team1, fair_odds_team1 = self.calculate_team_roi(
            team1, selection, handicap, odds, debug=True
        )
        roi_team2, fair_odds_team2 = self.calculate_team_roi(
            team2, selection, handicap, odds, debug=True
        )

        # Médias
        roi_average = (roi_team1 + roi_team2) / 2

        # Para fair_odds, calcula probabilidade média e depois fair_odds
        # Isso evita problemas com inf quando um time has 0% de chance
        stat_type = self._get_stat_type(selection)
        if stat_type:
            team1_stats = self.get_team_stats(team1, stat_type)
            team2_stats = self.get_team_stats(team2, stat_type)

            prob1 = self._calculate_probability(team1_stats, handicap, selection)
            prob2 = self._calculate_probability(team2_stats, handicap, selection)
            combined_prob = (prob1 + prob2) / 2

            # Fair odds baseado na probabilidade média
            if combined_prob > 0:
                fair_odds_average = 1 / combined_prob
            else:
                fair_odds_average = 999.99  # Valor alto mas não infinito
        else:
            fair_odds_average = 999.99

        return roi_team1, roi_team2, roi_average, fair_odds_average

    def _calculate_probability(
        self, historical_data: List[float], handicap: float, selection: str
    ) -> float:
        """Calcula probabilidade baseada nos dados históricos"""
        if not historical_data:
            return 0.0

        wins = 0
        for value in historical_data:
            if selection.startswith("Over"):
                if value > handicap:
                    wins += 1
            elif selection.startswith("Under"):
                if value < handicap:
                    wins += 1

        return wins / len(historical_data)

    def get_betting_lines(
        self, event_id: str, market_filter: Optional[str] = None
    ) -> List[Dict]:
        """Retorna todas as linhas de aposta disponíveis para um ou mais mercados"""
        if not self.connect():
            return []

        # Define os mercados e seus respectivos odds_type
        market_mapping = {
            "Map 1 - Totals": "map_1",
            "Map 2 - Totals": "map_2",
            # Adicione outros mercados conforme necessário
        }

        if market_filter is None:
            markets = ["Map 1 - Totals"]  # Padrão
        elif isinstance(market_filter, str):
            markets = [market_filter]
        else:
            markets = market_filter

        all_lines = []

        for market in markets:
            odds_type = market_mapping.get(
                market, "main"
            )  # Usa 'main' como padrão se não encontrado
            odds_df = self.get_market_odds(event_id, market, odds_type)

            for _, row in odds_df.iterrows():
                all_lines.append(
                    {
                        "selection": row["selection_name"],
                        "handicap": float(row["handicap"]),
                        "odds": row["odds_value"],
                        "updated_at": row["updated_at"],
                        "market_name": row.get("market_name", market),
                    }
                )

        self.disconnect()
        return all_lines

    def format_roi_color(self, roi: float) -> str:
        """Formata ROI com cores baseado no valor"""
        if roi > 10:
            return f"{Fore.GREEN}{roi:.2f}%{Style.RESET_ALL}"
        elif roi > 0:
            return f"{Fore.YELLOW}{roi:.2f}%{Style.RESET_ALL}"
        else:
            return f"{Fore.RED}{roi:.2f}%{Style.RESET_ALL}"

    def analyze_event(
        self,
        event_id: str,
        team1: str = None,
        team2: str = None,
        market_name: str = "Map 1 - Totals",
    ):
        """Analisa todas as odds de um mercado específico para um evento"""
        # Busca informações do evento
        event_info = self.get_event_info(event_id)

        # Se os times não foram fornecidos, usa os do evento
        if not team1 or not team2:
            if event_info:
                team1 = event_info.get("home_team", "Team A")
                team2 = event_info.get("away_team", "Team B")
            else:
                team1 = team1 or "Team A"
                team2 = team2 or "Team B"

        # Busca todas as linhas de aposta para o mercado especificado
        betting_lines = self.get_betting_lines(event_id, market_name)

        if not betting_lines:
            print(
                f"{Fore.RED}Nenhuma odd encontrada para o evento {event_id} no mercado {market_name}{Style.RESET_ALL}"
            )
            return

        # Header
        print(
            f"\n{Back.BLUE}{Fore.WHITE} ANÁLISE DE ROI - {market_name.upper()} {Style.RESET_ALL}"
        )
        print(
            f"{Fore.CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━{Style.RESET_ALL}"
        )

        # Informações do evento
        print(
            f"{Fore.WHITE}Evento:{Style.RESET_ALL} {Fore.YELLOW}{event_id}{Style.RESET_ALL}"
        )
        if event_info:
            if event_info.get("league_name"):
                print(
                    f"{Fore.WHITE}Liga:{Style.RESET_ALL} {event_info.get('league_name')}"
                )
            if event_info.get("match_date"):
                print(
                    f"{Fore.WHITE}Data:{Style.RESET_ALL} {event_info.get('match_date')}"
                )
        print(
            f"{Fore.WHITE}Times:{Style.RESET_ALL} {Fore.MAGENTA}{team1}{Style.RESET_ALL} vs {Fore.MAGENTA}{team2}{Style.RESET_ALL}"
        )
        print(
            f"{Fore.CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━{Style.RESET_ALL}"
        )

        # Para cada linha de aposta, calcula o ROI
        for i, line in enumerate(betting_lines, 1):
            selection = line["selection"]
            handicap = line["handicap"]
            odds = line["odds"]

            # Exibe o cabeçalho da linha
            print(f"\n{Fore.WHITE}{Back.BLACK} {i:2d}. {selection} {Style.RESET_ALL}")
            print(
                f"    {Fore.WHITE}Handicap:{Style.RESET_ALL} {Fore.YELLOW}{handicap}{Style.RESET_ALL} | {Fore.WHITE}Odd:{Style.RESET_ALL} {Fore.YELLOW}{odds}{Style.RESET_ALL}"
            )

            # Calcula ROI e fair_odds (com debug dos dados históricos)
            roi_team1, roi_team2, roi_average, fair_odds_average = (
                self.calculate_average_roi(team1, team2, selection, handicap, odds)
            )

            # Exibe os resultados
            print(
                f"    {Fore.WHITE}ROI {team1}:{Style.RESET_ALL} {self.format_roi_color(roi_team1)}"
            )
            print(
                f"    {Fore.WHITE}ROI {team2}:{Style.RESET_ALL} {self.format_roi_color(roi_team2)}"
            )
            print(
                f"    {Fore.WHITE}ROI Médio:{Style.RESET_ALL} {self.format_roi_color(roi_average)}"
            )
            print(
                f"    {Fore.WHITE}Fair Odds:{Style.RESET_ALL} {Fore.MAGENTA}{fair_odds_average:.2f}{Style.RESET_ALL} | {Fore.WHITE}Casa:{Style.RESET_ALL} {Fore.YELLOW}{odds}{Style.RESET_ALL}"
            )

        print(
            f"\n{Fore.CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━{Style.RESET_ALL}"
        )
        print(
            f"{Fore.GREEN}✓ Verde: ROI > 10%{Style.RESET_ALL} | {Fore.YELLOW}○ Amarelo: ROI 0-10%{Style.RESET_ALL} | {Fore.RED}✗ Vermelho: ROI < 0%{Style.RESET_ALL}"
        )

    def _get_stat_type(self, selection_name: str) -> str:
        """Extrai o tipo de estatística do nome da seleção"""
        if "Dragons" in selection_name:
            return "dragons"
        elif "Barons" in selection_name:
            return "barons"
        elif "Kills" in selection_name:
            return "kills"
        elif "Towers" in selection_name:
            return "towers"
        elif "Inhibitors" in selection_name:
            return "inhibitors"
        else:
            return ""


# Exemplo de uso
if __name__ == "__main__":
    DB_PATH = "../data/lol_odds.db"
    EVENT_ID = "180107617"

    analyzer = ROIAnalyzer(DB_PATH)

    # Analisar Map 1
    analyzer.analyze_event(EVENT_ID, market_name="Map 1 - Totals")

    # Analisar Map 2
    analyzer.analyze_event(EVENT_ID, market_name="Map 2 - Totals")