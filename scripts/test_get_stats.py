import sqlite3
from typing import List, Tuple
from colorama import init, Fore, Back, Style

# Inicializa colorama
init()


def get_teams_from_event(event_id: str) -> Tuple[str, str]:
    """Busca os nomes dos times de um evento específico"""
    try:
        # Conecta ao banco lol_odds.db
        odds_conn = sqlite3.connect("../data/lol_odds.db")
        cursor = odds_conn.cursor()

        # Busca informações do evento
        query = """
        SELECT home_team_id, away_team_id
        FROM events
        WHERE event_id = ?
        """
        cursor.execute(query, (event_id,))
        result = cursor.fetchone()

        if not result:
            print(f"{Fore.RED}❌ Evento {event_id} não encontrado{Style.RESET_ALL}")
            odds_conn.close()
            return None, None

        home_team_id, away_team_id = result

        # Busca nomes dos times
        home_query = "SELECT name FROM teams WHERE team_id = ?"
        cursor.execute(home_query, (home_team_id,))
        home_result = cursor.fetchone()
        home_team = home_result[0] if home_result else f"Team {home_team_id}"

        away_query = "SELECT name FROM teams WHERE team_id = ?"
        cursor.execute(away_query, (away_team_id,))
        away_result = cursor.fetchone()
        away_team = away_result[0] if away_result else f"Team {away_team_id}"

        odds_conn.close()
        return home_team, away_team

    except Exception as e:
        print(f"{Fore.RED}Erro ao buscar times do evento: {e}{Style.RESET_ALL}")
        return None, None


def get_team_stats_for_event(
    team_name: str, stat_type: str, limit: int = 10
) -> List[float]:
    """
    Busca estatísticas históricas reais de uma equipe para análise de ROI
    Descarta jogos com inhibitors = 0 APENAS para a métrica inhibitors
    """
    print(
        f"\n{Back.BLUE}{Fore.WHITE} BUSCANDO STATS: {team_name} - {stat_type.upper()} {Style.RESET_ALL}"
    )
    print(f"{Fore.CYAN}{'-' * 80}{Style.RESET_ALL}")

    try:
        # Conecta ao banco lol_esports.db
        esports_conn = sqlite3.connect("../data/lol_esports.db")
        cursor = esports_conn.cursor()

        # 1. Busca o team_id pelo nome
        cursor.execute("SELECT team_id, name FROM teams WHERE name = ?", (team_name,))
        team_result = cursor.fetchone()

        if not team_result:
            print(f"{Fore.RED}❌ Team '{team_name}' não encontrado{Style.RESET_ALL}")
            esports_conn.close()
            return []

        team_id = team_result[0]
        print(f"{Fore.GREEN}✅ Team: {team_result[1]} (ID: {team_id}){Style.RESET_ALL}")

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
            print(f"{Fore.RED}❌ Nenhuma partida encontrada{Style.RESET_ALL}")
            esports_conn.close()
            return []

        print(f"{Fore.CYAN}📊 {len(matches)} partidas encontradas{Style.RESET_ALL}")

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

        print(f"{Fore.CYAN}🗺️ {len(all_maps)} mapas disponíveis{Style.RESET_ALL}")

        # 4. Busca estatísticas dos mapas
        map_ids = [str(map_data[0]) for map_data in all_maps]
        placeholders = ",".join(["?"] * len(map_ids))

        # Para inhibitors, precisamos também buscar os valores para validação
        if stat_type == "inhibitors":
            query_stats = f"""
            SELECT ms.map_id, ms.stat_name, ms.home_value, ms.away_value
            FROM map_statistics ms
            WHERE ms.map_id IN ({placeholders})
            AND ms.stat_name = ?
            ORDER BY ms.map_id
            """
        else:
            query_stats = f"""
            SELECT ms.map_id, ms.stat_name, ms.home_value, ms.away_value
            FROM map_statistics ms
            WHERE ms.map_id IN ({placeholders})
            AND ms.stat_name = ?
            ORDER BY ms.map_id
            """

        cursor.execute(query_stats, map_ids + [stat_type])
        all_stats = cursor.fetchall()

        # 5. Processa estatísticas
        valid_stats = []
        discarded_count = 0

        print(f"\n{Fore.BLUE}🔍 Processando {stat_type}:{Style.RESET_ALL}")

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
                    discarded_count += 1
                    print(
                        f"   ❌ Map {map_id}: {total_value} (DESCARTADO - inhibitors = 0)"
                    )
                    continue

                valid_stats.append(total_value)
                print(f"   ✅ Map {map_id}: {home_val} + {away_val} = {total_value}")

            except (ValueError, TypeError):
                discarded_count += 1
                print(f"   ❌ Map {map_id}: ERRO - valores inválidos")
                continue

        esports_conn.close()

        print(f"\n{Fore.BLUE}📊 RESULTADO:{Style.RESET_ALL}")
        print(f"   Mapas válidos: {len(valid_stats)}")
        if stat_type == "inhibitors":
            print(f"   Mapas descartados (inhibitors=0): {discarded_count}")
        print(f"   Dados finais: {valid_stats}")

        return valid_stats

    except Exception as e:
        print(f"{Fore.RED}💥 ERRO: {e}{Style.RESET_ALL}")
        return []


def test_event_stats(event_id: str):
    """Testa todas as estatísticas para um evento específico"""
    print(
        f"\n{Back.GREEN}{Fore.WHITE} TESTE COMPLETO - EVENT {event_id} {Style.RESET_ALL}"
    )
    print(f"{Fore.CYAN}{'=' * 80}{Style.RESET_ALL}")

    # Busca os times do evento
    home_team, away_team = get_teams_from_event(event_id)

    if not home_team or not away_team:
        print(
            f"{Fore.RED}❌ Não foi possível buscar os times do evento{Style.RESET_ALL}"
        )
        return

    print(
        f"{Fore.WHITE}Times do evento:{Style.RESET_ALL} {Fore.MAGENTA}{home_team}{Style.RESET_ALL} vs {Fore.MAGENTA}{away_team}{Style.RESET_ALL}"
    )

    # Stats para testar
    stats = ["kills", "dragons", "barons", "towers", "inhibitors"]

    results = {}

    for team in [home_team, away_team]:
        results[team] = {}

        print(f"\n{Back.YELLOW}{Fore.BLACK} ANALISANDO: {team} {Style.RESET_ALL}")

        for stat in stats:
            print(f"\n{Fore.YELLOW}🧪 {stat.upper()}:{Style.RESET_ALL}")
            result = get_team_stats_for_event(team, stat, 10)
            results[team][stat] = result

            if result:
                avg = sum(result) / len(result)
                min_val = min(result)
                max_val = max(result)
                print(
                    f"{Fore.GREEN}   ✅ Média: {avg:.1f} | Min: {min_val} | Max: {max_val}{Style.RESET_ALL}"
                )
            else:
                print(f"{Fore.RED}   ❌ Nenhum dado válido encontrado{Style.RESET_ALL}")

    # Resumo final
    print(
        f"\n{Back.CYAN}{Fore.WHITE} RESUMO FINAL - EVENT {event_id} {Style.RESET_ALL}"
    )
    print(f"{Fore.CYAN}{'=' * 80}{Style.RESET_ALL}")

    for team in [home_team, away_team]:
        print(f"\n{Fore.WHITE}📊 {team}:{Style.RESET_ALL}")
        for stat in stats:
            data = results[team][stat]
            if data:
                avg = sum(data) / len(data)
                print(f"   {stat}: {len(data)} mapas, média {avg:.1f}")
            else:
                print(f"   {stat}: {Fore.RED}SEM DADOS{Style.RESET_ALL}")


def test_inhibitors_investigation():
    """Investiga especificamente o problema com inhibitors"""
    print(
        f"\n{Back.RED}{Fore.WHITE} INVESTIGAÇÃO ESPECIAL - INHIBITORS {Style.RESET_ALL}"
    )
    print(f"{Fore.CYAN}{'=' * 80}{Style.RESET_ALL}")

    try:
        esports_conn = sqlite3.connect("../data/lol_esports.db")
        cursor = esports_conn.cursor()

        # Busca todos os mapas recentes e suas stats de inhibitors
        query = """
        SELECT ms.map_id, ms.home_value, ms.away_value,
               gm.match_id, gm.map_number
        FROM map_statistics ms
        JOIN game_maps gm ON ms.map_id = gm.map_id
        WHERE ms.stat_name = 'inhibitors'
        ORDER BY gm.match_id DESC, gm.map_number ASC
        LIMIT 20
        """

        cursor.execute(query)
        inhibitor_data = cursor.fetchall()

        print(
            f"{Fore.CYAN}📊 Últimos 20 mapas - Estatísticas de Inhibitors:{Style.RESET_ALL}"
        )

        valid_count = 0
        invalid_count = 0

        for data in inhibitor_data:
            map_id, home_val, away_val, match_id, map_num = data

            try:
                home_inhibs = float(home_val) if home_val else 0.0
                away_inhibs = float(away_val) if away_val else 0.0
                total_inhibs = home_inhibs + away_inhibs

                if total_inhibs > 0:
                    valid_count += 1
                    status = f"{Fore.GREEN}✅ VÁLIDO{Style.RESET_ALL}"
                else:
                    invalid_count += 1
                    status = f"{Fore.RED}❌ INVÁLIDO{Style.RESET_ALL}"

                print(
                    f"   Map {map_id} (Match {match_id}, Game {map_num}): {home_inhibs} + {away_inhibs} = {total_inhibs} {status}"
                )

            except (ValueError, TypeError):
                invalid_count += 1
                print(f"   Map {map_id}: ERRO nos valores - {status}")

        print(f"\n{Fore.BLUE}📈 RESUMO:{Style.RESET_ALL}")
        print(f"   Mapas válidos (inhibitors > 0): {valid_count}")
        print(f"   Mapas inválidos (inhibitors = 0): {invalid_count}")
        print(
            f"   Taxa de dados válidos: {(valid_count / (valid_count + invalid_count) * 100):.1f}%"
        )

        esports_conn.close()

    except Exception as e:
        print(f"{Fore.RED}💥 ERRO na investigação: {e}{Style.RESET_ALL}")


if __name__ == "__main__":
    print(
        f"{Back.CYAN}{Fore.WHITE} TESTE AVANÇADO - STATS POR EVENT_ID {Style.RESET_ALL}"
    )

    # Event ID para testar (DRX vs DN Freecs)
    EVENT_ID = "179715703"

    # Primeiro, investiga o problema com inhibitors
    test_inhibitors_investigation()

    # Depois, testa o evento completo
    test_event_stats(EVENT_ID)

    print(f"\n{Back.GREEN}{Fore.WHITE} TESTE CONCLUÍDO {Style.RESET_ALL}")
    print(f"Agora a função filtra automaticamente jogos com inhibitors = 0")
