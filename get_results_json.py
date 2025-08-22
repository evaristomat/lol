import asyncio
import json
import os
from datetime import datetime, timedelta
from src.core.bet365_client import Bet365Client


class LoLResultsChecker:
    def __init__(self):
        self.client = Bet365Client()
        self.lol_sport_id = 151
        self.output_dir = "lol_results"

        # Criar diretório de saída se não existir
        os.makedirs(self.output_dir, exist_ok=True)

    def _is_lol_event(self, event):
        """Filtro preciso para identificar apenas League of Legends que começam com 'LOL -'"""
        league_name = event.get("league", {}).get("name", "").strip()

        # Filtro principal: deve começar com "LOL -"
        starts_with_lol = league_name.startswith("LOL -")

        # Filtro adicional para garantir que não pegue outros esports
        non_lol_keywords = [
            "VALORANT",
            "CS2",
            "CS:GO",
            "DOTA",
            "OVERWATCH",
            "RAINBOW SIX",
            "R6",
            "ROCKET LEAGUE",
            "FIFA",
            "CALL OF DUTY",
            "COD",
            "STARCRAFT",
            "WARCRAFT",
            "HEARTHSTONE",
            "FORTNITE",
            "PUBG",
        ]

        # Verificar se NÃO contém palavras-chave de outros esports
        is_not_other = not any(
            keyword.lower() in league_name.lower() for keyword in non_lol_keywords
        )

        return starts_with_lol and is_not_other

    async def get_recent_lol_events(self, days_back=2):
        """Busca eventos de LoL dos últimos X dias"""
        events = []

        for i in range(days_back + 1):
            target_date = datetime.now() - timedelta(days=i)
            day_str = target_date.strftime("%Y%m%d")

            print(f"📅 Buscando eventos para {target_date.strftime('%Y-%m-%d')}...")

            try:
                daily_events = await self.client.upcoming(
                    sport_id=self.lol_sport_id, day=day_str
                )

                if daily_events.get("success") == 1 and daily_events.get("results"):
                    # Filtro preciso para LoL (apenas começa com "LOL -")
                    lol_events = [
                        event
                        for event in daily_events["results"]
                        if self._is_lol_event(event)
                    ]

                    events.extend(lol_events)
                    print(f"   ✅ Encontrados {len(lol_events)} jogos de LoL")

                    # Mostrar quais ligas foram encontradas (para debug)
                    for event in lol_events:
                        league_name = event.get("league", {}).get("name", "N/A")
                        print(f"      🎮 {league_name}")

                else:
                    print(f"   ⚠️  Nenhum evento encontrado")

            except Exception as e:
                print(f"   ❌ Erro ao buscar eventos: {str(e)}")

        return events

    async def get_detailed_results(self, events):
        """Busca resultados detalhados apenas para jogos finalizados"""
        finished_games = []

        print(f"\n🔍 Verificando resultados para {len(events)} jogos...")

        for i, event in enumerate(events, 1):
            event_id = event.get("id")
            home_team = event.get("home", {}).get("name", "Unknown")
            away_team = event.get("away", {}).get("name", "Unknown")
            league_name = event.get("league", {}).get("name", "Unknown")

            print(f"[{i}/{len(events)}] {league_name}: {home_team} vs {away_team}")

            try:
                # CORREÇÃO AQUI: usar self.client em vez de client
                result_data = await self.client.result(event_id=event_id)

                if result_data.get("success") == 1 and result_data.get("results"):
                    result = result_data["results"][0]

                    # Apenas jogos finalizados
                    if result.get("time_status") == "3":
                        result["event_data"] = event
                        finished_games.append(result)

                        # Salvar JSON individual
                        await self.save_game_json(result, event)

                        print(f"   ✅ Finalizado: {result.get('ss', 'N/A')}")
                    else:
                        status = result.get("time_status", "0")
                        status_text = (
                            "Agendado"
                            if status == "1"
                            else "Ao Vivo"
                            if status == "2"
                            else f"Status {status}"
                        )
                        print(f"   ⏳ {status_text}")
                else:
                    print(f"   ❌ Sem dados de resultado")

            except Exception as e:
                print(f"   ❌ Erro: {str(e)}")

            await asyncio.sleep(0.3)

        return finished_games

    async def save_game_json(self, result, event):
        """Salva os dados completos do jogo em JSON"""
        try:
            # Criar nome do arquivo seguro
            home_team = event["home"]["name"].replace("/", "-").replace("\\", "-")
            away_team = event["away"]["name"].replace("/", "-").replace("\\", "-")
            league_name = event["league"]["name"].replace("/", "-").replace("\\", "-")
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

            filename = f"{league_name}_{home_team}_vs_{away_team}_{timestamp}.json"
            filename = "".join(
                c for c in filename if c.isalnum() or c in [" ", "_", "-"]
            ).rstrip()
            filename = filename.replace(" ", "_")[:100] + ".json"

            filepath = os.path.join(self.output_dir, filename)

            # Dados completos para salvar
            game_data = {
                "metadata": {
                    "exported_at": datetime.now().isoformat(),
                    "source": "Bet365 API",
                    "version": "1.0",
                },
                "event_info": event,
                "result_info": result,
                "summary": {
                    "home_team": home_team,
                    "away_team": away_team,
                    "final_score": result.get("ss", "N/A"),
                    "league": league_name,
                    "match_date": datetime.fromtimestamp(
                        int(result["time"])
                    ).isoformat()
                    if result.get("time")
                    else None,
                    "maps_played": len(result.get("period_stats", {})),
                    "status": "completed",
                },
            }

            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(game_data, f, indent=2, ensure_ascii=False)

            print(f"   💾 JSON salvo: {filename}")

        except Exception as e:
            print(f"   ❌ Erro ao salvar JSON: {str(e)}")

    async def print_detailed_results(self, results):
        """Exibe resultados detalhados com estatísticas completas dos mapas"""
        if not results:
            print("❌ Nenhum jogo finalizado encontrado")
            return

        print(f"\n{'=' * 80}")
        print(f"🎯 LEAGUE OF LEGENDS - RESULTADOS DETALHADOS")
        print(f"📊 Total de jogos finalizados: {len(results)}")
        print(f"💾 Arquivos salvos em: {self.output_dir}")
        print(f"{'=' * 80}")

        # Agrupar por liga
        leagues = {}
        for result in results:
            league_name = result["event_data"]["league"]["name"]
            if league_name not in leagues:
                leagues[league_name] = []
            leagues[league_name].append(result)

        # Mostrar por liga com detalhes completos
        for league_name, league_games in leagues.items():
            print(f"\n🏆 {league_name}")
            print(f"📋 {len(league_games)} jogos finalizados")
            print("=" * 60)

            for game in league_games:
                event_data = game["event_data"]
                home = event_data["home"]["name"]
                away = event_data["away"]["name"]
                score = game.get("ss", "N/A")

                print(f"\n⚔️  {home} vs {away}")
                print(f"📊 Placar Final: {score}")

                # Informações do jogo
                if game.get("time"):
                    try:
                        dt = datetime.fromtimestamp(int(game["time"]))
                        print(f"⏰ Horário: {dt.strftime('%Y-%m-%d %H:%M:%S')}")
                    except:
                        print(f"⏰ Timestamp: {game['time']}")

                # Estatísticas detalhadas por mapa
                period_stats = game.get("period_stats", {})
                if period_stats:
                    print(f"\n📈 ESTATÍSTICAS DETALHADAS:")
                    for period, stats in period_stats.items():
                        print(f"\n   🎮 Mapa {period}:")
                        for stat_name, values in stats.items():
                            if isinstance(values, list) and len(values) == 2:
                                home_val, away_val = values
                                print(
                                    f"     {stat_name.upper():<12} {home_val:>6} - {away_val:<6}"
                                )

                print("-" * 50)

            print()

    async def save_summary_json(self, all_results):
        """Salva um arquivo de resumo com todos os jogos"""
        try:
            summary_data = {
                "exported_at": datetime.now().isoformat(),
                "total_games": len(all_results),
                "leagues": {},
                "games": [],
            }

            # Organizar por liga
            for result in all_results:
                league_name = result["event_data"]["league"]["name"]
                if league_name not in summary_data["leagues"]:
                    summary_data["leagues"][league_name] = 0
                summary_data["leagues"][league_name] += 1

                # Adicionar dados do jogo
                game_info = {
                    "league": league_name,
                    "home_team": result["event_data"]["home"]["name"],
                    "away_team": result["event_data"]["away"]["name"],
                    "score": result.get("ss", "N/A"),
                    "maps": len(result.get("period_stats", {})),
                    "timestamp": result.get("time"),
                    "match_date": datetime.fromtimestamp(
                        int(result["time"])
                    ).isoformat()
                    if result.get("time")
                    else None,
                }
                summary_data["games"].append(game_info)

            # Salvar arquivo de resumo
            summary_path = os.path.join(self.output_dir, "summary_results.json")
            with open(summary_path, "w", encoding="utf-8") as f:
                json.dump(summary_data, f, indent=2, ensure_ascii=False)

            print(f"📋 Arquivo de resumo salvo: summary_results.json")

        except Exception as e:
            print(f"❌ Erro ao salvar resumo: {str(e)}")

    async def close(self):
        await self.client.close()


async def main():
    checker = LoLResultsChecker()

    try:
        # Buscar eventos dos últimos 2 dias
        print("🔍 BUSCANDO APENAS LEAGUE OF LEGENDS (LOL -)")
        print("📋 Filtro: Apenas ligas que começam com 'LOL -'")
        print("=" * 60)

        events = await checker.get_recent_lol_events(days_back=2)

        if not events:
            print("❌ Nenhum jogo de LoL encontrado")
            return

        print(f"\n✅ Total de jogos de LoL encontrados: {len(events)}")

        # Buscar resultados apenas dos finalizados
        finished_games = await checker.get_detailed_results(events)

        if not finished_games:
            print("❌ Nenhum jogo finalizado encontrado")
            return

        # Mostrar resultados detalhados
        await checker.print_detailed_results(finished_games)

        # Salvar arquivo de resumo
        await checker.save_summary_json(finished_games)

        # Estatísticas
        print(f"\n{'=' * 50}")
        print("📈 ESTATÍSTICAS FINAIS")
        print(f"{'=' * 50}")
        success_rate = (len(finished_games) / len(events)) * 100 if events else 0
        print(f"Jogos de LoL encontrados: {len(events)}")
        print(f"Jogos finalizados: {len(finished_games)}")
        print(f"Taxa de sucesso: {success_rate:.1f}%")
        print(f"Arquivos JSON salvos: {len(finished_games)}")

        # Listar ligas encontradas
        leagues_found = set()
        for game in finished_games:
            leagues_found.add(game["event_data"]["league"]["name"])

        print(f"\n🏆 Ligas encontradas:")
        for league in sorted(leagues_found):
            print(f"   • {league}")

    except Exception as e:
        print(f"❌ Erro: {str(e)}")
        import traceback

        traceback.print_exc()
    finally:
        await checker.close()


if __name__ == "__main__":
    asyncio.run(main())
