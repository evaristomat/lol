import asyncio
from datetime import datetime
from src.core.bet365_client import Bet365Client


async def check_result():
    client = Bet365Client()

    try:
        event_id = "179764529"
        print(f"🔍 Verificando resultado para event_id: {event_id}")

        # Buscar resultado
        result_data = await client.result(event_id=event_id)

        if result_data.get("success") == 1:
            results = result_data.get("results", [])

            if results and isinstance(results, list):
                result = results[0]  # Pegar o primeiro (e único) resultado

                print("\n" + "=" * 60)
                print("🎯 RESULTADO DO JOGO - LCK CL")
                print("=" * 60)

                # Informações básicas
                print(f"🏆 Liga: {result.get('league', {}).get('name', 'N/A')}")
                print(
                    f"🏠 Casa: {result.get('o_home', {}).get('name', result.get('home', {}).get('name', 'N/A'))}"
                )
                print(f"✈️ Visitante: {result.get('away', {}).get('name', 'N/A')}")

                # Score final
                ss = result.get("ss", "")
                print(f"📊 Placar Final: {ss if ss else 'N/A'}")

                # Status e tempo
                time_status = result.get("time_status", "")
                status_map = {"1": "Não Iniciado", "2": "Ao Vivo", "3": "Finalizado"}
                print(
                    f"📋 Status: {status_map.get(time_status, f'Desconhecido ({time_status})')}"
                )

                # Timestamp
                if result.get("time"):
                    try:
                        dt = datetime.fromtimestamp(int(result["time"]))
                        print(f"⏰ Horário do Jogo: {dt.strftime('%Y-%m-%d %H:%M:%S')}")
                    except:
                        print(f"⏰ Timestamp: {result['time']}")

                # Estatísticas por período (mapas)
                period_stats = result.get("period_stats", {})
                if period_stats:
                    print(f"\n📈 ESTATÍSTICAS POR MAPA:")
                    for period, stats in period_stats.items():
                        print(f"\n   🎮 Mapa {period}:")
                        for stat_name, values in stats.items():
                            if isinstance(values, list) and len(values) == 2:
                                home_val, away_val = values
                                print(
                                    f"     {stat_name.upper()}: {home_val} - {away_val}"
                                )

                # Informações adicionais
                print(f"\n🔧 Informações Técnicas:")
                print(f"   Bet365 ID: {result.get('bet365_id')}")
                print(f"   Sport ID: {result.get('sport_id')}")
                print(f"   League ID: {result.get('league', {}).get('id')}")
                print(f"   Home ID: {result.get('home', {}).get('id')}")
                print(f"   Away ID: {result.get('away', {}).get('id')}")

            else:
                print("❌ Nenhum resultado encontrado na lista")
        else:
            print(f"❌ API retornou erro: {result_data.get('error')}")

    except Exception as e:
        print(f"❌ Erro: {str(e)}")
        import traceback

        traceback.print_exc()
    finally:
        await client.close()


async def check_multiple_results():
    """Verificar múltiplos resultados de uma vez"""
    client = Bet365Client()

    # Lista de event_ids para verificar (pode vir de um CSV)
    event_ids = ["178588919"]  # Adicione mais IDs aqui

    try:
        for event_id in event_ids:
            print(f"\n{'=' * 50}")
            print(f"🔍 Verificando evento ID: {event_id}")
            print(f"{'=' * 50}")

            result_data = await client.result(event_id=event_id)

            if result_data.get("success") == 1:
                results = result_data.get("results", [])

                if results:
                    result = results[0]
                    ss = result.get("ss", "N/A")
                    home = result.get("o_home", {}).get(
                        "name", result.get("home", {}).get("name", "N/A")
                    )
                    away = result.get("away", {}).get("name", "N/A")

                    print(f"⚔️  {home} vs {away}")
                    print(f"📊 Placar: {ss}")
                    print(f"🆔 Bet365 ID: {result.get('bet365_id')}")

                    # Verificar se o jogo foi finalizado
                    if result.get("time_status") == "3":
                        print("✅ JOGO FINALIZADO")
                    else:
                        print("⏳ Jogo ainda não finalizado")
                else:
                    print("❌ Nenhum dado de resultado")
            else:
                print(f"❌ Erro na API: {result_data.get('error')}")

            print(f"{'-' * 50}")

    except Exception as e:
        print(f"❌ Erro: {str(e)}")
    finally:
        await client.close()


if __name__ == "__main__":
    print("🧪 VERIFICADOR DE RESULTADOS BET365")
    print("=" * 60)

    # Verificar um jogo específico com detalhes
    asyncio.run(check_result())

    # Ou verificar múltiplos jogos (útil para batch)
    # asyncio.run(check_multiple_results())
