#!/usr/bin/env python3
"""
Script para investigar jogos que são sempre atualizados
Versão detalhada com mais informações sobre os eventos
"""

import asyncio
import aiohttp
import json
from datetime import datetime, timezone


def format_timestamp(timestamp):
    """Converte timestamp para formato legível"""
    if timestamp:
        try:
            dt = datetime.fromtimestamp(int(timestamp), tz=timezone.utc)
            return dt.strftime("%Y-%m-%d %H:%M:%S UTC")
        except (ValueError, TypeError):
            return f"Timestamp inválido: {timestamp}"
    return "N/A"


def get_status_description(status):
    """Retorna descrição do status do jogo"""
    status_map = {
        "0": "Não iniciado",
        "1": "Ao vivo",
        "2": "Pausado/Intervalo",
        "3": "Finalizado",
        "4": "Cancelado",
        "5": "Adiado",
    }
    return status_map.get(str(status), f"Status desconhecido: {status}")


async def check_problematic_events(events_to_check):
    """Verifica o status atual dos eventos problemáticos com detalhes"""

    API_TOKEN = "148381-MKvIM2221vISOJ"  # Substitua pelo seu token
    BASE_URL = "https://api.b365api.com/v1/bet365/result"

    async with aiohttp.ClientSession() as session:
        print(f"📊 Analisando {len(events_to_check)} eventos problemáticos...")
        print("=" * 80)

        finalized_count = 0
        ongoing_count = 0
        error_count = 0

        for i, event_id in enumerate(events_to_check, 1):
            url = f"{BASE_URL}?token={API_TOKEN}&event_id={event_id}"

            try:
                async with session.get(url) as response:
                    data = await response.json()

                    if data.get("success") == 1 and data.get("results"):
                        event_data = data["results"][0]

                        # Extrair informações básicas
                        status = event_data.get("time_status")
                        score = event_data.get("ss", "N/A")
                        timestamp = event_data.get("time")

                        # Informações dos times
                        home_team = event_data.get("home", {})
                        away_team = event_data.get("away", {})
                        league = event_data.get("league", {})

                        print(
                            f"\n🎮 EVENTO {i}/{len(events_to_check)} - ID: {event_id}"
                        )
                        print(f"   🏆 Liga: {league.get('name', 'N/A')}")
                        print(
                            f"   ⚔️  Times: {home_team.get('name', 'N/A')} vs {away_team.get('name', 'N/A')}"
                        )
                        print(f"   📅 Data/Hora: {format_timestamp(timestamp)}")
                        print(
                            f"   📊 Status: {get_status_description(status)} ({status})"
                        )
                        print(f"   🏅 Placar: {score}")

                        # Contar por status
                        if str(status) == "3":
                            finalized_count += 1
                            print(f"   ✅ FINALIZADO")
                        elif str(status) in ["1", "2"]:
                            ongoing_count += 1
                            print(f"   🔄 EM ANDAMENTO/PAUSADO")

                        # Verificar estatísticas detalhadas
                        period_stats = event_data.get("period_stats", {})
                        if period_stats:
                            print(f"   📈 Estatísticas: {len(period_stats)} mapas")
                            for map_num, stats in period_stats.items():
                                print(
                                    f"      🗺️  Mapa {map_num}: {len(stats)} estatísticas"
                                )
                                # Mostrar algumas estatísticas importantes
                                for stat_name, values in list(stats.items())[:3]:
                                    if isinstance(values, list) and len(values) == 2:
                                        print(
                                            f"         • {stat_name}: {values[0]} - {values[1]}"
                                        )
                        else:
                            print(f"   📈 Estatísticas: Nenhuma disponível")

                        # Verificar scores por período
                        scores = event_data.get("scores", {})
                        if scores:
                            print(f"   🏆 Scores por período:")
                            for period, score_data in scores.items():
                                home_score = score_data.get("home", "0")
                                away_score = score_data.get("away", "0")
                                print(
                                    f"      • Período {period}: {home_score}-{away_score}"
                                )

                        # Verificar eventos do jogo
                        events = event_data.get("events", [])
                        if events:
                            print(
                                f"   📋 Eventos do jogo: {len(events)} eventos registrados"
                            )
                            # Mostrar os últimos 3 eventos
                            for event in events[-3:]:
                                print(
                                    f"      • {event.get('text', 'Evento sem descrição')}"
                                )

                        # Informações extras
                        extra = event_data.get("extra", {})
                        if extra:
                            print(f"   ℹ️  Informações extras:")
                            for key, value in extra.items():
                                print(f"      • {key}: {value}")

                    else:
                        print(
                            f"\n❌ EVENTO {i}/{len(events_to_check)} - ID: {event_id}"
                        )
                        print(f"   Erro ao buscar: {data}")
                        error_count += 1

            except Exception as e:
                print(f"\n❌ EVENTO {i}/{len(events_to_check)} - ID: {event_id}")
                print(f"   Exceção: {e}")
                error_count += 1

            await asyncio.sleep(1)  # Respeitar rate limit

        # Resumo final
        print("\n" + "=" * 80)
        print("📊 RESUMO DA INVESTIGAÇÃO:")
        print(f"   📅 Total de eventos analisados: {len(events_to_check)}")
        print(f"   ✅ Eventos finalizados: {finalized_count}")
        print(f"   🔄 Eventos em andamento/pausados: {ongoing_count}")
        print(f"   ❌ Erros: {error_count}")
        print(
            f"   📈 Taxa de finalização: {finalized_count / len(events_to_check) * 100:.1f}%"
        )

        # Análise do problema
        print("\n🔍 ANÁLISE DO PROBLEMA:")
        if ongoing_count > 0:
            print(f"   ⚠️  {ongoing_count} eventos ainda não finalizados")
            print(f"   🔄 Estes podem estar causando atualizações desnecessárias")
            print(
                f"   💡 Considere implementar lógica para ignorar eventos antigos não finalizados"
            )

        if finalized_count > 0:
            print(f"   ✅ {finalized_count} eventos estão corretamente finalizados")
            print(
                f"   🤔 Se estes estão sendo atualizados, há um bug na lógica de verificação"
            )


# Lista de eventos problemáticos dos logs (originais - eventos "fantasma")
original_problematic_events = [
    179606652,  # DRX.Ch vs KT Rolster.Ch
    179606653,  # Nongshim.EA vs Hanwha Life Esports.Ch
    179606650,  # Gen.G.GA vs T1.EA
    179606651,  # DN Freecs.Ch vs Dplus KIA.Ch
    179606648,  # KT Rolster.Ch vs BNK FearX.Y
    179606649,  # OKSavingsBank BRION.Ch vs Nongshim.EA
]

# Eventos que estão sendo constantemente atualizados (dos logs recentes)
constantly_updated_events = [
    # Eventos de 22/08/2025
    179606654,  # T1.EA vs BNK FearX.Y
    179606655,  # Dplus KIA.Ch vs OKSavingsBank BRION.Ch
    179712948,  # Invictus Gaming vs FunPlus Phoenix
    179715709,  # Dplus KIA vs DRX
    179715872,  # CTBC Flying Oyster vs GAM Esports
    179606993,  # Misa Esports vs The Forbidden Five
    179606996,  # Bushido Wildcats vs Dark Passage
    179693611,  # Ici Japon Corp. Esport vs Team BDS.A
    179997910,  # Rich Gang vs Verdant
    179606997,  # Papara SuperMassive vs BoostGate Esports
    # Eventos LCK CL que sempre são atualizados
    179217632,  # KT Rolster.Ch vs DRX.Ch
    179217634,  # Nongshim.EA vs Dplus KIA.Ch
    179217628,  # Gen.G.GA vs BNK FearX.Y
    179217631,  # DN Freecs.Ch vs OKSavingsBank BRION.Ch
    179217626,  # Nongshim.EA vs Hanwha Life Esports.Ch
    179217627,  # KT Rolster.Ch vs T1.EA
    # Eventos de outras ligas que são constantemente atualizados
    179764529,  # Deep Cross Gaming vs Frank Esports (PCS)
    179764528,  # West Point Esports vs CFO Academy (PCS)
    179764527,  # HELL PIGS vs PSG Talon.A (PCS)
    179784702,  # Geekay Esports vs GNG Amazigh (Arabian League)
    179784701,  # Anubis Gaming vs FN Esports (Arabian League)
    178588918,  # GNG Amazigh vs Anubis Gaming (Arabian League)
    178588919,  # FN Esports vs Geekay (Arabian League)
    # Eventos de ligas menores
    179118946,  # KaBuM! IDL vs Corinthians Esports (CD Split 2)
    178950944,  # Frank Esports vs HELL PIGS (PCS)
    179310209,  # TeamOrangeGaming vs Kaufland HK (Prime League)
    178351322,  # Deep Cross Gaming vs West Point Esports (PCS)
    179368787,  # Unicorns of Love SE vs AF willhaben (Prime League)
    # Eventos japoneses
    178892801,  # QT DIG vs Burning Core Toyama (LJL)
    178246749,  # REJECT vs DFM Academy (LJL)
    178246748,  # Burning Core Toyama vs Yang Yang Gaming (LJL)
]

# Eventos diversos que aparecem nos logs
additional_events = [
    # Eventos europeus de ligas secundárias
    179503965,  # Nightbirds vs Tan'i eSports CZ
    179503964,  # eSuba vs Entropiq
    178893305,  # Zerance Bloom vs Galions Pearl
    179338281,  # Solary Academy vs Vitality Rising Bees
    178440003,  # Nexus Reapers vs Vitality Rising Bees
    178847819,  # BNK FearX.Y vs DRX.Ch
    178847820,  # Hanwha Life Esports.Ch vs Dplus KIA.Ch
    178847817,  # DN Freecs.Ch vs Nongshim.EA
    178847818,  # Gen.G.GA vs T1.EA
    178797834,  # EKO eSports vs Axolotl
    178847814,  # OKSavingsBank BRION.Ch vs Dplus KIA.Ch
    178847816,  # BNK FearX.Y vs KT Rolster.Ch
    # Eventos brasileiros
    179544627,  # RED Canids.A vs KaBuM! IDL
    179544628,  # Corinthians Esports vs Alpha7 Esports
    178790447,  # Rise Gaming vs STELLAE Gaming
    178790437,  # STELLAE Gaming vs RED Canids.A
    178950943,  # CFO Academy vs wangting
    178351321,  # PSG Talon.A vs Ground Zero Gaming
    178741467,  # JD Gaming vs Weibo Gaming
    178790438,  # Flamenco MDL vs Vivo Keyd Stars.A
    178790439,  # Corinthians Esports vs e-Champ Gaming
    178790440,  # KaBuM! IDL vs Alpha7 Esports
    178790441,  # Los Grandes vs Rise Gaming
    # Eventos da LCK CL históricos
    178459579,  # Hanwha Life Esports.Ch vs Nongshim.EA
    178459580,  # DRX.Ch vs BNK FearX.Y
    178459576,  # T1.EA vs KT Rolster.Ch
    178459578,  # OKSavingsBank BRION.Ch vs DN Freecs.Ch
    178459574,  # Gen.G.GA vs DRX.Ch
    178459575,  # Dplus KIA.Ch vs Hanwha Life Esports.Ch
    178847808,  # KT Rolster.Ch vs Gen.G.GA
    178847810,  # Nongshim.EA vs OKSavingsBank BRION.Ch
    178847812,  # DN Freecs.Ch vs Hanwha Life Esports.Ch
    178847813,  # DRX.Ch vs T1.EA
    # Eventos diversos de playoffs
    178797832,  # Colossal Gaming vs Zena Esports
    178819242,  # Colossal Gaming vs Zena Esports (repetido)
    178623470,  # CGN Esports vs Eintracht Spandau
    178352870,  # Alpha7 Esports vs Corinthians Esports
]

# Combinar todos os eventos para análise
all_events = original_problematic_events + constantly_updated_events + additional_events


async def main():
    """Função principal"""
    print("🔍 INVESTIGAÇÃO DETALHADA DE EVENTOS PROBLEMÁTICOS")
    print(f"⏰ Executado em: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Opção de escolher qual conjunto analisar
    print("\nEscolha qual conjunto de eventos analisar:")
    print("1. Eventos 'fantasma' originais (6 eventos)")
    print("2. Eventos constantemente atualizados (35+ eventos)")
    print("3. Eventos adicionais diversos (40+ eventos)")
    print("4. TODOS os eventos (80+ eventos)")
    print("5. Amostra representativa (20 eventos)")

    choice = input("Digite sua escolha (1-5): ").strip()

    if choice == "1":
        events_to_check = original_problematic_events
        print(f"\n🎯 Analisando {len(events_to_check)} eventos 'fantasma' originais...")
    elif choice == "2":
        events_to_check = constantly_updated_events
        print(
            f"\n🔄 Analisando {len(events_to_check)} eventos constantemente atualizados..."
        )
    elif choice == "3":
        events_to_check = additional_events
        print(f"\n📋 Analisando {len(events_to_check)} eventos adicionais diversos...")
    elif choice == "4":
        events_to_check = all_events
        print(f"\n🌐 Analisando TODOS os {len(events_to_check)} eventos...")
    elif choice == "5":
        # Amostra representativa: alguns de cada categoria
        events_to_check = (
            original_problematic_events[:3]
            + constantly_updated_events[:10]
            + additional_events[:7]
        )
        print(
            f"\n📊 Analisando amostra representativa de {len(events_to_check)} eventos..."
        )
    else:
        print("❌ Escolha inválida. Analisando eventos originais...")
        events_to_check = original_problematic_events

    await check_problematic_events(events_to_check)

    print("\n💡 RECOMENDAÇÕES BASEADAS NA ANÁLISE:")
    print("   1. Eventos com status 2 (pausado) podem estar 'presos' na API")
    print("   2. Considere implementar timeout para eventos muito antigos")
    print(
        "   3. Adicione lógica para detectar eventos 'fantasma' (pausados há muito tempo)"
    )
    print("   4. Implemente cache mais inteligente baseado na idade do evento")
    print(
        "   5. Eventos de ligas menores (LCK CL, PCS, Arabian League) são mais propensos a problemas"
    )
    print("   6. Considere intervalos de atualização diferentes por tipo de liga")
    print("   7. Eventos de playoffs/torneios podem ter status instáveis")
    print("\n🔧 AÇÕES SUGERIDAS:")
    print("   • Execute o script de limpeza para eventos com status 2 antigos")
    print("   • Implemente o método needs_update() melhorado")
    print("   • Configure intervalos de atualização por liga")
    print("   • Monitore eventos que ficam em status 2 por mais de 24h")


if __name__ == "__main__":
    asyncio.run(main())
