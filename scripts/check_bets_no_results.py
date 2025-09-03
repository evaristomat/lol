import sqlite3
import pandas as pd
import json
import asyncio
import httpx
import os
from datetime import datetime
from tabulate import tabulate
from dotenv import load_dotenv
from typing import Dict, Any, List, Optional

# Carregar variáveis de ambiente
load_dotenv()


class EventAnalyzer:
    def __init__(self, db_path: str):
        self.db_path = db_path

    def analyze_pending_events(self):
        """
        Analisa eventos com apostas pendentes em eventos passados
        """
        try:
            # Conectar ao banco de dados
            conn = sqlite3.connect(self.db_path)

            print("🔍 Analisando eventos com apostas pendentes em eventos passados...")

            # Consulta para obter eventos únicos com apostas pendentes
            events_query = """
            SELECT DISTINCT
                e.event_id,
                e.league_name,
                datetime(e.match_date) as match_date,
                e.home_team,
                e.away_team,
                e.status as event_status,
                e.home_score,
                e.away_score,
                e.winner,
                COUNT(b.id) as pending_bets_count
            FROM events e
            JOIN bets b ON e.event_id = b.event_id
            WHERE b.bet_status = 'pending'
            AND date(e.match_date) < date('now')
            GROUP BY e.event_id
            ORDER BY e.match_date DESC, e.league_name
            """

            df_events = pd.read_sql_query(events_query, conn)

            if df_events.empty:
                print("✅ Nenhum evento passado com apostas pendentes encontrado!")
                return None

            # Consulta para obter todas as apostas pendentes
            bets_query = """
            SELECT 
                b.id as bet_id,
                b.event_id,
                b.market_name,
                b.selection_line,
                b.handicap,
                b.house_odds,
                b.roi_average,
                b.fair_odds,
                datetime(b.created_at) as bet_created
            FROM bets b
            JOIN events e ON b.event_id = e.event_id
            WHERE b.bet_status = 'pending'
            AND date(e.match_date) < date('now')
            ORDER BY e.match_date DESC, b.event_id
            """

            df_bets = pd.read_sql_query(bets_query, conn)

            # Preparar dados para relatório
            report = {
                "generated_at": datetime.now().isoformat(),
                "database_path": self.db_path,
                "summary": {
                    "total_events": len(df_events),
                    "total_pending_bets": len(df_bets),
                    "leagues_count": df_events["league_name"].nunique(),
                },
                "events": [],
                "problematic_event_ids": df_events["event_id"].tolist(),
            }

            # Processar cada evento individualmente
            for _, event_row in df_events.iterrows():
                event_id = event_row["event_id"]

                # Obter apostas para este evento específico
                event_bets = df_bets[df_bets["event_id"] == event_id]

                event_data = {
                    "event_id": event_id,
                    "league_name": event_row["league_name"],
                    "match_date": event_row["match_date"],
                    "home_team": event_row["home_team"],
                    "away_team": event_row["away_team"],
                    "event_status": event_row["event_status"],
                    "home_score": event_row["home_score"],
                    "away_score": event_row["away_score"],
                    "winner": event_row["winner"],
                    "pending_bets_count": event_row["pending_bets_count"],
                    "pending_bets": event_bets.to_dict("records"),
                }

                report["events"].append(event_data)

            # Salvar relatório completo
            report_filename = f"detailed_pending_events_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            with open(report_filename, "w", encoding="utf-8") as f:
                json.dump(report, f, ensure_ascii=False, indent=2)

            # Salvar IDs dos eventos problemáticos
            with open("problematic_event_ids.json", "w") as f:
                json.dump(df_events["event_id"].tolist(), f)

            # Exibir resumo no console
            print("\n" + "=" * 90)
            print("DETALHAMENTO DE EVENTOS COM APOSTAS PENDENTES")
            print("=" * 90)
            print(f"Total de eventos passados com apostas pendentes: {len(df_events)}")
            print(f"Total de apostas pendentes em eventos passados: {len(df_bets)}")
            print(
                f"Ligas diferentes com problemas: {df_events['league_name'].nunique()}"
            )

            # Tabela resumo de eventos
            print("\n" + "-" * 90)
            print("RESUMO POR EVENTO (ORDENADO POR DATA MAIS RECENTE)")
            print("-" * 90)

            events_table = df_events.copy()
            events_table["match_date"] = pd.to_datetime(
                events_table["match_date"]
            ).dt.strftime("%Y-%m-%d")

            events_display = events_table[
                [
                    "event_id",
                    "league_name",
                    "match_date",
                    "home_team",
                    "away_team",
                    "event_status",
                    "pending_bets_count",
                ]
            ]

            events_display = events_display.rename(
                columns={
                    "event_id": "ID Evento",
                    "league_name": "Liga",
                    "match_date": "Data",
                    "home_team": "Time Casa",
                    "away_team": "Time Fora",
                    "event_status": "Status",
                    "pending_bets_count": "Apostas Pendentes",
                }
            )

            print(
                tabulate(
                    events_display, headers="keys", tablefmt="psql", showindex=False
                )
            )

            # Estatísticas por liga
            print("\n" + "=" * 90)
            print("ESTATÍSTICAS POR LIGA")
            print("=" * 90)

            league_stats = (
                df_events.groupby("league_name")
                .agg({"event_id": "count", "pending_bets_count": "sum"})
                .reset_index()
            )

            league_stats = league_stats.rename(
                columns={
                    "league_name": "Liga",
                    "event_id": "Eventos",
                    "pending_bets_count": "Total Apostas",
                }
            ).sort_values("Total Apostas", ascending=False)

            print(
                tabulate(league_stats, headers="keys", tablefmt="psql", showindex=False)
            )

            print(f"\n✅ Relatório completo salvo como: {report_filename}")
            print(
                "✅ IDs dos eventos problemáticos salvos em: problematic_event_ids.json"
            )

            return report

        except Exception as e:
            print(f"❌ Erro durante a análise: {str(e)}")
            return None
        finally:
            if conn:
                conn.close()


class ResultsChecker:
    def __init__(self):
        self.base_url = "https://api.betsapi.com"
        self.api_key = os.getenv("BETSAPI_API_KEY")
        if not self.api_key:
            raise ValueError("BETSAPI_API_KEY não encontrada nas variáveis de ambiente")

    async def check_event_result(self, event_id: str) -> Dict[str, Any]:
        """Verifica o resultado de um evento específico"""
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                params = {"token": self.api_key, "event_id": event_id}
                response = await client.get(
                    f"{self.base_url}/v1/bet365/result", params=params
                )
                response.raise_for_status()

                data = response.json()
                return data

        except Exception as e:
            return {"error": str(e)}

    def analyze_result_data(self, result_data: Dict[str, Any]) -> Dict[str, Any]:
        """Analisa os dados de resultado para determinar o status real do evento"""
        if "error" in result_data:
            return {
                "status": "ERROR",
                "details": f"Erro na API: {result_data['error']}",
                "time_status": None,
                "has_ss": False,
                "has_events": False,
                "has_period_stats": False,
            }

        if result_data.get("success") == 0:
            return {
                "status": "NO_RESULT",
                "details": result_data.get("error", "Erro desconhecido"),
                "time_status": None,
                "has_ss": False,
                "has_events": False,
                "has_period_stats": False,
            }

        if "results" not in result_data or not result_data["results"]:
            return {
                "status": "NO_DATA",
                "details": "Nenhum dado de resultado encontrado",
                "time_status": None,
                "has_ss": False,
                "has_events": False,
                "has_period_stats": False,
            }

        # Analisar o primeiro resultado (normalmente é o único)
        result = result_data["results"][0]
        time_status = result.get("time_status")
        ss = result.get("ss")
        events = result.get("events", [])
        period_stats = result.get("period_stats", {})

        # Determinar status baseado em time_status e dados disponíveis
        if time_status == "3":  # Evento finalizado
            if ss and ss != "0-0" and ss != "0-0-0":
                status = "HAS_RESULT"
                details = f"Evento finalizado com placar: {ss}"
            elif events and any("Winner" in event.get("text", "") for event in events):
                status = "HAS_RESULT"
                details = "Evento finalizado (Winner encontrado nos eventos)"
            elif period_stats:
                status = "HAS_RESULT"
                details = "Evento finalizado (stats de período disponíveis)"
            else:
                status = "FINALIZADO_SEM_DADOS"
                details = "Evento marcado como finalizado mas sem dados completos"

        elif time_status == "2":  # Evento ao vivo
            if ss and ss != "0-0" and ss != "0-0-0":
                status = "LIVE_WITH_SCORE"
                details = f"Evento ao vivo com placar: {ss}"
            elif events:
                status = "LIVE_WITH_EVENTS"
                details = f"Evento ao vivo com {len(events)} eventos registrados"
            else:
                status = "LIVE_NO_DATA"
                details = "Evento ao vivo sem dados detalhados"

        elif time_status == "1":  # Evento não iniciado
            status = "NOT_STARTED"
            details = "Evento não iniciado"

        else:
            status = "UNKNOWN_STATUS"
            details = f"Status desconhecido: {time_status}"

        return {
            "status": status,
            "details": details,
            "time_status": time_status,
            "has_ss": bool(ss and ss != "0-0" and ss != "0-0-0"),
            "has_events": bool(events),
            "has_period_stats": bool(period_stats),
            "ss": ss,
            "events_count": len(events),
            "period_stats_keys": list(period_stats.keys()) if period_stats else [],
        }

    async def check_problematic_events(self):
        """
        Verifica os resultados dos eventos problemáticos usando a API BetsAPI
        """
        # Verificar se a API key está disponível
        if not os.getenv("BETSAPI_API_KEY"):
            print("❌ Erro: BETSAPI_API_KEY não encontrada nas variáveis de ambiente")
            print(
                "   Certifique-se de ter um arquivo .env com BETSAPI_API_KEY=sua_chave_aqui"
            )
            return

        # Carrega eventos problemáticos
        try:
            with open("problematic_event_ids.json", "r") as f:
                event_ids = json.load(f)
        except FileNotFoundError:
            print("❌ Arquivo problematic_event_ids.json não encontrado")
            print("   Execute primeiro a análise de eventos pendentes")
            return

        if not event_ids:
            print("✅ Nenhum evento problemático encontrado")
            return

        print(f"🔍 Verificando {len(event_ids)} eventos problemáticos")
        print("=" * 80)

        results = []

        for i, event_id in enumerate(event_ids, 1):
            print(f"[{i}/{len(event_ids)}] Verificando evento: {event_id}")

            # Fazer requisição para a API
            result_data = await self.check_event_result(event_id)

            # Analisar resposta
            analysis = self.analyze_result_data(result_data)

            # Armazenar resultado
            results.append(
                {
                    "event_id": event_id,
                    **analysis,
                    "timestamp": datetime.now().isoformat(),
                    "raw_data": result_data,  # Incluir dados brutos para referência
                }
            )

            # Exibir resultado
            status_colors = {
                "HAS_RESULT": "✅",
                "FINALIZADO_SEM_DADOS": "⚠️",
                "LIVE_WITH_SCORE": "🔴",
                "LIVE_WITH_EVENTS": "🔴",
                "LIVE_NO_DATA": "🔴",
                "NOT_STARTED": "⏰",
                "NO_RESULT": "❓",
                "ERROR": "❌",
                "NO_DATA": "❓",
                "UNKNOWN_STATUS": "❓",
            }

            print(
                f"   {status_colors.get(analysis['status'], '❓')} Status: {analysis['status']}"
            )
            print(f"   📋 Detalhes: {analysis['details']}")

            if analysis["time_status"]:
                print(f"   🕒 Time Status: {analysis['time_status']}")

            if analysis["has_ss"]:
                print(f"   📊 Placar: {analysis['ss']}")

            if analysis["has_events"]:
                print(f"   📈 Eventos: {analysis['events_count']} registrados")

            if analysis["has_period_stats"]:
                print(f"   📊 Stats: {len(analysis['period_stats_keys'])} períodos")

            print("-" * 60)

            # Pequena pausa para evitar rate limit
            await asyncio.sleep(1)

        # Salvar resultados
        report = {
            "generated_at": datetime.now().isoformat(),
            "total_events": len(results),
            "results": results,
        }

        report_filename = f"detailed_events_verification_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_filename, "w", encoding="utf-8") as f:
            json.dump(report, f, ensure_ascii=False, indent=2)

        # Estatísticas finais
        status_counts = {}
        for result in results:
            status = result["status"]
            status_counts[status] = status_counts.get(status, 0) + 1

        print("\n" + "=" * 80)
        print("RELATÓRIO FINAL DETALHADO")
        print("=" * 80)

        # Agrupar por tipo de status
        final_statuses = [
            s for s in status_counts.keys() if "HAS_RESULT" in s or "FINALIZADO" in s
        ]
        live_statuses = [s for s in status_counts.keys() if "LIVE" in s]
        other_statuses = [
            s
            for s in status_counts.keys()
            if s not in final_statuses and s not in live_statuses
        ]

        print("🎯 EVENTOS FINALIZADOS:")
        for status in final_statuses:
            print(f"   {status_counts.get(status, 0)} - {status}")

        print("\n🔴 EVENTOS AO VIVO:")
        for status in live_statuses:
            print(f"   {status_counts.get(status, 0)} - {status}")

        print("\n❓ OUTROS STATUS:")
        for status in other_statuses:
            print(f"   {status_counts.get(status, 0)} - {status}")

        print(f"\n📊 Total verificado: {len(results)}")
        print(f"💾 Relatório salvo em: {report_filename}")

        return results


async def main():
    """Função principal que orquestra a análise e verificação"""
    # Configuração
    DB_PATH = "../data/bets.db"  # Ajuste o caminho conforme necessário

    print("=" * 90)
    print("SISTEMA INTEGRADO DE ANÁLISE DE EVENTOS PROBLEMÁTICOS")
    print("=" * 90)

    # Etapa 1: Análise de eventos pendentes
    print("\n📊 ETAPA 1: ANALISANDO EVENTOS COM APOSTAS PENDENTES")
    print("-" * 60)

    analyzer = EventAnalyzer(DB_PATH)
    analysis_report = analyzer.analyze_pending_events()

    if not analysis_report:
        print("❌ Falha na análise de eventos pendentes. Abortando.")
        return

    # Etapa 2: Verificação de resultados na API
    print("\n🌐 ETAPA 2: VERIFICANDO RESULTADOS NA API")
    print("-" * 60)

    checker = ResultsChecker()
    verification_results = await checker.check_problematic_events()

    print("\n" + "=" * 90)
    print("PROCESSO CONCLUÍDO")
    print("=" * 90)

    if analysis_report:
        print(f"📋 Eventos analisados: {analysis_report['summary']['total_events']}")
        print(
            f"💰 Apostas pendentes: {analysis_report['summary']['total_pending_bets']}"
        )

    if verification_results:
        has_result_count = sum(
            1 for r in verification_results if r["status"] == "HAS_RESULT"
        )
        print(
            f"✅ Eventos com resultados: {has_result_count}/{len(verification_results)}"
        )

    print("\n💡 Próximos passos:")
    print("   - Verifique os relatórios gerados para detalhes completos")
    print("   - Use os dados para resolver manualmente as apostas pendentes")
    print("   - Considere implementar atualizações automáticas baseadas nos resultados")


if __name__ == "__main__":
    # Executar o processo completo
    asyncio.run(main())
