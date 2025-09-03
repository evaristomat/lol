import sqlite3
import pandas as pd
from datetime import datetime
from tabulate import tabulate
import json


def analyze_pending_events(db_path):
    """
    Analisa detalhadamente cada evento único com apostas pendentes em eventos passados
    """
    try:
        # Conectar ao banco de dados
        conn = sqlite3.connect(db_path)

        print(
            "🔍 Analisando eventos únicos com apostas pendentes em eventos passados..."
        )

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
            "database_path": db_path,
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
        report_filename = (
            f"detailed_pending_events_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        )
        with open(report_filename, "w", encoding="utf-8") as f:
            json.dump(report, f, ensure_ascii=False, indent=2)

        # Salvar IDs dos eventos problemáticos
        with open("problematic_event_ids.json", "w") as f:
            json.dump(df_events["event_id"].tolist(), f)

        # Exibir resumo no console
        print("\n" + "=" * 90)
        print("DETALHAMENTO DE EVENTOS ÚNICOS COM APOSTAS PENDENTES")
        print("=" * 90)
        print(f"Total de eventos passados com apostas pendentes: {len(df_events)}")
        print(f"Total de apostas pendentes em eventos passados: {len(df_bets)}")
        print(f"Ligas diferentes com problemas: {df_events['league_name'].nunique()}")

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
            tabulate(events_display, headers="keys", tablefmt="psql", showindex=False)
        )

        # Detalhar alguns eventos como exemplo
        print("\n" + "=" * 90)
        print("DETALHES DE ALGUNS EVENTOS (EXEMPLOS)")
        print("=" * 90)

        # Selecionar alguns eventos para detalhar (os 5 mais recentes)
        sample_events = df_events.head(5)

        for _, event_row in sample_events.iterrows():
            event_id = event_row["event_id"]
            event_bets = df_bets[df_bets["event_id"] == event_id]

            print(f"\n📋 Evento: {event_row['league_name']}")
            print(f"   ⚽ {event_row['home_team']} vs {event_row['away_team']}")
            print(f"   📅 {event_row['match_date']}")
            print(f"   🏷️  Status: {event_row['event_status']}")
            print(
                f"   📊 Placar: {event_row['home_score'] or 'N/A'} - {event_row['away_score'] or 'N/A'}"
            )
            print(f"   🏆 Vencedor: {event_row['winner'] or 'N/A'}")
            print(f"   💰 Apostas pendentes: {event_row['pending_bets_count']}")

            if not event_bets.empty:
                print("   📋 Detalhes das apostas:")

                bets_display = event_bets[
                    [
                        "bet_id",
                        "market_name",
                        "selection_line",
                        "handicap",
                        "house_odds",
                    ]
                ]

                bets_display = bets_display.rename(
                    columns={
                        "bet_id": "ID",
                        "market_name": "Mercado",
                        "selection_line": "Seleção",
                        "handicap": "Handicap",
                        "house_odds": "Odds",
                    }
                )

                print(
                    tabulate(
                        bets_display, headers="keys", tablefmt="psql", showindex=False
                    )
                )

            print("-" * 70)

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

        print(tabulate(league_stats, headers="keys", tablefmt="psql", showindex=False))

        # Recomendações
        print("\n" + "=" * 90)
        print("RECOMENDAÇÕES POR PRIORIDADE")
        print("=" * 90)

        # Priorizar eventos mais antigos
        oldest_events = df_events.sort_values("match_date").head(3)

        for i, (_, event_row) in enumerate(oldest_events.iterrows(), 1):
            print(
                f"{i}. Priorizar evento {event_row['event_id']} ({event_row['league_name']}) - "
                f"{event_row['match_date']} - {event_row['pending_bets_count']} apostas pendentes"
            )

        print(f"\n✅ Relatório completo salvo como: {report_filename}")
        print("✅ IDs dos eventos problemáticos salvos em: problematic_event_ids.json")

        return report

    except Exception as e:
        print(f"❌ Erro durante a análise: {str(e)}")
        return None
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    # Configuração
    DB_PATH = "../data/bets.db"  # Ajuste o caminho conforme necessário

    print("=" * 90)
    print("ANÁLISE DETALHADA DE EVENTOS ÚNICOS COM APOSTAS PENDENTES")
    print("=" * 90)

    # Executar análise
    report = analyze_pending_events(DB_PATH)

    if report:
        print(f"\n📋 Resumo final:")
        print(f"   - Eventos problemáticos: {report['summary']['total_events']}")
        print(
            f"   - Apostas pendentes totais: {report['summary']['total_pending_bets']}"
        )
        print(f"   - Ligas diferentes afetadas: {report['summary']['leagues_count']}")

    print("\nAnálise detalhada concluída! ✅")
    print(
        "\nNota: Este é apenas um relatório visual. Nenhuma alteração foi feita no banco de dados."
    )
