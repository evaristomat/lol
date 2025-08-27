import sqlite3
import pandas as pd
import numpy as np
import warnings
from datetime import datetime
import os

warnings.filterwarnings("ignore")


class BettingAnalysisDB:
    def __init__(self, db_path="data/bets.db"):
        self.db_path = db_path
        if not os.path.exists(db_path):
            raise FileNotFoundError(f"Banco de dados não encontrado: {db_path}")

    def load_data(self):
        """Carrega dados do banco de dados e prepara DataFrame"""
        conn = sqlite3.connect(self.db_path)

        # Query correta baseada na estrutura real
        query = """
        SELECT 
            b.id,
            b.event_id,
            b.market_name,
            b.selection_line,
            b.handicap,
            b.house_odds as odds,
            b.roi_average as estimated_roi,
            b.fair_odds,
            b.actual_value,
            b.bet_status,
            b.stake,
            b.potential_win,
            b.actual_win,
            b.result_verified,
            b.created_at,
            e.league_name,
            e.match_date,
            e.home_team,
            e.away_team,
            e.status as event_status,
            e.winner
        FROM bets b
        JOIN events e ON b.event_id = e.event_id
        WHERE b.result_verified = 1
        ORDER BY e.match_date DESC
        """

        df = pd.read_sql_query(query, conn)
        conn.close()

        # Preparar dados
        df["match_date"] = pd.to_datetime(df["match_date"])
        df["profit"] = df["actual_win"]  # actual_win já é o lucro real
        df["status"] = df["bet_status"].map({"won": "win", "lost": "lose"})
        df["market"] = df["market_name"] + " - " + df["selection_line"]
        df["league"] = df["league_name"]

        print(f"Dados carregados: {len(df)} apostas verificadas")
        print(
            f"Período: {df['match_date'].min().strftime('%d/%m/%Y')} a {df['match_date'].max().strftime('%d/%m/%Y')}"
        )

        return df

    def simplified_betting_analysis(self):
        """Análise completa de apostas focada em mercados, ROI e ligas"""

        df = self.load_data()

        if df.empty:
            print("Nenhuma aposta verificada encontrada no banco de dados!")
            return {}

        print("=" * 100)
        print("ANÁLISE COMPLETA DE APOSTAS - IDENTIFICAÇÃO DE MERCADOS LUCRATIVOS")
        print("=" * 100)

        # Salvar dados originais
        df_original = df.copy()
        initial_bets = len(df)
        initial_profit = df["profit"].sum()
        initial_win_rate = df["status"].eq("win").mean() * 100
        initial_roi = (initial_profit / initial_bets * 100) if initial_bets > 0 else 0

        print(f"\nESTATÍSTICAS INICIAIS (TODAS AS APOSTAS):")
        print(f"   Total de apostas: {initial_bets}")
        print(f"   Lucro total: {initial_profit:.2f} unidades")
        print(f"   Win rate: {initial_win_rate:.1f}%")
        print(f"   ROI real: {initial_roi:.1f}%")

        # ========================================================================
        # 1. ANÁLISE DE FAIXAS DE ODDS - FILTRO PRINCIPAL
        # ========================================================================
        print(f"\n{'=' * 100}")
        print("1. ANÁLISE DE FAIXAS DE ODDS - FILTRO PRINCIPAL")
        print("=" * 100)

        def categorize_odds_detailed(odds):
            if pd.isna(odds):
                return "N/A"
            elif odds <= 1.30:
                return "1.00-1.30"
            elif odds <= 1.50:
                return "1.30-1.50"
            elif odds <= 1.70:
                return "1.50-1.70"
            elif odds <= 1.90:
                return "1.70-1.90"
            elif odds <= 2.10:
                return "1.90-2.10"
            elif odds <= 2.50:
                return "2.10-2.50"
            elif odds <= 3.00:
                return "2.50-3.00"
            else:
                return "3.00+"

        df["odds_range"] = df["odds"].apply(categorize_odds_detailed)

        odds_analysis = (
            df.groupby("odds_range")
            .agg(
                {
                    "profit": ["sum", "count"],
                    "status": lambda x: (x == "win").mean() * 100,
                }
            )
            .round(2)
        )

        odds_analysis.columns = ["Total_Profit", "Bets", "Win_Rate"]
        odds_analysis["ROI"] = (
            odds_analysis["Total_Profit"] / odds_analysis["Bets"] * 100
        ).round(1)

        odds_order = [
            "1.00-1.30",
            "1.30-1.50",
            "1.50-1.70",
            "1.70-1.90",
            "1.90-2.10",
            "2.10-2.50",
            "2.50-3.00",
            "3.00+",
        ]
        odds_analysis = odds_analysis.reindex(
            [o for o in odds_order if o in odds_analysis.index]
        )

        print(f"\nPERFORMANCE POR FAIXA DE ODDS (TODAS AS APOSTAS):")
        print(
            f"{'Faixa Odds':<12} {'Lucro':<10} {'Apostas':<10} {'ROI':<10} {'Win Rate':<10}"
        )
        print("-" * 62)

        for odds_range, row in odds_analysis.iterrows():
            icon = "💎" if row["ROI"] > 15 else "✅" if row["ROI"] > 0 else "❌"
            percentage = row["Bets"] / len(df) * 100
            print(
                f"{icon} {odds_range:<10} {row['Total_Profit']:>8.2f} "
                f"{int(row['Bets']):>8} ({percentage:>4.1f}%) {row['ROI']:>8.1f}% {row['Win_Rate']:>8.1f}%"
            )

        # APLICAR FILTRO DE ODDS >= 1.50
        print(f"\n*** APLICANDO FILTRO PRINCIPAL: ODDS >= 1.50 ***")
        df = df[df["odds"] >= 1.50].copy()

        removed_bets = initial_bets - len(df)
        removed_profit = df_original[df_original["odds"] < 1.50]["profit"].sum()

        print(
            f"   Apostas removidas: {removed_bets} ({removed_bets / initial_bets * 100:.1f}%)"
        )
        print(f"   Prejuízo evitado: {removed_profit:.2f}u")
        print(f"   Apostas restantes: {len(df)}")

        # Recalcular estatísticas após filtro
        total_bets = len(df)
        total_profit = df["profit"].sum()
        win_rate = df["status"].eq("win").mean() * 100
        roi_real_total = (total_profit / total_bets * 100) if total_bets > 0 else 0

        print(f"\nESTATÍSTICAS APÓS FILTRO DE ODDS >= 1.50:")
        print(f"   Total de apostas: {total_bets}")
        print(f"   Lucro total: {total_profit:.2f} unidades")
        print(f"   Win rate: {win_rate:.1f}%")
        print(f"   ROI real: {roi_real_total:.1f}%")
        print(
            f"   Melhoria no ROI: {roi_real_total - initial_roi:+.1f} pontos percentuais"
        )

        # ========================================================================
        # 2. ANÁLISE DETALHADA POR MERCADO (DIVIDIDA POR MAPA)
        # ========================================================================
        print(f"\n{'=' * 100}")
        print("2. ANÁLISE DETALHADA POR MERCADO - DIVIDIDA POR MAPA")
        print("=" * 100)

        # Análise por mapa separadamente
        for map_name in ["Map 1", "Map 2"]:
            map_df = df[df["market_name"].str.contains(map_name)]

            if len(map_df) == 0:
                continue

            print(f"\n{map_name.upper()} - MERCADOS COM >=5 APOSTAS:")
            print(f"{'Mercado':<45} {'Lucro':<8} {'Apostas':<8} {'ROI':<8} {'WR':<8}")
            print("-" * 77)

            market_analysis_map = (
                map_df.groupby("market")
                .agg(
                    {
                        "profit": ["sum", "count"],
                        "status": lambda x: (x == "win").mean() * 100,
                        "odds": "mean",
                    }
                )
                .round(2)
            )

            market_analysis_map.columns = [
                "Total_Profit",
                "Bets",
                "Win_Rate",
                "Avg_Odds",
            ]
            market_analysis_map["ROI_Real"] = (
                market_analysis_map["Total_Profit"] / market_analysis_map["Bets"] * 100
            ).round(1)
            market_analysis_map = market_analysis_map.sort_values(
                "Total_Profit", ascending=False
            )

            # Filtrar apenas mercados com pelo menos 5 apostas
            relevant_markets_map = market_analysis_map[market_analysis_map["Bets"] >= 5]

            for market, row in relevant_markets_map.iterrows():
                icon = (
                    "💎"
                    if row["Total_Profit"] > 10
                    else "✅"
                    if row["Total_Profit"] > 0
                    else "❌"
                )
                market_short = market.replace(f"{map_name} - Totals - ", "")
                print(
                    f"{icon} {market_short[:43]:<43} {row['Total_Profit']:>6.2f} "
                    f"{int(row['Bets']):>7} {row['ROI_Real']:>6.1f}% {row['Win_Rate']:>6.1f}%"
                )

        # Consolidar mercados lucrativos de ambos os mapas
        market_analysis = (
            df.groupby("market")
            .agg(
                {
                    "profit": ["sum", "count"],
                    "status": lambda x: (x == "win").mean() * 100,
                    "odds": "mean",
                }
            )
            .round(2)
        )

        market_analysis.columns = ["Total_Profit", "Bets", "Win_Rate", "Avg_Odds"]
        market_analysis["ROI_Real"] = (
            market_analysis["Total_Profit"] / market_analysis["Bets"] * 100
        ).round(1)
        market_analysis = market_analysis.sort_values("Total_Profit", ascending=False)

        # Filtrar apenas mercados com pelo menos 5 apostas
        relevant_markets = market_analysis[market_analysis["Bets"] >= 5]
        profitable_markets = relevant_markets[
            relevant_markets["Total_Profit"] > 0
        ].index.tolist()

        print(f"\nRESUMO CONSOLIDADO:")
        print(f"MERCADOS LUCRATIVOS (>=5 apostas): {len(profitable_markets)}")
        print(
            f"MERCADOS COM PREJUÍZO: {len(relevant_markets) - len(profitable_markets)}"
        )

        # ========================================================================
        # 3. ANÁLISE DE FAIXAS DE ROI
        # ========================================================================
        print(f"\n{'=' * 100}")
        print("3. ANÁLISE POR FAIXAS DE ROI (APENAS MERCADOS LUCRATIVOS)")
        print("=" * 100)

        df_profitable_markets = df[df["market"].isin(profitable_markets)]

        print(
            f"\nApostas em mercados lucrativos: {len(df_profitable_markets)} ({len(df_profitable_markets) / total_bets * 100:.1f}% do total)"
        )

        roi_ranges = [0, 10, 15, 20, 25, 30, 40, 50]
        roi_analysis = []

        print(f"\nPERFORMANCE POR FAIXA DE ROI (MERCADOS LUCRATIVOS):")
        print(
            f"{'Faixa ROI':<12} {'Apostas':<10} {'Lucro':<12} {'ROI Real':<10} {'Win Rate':<10}"
        )
        print("-" * 66)

        for min_roi in roi_ranges:
            df_roi = df_profitable_markets[
                df_profitable_markets["estimated_roi"] >= min_roi
            ]
            if len(df_roi) > 0:
                roi_profit = df_roi["profit"].sum()
                roi_bets = len(df_roi)
                roi_wr = (df_roi["status"] == "win").mean() * 100
                roi_real = (roi_profit / roi_bets * 100) if roi_bets > 0 else 0

                if roi_real > 5:
                    roi_analysis.append(min_roi)

                icon = "✅" if roi_real > 5 else "❌"
                print(
                    f"{icon} {f'{min_roi}%+':<10} {roi_bets:<10} {roi_profit:>11.2f} "
                    f"{roi_real:>9.1f}% {roi_wr:>9.1f}%"
                )

        min_roi_threshold = roi_analysis[0] if roi_analysis else 0
        print(f"\nFAIXA DE ROI RECOMENDADA: {min_roi_threshold}%+")

        # ========================================================================
        # 4. ANÁLISE POR MAPAS
        # ========================================================================
        print(f"\n{'=' * 100}")
        print("4. ANÁLISE POR MAPAS (APÓS FILTRO DE ODDS)")
        print("=" * 100)

        df["map"] = df["market_name"].str.extract(r"(Map \d+)")

        map_analysis = (
            df.groupby("map")
            .agg(
                {
                    "profit": ["sum", "count"],
                    "status": lambda x: (x == "win").mean() * 100,
                }
            )
            .round(2)
        )

        map_analysis.columns = ["Total_Profit", "Bets", "Win_Rate"]
        map_analysis["ROI_Real"] = (
            map_analysis["Total_Profit"] / map_analysis["Bets"] * 100
        ).round(1)

        print(f"\nPERFORMANCE POR MAPA (ODDS >= 1.50):")
        print(
            f"{'Mapa':<15} {'Lucro':<10} {'Apostas':<10} {'ROI Real':<10} {'Win Rate':<10}"
        )
        print("-" * 60)

        for map_name, row in map_analysis.iterrows():
            icon = (
                "💎"
                if row["Total_Profit"] > 20
                else "✅"
                if row["Total_Profit"] > 0
                else "❌"
            )
            print(
                f"{icon} {map_name:<13} {row['Total_Profit']:>8.2f} "
                f"{int(row['Bets']):>9} {row['ROI_Real']:>9.1f}% {row['Win_Rate']:>9.1f}%"
            )

        print(f"\nANÁLISE DETALHADA POR MAPA E TIPO:")
        for map_name in ["Map 1", "Map 2"]:
            if map_name in df["map"].values:
                map_df = df[df["map"] == map_name]

                over_df = map_df[map_df["selection_line"].str.contains("Over")]
                under_df = map_df[map_df["selection_line"].str.contains("Under")]

                print(f"\n{map_name}:")
                print(
                    f"   Total: {len(map_df)} apostas | Lucro: {map_df['profit'].sum():.2f}u"
                )

                if len(over_df) > 0:
                    over_profit = over_df["profit"].sum()
                    over_roi = over_profit / len(over_df) * 100
                    over_wr = (over_df["status"] == "win").mean() * 100
                    print(
                        f"   Over: {len(over_df)} apostas | Lucro: {over_profit:.2f}u | ROI: {over_roi:.1f}% | WR: {over_wr:.1f}%"
                    )

                if len(under_df) > 0:
                    under_profit = under_df["profit"].sum()
                    under_roi = under_profit / len(under_df) * 100
                    under_wr = (under_df["status"] == "win").mean() * 100
                    print(
                        f"   Under: {len(under_df)} apostas | Lucro: {under_profit:.2f}u | ROI: {under_roi:.1f}% | WR: {under_wr:.1f}%"
                    )

        # ========================================================================
        # 5. ANÁLISE POR LIGAS AGRUPADAS
        # ========================================================================
        print(f"\n{'=' * 100}")
        print("5. ANÁLISE POR LIGAS AGRUPADAS (APENAS MERCADOS LUCRATIVOS)")
        print("=" * 100)

        # Mapeamento de ligas similares
        league_mapping = {
            "LOL - LCK": ["LCK", "LOL - LCK"],
            "LPL": ["LPL", "LOL - LPL Split 3"],
            "LEC": ["LEC", "LOL - LEC Summer"],
            "TCL": ["TCL", "LOL - TCL Summer"],
            "LCP": ["LCP", "LOL - LCP Season Finals"],
            "NLC": ["NLC", "LOL - NLC Summer Playoffs"],
            "LTA N": [
                "LTA N",
                "LOL - LTA North Split 3",
            ],
            "LTA S": [
                "LTA S",
                "LOL - LTA South Split 3",
            ],
            "ROL": ["ROL", "LOL - ROL Summer"],
            "LFL": ["LFL", "LOL - LFL Summer"],
            "EBL": ["EBL", "LOL - EBL Summer Playoffs"],
            "NACL": ["NACL", "LOL - NACL Split 2 Playoffs"],
            "LCK CL": ["LCKC", "LOL - LCK CL Rounds 3-5"],
        }

        # Criar mapeamento reverso
        reverse_mapping = {}
        for group, leagues in league_mapping.items():
            for league in leagues:
                reverse_mapping[league] = group

        # Aplicar agrupamento
        df_profitable_markets["league_group"] = df_profitable_markets["league"].map(
            reverse_mapping
        )
        df_profitable_markets["league_group"] = df_profitable_markets[
            "league_group"
        ].fillna(df_profitable_markets["league"])

        league_analysis = (
            df_profitable_markets.groupby("league_group")
            .agg(
                {
                    "profit": ["sum", "count"],
                    "status": lambda x: (x == "win").mean() * 100,
                }
            )
            .round(2)
        )

        league_analysis.columns = ["Total_Profit", "Bets", "Win_Rate"]
        league_analysis["ROI_Real"] = (
            league_analysis["Total_Profit"] / league_analysis["Bets"] * 100
        ).round(1)
        league_analysis = league_analysis.sort_values("Total_Profit", ascending=False)

        profitable_leagues = league_analysis[
            league_analysis["Total_Profit"] > 0
        ].index.tolist()

        print(f"\nTODAS AS LIGAS AGRUPADAS (EM MERCADOS LUCRATIVOS):")
        print(
            f"{'Liga Agrupada':<30} {'Lucro':<10} {'Apostas':<10} {'ROI Real':<10} {'Win Rate':<10}"
        )
        print("-" * 70)

        for league, row in league_analysis.iterrows():
            icon = (
                "💎"
                if row["Total_Profit"] > 20
                else "✅"
                if row["Total_Profit"] > 0
                else "❌"
            )
            print(
                f"{icon} {league[:28]:<28} {row['Total_Profit']:>8.2f} "
                f"{int(row['Bets']):>9} {row['ROI_Real']:>9.1f}% {row['Win_Rate']:>9.1f}%"
            )

        print(f"\n💎 LIGAS LUCRATIVAS AGRUPADAS: {len(profitable_leagues)}")
        print(
            f"❌ LIGAS COM PREJUÍZO: {len(league_analysis) - len(profitable_leagues)}"
        )

        # Detalhamento dos grupos principais
        print(f"\n🔍 DETALHAMENTO DOS GRUPOS PRINCIPAIS:")
        major_leagues = ["LCK", "LPL", "LEC", "TCL", "LCP", "LTA"]

        for major in major_leagues:
            if major in league_analysis.index:
                row = league_analysis.loc[major]
                component_leagues = league_mapping.get(major, [major])

                print(f"\n📋 {major}:")
                print(f"   Componentes: {', '.join(component_leagues)}")
                print(f"   Lucro total: {row['Total_Profit']:.2f}u")
                print(f"   Apostas: {int(row['Bets'])}")
                print(f"   ROI: {row['ROI_Real']:.1f}%")
                print(f"   Win Rate: {row['Win_Rate']:.1f}%")

        # ========================================================================
        # 6. APLICAR FILTROS E COMPARAR
        # ========================================================================
        print(f"\n{'=' * 100}")
        print("6. APLICAÇÃO DOS FILTROS")
        print("=" * 100)

        df_filtered = df.copy()
        print(f"\nApós filtro de ODDS >= 1.50: {len(df_filtered)} apostas")

        df_filtered = df_filtered[df_filtered["market"].isin(profitable_markets)]
        print(f"Após filtro de MERCADOS LUCRATIVOS: {len(df_filtered)} apostas")

        df_filtered = df_filtered[df_filtered["estimated_roi"] >= min_roi_threshold]
        print(f"Após filtro de ROI >= {min_roi_threshold}%: {len(df_filtered)} apostas")

        df_filtered = df_filtered[df_filtered["league"].isin(profitable_leagues)]
        print(f"Após filtro de LIGAS LUCRATIVAS: {len(df_filtered)} apostas")

        # ========================================================================
        # 7. TABELA COMPARATIVA
        # ========================================================================
        print(f"\n{'=' * 100}")
        print("7. TABELA COMPARATIVA - ANTES vs DEPOIS DOS FILTROS")
        print("=" * 100)

        filtered_bets = len(df_filtered)
        filtered_profit = df_filtered["profit"].sum()
        filtered_wr = (
            (df_filtered["status"] == "win").mean() * 100 if filtered_bets > 0 else 0
        )
        filtered_roi = (
            (filtered_profit / filtered_bets * 100) if filtered_bets > 0 else 0
        )

        print(f"\n{'Métrica':<25} {'Inicial':<15} {'Final':<15} {'Variação':<15}")
        print("-" * 70)

        variation_bets = (filtered_bets - initial_bets) / initial_bets * 100
        print(
            f"{'Total de Apostas':<25} {initial_bets:<15} {filtered_bets:<15} {variation_bets:>+14.1f}%"
        )

        variation_profit = (
            ((filtered_profit - initial_profit) / abs(initial_profit) * 100)
            if initial_profit != 0
            else 0
        )
        print(
            f"{'Lucro Total':<25} {f'{initial_profit:.2f}':<15} {f'{filtered_profit:.2f}':<15} {variation_profit:>+14.1f}%"
        )

        variation_wr = (
            ((filtered_wr - initial_win_rate) / initial_win_rate * 100)
            if initial_win_rate != 0
            else 0
        )
        print(
            f"{'Win Rate':<25} {f'{initial_win_rate:.1f}%':<15} {f'{filtered_wr:.1f}%':<15} {variation_wr:>+14.1f}%"
        )

        variation_roi = (
            ((filtered_roi - initial_roi) / abs(initial_roi) * 100)
            if initial_roi != 0
            else 0
        )
        print(
            f"{'ROI Real':<25} {f'{initial_roi:.1f}%':<15} {f'{filtered_roi:.1f}%':<15} {variation_roi:>+14.1f}%"
        )

        # ========================================================================
        # 8. RESUMO EXECUTIVO
        # ========================================================================
        print(f"\n{'=' * 100}")
        print("8. RESUMO EXECUTIVO")
        print("=" * 100)

        print(f"\nFILTROS APLICADOS:")
        print(f"   1. Odds >= 1.50 (removeu {removed_bets} apostas)")
        print(f"   2. {len(profitable_markets)} mercados lucrativos identificados")
        print(f"   3. ROI estimado mínimo: {min_roi_threshold}%")
        print(f"   4. {len(profitable_leagues)} ligas lucrativas")

        print(f"\nIMPACTO DOS FILTROS:")
        print(
            f"   Redução de {100 - (filtered_bets / initial_bets * 100):.1f}% nas apostas"
        )
        print(f"   ROI melhorou de {initial_roi:.1f}% para {filtered_roi:.1f}%")
        print(f"   Lucro melhorou de {initial_profit:.2f}u para {filtered_profit:.2f}u")

        if "Map 1" in map_analysis.index and "Map 2" in map_analysis.index:
            map1_roi = map_analysis.loc["Map 1", "ROI_Real"]
            map2_roi = map_analysis.loc["Map 2", "ROI_Real"]
            print(f"\nINSIGHTS DE MAPAS:")
            print(f"   Map 1 ROI: {map1_roi:.1f}%")
            print(f"   Map 2 ROI: {map2_roi:.1f}%")
            better_map = "Map 1" if map1_roi > map2_roi else "Map 2"
            print(f"   Melhor performance: {better_map}")

        print(f"\nTOP 5 MERCADOS RECOMENDADOS:")
        top_markets = relevant_markets[
            relevant_markets.index.isin(profitable_markets)
        ].head(5)
        for i, (market, row) in enumerate(top_markets.iterrows(), 1):
            print(
                f"   {i}. {market}: {row['Total_Profit']:.2f} lucro | ROI: {row['ROI_Real']:.1f}%"
            )

        print(f"\nTOP 5 LIGAS RECOMENDADAS:")
        top_leagues = league_analysis[
            league_analysis.index.isin(profitable_leagues)
        ].head(5)
        for i, (league, row) in enumerate(top_leagues.iterrows(), 1):
            print(
                f"   {i}. {league}: {row['Total_Profit']:.2f} lucro | ROI: {row['ROI_Real']:.1f}%"
            )

        print(f"\n{'=' * 100}")
        print("ANÁLISE COMPLETA FINALIZADA!")
        print("=" * 100)

        return {
            "before": {
                "bets": initial_bets,
                "profit": initial_profit,
                "roi": initial_roi,
                "win_rate": initial_win_rate,
            },
            "after": {
                "bets": filtered_bets,
                "profit": filtered_profit,
                "roi": filtered_roi,
                "win_rate": filtered_wr,
            },
            "filters": {
                "markets": profitable_markets,
                "min_roi": min_roi_threshold,
                "leagues": profitable_leagues,
            },
            "top_markets": top_markets.to_dict("index"),
            "top_leagues": top_leagues.to_dict("index"),
        }

    def save_filters_to_db(self, filters):
        """Salva filtros no banco para uso pela estratégia"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS strategy_filters (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            filter_type TEXT NOT NULL,
            filter_value TEXT NOT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        """)

        cursor.execute("DELETE FROM strategy_filters")

        for market in filters["markets"]:
            cursor.execute(
                "INSERT INTO strategy_filters (filter_type, filter_value) VALUES ('market', ?)",
                (market,),
            )

        for league in filters["leagues"]:
            cursor.execute(
                "INSERT INTO strategy_filters (filter_type, filter_value) VALUES ('league', ?)",
                (league,),
            )

        cursor.execute(
            "INSERT INTO strategy_filters (filter_type, filter_value) VALUES ('min_roi', ?)",
            (str(filters["min_roi"]),),
        )
        cursor.execute(
            "INSERT INTO strategy_filters (filter_type, filter_value) VALUES ('min_odds', '1.50')"
        )

        conn.commit()
        conn.close()
        print(f"\nFiltros salvos no banco de dados!")


def main():
    """Função principal"""
    db_path = "data/bets.db"

    try:
        analyzer = BettingAnalysisDB(db_path)
        results = analyzer.simplified_betting_analysis()

        if results:
            filters = results["filters"]
            analyzer.save_filters_to_db(filters)

    except FileNotFoundError as e:
        print(f"Erro: {e}")
    except Exception as e:
        print(f"Erro durante a análise: {e}")


if __name__ == "__main__":
    main()
