import sqlite3
import pandas as pd


def debug_betting_data(db_path="data/bets.db"):
    """Debug dos dados de apostas para identificar o problema"""

    conn = sqlite3.connect(db_path)

    # Verificar estrutura da tabela bets
    print("=== ESTRUTURA DA TABELA BETS ===")
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(bets)")
    columns = cursor.fetchall()
    for col in columns:
        print(f"{col[1]}: {col[2]}")

    # Verificar dados sample
    print("\n=== SAMPLE DOS DADOS ===")
    query = """
    SELECT 
        id, event_id, market_name, selection_line, bet_status,
        stake, potential_win, actual_win, house_odds, result_verified
    FROM bets 
    WHERE result_verified = 1
    LIMIT 10
    """

    df_sample = pd.read_sql_query(query, conn)
    print(df_sample.to_string())

    # Verificar distribuição de bet_status
    print("\n=== DISTRIBUIÇÃO DE BET_STATUS ===")
    status_query = """
    SELECT bet_status, COUNT(*) as count, result_verified
    FROM bets 
    GROUP BY bet_status, result_verified
    ORDER BY result_verified DESC, count DESC
    """

    status_df = pd.read_sql_query(status_query, conn)
    print(status_df)

    # Verificar cálculo de lucro
    print("\n=== ANÁLISE DE LUCRO ===")
    profit_query = """
    SELECT 
        bet_status,
        COUNT(*) as count,
        AVG(stake) as avg_stake,
        AVG(potential_win) as avg_potential_win,
        AVG(actual_win) as avg_actual_win,
        SUM(actual_win - stake) as total_profit_method1,
        SUM(actual_win) - SUM(stake) as total_profit_method2
    FROM bets 
    WHERE result_verified = 1
    GROUP BY bet_status
    """

    profit_df = pd.read_sql_query(profit_query, conn)
    print(profit_df)

    # Verificar se actual_win está sendo calculado corretamente
    print("\n=== VERIFICAÇÃO ACTUAL_WIN ===")
    win_query = """
    SELECT 
        bet_status,
        house_odds,
        stake,
        potential_win,
        actual_win,
        CASE 
            WHEN bet_status = 'won' THEN stake * house_odds
            WHEN bet_status = 'lost' THEN 0
            ELSE actual_win
        END as calculated_win,
        actual_win - stake as profit
    FROM bets 
    WHERE result_verified = 1
    LIMIT 20
    """

    win_df = pd.read_sql_query(win_query, conn)
    print(win_df.to_string())

    conn.close()


def fixed_betting_analysis(db_path="data/bets.db"):
    """Análise corrigida baseada no debug"""

    conn = sqlite3.connect(db_path)

    # Query corrigida
    query = """
    SELECT 
        b.id,
        b.event_id,
        b.market_name,
        b.selection_line,
        b.handicap,
        b.house_odds as odds,
        b.roi_average as estimated_roi,
        b.bet_status,
        b.stake,
        b.actual_win,
        CASE 
            WHEN b.bet_status = 'won' THEN b.actual_win - b.stake
            WHEN b.bet_status = 'lost' THEN -b.stake
            ELSE b.actual_win - b.stake
        END as profit,
        e.league_name,
        e.match_date
    FROM bets b
    JOIN events e ON b.event_id = e.event_id
    WHERE b.result_verified = 1
    ORDER BY e.match_date DESC
    """

    df = pd.read_sql_query(query, conn)
    conn.close()

    if df.empty:
        print("Nenhuma aposta verificada encontrada!")
        return

    # Preparar dados
    df["match_date"] = pd.to_datetime(df["match_date"])
    df["status"] = df["bet_status"].map({"won": "win", "lost": "lose"})
    df["market"] = df["market_name"] + " - " + df["selection_line"]
    df["league"] = df["league_name"]

    print(f"=== ANÁLISE CORRIGIDA ===")
    print(f"Total de apostas: {len(df)}")
    print(f"Total de lucro: {df['profit'].sum():.2f}")
    print(f"Win rate: {df['status'].eq('win').mean() * 100:.1f}%")
    print(f"ROI: {(df['profit'].sum() / len(df) * 100):.1f}%")

    # Verificar se temos apostas ganhas
    won_bets = df[df["status"] == "win"]
    lost_bets = df[df["status"] == "lose"]

    print(f"\nApostas ganhas: {len(won_bets)} (lucro: {won_bets['profit'].sum():.2f})")
    print(
        f"Apostas perdidas: {len(lost_bets)} (prejuízo: {lost_bets['profit'].sum():.2f})"
    )

    # Análise por mercado - apenas com apostas suficientes
    market_analysis = (
        df.groupby("market")
        .agg(
            {
                "profit": ["sum", "count"],
                "status": lambda x: (x == "win").mean() * 100,
            }
        )
        .round(2)
    )

    market_analysis.columns = ["Total_Profit", "Bets", "Win_Rate"]
    market_analysis["ROI_Real"] = (
        market_analysis["Total_Profit"] / market_analysis["Bets"] * 100
    ).round(1)
    market_analysis = market_analysis.sort_values("Total_Profit", ascending=False)

    # Filtrar mercados com pelo menos 10 apostas
    relevant_markets = market_analysis[market_analysis["Bets"] >= 10]

    print(f"\n=== TOP 10 MERCADOS (≥10 apostas) ===")
    print(f"{'Mercado':<40} {'Lucro':<8} {'Apostas':<8} {'ROI':<8} {'WR':<8}")
    print("-" * 72)

    for market, row in relevant_markets.head(10).iterrows():
        icon = "✅" if row["Total_Profit"] > 0 else "❌"
        print(
            f"{icon} {market[:38]:<38} {row['Total_Profit']:>6.2f} "
            f"{int(row['Bets']):>7} {row['ROI_Real']:>6.1f}% {row['Win_Rate']:>6.1f}%"
        )


if __name__ == "__main__":
    print("=== DEBUG DOS DADOS ===")
    debug_betting_data("data/bets.db")

    print("\n" + "=" * 80)
    print("=== ANÁLISE CORRIGIDA ===")
    fixed_betting_analysis("data/bets.db")
