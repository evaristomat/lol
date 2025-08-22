import sqlite3
import pandas as pd


# Conecta ao banco de dados
def get_odds_by_event(event_id):
    db_path = "../data/lol_odds.db"

    try:
        conn = sqlite3.connect(db_path)

        # Consulta para obter as odds do evento específico incluindo handicap
        query = """
        SELECT market_name, selection_name, odds_value, handicap, updated_at
        FROM current_odds
        WHERE event_id = ?
        ORDER BY market_name, selection_name
        """

        df = pd.read_sql_query(query, conn, params=[event_id])

        if df.empty:
            print(f"\nNenhuma odd encontrada para o evento ID {event_id}")
        else:
            print(f"\nOdds para o evento ID {event_id}:")
            for market_name, group in df.groupby("market_name"):
                print(f"\nMercado: {market_name}")
                print("-" * 60)
                for _, row in group.iterrows():
                    # Formata a exibição para incluir o handicap quando disponível
                    if pd.notna(row["handicap"]) and row["handicap"] != "":
                        print(
                            f"{row['selection_name']} {row['handicap']}: {row['odds_value']} (Atualizado em: {row['updated_at']})"
                        )
                    else:
                        print(
                            f"{row['selection_name']}: {row['odds_value']} (Atualizado em: {row['updated_at']})"
                        )

    except sqlite3.Error as e:
        print(f"Erro ao acessar o banco de dados: {e}")
    finally:
        if conn:
            conn.close()


# Executa a função com o ID do evento
get_odds_by_event(179606652)
