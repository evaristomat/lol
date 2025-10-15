import sqlite3
from pathlib import Path

# Caminho para o banco de dados
DB_PATH = Path(__file__).parent.parent / "data" / "bets.db"

# Data que você quer excluir (ajuste se quiser outro dia)
TARGET_DATE = "2025-10-16"

# Nome exato da liga
TARGET_LEAGUE = "LOL - World Champs"


def delete_events_by_date_and_league(
    db_path: Path, league_name: str, match_date_prefix: str
):
    """Remove eventos e apostas relacionadas de uma liga e data específica."""
    if not db_path.exists():
        print(f"❌ Erro: Banco de dados não encontrado em {db_path}")
        return

    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Buscar os event_ids correspondentes para feedback
        cursor.execute(
            """
            SELECT event_id FROM events
            WHERE league_name = ? AND match_date LIKE ?
        """,
            (league_name, f"{match_date_prefix}%"),
        )
        event_ids = [str(row[0]) for row in cursor.fetchall()]

        if not event_ids:
            print("⚠️ Nenhum evento encontrado com esses critérios.")
            conn.close()
            return

        print(f"🧹 Encontrados {len(event_ids)} eventos para exclusão:")
        print(", ".join(event_ids))

        # Remover apostas relacionadas
        cursor.execute(
            f"DELETE FROM bets WHERE event_id IN ({','.join(['?'] * len(event_ids))})",
            event_ids,
        )
        deleted_bets = cursor.rowcount

        # Remover resultados verificados
        cursor.execute(
            f"DELETE FROM results_verification WHERE event_id IN ({','.join(['?'] * len(event_ids))})",
            event_ids,
        )
        deleted_results = cursor.rowcount

        # Remover eventos principais
        cursor.execute(
            f"DELETE FROM events WHERE event_id IN ({','.join(['?'] * len(event_ids))})",
            event_ids,
        )
        deleted_events = cursor.rowcount

        conn.commit()
        conn.close()

        print(f"✅ Exclusão concluída:")
        print(f"   - {deleted_events} eventos removidos da tabela 'events'")
        print(f"   - {deleted_bets} apostas removidas da tabela 'bets'")
        print(
            f"   - {deleted_results} verificações removidas da tabela 'results_verification'"
        )

    except sqlite3.Error as e:
        print(f"❌ Erro ao acessar o banco de dados: {e}")


if __name__ == "__main__":
    delete_events_by_date_and_league(DB_PATH, TARGET_LEAGUE, TARGET_DATE)
