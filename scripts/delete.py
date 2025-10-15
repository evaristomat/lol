import sqlite3
from pathlib import Path

# Lista de IDs de eventos a serem removidos
event_ids_to_delete = [
    "183017512",
    "182052047",
    "183017503",
    "183017505",
    "183017508",
    "183017511",
    "183017510",
    "183048751",
    "183017509",
    "183017506",
]

# Defina o caminho para o seu bets.db
# Ajuste este caminho se o seu arquivo estiver em outro lugar!
DB_PATH = Path(__file__).parent.parent / "data" / "bets.db"


def delete_events_from_db(db_path: Path, event_ids: list):
    """Remove eventos e apostas relacionadas do bets.db."""
    if not db_path.exists():
        print(f"❌ Erro: Banco de dados não encontrado em {db_path}")
        return

    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # O comando DELETE com WHERE IN é eficiente para múltiplos IDs

        # 1. Remover apostas relacionadas (tabela 'bets')
        # Isso é importante para garantir a integridade, embora a FK com ON DELETE CASCADE
        # devesse cuidar disso se estivesse configurada corretamente. É mais seguro fazer manualmente.
        cursor.execute(
            f"DELETE FROM bets WHERE event_id IN ({','.join(['?'] * len(event_ids))})",
            event_ids,
        )
        deleted_bets = cursor.rowcount

        # 2. Remover eventos (tabela 'events')
        cursor.execute(
            f"DELETE FROM events WHERE event_id IN ({','.join(['?'] * len(event_ids))})",
            event_ids,
        )
        deleted_events = cursor.rowcount

        # 3. Remover registros de verificação de resultados (tabela 'results_verification')
        cursor.execute(
            f"DELETE FROM results_verification WHERE event_id IN ({','.join(['?'] * len(event_ids))})",
            event_ids,
        )
        deleted_results = cursor.rowcount

        conn.commit()
        conn.close()

        print(f"✅ Eventos removidos com sucesso do {db_path.name}:")
        print(f"   - {deleted_events} eventos excluídos da tabela 'events'.")
        print(f"   - {deleted_bets} apostas excluídas da tabela 'bets'.")
        print(
            f"   - {deleted_results} resultados de verificação excluídos da tabela 'results_verification'."
        )

    except sqlite3.Error as e:
        print(f"❌ Erro ao acessar o banco de dados: {e}")


if __name__ == "__main__":
    delete_events_from_db(DB_PATH, event_ids_to_delete)
