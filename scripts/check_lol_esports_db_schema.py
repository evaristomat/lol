import sqlite3
from pathlib import Path


def inspect_esports_db_schema(db_path: str = None):
    """Inspeciona o schema do banco de dados de resultados de eSports"""
    if db_path is None:
        db_path = Path(__file__).parent.parent / "data" / "lol_esports.db"

    if not Path(db_path).exists():
        print(f"❌ Banco de dados não encontrado: {db_path}")
        return

    print("=" * 80)
    print("🔍 INSPEÇÃO DO SCHEMA - LOL ESPORTS DATABASE")
    print("=" * 80)

    try:
        with sqlite3.connect(db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            # 1. Listar todas as tabelas
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = cursor.fetchall()

            print("📋 TABELAS ENCONTRADAS:")
            print("-" * 40)
            for table in tables:
                print(f"  {table['name']}")
            print()

            # 2. Detalhes de cada tabela
            for table in tables:
                table_name = table["name"]
                print(f"📊 ESTRUTURA DA TABELA: {table_name}")
                print("-" * 40)

                # Obter informações das colunas
                cursor.execute(f"PRAGMA table_info({table_name})")
                columns = cursor.fetchall()

                for col in columns:
                    pk = " 🔑" if col["pk"] else ""
                    print(f"  {col['name']}: {col['type']}{pk}")

                # Obter índices
                cursor.execute(f"PRAGMA index_list({table_name})")
                indexes = cursor.fetchall()

                if indexes:
                    print(f"  📑 ÍNDICES:")
                    for idx in indexes:
                        cursor.execute(f"PRAGMA index_info({idx['name']})")
                        idx_info = cursor.fetchall()
                        idx_columns = [info["name"] for info in idx_info]
                        print(f"    {idx['name']}: {', '.join(idx_columns)}")

                print()

            # 3. Exemplo de dados de algumas tabelas importantes
            important_tables = ["matches", "teams", "game_maps", "map_statistics"]

            print("📊 EXEMPLO DE DADOS:")
            print("-" * 40)

            for table_name in important_tables:
                if table_name in [t["name"] for t in tables]:
                    cursor.execute(f"SELECT * FROM {table_name} LIMIT 3")
                    sample_data = cursor.fetchall()

                    if sample_data:
                        print(f"  {table_name.upper()}:")
                        for i, row in enumerate(sample_data):
                            print(f"    {i + 1}. {dict(row)}")
                        print()

            # 4. Verificar relacionamentos e chaves estrangeiras
            print("🔗 RELACIONAMENTOS E CHAVES ESTRANGEIRAS:")
            print("-" * 40)

            for table_name in [t["name"] for t in tables]:
                cursor.execute(f"PRAGMA foreign_key_list({table_name})")
                fk_info = cursor.fetchall()

                if fk_info:
                    print(f"  {table_name}:")
                    for fk in fk_info:
                        print(f"    {fk['from']} → {fk['table']}.{fk['to']}")
                    print()

            # 5. Estatísticas básicas
            print("📈 ESTATÍSTICAS BÁSICAS:")
            print("-" * 40)

            for table_name in [t["name"] for t in tables]:
                cursor.execute(f"SELECT COUNT(*) as count FROM {table_name}")
                count = cursor.fetchone()["count"]
                print(f"  {table_name}: {count} registros")

            print()

    except Exception as e:
        print(f"❌ Erro ao inspecionar o banco de dados: {e}")


if __name__ == "__main__":
    inspect_esports_db_schema()
