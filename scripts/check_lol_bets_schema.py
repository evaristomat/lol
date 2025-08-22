import sqlite3
import pandas as pd
from pathlib import Path


def get_database_structure(db_path):
    """
    Retorna a estrutura completa de um banco de dados SQLite
    """
    if not Path(db_path).exists():
        return f"Erro: Arquivo {db_path} não encontrado!"

    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Obter todas as tabelas
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()

        database_structure = {"database_path": db_path, "tables": {}}

        for table in tables:
            table_name = table[0]
            table_info = {}

            # Obter schema da tabela
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = cursor.fetchall()

            # Obter índices
            cursor.execute(f"PRAGMA index_list({table_name})")
            indexes = cursor.fetchall()

            # Obter chaves estrangeiras
            cursor.execute(f"PRAGMA foreign_key_list({table_name})")
            foreign_keys = cursor.fetchall()

            # Estruturar informações das colunas
            table_info["columns"] = []
            for col in columns:
                column_info = {
                    "cid": col[0],
                    "name": col[1],
                    "type": col[2],
                    "notnull": bool(col[3]),
                    "default_value": col[4],
                    "pk": bool(col[5]),
                }
                table_info["columns"].append(column_info)

            # Estruturar informações dos índices
            table_info["indexes"] = []
            for idx in indexes:
                index_info = {"seq": idx[0], "name": idx[1], "unique": bool(idx[2])}
                table_info["indexes"].append(index_info)

            # Estruturar informações de chaves estrangeiras
            table_info["foreign_keys"] = []
            for fk in foreign_keys:
                fk_info = {
                    "id": fk[0],
                    "seq": fk[1],
                    "table": fk[2],
                    "from": fk[3],
                    "to": fk[4],
                    "on_update": fk[5],
                    "on_delete": fk[6],
                    "match": fk[7],
                }
                table_info["foreign_keys"].append(fk_info)

            # Contar número de registros
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            record_count = cursor.fetchone()[0]
            table_info["record_count"] = record_count

            # Obter exemplo de dados (apenas 3 registros)
            if record_count > 0:
                cursor.execute(f"SELECT * FROM {table_name} LIMIT 3")
                sample_data = cursor.fetchall()
                column_names = [description[0] for description in cursor.description]

                table_info["sample_data"] = {
                    "columns": column_names,
                    "records": sample_data,
                }

            database_structure["tables"][table_name] = table_info

        conn.close()
        return database_structure

    except Exception as e:
        return f"Erro ao acessar o banco de dados: {str(e)}"


def print_database_structure(db_structure):
    """
    Imprime a estrutura do banco de dados de forma organizada
    """
    if isinstance(db_structure, str) and db_structure.startswith("Erro"):
        print(db_structure)
        return

    print(f"=== ESTRUTURA DO BANCO DE DADOS ===")
    print(f"Arquivo: {db_structure['database_path']}")
    print(f"Total de tabelas: {len(db_structure['tables'])}")
    print("=" * 50)

    for table_name, table_info in db_structure["tables"].items():
        print(f"\n📊 TABELA: {table_name}")
        print(f"   Registros: {table_info['record_count']}")

        print(f"\n   COLUNAS:")
        for col in table_info["columns"]:
            pk_flag = " 🔑" if col["pk"] else ""
            nn_flag = " NOT NULL" if col["notnull"] else ""
            default_flag = (
                f" DEFAULT {col['default_value']}" if col["default_value"] else ""
            )
            print(f"     {col['name']}: {col['type']}{nn_flag}{default_flag}{pk_flag}")

        if table_info["indexes"]:
            print(f"\n   ÍNDICES:")
            for idx in table_info["indexes"]:
                unique_flag = " UNIQUE" if idx["unique"] else ""
                print(f"     {idx['name']}{unique_flag}")

        if table_info["foreign_keys"]:
            print(f"\n   CHAVES ESTRANGEIRAS:")
            for fk in table_info["foreign_keys"]:
                print(f"     {fk['from']} → {fk['table']}.{fk['to']}")

        if "sample_data" in table_info and table_info["sample_data"]["records"]:
            print(f"\n   DADOS DE EXEMPLO:")
            df_sample = pd.DataFrame(
                table_info["sample_data"]["records"],
                columns=table_info["sample_data"]["columns"],
            )
            print(df_sample.to_string(index=False))

        print("-" * 50)


# Uso do código
if __name__ == "__main__":
    # Caminho para seu banco de dados
    db_path = "../data/bets.db"  # Ajuste se necessário

    # Obter estrutura completa
    structure = get_database_structure(db_path)

    # Imprimir estrutura de forma organizada
    print_database_structure(structure)

    # Também pode salvar em JSON se quiser
    import json

    with open("database_structure.json", "w", encoding="utf-8") as f:
        json.dump(structure, f, indent=2, default=str, ensure_ascii=False)

    print("\n✅ Estrutura salva em 'database_structure.json'")
