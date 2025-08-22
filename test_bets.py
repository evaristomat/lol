import sqlite3
import pandas as pd
import json
from pathlib import Path


def extract_database_structure(db_path="data/bets.db"):
    """
    Extrai toda a estrutura do banco bets.db e retorna como DataFrames
    """
    try:
        # Conectar ao banco
        conn = sqlite3.connect(db_path)

        # 1. Listar todas as tabelas
        tables_query = "SELECT name FROM sqlite_master WHERE type='table';"
        tables = pd.read_sql(tables_query, conn)

        print("📊 TABELAS ENCONTRADAS:")
        print(tables)
        print("\n" + "=" * 50 + "\n")

        # 2. Extrair estrutura de cada tabela
        table_structures = {}
        table_data = {}

        for table_name in tables["name"]:
            # Estrutura da tabela
            structure_query = f"PRAGMA table_info({table_name});"
            structure = pd.read_sql(structure_query, conn)
            table_structures[table_name] = structure

            # Dados da tabela (apenas primeiras linhas para análise)
            data_query = f"SELECT * FROM {table_name} LIMIT 5;"
            try:
                data = pd.read_sql(data_query, conn)
                table_data[table_name] = data
            except Exception as e:
                table_data[table_name] = f"Erro ao acessar dados: {e}"

            print(f"📋 ESTRUTURA DA TABELA: {table_name}")
            print(structure)
            print(f"\n📝 AMOSTRA DE DADOS ({table_name}):")
            print(table_data[table_name])
            print("\n" + "-" * 30 + "\n")

        # 3. Salvar metadados completos
        metadata = {
            "tables": tables.to_dict(),
            "structures": {k: v.to_dict() for k, v in table_structures.items()},
            "data_samples": {
                k: (v.to_dict() if isinstance(v, pd.DataFrame) else v)
                for k, v in table_data.items()
            },
        }

        with open("database_metadata.json", "w", encoding="utf-8") as f:
            json.dump(metadata, f, indent=2, ensure_ascii=False)

        print("✅ Metadados salvos em 'database_metadata.json'")

        # 4. Verificar tamanho do banco
        size_mb = Path(db_path).stat().st_size / (1024 * 1024)
        print(f"📦 Tamanho do banco: {size_mb:.2f} MB")

        conn.close()
        return tables, table_structures, table_data

    except Exception as e:
        print(f"❌ Erro ao acessar o banco: {e}")
        return None, None, None


def get_full_table_data(db_path="bets.db", table_name=None):
    """
    Extrai todos os dados de uma tabela específica
    """
    conn = sqlite3.connect(db_path)

    if table_name:
        query = f"SELECT * FROM {table_name};"
        data = pd.read_sql(query, conn)
    else:
        # Se não especificar tabela, pega todas
        tables_query = "SELECT name FROM sqlite_master WHERE type='table';"
        tables = pd.read_sql(tables_query, conn)
        data = {}

        for table in tables["name"]:
            query = f"SELECT * FROM {table};"
            data[table] = pd.read_sql(query, conn)

    conn.close()
    return data


# Executar a extração
if __name__ == "__main__":
    print("🔍 ANALISANDO BANCO DE DADOS BETS.DB...")
    tables, structures, samples = extract_database_structure()

    # Exemplo: extrair dados completos das tabelas mais importantes
    if tables is not None:
        important_tables = [
            "jogos",
            "odds",
            "apostas",
            "eventos",
        ]  # Ajuste conforme suas tabelas

        full_data = {}
        for table in important_tables:
            if table in tables["name"].values:
                full_data[table] = get_full_table_data(table_name=table)
                print(
                    f"✅ Dados completos de '{table}' extraídos ({len(full_data[table])} registros)"
                )

        # Salvar dados completos em CSV para análise
        for table_name, data in full_data.items():
            csv_filename = f"{table_name}_full_data.csv"
            data.to_csv(csv_filename, index=False, encoding="utf-8")
            print(f"💾 {table_name} salvo como {csv_filename}")
