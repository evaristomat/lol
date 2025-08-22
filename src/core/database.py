import sqlite3
import os
from datetime import datetime
from pathlib import Path

# Configurações diretas - SEM import de config
BASE_DIR = Path(__file__).parent.parent.parent  # Volta 3 níveis: src/core/../../
DATA_DIR = BASE_DIR / "data"
DB_PATH = DATA_DIR / "lol_esports.db"


class LoLDatabase:
    def __init__(self):
        self.db_path = DB_PATH
        self._ensure_directories()
        self.init_database()

    def _ensure_directories(self):
        """Garante que os diretórios existem"""
        DATA_DIR.mkdir(parents=True, exist_ok=True)

    def init_database(self):
        """Inicializa todas as tabelas do banco"""
        conn = self.get_connection()
        cursor = conn.cursor()

        # Tabela de ligas
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS leagues (
                league_id INTEGER PRIMARY KEY,
                name TEXT UNIQUE NOT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Tabela de times
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS teams (
                team_id INTEGER PRIMARY KEY,
                name TEXT UNIQUE NOT NULL,
                image_id TEXT,
                country_code TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Tabela de jogos
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS matches (
                match_id INTEGER PRIMARY KEY AUTOINCREMENT,
                bet365_id TEXT UNIQUE NOT NULL,
                sport_id INTEGER,
                league_id INTEGER,
                home_team_id INTEGER,
                away_team_id INTEGER,
                event_time DATETIME,
                time_status INTEGER,
                final_score TEXT,
                retrieved_at DATETIME,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (league_id) REFERENCES leagues (league_id),
                FOREIGN KEY (home_team_id) REFERENCES teams (team_id),
                FOREIGN KEY (away_team_id) REFERENCES teams (team_id)
            )
        """)
        # Tabela de mapas
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS game_maps (
                map_id INTEGER PRIMARY KEY AUTOINCREMENT,
                match_id INTEGER,
                map_number INTEGER,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (match_id) REFERENCES matches (match_id),
                UNIQUE(match_id, map_number)
            )
        """)

        # Tabela de estatísticas por mapa
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS map_statistics (
                stat_id INTEGER PRIMARY KEY AUTOINCREMENT,
                map_id INTEGER,
                stat_name TEXT,
                home_value TEXT,
                away_value TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (map_id) REFERENCES game_maps (map_id),
                UNIQUE(map_id, stat_name)
            )
        """)

        # Tabela de logs
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS update_logs (
                log_id INTEGER PRIMARY KEY AUTOINCREMENT,
                update_type TEXT,
                items_processed INTEGER,
                new_items INTEGER,
                success BOOLEAN,
                error_message TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)

        conn.commit()
        conn.close()
        print("✅ Banco de dados inicializado com sucesso!")

    def get_connection(self):
        return sqlite3.connect(self.db_path)

    def log_update(
        self, update_type, items_processed, new_items, success, error_message=None
    ):
        """Registra log de atualização"""
        conn = self.get_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            INSERT INTO update_logs (update_type, items_processed, new_items, success, error_message)
            VALUES (?, ?, ?, ?, ?)
        """,
            (update_type, items_processed, new_items, success, error_message),
        )

        conn.commit()
        conn.close()
