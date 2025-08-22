import os
from dotenv import load_dotenv
from pathlib import Path

# Carregar variáveis do .env
load_dotenv()


class Settings:
    # Configurações da API (da sua versão original)
    BETSAPI_API_KEY: str = os.getenv("BETSAPI_API_KEY", "")
    BASE_URL: str = "https://api.b365api.com"
    CACHE_TTL: int = int(os.getenv("CACHE_TTL", 300))
    REQUEST_TIMEOUT: int = 30

    # Paths (da nova versão)
    BASE_DIR = Path(__file__).parent.parent
    DATA_DIR = BASE_DIR / "data"
    DATABASE_PATH = DATA_DIR / "lol_esports.db"
    JSON_BACKUP_DIR = DATA_DIR / "json_backups"

    # App Settings (da nova versão)
    UPDATE_DAYS_BACK = 2
    INITIAL_DAYS_BACK = 10
    REQUEST_DELAY = 0.3  # Delay entre requests

    # Database Settings (da nova versão)
    DB_TIMEOUT = 30
    DB_JOURNAL_MODE = "WAL"

    # Garantir que diretórios existam
    def __init__(self):
        self.DATA_DIR.mkdir(exist_ok=True)
        self.JSON_BACKUP_DIR.mkdir(exist_ok=True)


# Instância única das configurações
settings = Settings()
