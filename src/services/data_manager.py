import asyncio
from datetime import datetime, timedelta
from pathlib import Path
import json
from src.core.database import LoLDatabase
from src.core.bet365_client import Bet365Client


class LoLDataManager:
    def __init__(self):
        self.db = LoLDatabase()
        self.client = Bet365Client()
        self.json_backup_dir = Path("../data/json_backups")
        self.json_backup_dir.mkdir(parents=True, exist_ok=True)

    async def initialize_database(self, days_back=10):
        """Popula o banco com dados históricos"""
        print(f"📊 Inicializando banco com últimos {days_back} dias...")
        # [TODO: Implementar]

    async def daily_update(self):
        """Atualização dos últimos 2 dias"""
        print("🔄 Executando atualização diária...")
        # [TODO: Implementar]

    def _save_json_backup(self, data, filename):
        """Salva backup do JSON original"""
        backup_path = (
            self.json_backup_dir / f"{datetime.now().strftime('%Y-%m')}" / filename
        )
        backup_path.parent.mkdir(exist_ok=True)

        with open(backup_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
