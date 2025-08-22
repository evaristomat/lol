import os
import requests
import sqlite3
import logging
from typing import Dict, Any, List
from pathlib import Path


class TelegramNotifier:
    def __init__(self):
        self.token = os.getenv("TELEGRAM_BOT_TOKEN")
        self.chat_id = os.getenv("TELEGRAM_CHAT_ID")
        self.enabled = bool(self.token and self.chat_id)

        if not self.enabled:
            logging.warning(
                "Telegram notifications disabled - missing token or chat_id"
            )

    def send_message(self, message: str) -> bool:
        """Envia mensagem para o Telegram"""
        if not self.enabled:
            return False

        try:
            url = f"https://api.telegram.org/bot{self.token}/sendMessage"
            payload = {"chat_id": self.chat_id, "text": message, "parse_mode": "HTML"}

            response = requests.post(url, json=payload, timeout=10)
            return response.status_code == 200
        except Exception as e:
            logging.error(f"Error sending Telegram message: {e}")
            return False

    def format_bet_message(self, bet_data: Dict[str, Any]) -> str:
        """Formata mensagem de nova aposta"""
        return f"""
🎯 <b>NOVA APOSTA ENCONTRADA!</b>

⚔️ <b>Partida:</b> {bet_data.get("team1", "N/A")} vs {bet_data.get("team2", "N/A")}
📊 <b>Tipo:</b> {bet_data.get("bet_type", "N/A")}
💰 <b>Valor:</b> {bet_data.get("bet_value", 0):.2f}
🎲 <b>Odds:</b> {bet_data.get("odds", 0):.2f}
📈 <b>Expected Value:</b> {bet_data.get("expected_value", 0):.2f}%
        """.strip()

    def format_match_message(self, match_data: Dict[str, Any]) -> str:
        """Formata mensagem de novo jogo"""
        return f"""
🆕 <b>NOVO JOGO DETECTADO!</b>

⚔️ <b>Partida:</b> {match_data.get("home_team", "N/A")} vs {match_data.get("away_team", "N/A")}
🏆 <b>Campeonato:</b> {match_data.get("league_name", "N/A")}
⏰ <b>Data:</b> {match_data.get("time", "N/A")}
        """.strip()


# Instância global para fácil acesso
telegram_notifier = TelegramNotifier()
