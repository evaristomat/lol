import requests
import logging
import os
from typing import Optional

logger = logging.getLogger("telegram_notifier")


class TelegramNotifier:
    def __init__(self):
        self.token = os.getenv("TELEGRAM_BOT_TOKEN")
        self.chat_id = os.getenv("TELEGRAM_CHAT_ID")

        if not self.token or not self.chat_id:
            logger.warning("Variáveis de ambiente do Telegram não configuradas")
            print("⚠️ Variáveis de ambiente do Telegram não configuradas")
        else:
            self.base_url = f"https://api.telegram.org/bot{self.token}"
            print("✅ Variáveis de ambiente do Telegram carregadas com sucesso")

    def send_message(self, text: str, parse_mode: Optional[str] = None) -> bool:
        """Envia mensagem para o Telegram"""
        if not self.token or not self.chat_id:
            logger.warning("Token ou Chat ID do Telegram não configurados")
            print("❌ Token ou Chat ID do Telegram não configurados")
            return False

        url = f"{self.base_url}/sendMessage"
        payload = {"chat_id": self.chat_id, "text": text, "parse_mode": parse_mode}

        try:
            response = requests.post(url, json=payload, timeout=10)
            response.raise_for_status()
            print("✅ Mensagem enviada com sucesso para o Telegram")
            return True
        except Exception as e:
            logger.error(f"Erro ao enviar mensagem para o Telegram: {e}")
            print(f"❌ Erro ao enviar mensagem para o Telegram: {e}")
            return False

    def notify_new_event(
        self, team1: str, team2: str, league: str, competition: str
    ) -> bool:
        """Notifica sobre novo evento disponível na Bet365"""
        message = (
            f"🎮 **NOVO JOGO DISPONÍVEL**\n\n"
            f"🆚 **{team1}** vs **{team2}**\n"
            f"🏆 **Liga:** {league}\n"
            f"🎯 **Competição:** {competition}\n\n"
            f"💰 Apostas disponíveis na Bet365!"
        )

        return self.send_message(message, parse_mode="Markdown")
