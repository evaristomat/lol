import asyncio
import json
import logging
import os
import sqlite3
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import aiohttp

# Adicionar o diretório pai ao path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.core.bet365_client import Bet365Client
from src.services.telegram_notifier import TelegramNotifier

# Verificar se as variáveis do Telegram estão configuradas
telegram_token = os.getenv("TELEGRAM_BOT_TOKEN")
telegram_chat_id = os.getenv("TELEGRAM_CHAT_ID")

if not telegram_token or not telegram_chat_id:
    print("⚠️ Variáveis de ambiente do Telegram não configuradas")
else:
    print("✅ Variáveis de ambiente do Telegram configuradas")


# Códigos de cores ANSI
class Colors:
    RESET = "\033[0m"
    BOLD = "\033[1m"
    RED = "\033[91m"
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    BLUE = "\033[94m"
    MAGENTA = "\033[95m"
    CYAN = "\033[96m"
    WHITE = "\033[97m"
    GRAY = "\033[90m"


# Configurar logging estruturado com cores
class ColoredFormatter(logging.Formatter):
    """Formata logs com cores para o terminal"""

    FORMATS = {
        logging.DEBUG: f"{Colors.GRAY}%(asctime)s | %(levelname)-8s | %(funcName)-20s | %(message)s{Colors.RESET}",
        logging.INFO: f"{Colors.GREEN}%(asctime)s | %(levelname)-8s | %(message)s{Colors.RESET}",
        logging.WARNING: f"{Colors.YELLOW}%(asctime)s | %(levelname)-8s | %(funcName)-20s | %(message)s{Colors.RESET}",
        logging.ERROR: f"{Colors.RED}%(asctime)s | %(levelname)-8s | %(funcName)-20s | %(message)s{Colors.RESET}",
        logging.CRITICAL: f"{Colors.RED}{Colors.BOLD}%(asctime)s | %(levelname)-8s | %(funcName)-20s | %(message)s{Colors.RESET}",
    }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt, datefmt="%H:%M:%S")
        return formatter.format(record)


def setup_logging():
    """Configura sistema de logging estruturado com cores"""
    log_dir = Path(__file__).parent.parent / "logs"
    log_dir.mkdir(exist_ok=True)

    log_file = log_dir / f"lol_odds_{datetime.now():%Y%m%d_%H%M%S}.log"

    # Logger principal
    logger = logging.getLogger("lol_odds")
    logger.setLevel(logging.INFO)

    # Remover handlers existentes
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)

    # Handler para arquivo (sem cores)
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_formatter = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(funcName)-20s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    file_handler.setFormatter(file_formatter)

    # Handler para console (com cores)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(ColoredFormatter())

    # Adicionar handlers
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger


logger = setup_logging()


class RateLimiter:
    """Controla a taxa de requisições para respeitar os limites da API"""

    def __init__(self, max_requests=3500, time_window=3600):
        self.max_requests = max_requests
        self.time_window = time_window
        self.requests = []
        self.lock = asyncio.Lock()

    async def acquire(self):
        async with self.lock:
            now = time.time()

            # Remove requisições antigas
            self.requests = [
                req_time
                for req_time in self.requests
                if now - req_time < self.time_window
            ]

            # Verifica se podemos fazer mais requisições
            if len(self.requests) >= self.max_requests:
                # Calcula quando a próxima requisição pode ser feita
                oldest_req = min(self.requests)
                wait_time = self.time_window - (now - oldest_req)
                if wait_time > 0:
                    logger.debug(f"⏰ Rate limit atingido, aguardando {wait_time:.2f}s")
                    await asyncio.sleep(wait_time)
                    now = time.time()
                    # Atualiza a lista após esperar
                    self.requests = [
                        req_time
                        for req_time in self.requests
                        if now - req_time < self.time_window
                    ]

            # Adiciona a nova requisição
            self.requests.append(now)


class LoLOddsDatabase:
    def __init__(self, db_path: str = None):
        if db_path is None:
            data_dir = Path(__file__).parent.parent / "data"
            data_dir.mkdir(exist_ok=True)
            db_path = data_dir / "lol_odds.db"

        self.db_path = str(db_path)
        self.client = Bet365Client()
        self.telegram_notifier = TelegramNotifier()
        self.lol_sport_id = 151
        self.rate_limiter = RateLimiter(max_requests=3500, time_window=3600)
        self.semaphore = asyncio.Semaphore(10)
        self.odds_cache = {}
        self.init_database()
        logger.info(f"📀 Database inicializado: {self.db_path}")

    def init_database(self):
        """Inicializa o banco de dados com as tabelas necessárias incluindo teams"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("PRAGMA foreign_keys = ON")

            # Tabela de times
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS teams (
                    id INTEGER PRIMARY KEY,
                    team_id TEXT UNIQUE NOT NULL,
                    name TEXT NOT NULL,
                    region TEXT,
                    created_at TEXT DEFAULT (datetime('now')),
                    updated_at TEXT DEFAULT (datetime('now'))
                )
            """
            )

            # Tabela principal de eventos
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS events (
                    id INTEGER PRIMARY KEY,
                    event_id TEXT UNIQUE NOT NULL,
                    home_team_id INTEGER NOT NULL,
                    away_team_id INTEGER NOT NULL,
                    league_name TEXT NOT NULL,
                    match_date TEXT,
                    match_timestamp INTEGER,
                    status TEXT DEFAULT 'upcoming',
                    created_at TEXT DEFAULT (datetime('now')),
                    updated_at TEXT DEFAULT (datetime('now')),
                    FOREIGN KEY (home_team_id) REFERENCES teams (id),
                    FOREIGN KEY (away_team_id) REFERENCES teams (id)
                )
            """
            )

            # Tabela de odds atuais
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS current_odds (
                    id INTEGER PRIMARY KEY,
                    event_id TEXT NOT NULL,
                    odds_type TEXT NOT NULL,
                    market_name TEXT NOT NULL,
                    selection_name TEXT NOT NULL,
                    odds_value REAL NOT NULL,
                    handicap TEXT,
                    updated_at TEXT DEFAULT (datetime('now')),
                    raw_data TEXT,
                    FOREIGN KEY (event_id) REFERENCES events (event_id) ON DELETE CASCADE
                )
            """
            )

            # Tabela de resultados
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS results (
                    id INTEGER PRIMARY KEY,
                    event_id TEXT NOT NULL,
                    final_score TEXT,
                    map_scores TEXT,
                    period_stats TEXT,
                    match_duration INTEGER,
                    completed_at TEXT,
                    raw_result TEXT,
                    FOREIGN KEY (event_id) REFERENCES events (event_id) ON DELETE CASCADE
                )
            """
            )

            # Índices para performance
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_teams_team_id ON teams (team_id)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_events_timestamp ON events (match_timestamp)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_events_status ON events (status)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_current_odds_event ON current_odds (event_id)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_current_odds_market ON current_odds (market_name)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_current_odds_updated ON current_odds (updated_at)"
            )

            conn.commit()

    def _get_or_create_team(
        self, conn, team_id: str, team_name: str, region: str = None
    ) -> str:
        """Obtém ou cria um time e retorna o team_id real da API"""
        cursor = conn.execute("SELECT team_id FROM teams WHERE team_id = ?", (team_id,))
        existing_team = cursor.fetchone()

        if existing_team:
            return existing_team[0]

        cursor = conn.execute(
            "INSERT INTO teams (team_id, name, region) VALUES (?, ?, ?)",
            (team_id, team_name, region),
        )
        return team_id

    def _is_lol_event(self, event: Dict) -> bool:
        """Filtro para identificar apenas League of Legends"""
        league_name = event.get("league", {}).get("name", "").strip()
        starts_with_lol = league_name.startswith("LOL -")

        non_lol_keywords = [
            "VALORANT",
            "CS2",
            "CS:GO",
            "DOTA",
            "OVERWATCH",
            "RAINBOW SIX",
            "R6",
            "ROCKET LEAGUE",
            "FIFA",
            "CALL OF DUTY",
            "COD",
            "STARCRAFT",
            "WARCRAFT",
            "HEARTHSTONE",
            "FORTNITE",
            "PUBG",
        ]

        is_not_other = not any(
            keyword.lower() in league_name.lower() for keyword in non_lol_keywords
        )

        return starts_with_lol and is_not_other

    async def fetch_upcoming_events(self, days_ahead: int = 10) -> List[Dict]:
        """Busca eventos futuros de LoL - padrão aumentado para 10 dias"""
        events = []
        logger.info(f"🔍 Buscando eventos para os próximos {days_ahead} dias")

        for i in range(days_ahead + 1):
            target_date = datetime.now() + timedelta(days=i)
            day_str = target_date.strftime("%Y%m%d")

            try:
                logger.info(
                    f"📅 Buscando eventos para {target_date.strftime('%Y-%m-%d')}"
                )
                await self.rate_limiter.acquire()

                daily_events = await self.client.upcoming(
                    sport_id=self.lol_sport_id, day=day_str
                )

                if daily_events.get("success") == 1 and daily_events.get("results"):
                    lol_events = [
                        event
                        for event in daily_events["results"]
                        if self._is_lol_event(event)
                    ]
                    events.extend(lol_events)

                    if lol_events:
                        logger.info(f"   ✅ {len(lol_events)} jogos de LoL encontrados")
                    else:
                        logger.info("   ℹ️  Nenhum jogo de LoL encontrado")
                else:
                    logger.info(f"   ℹ️  Nenhum evento encontrado para {day_str}")

            except Exception as e:
                logger.error(f"❌ Erro ao buscar eventos para {day_str}: {str(e)}")

        logger.info(f"📊 Total de eventos encontrados: {len(events)}")
        return events

    def save_events(self, events: List[Dict]) -> Dict[str, int]:
        """Salva eventos no banco, incluindo informações dos times e notificações do Telegram"""
        stats = {
            "new": 0,
            "existing": 0,
            "teams_created": 0,
            "notifications_sent": 0,
            "notification_errors": 0,
        }

        with sqlite3.connect(self.db_path) as conn:
            for event in events:
                event_id = event.get("id")

                cursor = conn.execute(
                    "SELECT 1 FROM events WHERE event_id = ?", (event_id,)
                )
                exists = cursor.fetchone() is not None

                if exists:
                    stats["existing"] += 1
                    continue

                home_team_info = event.get("home", {})
                away_team_info = event.get("away", {})
                home_team_id = home_team_info.get("id")
                away_team_id = away_team_info.get("id")
                home_team_name = home_team_info.get("name", "Unknown")
                away_team_name = away_team_info.get("name", "Unknown")

                home_api_id = self._get_or_create_team(
                    conn, home_team_id, home_team_name
                )
                away_api_id = self._get_or_create_team(
                    conn, away_team_id, away_team_name
                )

                if home_api_id and away_api_id:
                    stats["teams_created"] += 1

                league_name = event.get("league", {}).get("name", "Unknown")

                match_date = None
                match_timestamp = None
                if event.get("time"):
                    try:
                        match_timestamp = int(event["time"])
                        match_date = datetime.fromtimestamp(match_timestamp).strftime(
                            "%Y-%m-%d %H:%M:%S"
                        )
                    except:
                        pass

                conn.execute(
                    """
                    INSERT INTO events 
                    (event_id, home_team_id, away_team_id, league_name, match_date, match_timestamp)
                    VALUES (?, ?, ?, ?, ?, ?)
                """,
                    (
                        event_id,
                        home_api_id,
                        away_api_id,
                        league_name,
                        match_date,
                        match_timestamp,
                    ),
                )

                stats["new"] += 1
                logger.info(
                    f"✅ Novo evento: {home_team_name} vs {away_team_name} - {league_name}"
                )

                try:
                    display_date = "Data não definida"
                    if match_date:
                        try:
                            dt = datetime.strptime(match_date, "%Y-%m-%d %H:%M:%S")
                            display_date = dt.strftime("%d/%m/%Y às %H:%M")
                        except:
                            display_date = match_date

                    success = self.telegram_notifier.notify_new_event(
                        home_team=home_team_name,
                        away_team=away_team_name,
                        league_name=league_name,
                        match_date=display_date,
                    )

                    if success:
                        stats["notifications_sent"] += 1
                    else:
                        stats["notification_errors"] += 1

                except Exception as e:
                    stats["notification_errors"] += 1
                    logger.error(
                        f"❌ Erro ao enviar notificação para Telegram: {str(e)}"
                    )

            conn.commit()

        logger.info(
            f"📝 Eventos processados - "
            f"Novos: {stats['new']}, "
            f"Existentes: {stats['existing']}"
        )
        return stats

    async def fetch_and_save_odds(
        self, hours_old_threshold: int = 2, batch_size: int = 10
    ):
        """Busca e salva odds em lotes para melhor performance"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                """
                SELECT DISTINCT e.event_id, ht.name, at.name
                FROM events e
                JOIN teams ht ON e.home_team_id = ht.team_id
                JOIN teams at ON e.away_team_id = at.team_id
                LEFT JOIN current_odds co ON e.event_id = co.event_id
                WHERE e.status = 'upcoming'
                AND (
                    co.event_id IS NULL
                    OR datetime(co.updated_at) < datetime('now', ?)
                )
                ORDER BY e.match_timestamp ASC
            """,
                (f"-{hours_old_threshold} hours",),
            )

            events_to_update = cursor.fetchall()

        if not events_to_update:
            logger.info("✅ Todos os eventos têm odds atualizadas")
            return 0

        logger.info(
            f"💰 Coletando odds para {len(events_to_update)} eventos em lotes de {batch_size}"
        )
        odds_collected = 0

        for i in range(0, len(events_to_update), batch_size):
            batch = events_to_update[i : i + batch_size]
            logger.info(
                f"   📦 Processando lote {i // batch_size + 1}/{(len(events_to_update) - 1) // batch_size + 1}"
            )

            tasks = []
            for event_id, home, away in batch:
                task = self._fetch_odds_for_event(event_id, home, away)
                tasks.append(task)

            batch_results = await asyncio.gather(*tasks, return_exceptions=True)

            for result in batch_results:
                if isinstance(result, Exception):
                    logger.error(f"      ❌ Erro em tarefa: {str(result)}")
                elif result:
                    odds_collected += 1

            if i + batch_size < len(events_to_update):
                await asyncio.sleep(0.5)

        logger.info(
            f"📊 Coleta finalizada: {odds_collected}/{len(events_to_update)} eventos com odds"
        )
        return odds_collected

    async def _fetch_odds_for_event(self, event_id: str, home: str, away: str) -> bool:
        """Busca odds para um evento específico com controle de concorrência"""
        async with self.semaphore:
            try:
                cache_key = f"{event_id}_{int(time.time() / 3600)}"
                if cache_key in self.odds_cache:
                    logger.debug(f"      ♻️  Usando cache para {home} vs {away}")
                    return True

                await self.rate_limiter.acquire()

                logger.debug(f"      📡 Buscando odds para {home} vs {away}")
                odds_data = await self.client.prematch(FI=event_id)

                if (
                    odds_data
                    and odds_data.get("success") == 1
                    and odds_data.get("results")
                ):
                    result = odds_data["results"][0]
                    self._save_odds_data(event_id, result)

                    self.odds_cache[cache_key] = True
                    if len(self.odds_cache) > 1000:
                        self.odds_cache.pop(next(iter(self.odds_cache)))

                    logger.debug(f"      ✅ Odds salvas para {home} vs {away}")
                    return True
                else:
                    logger.debug(f"      ⚠️ Sem odds disponíveis para {home} vs {away}")
                    return False

            except Exception as e:
                logger.error(
                    f"      ❌ Erro ao coletar odds para {home} vs {away}: {str(e)}"
                )
                return False

    def _save_odds_data(self, event_id: str, odds_data: Dict):
        """Processa e salva dados de odds INCLUINDO PLAYER ODDS"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("DELETE FROM current_odds WHERE event_id = ?", (event_id,))

            total_odds = 0

            # Seções regulares
            sections = ["main", "map_1", "map_2", "match", "schedule"]
            for section in sections:
                if section in odds_data and "sp" in odds_data[section]:
                    total_odds += self._process_odds_section(
                        conn, event_id, section, odds_data[section]["sp"]
                    )

            # Processar 'others'
            if "others" in odds_data and isinstance(odds_data["others"], list):
                for i, other_item in enumerate(odds_data["others"]):
                    if "sp" in other_item:
                        total_odds += self._process_odds_section(
                            conn, event_id, f"others_{i}", other_item["sp"]
                        )

            # PROCESSAR PLAYER ODDS
            if "player" in odds_data and "sp" in odds_data["player"]:
                player_odds = self._process_player_odds(
                    conn, event_id, odds_data["player"]["sp"]
                )
                total_odds += player_odds
                if player_odds > 0:
                    logger.debug(f"      🎮 {player_odds} player odds processadas")

            conn.commit()
            logger.debug(f"      📊 {total_odds} odds processadas")

    def _process_player_odds(self, conn, event_id: str, player_markets: Dict) -> int:
        """Processa mercados de jogadores"""
        odds_count = 0

        for market_key, market_data in player_markets.items():
            if not isinstance(market_data, dict) or "odds" not in market_data:
                continue

            market_name = market_data.get("name", market_key)

            for odd in market_data["odds"]:
                if isinstance(odd, dict):
                    try:
                        header = odd.get("header", "")
                        name = odd.get("name", "")
                        selection_name = f"{header} {name}".strip() if header else name
                        odds_value = float(odd.get("odds", 0))
                        handicap = odd.get("handicap", "")

                        if odds_value == 0:
                            continue

                        conn.execute(
                            """
                            INSERT INTO current_odds 
                            (event_id, odds_type, market_name, selection_name, odds_value, handicap, raw_data)
                            VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                            (
                                event_id,
                                "player",  # odds_type = player
                                market_name,
                                selection_name,
                                odds_value,
                                handicap,
                                json.dumps(odd),
                            ),
                        )

                        odds_count += 1
                    except Exception as e:
                        logger.error(f"❌ Erro ao salvar player odd: {str(e)}")

        return odds_count

    def _process_odds_section(
        self, conn, event_id: str, section: str, markets: Dict
    ) -> int:
        """Processa uma seção de mercados"""
        odds_count = 0

        for market_key, market_data in markets.items():
            if isinstance(market_data, list):
                market_name = f"{section}_{market_key}"
                for odd in market_data:
                    if isinstance(odd, dict):
                        odds_count += self._save_single_odd(
                            conn, event_id, section, market_name, odd
                        )
                continue

            if not isinstance(market_data, dict) or "odds" not in market_data:
                continue

            market_name = market_data.get("name", market_key)

            for odd in market_data["odds"]:
                if isinstance(odd, dict):
                    odds_count += self._save_single_odd(
                        conn, event_id, section, market_name, odd
                    )

        return odds_count

    def _save_single_odd(
        self, conn, event_id: str, section: str, market_name: str, odd: Dict
    ) -> int:
        """Salva uma odd individual"""
        try:
            header = odd.get("header", "")
            name = odd.get("name", "")
            selection_name = f"{header} {name}".strip() if header else name
            odds_value = float(odd.get("odds", 0))
            handicap = odd.get("handicap", "")

            if odds_value == 0:
                return 0

            conn.execute(
                """
                INSERT INTO current_odds 
                (event_id, odds_type, market_name, selection_name, odds_value, handicap, raw_data)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    event_id,
                    section,
                    market_name,
                    selection_name,
                    odds_value,
                    handicap,
                    json.dumps(odd),
                ),
            )

            return 1
        except Exception as e:
            logger.error(f"❌ Erro ao salvar odd: {str(e)}")
            return 0

    def generate_dashboard(self) -> str:
        """Gera um dashboard com estatísticas do banco"""
        with sqlite3.connect(self.db_path) as conn:
            dashboard = []
            dashboard.append("\n" + "=" * 60)
            dashboard.append("📊 DASHBOARD - LOL ODDS DATABASE")
            dashboard.append("=" * 60)

            cursor = conn.execute("SELECT COUNT(*) FROM events")
            total_events = cursor.fetchone()[0]

            cursor = conn.execute("SELECT COUNT(*) FROM current_odds")
            total_odds = cursor.fetchone()[0]

            cursor = conn.execute(
                "SELECT COUNT(*) FROM current_odds WHERE odds_type = 'player'"
            )
            player_odds = cursor.fetchone()[0]

            cursor = conn.execute("SELECT COUNT(*) FROM teams")
            total_teams = cursor.fetchone()[0]

            dashboard.append(f"\n📈 ESTATÍSTICAS GERAIS:")
            dashboard.append(f"  Total de Eventos: {total_events:,}")
            dashboard.append(f"  Total de Times: {total_teams:,}")
            dashboard.append(f"  Odds Atuais: {total_odds:,}")
            dashboard.append(f"  Player Odds: {player_odds:,}")

            if os.path.exists(self.db_path):
                size_mb = os.path.getsize(self.db_path) / 1024 / 1024
                dashboard.append(f"  Tamanho do Banco: {size_mb:.2f} MB")

            dashboard.append(f"\n🎮 PRÓXIMOS EVENTOS:")

            cursor = conn.execute(
                "SELECT COUNT(*) FROM events WHERE status = 'upcoming'"
            )
            upcoming = cursor.fetchone()[0]

            cursor = conn.execute(
                """
                SELECT COUNT(*) FROM events 
                WHERE match_timestamp BETWEEN strftime('%s', 'now') 
                AND strftime('%s', 'now', '+24 hours')
                AND status = 'upcoming'
            """
            )
            next_24h = cursor.fetchone()[0]

            dashboard.append(f"  Eventos Futuros: {upcoming}")
            dashboard.append(f"  Próximas 24h: {next_24h}")

            cursor = conn.execute(
                """
                SELECT 
                    COUNT(DISTINCT e.event_id) as total,
                    COUNT(DISTINCT co.event_id) as with_odds
                FROM events e
                LEFT JOIN current_odds co ON e.event_id = co.event_id
                WHERE e.status = 'upcoming'
            """
            )
            total, with_odds = cursor.fetchone()
            coverage = (with_odds / total * 100) if total > 0 else 0

            dashboard.append(f"\n📊 COBERTURA DE ODDS:")
            dashboard.append(
                f"  Eventos com Odds: {with_odds}/{total} ({coverage:.1f}%)"
            )

            # Player odds coverage
            cursor = conn.execute(
                """
                SELECT COUNT(DISTINCT event_id) 
                FROM current_odds 
                WHERE odds_type = 'player'
            """
            )
            events_with_player_odds = cursor.fetchone()[0]
            dashboard.append(f"  Eventos com Player Odds: {events_with_player_odds}")

            cursor = conn.execute("SELECT MAX(updated_at) FROM current_odds")
            last_update = cursor.fetchone()[0]
            if last_update:
                dashboard.append(f"\n⏰ Última Atualização de Odds: {last_update}")

            dashboard.append("=" * 60 + "\n")

            return "\n".join(dashboard)

    def cleanup_old_data(self, days_keep: int = 30):
        """Remove dados antigos"""
        cutoff_timestamp = int((datetime.now() - timedelta(days=days_keep)).timestamp())

        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                "DELETE FROM events WHERE match_timestamp < ?", (cutoff_timestamp,)
            )
            deleted_events = cursor.rowcount

            cursor = conn.execute(
                """
                DELETE FROM teams 
                WHERE team_id NOT IN (
                    SELECT home_team_id FROM events
                    UNION
                    SELECT away_team_id FROM events
                )
            """
            )
            deleted_teams = cursor.rowcount

            conn.commit()

        with sqlite3.connect(self.db_path) as conn:
            conn.execute("VACUUM")

        logger.info(
            f"🧹 Limpeza concluída - Removidos: {deleted_events} eventos, {deleted_teams} times"
        )

    async def run_update(self):
        """Executa atualização completa"""
        logger.info("=" * 60)
        logger.info("🚀 INICIANDO ATUALIZAÇÃO DO BANCO DE ODDS LOL")
        logger.info("=" * 60)

        start_time = datetime.now()

        try:
            logger.info("\n📅 FASE 1: Buscando eventos...")
            events = await self.fetch_upcoming_events(days_ahead=10)

            if events:
                stats = self.save_events(events)
                logger.info(
                    f"✅ Fase 1 concluída - {stats['new']} novos eventos adicionados"
                )
            else:
                logger.warning("ℹ️ Nenhum evento encontrado")

            logger.info("\n💰 FASE 2: Atualizando odds...")
            odds_collected = await self.fetch_and_save_odds(
                hours_old_threshold=2, batch_size=10
            )
            logger.info(
                f"✅ Fase 2 concluída - {odds_collected} eventos com odds atualizadas"
            )

            # 3. Limpeza (uma vez por semana)
            if datetime.now().weekday() == 0:  # Segunda-feira
                logger.info("\n🧹 FASE 3: Limpeza semanal...")
                self.cleanup_old_data(days_keep=30)

            # 4. Gerar e exibir dashboard
            dashboard = self.generate_dashboard()
            print(dashboard)

            # Salvar dashboard em arquivo
            dashboard_dir = Path(__file__).parent.parent / "reports"
            dashboard_dir.mkdir(exist_ok=True)
            dashboard_file = (
                dashboard_dir / f"dashboard_{datetime.now():%Y%m%d_%H%M%S}.txt"
            )
            dashboard_file.write_text(dashboard, encoding="utf-8")
            logger.info(f"💾 Dashboard salvo em: {dashboard_file}")

            elapsed = (datetime.now() - start_time).total_seconds()
            logger.info(f"\n✅ ATUALIZAÇÃO COMPLETA EM {elapsed:.1f} SEGUNDOS")

        except Exception as e:
            logger.error(f"❌ Erro durante atualização: {str(e)}", exc_info=True)
            raise

    async def close(self):
        """Fecha conexões"""
        await self.client.close()
        logger.info("🔌 Conexões fechadas")


async def main():
    """Função principal - executa atualização completa"""
    db = LoLOddsDatabase()

    try:
        await db.run_update()
    finally:
        await db.close()


if __name__ == "__main__":
    asyncio.run(main())
