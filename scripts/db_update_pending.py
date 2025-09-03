#!/usr/bin/env python3
"""
Script para adicionar eventos faltantes ao banco de eSports
Usa informações do banco de apostas e da API Bet365
"""

import asyncio
import sys
import sqlite3
import json
import logging
from datetime import datetime
from pathlib import Path
import os

# Configurar caminhos de importação
sys.path.insert(0, str(Path(__file__).parent.parent))

# Reduzir logging de bibliotecas de terceiros
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("anyio").setLevel(logging.WARNING)
logging.getLogger("asyncio").setLevel(logging.WARNING)

# Importações reais do seu projeto
from src.core.bet365_client import Bet365Client

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("add_missing_events.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger("add_missing_events")


class MissingEventsAdder:
    def __init__(self):
        self.client = Bet365Client()
        self.esports_db_path = "../data/lol_esports.db"
        self.bets_db_path = "../data/bets.db"
        self.problematic_events_file = "problematic_event_ids.json"

        # Verificar se os bancos de dados existem
        if not os.path.exists(self.esports_db_path):
            logger.error(
                f"❌ Banco de eSports não encontrado em: {self.esports_db_path}"
            )
            raise FileNotFoundError(f"Database not found: {self.esports_db_path}")

        if not os.path.exists(self.bets_db_path):
            logger.error(f"❌ Banco de apostas não encontrado em: {self.bets_db_path}")
            raise FileNotFoundError(f"Database not found: {self.bets_db_path}")

        logger.info(f"✅ Banco de eSports encontrado: {self.esports_db_path}")
        logger.info(f"✅ Banco de apostas encontrado: {self.bets_db_path}")

    async def add_missing_events(self):
        """Adiciona eventos faltantes ao banco de eSports"""
        logger.info("🚀 INICIANDO ADIÇÃO DE EVENTOS FALTANTES")
        logger.info("=" * 60)

        # Carregar eventos problemáticos
        event_ids = self.load_problematic_events()
        if not event_ids:
            logger.info("ℹ️  Nenhum evento problemático encontrado")
            return

        logger.info(f"📋 {len(event_ids)} eventos problemáticos para verificar")

        total_processed = 0
        total_added = 0
        total_errors = 0
        missing_events = []

        # Primeiro, identificar quais eventos estão faltando
        for event_id in event_ids:
            if not self.event_exists_in_esports(event_id):
                missing_events.append(event_id)
                logger.info(
                    f"   🔍 Evento {event_id} não encontrado no banco de eSports"
                )

        logger.info(f"📊 {len(missing_events)} eventos faltantes identificados")

        # Processar eventos faltantes
        for i, event_id in enumerate(missing_events, 1):
            logger.info(f"[{i}/{len(missing_events)}] Adicionando evento: {event_id}")

            try:
                # Obter informações do banco de apostas
                event_info = self.get_event_info_from_bets_db(event_id)
                if not event_info:
                    logger.error(
                        f"   ❌ Não foi possível obter informações do evento {event_id} no banco de apostas"
                    )
                    total_errors += 1
                    continue

                # Buscar informações adicionais da API
                api_data = await self.get_event_data_from_api(event_id)

                # Adicionar evento ao banco de eSports
                add_success = self.add_event_to_esports(event_id, event_info, api_data)

                if add_success:
                    total_added += 1
                    logger.info(f"   ✅ Evento {event_id} adicionado com sucesso")
                else:
                    total_errors += 1
                    logger.error(f"   ❌ Falha ao adicionar evento {event_id}")

                total_processed += 1

                # Pequena pausa para não sobrecarregar a API
                await asyncio.sleep(1)

            except Exception as e:
                logger.error(
                    f"   ❌ Erro inesperado ao processar evento {event_id}: {e}"
                )
                total_errors += 1
                continue

        # Relatório final
        logger.info("=" * 60)
        logger.info("📊 RELATÓRIO FINAL:")
        logger.info(f"   ✅ Eventos processados: {total_processed}")
        logger.info(f"   ➕ Eventos adicionados: {total_added}")
        logger.info(f"   ❌ Erros: {total_errors}")
        logger.info("=" * 60)

        # Salvar lista de eventos ainda faltantes (se houver)
        if total_errors > 0:
            self.save_remaining_events(missing_events, total_errors)

    def load_problematic_events(self):
        """Carrega os IDs dos eventos problemáticos"""
        if not os.path.exists(self.problematic_events_file):
            logger.warning(f"⚠️  Arquivo {self.problematic_events_file} não encontrado")
            return []

        try:
            with open(self.problematic_events_file, "r") as f:
                event_ids = json.load(f)
            return event_ids
        except Exception as e:
            logger.error(f"❌ Erro ao carregar eventos problemáticos: {e}")
            return []

    def event_exists_in_esports(self, bet365_id):
        """Verifica se o evento existe no banco de eSports"""
        try:
            conn = sqlite3.connect(self.esports_db_path)
            cursor = conn.cursor()

            cursor.execute(
                "SELECT 1 FROM matches WHERE bet365_id = ?",
                (bet365_id,),
            )

            exists = cursor.fetchone() is not None
            return exists

        except Exception as e:
            logger.error(
                f"Erro ao verificar evento {bet365_id} no banco de eSports: {e}"
            )
            return False
        finally:
            if conn:
                conn.close()

    def get_event_info_from_bets_db(self, event_id):
        """Obtém informações do evento do banco de apostas"""
        try:
            conn = sqlite3.connect(self.bets_db_path)
            cursor = conn.cursor()

            cursor.execute(
                """
                SELECT event_id, league_name, match_date, home_team, away_team, 
                       status, home_score, away_score, winner
                FROM events 
                WHERE event_id = ?
                """,
                (event_id,),
            )

            event_row = cursor.fetchone()
            if not event_row:
                return None

            # Converter para dicionário
            columns = [description[0] for description in cursor.description]
            event_info = dict(zip(columns, event_row))

            return event_info

        except Exception as e:
            logger.error(f"Erro ao buscar evento {event_id} no banco de apostas: {e}")
            return None
        finally:
            if conn:
                conn.close()

    async def get_event_data_from_api(self, event_id):
        """Obtém dados do evento da API Bet365"""
        try:
            result_data = await self.client.result(event_id)
            if result_data and result_data.get("success") == 1:
                return result_data.get("results", [{}])[0]
            else:
                logger.warning(
                    f"   ⚠️  Não foi possível obter dados da API para evento {event_id}"
                )
                return {}
        except Exception as e:
            logger.error(f"Erro ao buscar dados da API para evento {event_id}: {e}")
            return {}

    def add_event_to_esports(self, event_id, event_info, api_data):
        """Adiciona um evento ao banco de eSports"""
        conn = sqlite3.connect(self.esports_db_path)
        cursor = conn.cursor()

        try:
            # Extrair informações
            league_name = event_info.get("league_name", "Unknown League")
            home_team = event_info.get("home_team", "Unknown Home Team")
            away_team = event_info.get("away_team", "Unknown Away Team")
            match_date = event_info.get("match_date")

            # Obter ou criar IDs da liga e times
            league_id = self.get_or_create_league(cursor, league_name)
            home_team_id = self.get_or_create_team(cursor, home_team)
            away_team_id = self.get_or_create_team(cursor, away_team)

            # Extrair informações da API se disponíveis
            time_status = api_data.get("time_status", "1")  # Default para não iniciado
            final_score = api_data.get("ss")

            # Converter data do evento
            event_time = self.parse_date(match_date)

            # Inserir o evento
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

            cursor.execute(
                """
                INSERT INTO matches 
                (bet365_id, sport_id, league_id, home_team_id, away_team_id,
                 event_time, time_status, final_score, retrieved_at, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event_id,
                    151,  # sport_id para LoL
                    league_id,
                    home_team_id,
                    away_team_id,
                    event_time,
                    time_status,
                    final_score,
                    current_time,
                    current_time,
                    current_time,
                ),
            )

            # Obter o ID do match inserido
            match_id = cursor.lastrowid

            # Se houver dados de mapas da API, inseri-los
            if api_data.get("time_status") == "3" and api_data.get("period_stats"):
                self._add_map_stats(cursor, match_id, api_data["period_stats"])

            conn.commit()
            logger.info(f"   💾 Evento {event_id} inserido como match_id {match_id}")
            return True

        except Exception as e:
            conn.rollback()
            logger.error(
                f"Erro ao adicionar evento {event_id} ao banco de eSports: {e}"
            )
            return False
        finally:
            conn.close()

    def get_or_create_league(self, cursor, league_name):
        """Obtém ou cria uma liga no banco de eSports"""
        # Primeiro, tenta encontrar a liga
        cursor.execute(
            "SELECT league_id FROM leagues WHERE name = ?",
            (league_name,),
        )

        league_row = cursor.fetchone()
        if league_row:
            return league_row[0]

        # Se não encontrou, cria uma nova liga
        cursor.execute(
            "INSERT INTO leagues (name, created_at) VALUES (?, ?)",
            (league_name, datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
        )

        return cursor.lastrowid

    def get_or_create_team(self, cursor, team_name):
        """Obtém ou cria um time no banco de eSports"""
        # Primeiro, tenta encontrar o time
        cursor.execute(
            "SELECT team_id FROM teams WHERE name = ?",
            (team_name,),
        )

        team_row = cursor.fetchone()
        if team_row:
            return team_row[0]

        # Se não encontrou, cria um novo time
        cursor.execute(
            "INSERT INTO teams (name, created_at) VALUES (?, ?)",
            (team_name, datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
        )

        return cursor.lastrowid

    def parse_date(self, date_string):
        """Converte uma string de data para o formato do banco de dados"""
        if not date_string:
            return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        try:
            # Tenta converter de vários formatos possíveis
            for fmt in (
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%d",
                "%d/%m/%Y %H:%M:%S",
                "%d/%m/%Y",
            ):
                try:
                    dt = datetime.strptime(date_string, fmt)
                    return dt.strftime("%Y-%m-%d %H:%M:%S")
                except ValueError:
                    continue

            # Se nenhum formato funcionar, usa a data atual
            return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        except:
            return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def _add_map_stats(self, cursor, match_id, period_stats):
        """Adiciona estatísticas de mapas para um match"""
        map_count = 0
        stat_count = 0

        for map_number, stats in period_stats.items():
            try:
                # Inserir mapa
                cursor.execute(
                    "INSERT INTO game_maps (match_id, map_number, created_at) VALUES (?, ?, ?)",
                    (
                        match_id,
                        int(map_number),
                        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    ),
                )

                # Obter o ID do mapa inserido
                map_id = cursor.lastrowid
                map_count += 1

                # Inserir estatísticas
                for stat_name, values in stats.items():
                    if isinstance(values, list) and len(values) == 2:
                        cursor.execute(
                            "INSERT INTO map_statistics (map_id, stat_name, home_value, away_value, created_at) VALUES (?, ?, ?, ?, ?)",
                            (
                                map_id,
                                stat_name,
                                values[0],
                                values[1],
                                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            ),
                        )
                        stat_count += 1

            except (ValueError, TypeError) as e:
                logger.warning(f"      ⚠️  Erro ao processar mapa {map_number}: {e}")
                continue

        logger.info(
            f"      📊 {map_count} mapas e {stat_count} estatísticas adicionadas"
        )

    def save_remaining_events(self, missing_events, error_count):
        """Salva a lista de eventos que ainda estão faltantes"""
        remaining_events = missing_events[-error_count:] if error_count > 0 else []

        if remaining_events:
            with open("remaining_missing_events.json", "w") as f:
                json.dump(remaining_events, f)
            logger.info(
                f"💾 Lista de {len(remaining_events)} eventos ainda faltantes salva em remaining_missing_events.json"
            )

    async def close(self):
        """Fecha conexões"""
        await self.client.close()


async def main():
    logger.info("🎯 INICIANDO ADIÇÃO DE EVENTOS FALTANTES AO BANCO DE ESPORTS")
    logger.info("=" * 60)

    try:
        adder = MissingEventsAdder()
    except FileNotFoundError as e:
        logger.error(f"❌ {e}. Abortando.")
        return

    try:
        await adder.add_missing_events()
        logger.info("✅ Adição de eventos faltantes concluída!")

    except Exception as e:
        logger.error(f"❌ ERRO CRÍTICO durante a adição de eventos: {e}")
        import traceback

        logger.error(traceback.format_exc())
    finally:
        await adder.close()
        logger.info("👋 Conexões encerradas. Script finalizado.")


if __name__ == "__main__":
    # Executar o script
    asyncio.run(main())
