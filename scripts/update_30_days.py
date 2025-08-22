#!/usr/bin/env python3
"""
Script para atualizar os últimos 30 dias, verificando duplicatas
Versão com logging otimizado para reduzir poluição
"""

import asyncio
import sys
import sqlite3
import logging
from datetime import datetime, timedelta
from pathlib import Path

# Configurar caminhos de importação
sys.path.insert(0, str(Path(__file__).parent.parent))

# Reduzir logging de bibliotecas de terceiros
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("anyio").setLevel(logging.WARNING)
logging.getLogger("asyncio").setLevel(logging.WARNING)

# Importações reais do seu projeto
from src.core.bet365_client import Bet365Client
from src.core.database import LoLDatabase  # Assumindo que esta classe existe

# Configurar logging com mais detalhes
logging.basicConfig(
    level=logging.INFO,  # Mudado de DEBUG para INFO para reduzir ruído
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("database_updater.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger("database_updater")


class DatabaseUpdater:
    def __init__(self):
        self.db = LoLDatabase()
        self.client = Bet365Client()
        self.lol_sport_id = 151

    async def update_last_days(self, days_back=30):
        """Atualiza os últimos X dias, verificando duplicatas"""
        logger.info(f"🚀 INICIANDO ATUALIZAÇÃO DOS ÚLTIMOS {days_back} DIAS")
        logger.info("=" * 60)

        total_processed = 0
        total_new = 0
        total_updated = 0
        total_skipped = 0
        total_errors = 0

        for i in range(days_back):
            target_date = datetime.now() - timedelta(days=i)
            day_str = target_date.strftime("%Y%m%d")
            formatted_date = target_date.strftime("%Y-%m-%d")

            logger.info(f"📅 PROCESSANDO DIA: {formatted_date} ({i + 1}/{days_back})")

            # Buscar eventos do dia
            events = await self.get_events_for_day(day_str)
            if not events:
                logger.info(f"   ℹ️  Nenhum evento encontrado para {formatted_date}")
                continue

            # Filtrar apenas LoL
            lol_events = [event for event in events if self.is_lol_event(event)]
            logger.info(f"   🎯 {len(lol_events)} jogos de LoL encontrados")

            day_processed = 0
            day_new = 0
            day_updated = 0
            day_errors = 0

            for event in lol_events:
                event_id = event.get("id")
                home_team = event.get("home", {}).get("name", "Desconhecido")
                away_team = event.get("away", {}).get("name", "Desconhecido")
                league = event.get("league", {}).get("name", "Desconhecida")

                logger.info(
                    f"   ⚔️  Processando: {home_team} vs {away_team} ({league}) - ID: {event_id}"
                )

                result = await self.process_event(event)
                if result == "new":
                    day_new += 1
                    total_new += 1
                    logger.info(f"   ✅ NOVO EVENTO ADICIONADO: {event_id}")
                elif result == "updated":
                    day_updated += 1
                    total_updated += 1
                    logger.info(f"   🔄 EVENTO ATUALIZADO: {event_id}")
                elif result == "skipped":
                    total_skipped += 1
                    logger.info(f"   ⏭️  EVENTO PULADO: {event_id}")
                elif result == "error":
                    day_errors += 1
                    total_errors += 1
                    logger.error(f"   ❌ ERRO NO EVENTO: {event_id}")

                day_processed += 1
                total_processed += 1

            logger.info(f"   📊 RESUMO DO DIA {formatted_date}:")
            logger.info(f"      ➕ Novos: {day_new}")
            logger.info(f"      🔄 Atualizados: {day_updated}")
            logger.info(
                f"      ⏭️  Pulados: {day_processed - day_new - day_updated - day_errors}"
            )
            logger.info(f"      ❌ Erros: {day_errors}")
            logger.info(f"      📋 Total processado: {day_processed}")

            # Pequena pausa para não sobrecarregar a API
            await asyncio.sleep(1)

        # Relatório final
        logger.info("=" * 60)
        logger.info("🎉 ATUALIZAÇÃO CONCLUÍDA!")
        logger.info("📊 RELATÓRIO FINAL:")
        logger.info(f"   📅 Período: {days_back} dias")
        logger.info(f"   📋 Total de eventos processados: {total_processed}")
        logger.info(f"   ➕ Novos eventos: {total_new}")
        logger.info(f"   🔄 Eventos atualizados: {total_updated}")
        logger.info(f"   ⏭️  Eventos pulados: {total_skipped}")
        logger.info(f"   ❌ Erros: {total_errors}")
        logger.info("=" * 60)

    async def get_events_for_day(self, day_str):
        """Busca eventos de um dia específico"""
        try:
            logger.info(f"   🔍 Buscando eventos para o dia {day_str}...")
            events_data = await self.client.upcoming(
                sport_id=self.lol_sport_id, day=day_str
            )

            if events_data and events_data.get("success") == 1:
                events = events_data.get("results", [])
                logger.info(
                    f"   ✅ {len(events)} eventos encontrados para o dia {day_str}"
                )
                return events
            else:
                logger.warning(f"   ⚠️  Nenhum dado retornado para o dia {day_str}")
                return []

        except Exception as e:
            logger.error(f"   ❌ Erro ao buscar eventos para {day_str}: {e}")
            return []

    def is_lol_event(self, event):
        """Filtra apenas eventos de LoL"""
        league_name = event.get("league", {}).get("name", "").strip()
        return league_name.startswith("LOL -")

    def event_exists(self, event_id):
        """Verifica se o evento já existe no banco"""
        conn = self.db.get_connection()
        cursor = conn.cursor()

        cursor.execute("SELECT 1 FROM matches WHERE bet365_id = ?", (event_id,))
        exists = cursor.fetchone() is not None

        conn.close()
        return exists

    def needs_update(self, event_id):
        """Verifica se o evento precisa ser atualizado"""
        conn = self.db.get_connection()
        cursor = conn.cursor()

        # Buscar informações atuais do evento
        cursor.execute(
            """
            SELECT time_status, retrieved_at 
            FROM matches 
            WHERE bet365_id = ?
            """,
            (event_id,),
        )
        result = cursor.fetchone()

        if not result:
            conn.close()
            return False

        time_status, retrieved_at = result

        # Se o evento está finalizado, não precisa atualizar
        if time_status == 3:
            logger.info(
                f"   ⏭️  Evento {event_id} está finalizado, não precisa atualizar"
            )
            conn.close()
            return False

        # Verificar quando foi a última atualização
        if retrieved_at:
            try:
                # Converter para datetime, lidando com diferentes formatos
                if isinstance(retrieved_at, str):
                    # Tentar parsear com frações de segundo
                    try:
                        last_update = datetime.strptime(
                            retrieved_at, "%Y-%m-%d %H:%M:%S.%f"
                        )
                    except ValueError:
                        # Tentar parsear sem frações de segundo
                        last_update = datetime.strptime(
                            retrieved_at, "%Y-%m-%d %H:%M:%S"
                        )
                else:
                    last_update = retrieved_at

                time_since_update = datetime.now() - last_update

                # Atualizar apenas se passou mais de 1 hora da última atualização
                if time_since_update.total_seconds() < 10:
                    logger.info(
                        f"   ⏭️  Evento {event_id} atualizado recentemente, não precisa atualizar"
                    )
                    conn.close()
                    return False

            except (ValueError, TypeError) as e:
                logger.warning(
                    f"   ⚠️  Erro ao analisar data de atualização do evento {event_id}: {e}"
                )
                # Em caso de erro, assumir que precisa atualizar
                conn.close()
                return True

        conn.close()
        logger.info(f"   ✅ Evento {event_id} precisa ser atualizado")
        return True

    async def process_event(self, event):
        """Processa um evento e retorna o status"""
        event_id = event.get("id")

        if not event_id:
            logger.error("   ❌ Evento sem ID")
            return "error"

        # Verificar se já existe
        if self.event_exists(event_id):
            if self.needs_update(event_id):
                # Evento existe mas precisa ser atualizado
                return await self.update_existing_event(event)
            else:
                # Evento já existe e não precisa ser atualizado
                return "skipped"
        else:
            # Novo evento
            return await self.process_new_event(event)

    async def process_new_event(self, event):
        """Processa um novo evento"""
        event_id = event.get("id")

        try:
            logger.info(f"   📥 Buscando detalhes do novo evento {event_id}...")
            result_data = await self.client.result(event_id)
            if result_data and result_data.get("success") == 1:
                result = result_data.get("results", [{}])[0]
                success = self.save_event(event, result)
                if success:
                    return "new"
                else:
                    return "error"
            else:
                logger.error(f"   ❌ Erro ao buscar detalhes do evento {event_id}")
                return "error"
        except Exception as e:
            logger.error(f"   ❌ Erro ao processar novo evento {event_id}: {e}")
            return "error"

    async def update_existing_event(self, event):
        """Atualiza um evento existente"""
        event_id = event.get("id")

        try:
            logger.info(f"   📥 Buscando detalhes para atualizar evento {event_id}...")
            result_data = await self.client.result(event_id)
            if result_data and result_data.get("success") == 1:
                result = result_data.get("results", [{}])[0]
                success = self.update_event(event, result)
                if success:
                    return "updated"
                else:
                    return "error"
            else:
                logger.error(
                    f"   ❌ Erro ao buscar detalhes para atualizar evento {event_id}"
                )
                return "error"
        except Exception as e:
            logger.error(f"   ❌ Erro ao atualizar evento {event_id}: {e}")
            return "error"

    def save_event(self, event, result):
        """Salva um novo evento no banco"""
        conn = self.db.get_connection()
        cursor = conn.cursor()

        try:
            # 1. Salvar liga
            league_name = event.get("league", {}).get("name")
            league_id = event.get("league", {}).get("id")

            if league_id and league_name:
                cursor.execute(
                    "INSERT OR IGNORE INTO leagues (league_id, name) VALUES (?, ?)",
                    (league_id, league_name),
                )

            # 2. Salvar times
            home_team = event.get("home", {})
            away_team = event.get("away", {})

            if home_team.get("id"):
                cursor.execute(
                    "INSERT OR IGNORE INTO teams (team_id, name, image_id, country_code) VALUES (?, ?, ?, ?)",
                    (
                        home_team.get("id"),
                        home_team.get("name"),
                        home_team.get("image_id"),
                        home_team.get("cc"),
                    ),
                )

            if away_team.get("id"):
                cursor.execute(
                    "INSERT OR IGNORE INTO teams (team_id, name, image_id, country_code) VALUES (?, ?, ?, ?)",
                    (
                        away_team.get("id"),
                        away_team.get("name"),
                        away_team.get("image_id"),
                        away_team.get("cc"),
                    ),
                )

            # 3. Salvar jogo principal
            event_time = (
                datetime.fromtimestamp(int(event.get("time")))
                if event.get("time")
                else datetime.now()
            )
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[
                :-3
            ]  # Formato com milissegundos

            cursor.execute(
                """
                INSERT INTO matches 
                (bet365_id, sport_id, league_id, home_team_id, away_team_id, 
                 event_time, time_status, final_score, retrieved_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event.get("id"),
                    event.get("sport_id"),
                    event.get("league", {}).get("id"),
                    home_team.get("id"),
                    away_team.get("id"),
                    event_time,
                    event.get("time_status"),
                    result.get("ss"),
                    current_time,
                ),
            )

            # Obter o ID do jogo inserido
            match_id = cursor.lastrowid

            # 4. Salvar mapas e estatísticas
            self._save_map_stats(cursor, match_id, result)

            conn.commit()
            logger.info(
                f"   💾 Evento {event.get('id')} salvo com sucesso no banco de dados"
            )
            return True

        except Exception as e:
            conn.rollback()
            logger.error(f"   ❌ Erro ao salvar evento {event.get('id')}: {e}")
            return False
        finally:
            conn.close()

    def update_event(self, event, result):
        """Atualiza um evento existente"""
        conn = self.db.get_connection()
        cursor = conn.cursor()

        try:
            # Atualizar informações principais
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[
                :-3
            ]  # Formato com milissegundos

            cursor.execute(
                """
                UPDATE matches 
                SET time_status = ?, final_score = ?, retrieved_at = ?
                WHERE bet365_id = ?
                """,
                (
                    event.get("time_status"),
                    result.get("ss"),
                    current_time,
                    event.get("id"),
                ),
            )

            # Se o jogo foi finalizado, salvar estatísticas completas
            if result.get("time_status") == "3":
                # Obter match_id
                cursor.execute(
                    "SELECT match_id FROM matches WHERE bet365_id = ?",
                    (event.get("id"),),
                )
                match_row = cursor.fetchone()
                if match_row:
                    match_id = match_row[0]
                    self._save_map_stats(cursor, match_id, result)

            conn.commit()
            logger.info(
                f"   💾 Evento {event.get('id')} atualizado com sucesso no banco de dados"
            )
            return True

        except Exception as e:
            conn.rollback()
            logger.error(f"   ❌ Erro ao atualizar evento {event.get('id')}: {e}")
            return False
        finally:
            conn.close()

    def _save_map_stats(self, cursor, match_id, result):
        """Salva estatísticas de mapas (método auxiliar)"""
        period_stats = result.get("period_stats", {})
        if not period_stats:
            logger.info("   ℹ️  Nenhuma estatística de mapa encontrada para este evento")
            return

        map_count = 0
        stat_count = 0

        for map_number, stats in period_stats.items():
            try:
                # Salvar mapa
                cursor.execute(
                    "INSERT OR REPLACE INTO game_maps (match_id, map_number) VALUES (?, ?)",
                    (match_id, int(map_number)),
                )

                # Obter o ID do mapa
                map_id = cursor.lastrowid
                map_count += 1

                # Salvar estatísticas
                for stat_name, values in stats.items():
                    if isinstance(values, list) and len(values) == 2:
                        cursor.execute(
                            "INSERT OR REPLACE INTO map_statistics (map_id, stat_name, home_value, away_value) VALUES (?, ?, ?, ?)",
                            (map_id, stat_name, values[0], values[1]),
                        )
                        stat_count += 1
            except (ValueError, TypeError) as e:
                logger.warning(f"   ⚠️  Erro ao processar mapa {map_number}: {e}")
                continue

        logger.info(
            f"   📊 Estatísticas salvas: {map_count} mapas, {stat_count} estatísticas"
        )


async def main():
    logger.info("🎯 INICIANDO ATUALIZADOR DOS ÚLTIMOS 30 DIAS")
    logger.info("=" * 60)

    updater = DatabaseUpdater()

    try:
        await updater.update_last_days(days_back=30)
        logger.info("✅ Atualização concluída com sucesso!")

    except Exception as e:
        logger.error(f"❌ ERRO CRÍTICO durante a atualização: {e}")
        import traceback

        logger.error(traceback.format_exc())
    finally:
        await updater.client.close()
        logger.info("👋 Conexões encerradas. Script finalizado.")


if __name__ == "__main__":
    # Executar o script
    asyncio.run(main())
