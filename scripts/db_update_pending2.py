#!/usr/bin/env python3
"""
Script para atualizar resultados de eventos problemáticos no banco de eSports
Com verificações aprimoradas e logging detalhado
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
        logging.FileHandler("esports_results_updater_enhanced.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger("esports_results_updater")


class EsportsResultsUpdater:
    def __init__(self):
        self.client = Bet365Client()
        self.esports_db_path = "../data/lol_esports.db"
        self.problematic_events_file = "problematic_event_ids.json"

        # Verificar se o banco de dados existe
        if not os.path.exists(self.esports_db_path):
            logger.error(f"❌ Banco de dados não encontrado em: {self.esports_db_path}")
            raise FileNotFoundError(f"Database not found: {self.esports_db_path}")
        else:
            logger.info(f"✅ Banco de dados encontrado: {self.esports_db_path}")

    async def update_esports_results(self):
        """Atualiza resultados no banco de eSports para eventos problemáticos"""
        logger.info("🚀 INICIANDO ATUALIZAÇÃO DE RESULTADOS NO BANCO DE ESPORTS")
        logger.info("=" * 60)

        # Carregar eventos problemáticos
        event_ids = self.load_problematic_events()
        if not event_ids:
            logger.info("ℹ️  Nenhum evento problemático encontrado para atualizar")
            return

        logger.info(f"📋 {len(event_ids)} eventos problemáticos para processar")

        total_processed = 0
        total_updated = 0
        total_errors = 0
        total_not_found = 0

        for i, event_id in enumerate(event_ids, 1):
            logger.info(f"[{i}/{len(event_ids)}] Processando evento: {event_id}")

            try:
                # Verificar se o evento existe no banco de eSports
                match_info = self.get_match_info(event_id)
                if not match_info:
                    logger.warning(
                        f"   ⚠️  Evento {event_id} não encontrado no banco de eSports"
                    )
                    total_not_found += 1
                    continue

                # Buscar resultado na API
                result_data = await self.client.result(event_id)
                if not result_data or result_data.get("success") != 1:
                    logger.error(
                        f"   ❌ Erro ao buscar resultado para evento {event_id}"
                    )
                    total_errors += 1
                    continue

                # Processar resultado
                result = result_data.get("results", [{}])[0]

                # Verificar se há dados válidos para atualização
                if not self.has_valid_result_data(result):
                    logger.warning(f"   ⚠️  Nenhum dado válido para evento {event_id}")
                    continue

                # Atualizar o banco de dados
                update_success = self.update_match_and_stats(
                    match_info["match_id"], result
                )

                if update_success:
                    total_updated += 1
                    logger.info(f"   ✅ Evento {event_id} atualizado com sucesso")

                    # Verificar se a atualização foi realmente persistida
                    if self.verify_update(event_id, result):
                        logger.info(
                            f"   ✅ Alterações verificadas para evento {event_id}"
                        )
                    else:
                        logger.warning(
                            f"   ⚠️  Alterações não persistidas para evento {event_id}"
                        )
                else:
                    total_errors += 1
                    logger.error(f"   ❌ Falha ao atualizar evento {event_id}")

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
        logger.info(f"   🔄 Eventos atualizados: {total_updated}")
        logger.info(f"   ❌ Erros: {total_errors}")
        logger.info(f"   🔍 Eventos não encontrados: {total_not_found}")
        logger.info("=" * 60)

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

    def get_match_info(self, bet365_id):
        """Obtém informações do match no banco de eSports"""
        try:
            conn = sqlite3.connect(self.esports_db_path)
            cursor = conn.cursor()

            cursor.execute(
                """
                SELECT match_id, bet365_id, sport_id, league_id, home_team_id, away_team_id,
                       event_time, time_status, final_score, retrieved_at
                FROM matches 
                WHERE bet365_id = ?
                """,
                (bet365_id,),
            )

            match_row = cursor.fetchone()
            if not match_row:
                return None

            # Converter para dicionário
            columns = [description[0] for description in cursor.description]
            match_info = dict(zip(columns, match_row))

            return match_info

        except Exception as e:
            logger.error(f"Erro ao buscar match {bet365_id} no banco de eSports: {e}")
            return None
        finally:
            if conn:
                conn.close()

    def has_valid_result_data(self, result):
        """Verifica se os dados do resultado são válidos para atualização"""
        if not result:
            return False

        time_status = result.get("time_status")
        ss = result.get("ss")
        period_stats = result.get("period_stats", {})

        # Verificar se temos pelo menos algum dado válido
        has_valid_status = time_status and time_status in ["1", "2", "3"]
        has_valid_score = ss and ss.strip() and ss != "0-0"
        has_valid_stats = bool(period_stats)

        return has_valid_status or has_valid_score or has_valid_stats

    def update_match_and_stats(self, match_id, result):
        """Atualiza o match e suas estatísticas no banco de eSports"""
        conn = sqlite3.connect(self.esports_db_path)
        cursor = conn.cursor()

        try:
            # Extrair informações do resultado
            time_status = result.get("time_status")
            ss = result.get("ss")
            period_stats = result.get("period_stats", {})
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

            logger.info(
                f"   📝 Atualizando match_id {match_id}: status={time_status}, score={ss}"
            )

            # Atualizar informações principais do match
            cursor.execute(
                """
                UPDATE matches 
                SET time_status = ?, final_score = ?, retrieved_at = ?, updated_at = ?
                WHERE match_id = ?
                """,
                (time_status, ss, current_time, current_time, match_id),
            )

            # Verificar se a atualização afetou alguma linha
            if cursor.rowcount == 0:
                logger.warning(
                    f"   ⚠️  Nenhuma linha afetada ao atualizar match_id {match_id}"
                )

            # Se o evento foi finalizado, atualizar estatísticas dos mapas
            if time_status == "3" and period_stats:
                logger.info(
                    f"   📊 Atualizando estatísticas para {len(period_stats)} mapas"
                )
                self._update_map_stats(cursor, match_id, period_stats)

            conn.commit()
            logger.info(f"   💾 Commit realizado para match_id {match_id}")
            return True

        except Exception as e:
            conn.rollback()
            logger.error(f"Erro ao atualizar match {match_id}: {e}")
            return False
        finally:
            conn.close()

    def _update_map_stats(self, cursor, match_id, period_stats):
        """Atualiza estatísticas de mapas para um match finalizado"""
        # Primeiro, limpar estatísticas existentes para este match
        cursor.execute(
            """
            DELETE FROM map_statistics 
            WHERE map_id IN (
                SELECT map_id FROM game_maps WHERE match_id = ?
            )
            """,
            (match_id,),
        )

        cursor.execute(
            "DELETE FROM game_maps WHERE match_id = ?",
            (match_id,),
        )

        # Inserir novos dados de mapas e estatísticas
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
            f"      ✅ {map_count} mapas e {stat_count} estatísticas atualizadas"
        )

    def verify_update(self, bet365_id, expected_result):
        """Verifica se a atualização foi realmente persistida no banco de dados"""
        try:
            conn = sqlite3.connect(self.esports_db_path)
            cursor = conn.cursor()

            cursor.execute(
                "SELECT time_status, final_score FROM matches WHERE bet365_id = ?",
                (bet365_id,),
            )

            db_row = cursor.fetchone()
            if not db_row:
                return False

            db_time_status, db_final_score = db_row

            # Verificar se os valores correspondem ao esperado
            expected_time_status = expected_result.get("time_status")
            expected_ss = expected_result.get("ss")

            status_match = str(db_time_status) == str(expected_time_status)
            score_match = db_final_score == expected_ss

            return status_match and score_match

        except Exception as e:
            logger.error(f"Erro ao verificar atualização para {bet365_id}: {e}")
            return False
        finally:
            if conn:
                conn.close()

    async def close(self):
        """Fecha conexões"""
        await self.client.close()


async def main():
    logger.info("🎯 INICIANDO ATUALIZADOR DE RESULTADOS PARA BANCO DE ESPORTS")
    logger.info("=" * 60)

    try:
        updater = EsportsResultsUpdater()
    except FileNotFoundError:
        logger.error("❌ Não foi possível inicializar o atualizador. Abortando.")
        return

    try:
        await updater.update_esports_results()
        logger.info("✅ Atualização de resultados concluída!")

    except Exception as e:
        logger.error(f"❌ ERRO CRÍTICO durante a atualização: {e}")
        import traceback

        logger.error(traceback.format_exc())
    finally:
        await updater.close()
        logger.info("👋 Conexões encerradas. Script finalizado.")


if __name__ == "__main__":
    # Executar o script
    asyncio.run(main())
