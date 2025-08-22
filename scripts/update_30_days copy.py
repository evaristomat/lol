#!/usr/bin/env python3
"""
Script para atualizar os últimos 30 dias, verificando duplicatas
"""

import asyncio
import sys
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.core.database import LoLDatabase
from src.core.bet365_client import Bet365Client


class DatabaseUpdater:
    def __init__(self):
        self.db = LoLDatabase()
        self.client = Bet365Client()
        self.lol_sport_id = 151

    async def update_last_days(self, days_back=10):
        """Atualiza os últimos X dias, verificando duplicatas"""
        print(f"🔄 Atualizando últimos {days_back} dias...")
        print("=" * 50)

        total_processed = 0
        total_new = 0
        total_updated = 0
        total_skipped = 0

        for i in range(days_back):
            target_date = datetime.now() - timedelta(days=i)
            day_str = target_date.strftime("%Y%m%d")

            print(f"\n📅 Processando {target_date.strftime('%Y-%m-%d')}...")

            # Buscar eventos do dia
            events = await self.get_events_for_day(day_str)
            if not events:
                continue

            # Filtrar apenas LoL
            lol_events = [event for event in events if self.is_lol_event(event)]
            print(f"   🎯 {len(lol_events)} jogos de LoL encontrados")

            day_processed = 0
            day_new = 0
            day_updated = 0

            for event in lol_events:
                result = await self.process_event(event)
                if result == "new":
                    day_new += 1
                    total_new += 1
                elif result == "updated":
                    day_updated += 1
                    total_updated += 1
                elif result == "skipped":
                    total_skipped += 1

                day_processed += 1
                total_processed += 1

            print(
                f"   📊 Dia: {day_new} novos, {day_updated} atualizados, {day_processed} processados"
            )

            # Pequena pausa para não sobrecarregar a API
            await asyncio.sleep(1)

        # Log do update
        self.db.log_update(
            f"update_{days_back}_days",
            total_processed,
            total_new,
            True,
            f"New: {total_new}, Updated: {total_updated}, Skipped: {total_skipped}",
        )

        print(f"\n✅ ATUALIZAÇÃO CONCLUÍDA!")
        print(f"📊 Total processado: {total_processed} jogos")
        print(f"🆕 Novos: {total_new}")
        print(f"🔄 Atualizados: {total_updated}")
        print(f"⏭️  Pulados: {total_skipped}")

    async def get_events_for_day(self, day_str):
        """Busca eventos de um dia específico"""
        try:
            events_data = await self.client.upcoming(
                sport_id=self.lol_sport_id, day=day_str
            )

            if events_data and events_data.get("success") == 1:
                return events_data.get("results", [])
            return []

        except Exception as e:
            print(f"   ❌ Erro ao buscar eventos: {e}")
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

        # Eventos que não estão finalizados podem precisar de atualização
        cursor.execute(
            "SELECT 1 FROM matches WHERE bet365_id = ? AND time_status != 3",
            (event_id,),
        )
        needs_update = cursor.fetchone() is not None

        conn.close()
        return needs_update

    async def process_event(self, event):
        """Processa um evento e retorna o status"""
        event_id = event.get("id")

        # Verificar se já existe
        if self.event_exists(event_id):
            if self.needs_update(event_id):
                # Evento existe mas precisa ser atualizado
                return await self.update_existing_event(event)
            else:
                # Evento já existe e está finalizado
                print(f"   ⏭️  Pulado (já finalizado): {event_id}")
                return "skipped"
        else:
            # Novo evento
            return await self.process_new_event(event)

    async def process_new_event(self, event):
        """Processa um novo evento"""
        event_id = event.get("id")

        result_data = await self.client.result(event_id)
        if result_data and result_data.get("success") == 1:
            result = result_data.get("results", [{}])[0]
            self.save_event(event, result)
            print(f"   ✅ NOVO: {event_id}")
            return "new"
        else:
            print(f"   ❌ Erro ao buscar novo evento: {event_id}")
            return "error"

    async def update_existing_event(self, event):
        """Atualiza um evento existente"""
        event_id = event.get("id")

        result_data = await self.client.result(event_id)
        if result_data and result_data.get("success") == 1:
            result = result_data.get("results", [{}])[0]
            self.update_event(event, result)
            print(f"   🔄 ATUALIZADO: {event_id}")
            return "updated"
        else:
            print(f"   ❌ Erro ao atualizar evento: {event_id}")
            return "error"

    def save_event(self, event, result):
        """Salva um novo evento no banco"""
        # [Usar a mesma implementação do initialize_database]
        conn = self.db.get_connection()
        cursor = conn.cursor()

        try:
            # 1. Salvar liga
            league_name = event.get("league", {}).get("name")
            cursor.execute(
                "INSERT OR IGNORE INTO leagues (league_id, name) VALUES (?, ?)",
                (event.get("league", {}).get("id"), league_name),
            )

            # 2. Salvar times
            home_team = event.get("home", {})
            away_team = event.get("away", {})

            cursor.execute(
                "INSERT OR IGNORE INTO teams (team_id, name, image_id, country_code) VALUES (?, ?, ?, ?)",
                (
                    home_team.get("id"),
                    home_team.get("name"),
                    home_team.get("image_id"),
                    home_team.get("cc"),
                ),
            )
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
                    datetime.fromtimestamp(int(event.get("time"))),
                    event.get("time_status"),
                    result.get("ss"),
                    datetime.now(),
                ),
            )

            # 4. Salvar mapas e estatísticas
            self._save_map_stats(cursor, event.get("id"), result)

            conn.commit()

        except Exception as e:
            conn.rollback()
            print(f"❌ Erro ao salvar evento: {e}")
        finally:
            conn.close()

    def update_event(self, event, result):
        """Atualiza um evento existente"""
        conn = self.db.get_connection()
        cursor = conn.cursor()

        try:
            # Atualizar informações principais
            cursor.execute(
                """
                UPDATE matches 
                SET time_status = ?, final_score = ?, retrieved_at = ?
                WHERE bet365_id = ?
                """,
                (
                    event.get("time_status"),
                    result.get("ss"),
                    datetime.now(),
                    event.get("id"),
                ),
            )

            # Se o jogo foi finalizado, salvar estatísticas completas
            if result.get("time_status") == "3":
                self._save_map_stats(cursor, event.get("id"), result)

            conn.commit()

        except Exception as e:
            conn.rollback()
            print(f"❌ Erro ao atualizar evento: {e}")
        finally:
            conn.close()

    def _save_map_stats(self, cursor, event_id, result):
        """Salva estatísticas de mapas (método auxiliar)"""
        period_stats = result.get("period_stats", {})
        if not period_stats:
            return

        # Obter match_id
        cursor.execute("SELECT match_id FROM matches WHERE bet365_id = ?", (event_id,))
        match_id = cursor.fetchone()[0]

        for map_number, stats in period_stats.items():
            # Salvar mapa
            cursor.execute(
                "INSERT OR REPLACE INTO game_maps (match_id, map_number) VALUES (?, ?)",
                (match_id, int(map_number)),
            )
            cursor.execute("SELECT last_insert_rowid()")
            map_id = cursor.fetchone()[0]

            # Salvar estatísticas
            for stat_name, values in stats.items():
                if isinstance(values, list) and len(values) == 2:
                    cursor.execute(
                        "INSERT OR REPLACE INTO map_statistics (map_id, stat_name, home_value, away_value) VALUES (?, ?, ?, ?)",
                        (map_id, stat_name, values[0], values[1]),
                    )


async def main():
    print("🔄 ATUALIZADOR DOS ÚLTIMOS 30 DIAS")
    print("=" * 50)

    updater = DatabaseUpdater()

    try:
        await updater.update_last_days(days_back=30)
        print("✅ Atualização concluída!")

    except Exception as e:
        print(f"❌ Erro: {e}")
        import traceback

        traceback.print_exc()
    finally:
        await updater.client.close()


if __name__ == "__main__":
    asyncio.run(main())
