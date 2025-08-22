import asyncio
import os
import sys
import json
import csv
from dotenv import load_dotenv
from datetime import datetime, timedelta
import aiohttp
import aiofiles

# Adicionar o diretório src ao path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

load_dotenv()


from pathlib import Path


class Bet365DataCollector:
    def __init__(self):
        from src.core.bet365_client import Bet365Client

        self.client = Bet365Client()

        # Root do projeto: pasta lol_api
        project_root = Path(__file__).resolve().parent
        self.data_dir = project_root / "data" / "bet365"
        self.events_file = self.data_dir / "lol_events.csv"
        self.odds_dir = self.data_dir / "odds"

        # Criar diretórios
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.odds_dir.mkdir(parents=True, exist_ok=True)

        print(f"📁 Diretório de dados: {self.data_dir}")
        print(f"📁 Arquivo de eventos: {self.events_file}")
        print(f"📁 Pasta de odds: {self.odds_dir}")

    def print_separator(self, title):
        print(f"\n{'=' * 60}")
        print(f"  {title}")
        print("=" * 60)

    def format_timestamp(self, timestamp):
        """Converte timestamp para formato legíscript_dirvel"""
        try:
            return datetime.fromtimestamp(int(timestamp)).strftime("%Y-%m-%d %H:%M:%S")
        except:
            return timestamp

    async def get_lol_events_week_range(self):
        """Busca eventos de LoL para os próximos 7 dias"""
        events = []

        # Buscar eventos para cada dia da próxima semana
        for i in range(7):
            target_date = datetime.now() + timedelta(days=i)
            day_str = target_date.strftime("%Y%m%d")

            print(f"Buscando eventos para {target_date.strftime('%Y-%m-%d')}...")

            try:
                # Usar o método upcoming com parâmetro day
                daily_events = await self.client.upcoming(sport_id=151, day=day_str)

                if daily_events.get("results"):
                    # Filtrar apenas eventos de League of Legends
                    lol_events = [
                        event
                        for event in daily_events["results"]
                        if "LOL" in event.get("league", {}).get("name", "")
                        or "League of Legends"
                        in event.get("league", {}).get("name", "")
                        or "LoL" in event.get("league", {}).get("name", "")
                    ]

                    events.extend(lol_events)
                    print(f"  → Encontrados {len(lol_events)} eventos de LoL")
                else:
                    print(f"  → Nenhum evento encontrado")

            except Exception as e:
                print(f"  → Erro ao buscar eventos: {str(e)}")

        return events

    async def save_events_to_csv(self, events):
        """Salva eventos em arquivo CSV"""
        if not events:
            print("Nenhum evento para salvar")
            return False

        fieldnames = [
            "event_id",
            "league",
            "home_team",
            "away_team",
            "event_time",
            "timestamp",
            "sport_id",
            "retrieved_at",
        ]

        try:
            # CORREÇÃO: Usar modo síncrono para CSV ou aiofiles corretamente
            with open(self.events_file, mode="w", newline="", encoding="utf-8") as file:
                writer = csv.DictWriter(file, fieldnames=fieldnames)
                writer.writeheader()

                for event in events:
                    row = {
                        "event_id": event.get("id", ""),
                        "league": event.get("league", {}).get("name", ""),
                        "home_team": event.get("home", {}).get("name", ""),
                        "away_team": event.get("away", {}).get("name", ""),
                        "event_time": self.format_timestamp(event.get("time", "")),
                        "timestamp": event.get("time", ""),
                        "sport_id": 151,
                        "retrieved_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    }
                    writer.writerow(row)

            print(f"✅ Eventos salvos em: {self.events_file}")

            # Verificar se arquivo foi criado
            if os.path.exists(self.events_file):
                file_size = os.path.getsize(self.events_file)
                print(f"📏 Tamanho do arquivo: {file_size} bytes")
            else:
                print("❌ Arquivo não foi criado!")

            return True

        except Exception as e:
            print(f"❌ Erro ao salvar CSV: {str(e)}")
            return False

    async def get_and_save_odds_for_events(self, events, max_events=None):
        """Busca e salva odds para cada evento"""
        if not events:
            print("Nenhum evento para buscar odds")
            return

        if max_events:
            events = events[:max_events]

        successful = 0
        failed = 0

        for i, event in enumerate(events, 1):
            event_id = event.get("id")
            home_team = event.get("home", {}).get("name", "Unknown")
            away_team = event.get("away", {}).get("name", "Unknown")

            print(
                f"\n📊 [{i}/{len(events)}] Buscando odds para: {home_team} vs {away_team} (ID: {event_id})"
            )

            try:
                odds_data = await self.client.prematch(FI=event_id)

                if odds_data.get("results"):
                    # Criar nome do arquivo seguro
                    safe_filename = (
                        f"odds_{event_id}_{home_team}_vs_{away_team}.json".replace(
                            " ", "_"
                        ).replace("/", "-")[:100]
                        + ".json"
                    )
                    file_path = os.path.join(self.odds_dir, safe_filename)

                    # Salvar dados completos das odds
                    with open(file_path, "w", encoding="utf-8") as f:
                        json.dump(odds_data, f, indent=2, ensure_ascii=False)

                    print(f"   ✅ Odds salvas em: {safe_filename}")

                    # Verificar se arquivo foi criado
                    if os.path.exists(file_path):
                        file_size = os.path.getsize(file_path)
                        print(f"   📏 Tamanho: {file_size} bytes")
                    else:
                        print("   ❌ Arquivo de odds não foi criado!")

                    successful += 1

                    # Pequena pausa para não sobrecarregar a API
                    await asyncio.sleep(0.5)

                else:
                    print(f"   ⚠️  Nenhuma odd encontrada para este evento")
                    failed += 1

            except Exception as e:
                print(f"   ❌ Erro ao buscar odds: {str(e)}")
                failed += 1

        return successful, failed

    def list_data_files(self):
        """Lista os arquivos de dados criados"""
        print(f"\n📋 LISTANDO ARQUIVOS CRIADOS:")

        # Verificar arquivo CSV
        if os.path.exists(self.events_file):
            print(f"✅ CSV: {self.events_file}")
            with open(self.events_file, "r", encoding="utf-8") as f:
                lines = f.readlines()
                print(f"   Linhas: {len(lines)}")
        else:
            print(f"❌ CSV não encontrado: {self.events_file}")

        # Verificar arquivos JSON de odds
        if os.path.exists(self.odds_dir):
            json_files = [f for f in os.listdir(self.odds_dir) if f.endswith(".json")]
            print(f"✅ JSON Files: {len(json_files)} arquivos em {self.odds_dir}")
            for i, f in enumerate(json_files[:3]):  # Mostrar apenas os 3 primeiros
                print(f"   {i + 1}. {f}")
            if len(json_files) > 3:
                print(f"   ... e mais {len(json_files) - 3} arquivos")
        else:
            print(f"❌ Pasta de odds não encontrada: {self.odds_dir}")

    async def collect_complete_data(self):
        """Coleta completa de dados: eventos + odds"""
        try:
            self.print_separator("COLETOR DE DADOS BET365 - LEAGUE OF LEGENDS")
            print(f"Início: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

            # 1. Buscar eventos da próxima semana
            self.print_separator("1. BUSCANDO EVENTOS DA PRÓXIMA SEMANA")
            events = await self.get_lol_events_week_range()

            if not events:
                print("❌ Nenhum evento de LoL encontrado para a próxima semana")
                return

            print(f"\n🎯 Total de eventos de LoL encontrados: {len(events)}")

            # 2. Salvar eventos em CSV
            self.print_separator("2. SALVANDO EVENTOS EM CSV")
            await self.save_events_to_csv(events)

            # 3. Buscar e salvar odds para cada evento
            self.print_separator("3. BUSCANDO ODDS PARA OS EVENTOS")
            successful, failed = await self.get_and_save_odds_for_events(events)

            # 4. Resumo
            self.print_separator("4. RESUMO DA COLETA")
            print(f"✅ Eventos processados: {len(events)}")
            print(f"✅ Odds coletadas com sucesso: {successful}")
            print(f"❌ Falhas na coleta de odds: {failed}")

            # 5. Listar arquivos criados
            self.print_separator("5. VERIFICAÇÃO DE ARQUIVOS")
            self.list_data_files()

            # Mostrar alguns eventos como exemplo
            print(f"\n📅 Próximos eventos coletados:")
            for i, event in enumerate(events[:3], 1):  # Mostrar apenas 3
                event_time = self.format_timestamp(event.get("time", ""))
                print(
                    f"   {i}. {event['home']['name']} vs {event['away']['name']} - {event_time}"
                )

            if len(events) > 3:
                print(f"   ... e mais {len(events) - 3} eventos")

        except Exception as e:
            print(f"❌ Erro durante a coleta: {str(e)}")
            import traceback

            traceback.print_exc()
        finally:
            await self.client.close()
            print("\n🔒 Conexão com a API fechada")


async def main():
    """Função principal"""
    collector = Bet365DataCollector()
    await collector.collect_complete_data()


if __name__ == "__main__":
    asyncio.run(main())
