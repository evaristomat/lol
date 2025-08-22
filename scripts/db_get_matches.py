#!/usr/bin/env python3
"""
Atualização diária automática - verifica apenas últimos 2 dias
"""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from scripts.update_30_days import DatabaseUpdater


async def main():
    print("📅 ATUALIZADOR DIÁRIO")
    print("=" * 40)
    print("🔄 Verificando últimos 2 dias...")

    updater = DatabaseUpdater()

    try:
        # Apenas últimos 2 dias para atualização diária
        await updater.update_last_days(days_back=2)
        print("✅ Atualização diária concluída!")

    except Exception as e:
        print(f"❌ Erro na atualização diária: {e}")
        import traceback

        traceback.print_exc()
    finally:
        await updater.client.close()


if __name__ == "__main__":
    asyncio.run(main())
