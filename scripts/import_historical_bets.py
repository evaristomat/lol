import pandas as pd
import sqlite3
from datetime import datetime
import re
import hashlib


def parse_custom_date(date_str):
    """
    Converte datas no formato '22 Mai 2025 13:00' para datetime do banco
    """
    month_mapping = {
        "Jan": "01",
        "Fev": "02",
        "Mar": "03",
        "Abr": "04",
        "Mai": "05",
        "Jun": "06",
        "Jul": "07",
        "Ago": "08",
        "Set": "09",
        "Out": "10",
        "Nov": "11",
        "Dez": "12",
    }

    try:
        parts = date_str.split()
        day = parts[0].zfill(2)
        month_pt = parts[1]
        year = parts[2]
        time = parts[3] if len(parts) > 3 else "00:00"

        month_num = month_mapping.get(month_pt, "01")
        return f"{year}-{month_num}-{day} {time}:00"
    except Exception as e:
        print(f"Erro ao converter data: {date_str} - {e}")
        return None


def convert_bet_result(result_str):
    """
    Converte resultado para formato do banco: won/lost/pending
    """
    result_mapping = {
        "win": "won",
        "loss": "lost",
        "pending": "pending",
        "push": "push",
        "cancelled": "cancelled",
    }
    return result_mapping.get(result_str.lower(), "pending")


def parse_bet_type_and_line(bet_type, bet_line):
    """
    Combina bet_type e bet_line no formato exato do banco
    Retorna: market_name, selection_line, handicap
    """
    # Para apostas over/under, o market_name deve ser "Totals"
    if bet_type in ["over", "under"]:
        market_name = "Totals"

        # Extrai o nome específico para o selection_line
        if bet_line and isinstance(bet_line, str):
            # Remove números e pontos extras
            selection_line = re.sub(r"[\d\.]", "", bet_line).strip()
            selection_line = selection_line.replace("_", " ").title()

            # Extrai handicap
            numbers = re.findall(r"[-+]?\d*\.\d+|\d+", bet_line)
            handicap = float(numbers[0]) if numbers else 0.0

            # Monta selection_line no formato correto
            selection_line = f"{bet_type.capitalize()} {selection_line}"
        else:
            selection_line = bet_type.capitalize()
            handicap = 0.0

        return market_name, selection_line, handicap

    else:
        # Para outros tipos de apostas
        if bet_line and isinstance(bet_line, str):
            # Extrai o nome do mercado
            market_name = bet_line
            if " " in bet_line:
                market_name = bet_line.split()[0]

            # Remove underlines e title case
            market_name = market_name.replace("_", " ").title()

            # Extrai handicap
            numbers = re.findall(r"[-+]?\d*\.\d+|\d+", bet_line)
            handicap = float(numbers[0]) if numbers else 0.0

            # Monta selection_line
            selection_line = bet_type.replace("_", " ").title()
        else:
            market_name = ""
            selection_line = bet_type.replace("_", " ").title()
            handicap = 0.0

        return market_name, selection_line, handicap


def generate_event_id(league, home_team, away_team, match_time):
    """
    Gera event_id único no formato do banco
    """
    # Cria hash simples baseado nos dados
    base_string = f"{league}_{home_team}_{away_team}_{match_time}"
    return hashlib.md5(base_string.encode()).hexdigest()[:20]


def import_historical_bets(csv_file_path, db_file_path="../data/bets.db"):
    """
    Importa apenas as colunas relevantes do CSV para o banco
    """
    # Ler CSV mantendo apenas colunas necessárias
    df = pd.read_csv(csv_file_path)

    # Filtrar apenas mapas 1 e 2
    df = df[df["game"].isin([1, 2])]

    conn = sqlite3.connect(db_file_path)
    cursor = conn.cursor()

    processed = 0
    errors = 0

    for index, row in df.iterrows():
        try:
            # Converter dados básicos
            match_time = parse_custom_date(row["date"])
            bet_result = convert_bet_result(row["status"])

            # Gerar event_id
            event_id = generate_event_id(
                row["league"], row["t1"], row["t2"], match_time
            )

            # Verificar/criar evento
            cursor.execute(
                "SELECT event_id FROM events WHERE event_id = ?", (event_id,)
            )
            if not cursor.fetchone():
                cursor.execute(
                    """
                    INSERT INTO events (event_id, league_name, match_date, home_team, away_team, status)
                    VALUES (?, ?, ?, ?, ?, ?)
                """,
                    (
                        event_id,
                        row["league"],
                        match_time,
                        row["t1"],
                        row["t2"],
                        "finished",
                    ),
                )

            # Parse bet_type e bet_line para formato do banco
            market_name, selection_line, handicap = parse_bet_type_and_line(
                row["bet_type"], row["bet_line"]
            )

            # Ajustar market_name com número do mapa
            market_name = f"Map {int(row['game'])} - {market_name}"

            # Converter ROI (remove % e converte para decimal)
            roi_value = (
                float(str(row["ROI"]).replace("%", "").strip())
                if pd.notna(row["ROI"])
                else 0.0
            )

            # Usar bet_result como actual_value (convertendo para float)
            try:
                actual_value = float(row["bet_result"])
            except (ValueError, TypeError):
                actual_value = 0.0

            # Preparar dados para inserção
            bet_data = (
                event_id,
                market_name,
                selection_line,
                handicap,  # handicap
                float(row["odds"]),  # house_odds
                roi_value,  # roi_average
                float(row["fair_odds"]),  # fair_odds
                actual_value,  # actual_value (usando bet_result)
                bet_result,  # bet_status
                1.0,  # stake (assumindo 1.0)
                float(row["odds"]) - 1.0,  # potential_win
                float(row["profit"]) if pd.notna(row["profit"]) else 0.0,  # actual_win
                bet_result != "pending",  # result_verified
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            )

            # Inserir na tabela bets
            cursor.execute(
                """
                INSERT INTO bets 
                (event_id, market_name, selection_line, handicap, house_odds, roi_average, 
                 fair_odds, actual_value, bet_status, stake, potential_win, actual_win, 
                 result_verified, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                bet_data,
            )

            processed += 1

            if processed % 10 == 0:
                print(f"Processadas {processed} apostas...")
                print(f"Exemplo: {market_name} - {selection_line}")
                print(f"Actual Value: {actual_value}")

        except Exception as e:
            errors += 1
            print(f"Erro na linha {index}: {e}")
            continue

    conn.commit()
    conn.close()

    print(f"\n✅ Importação concluída!")
    print(f"📊 Apostas importadas: {processed}")
    print(f"❌ Erros: {errors}")
    print(f"🎯 Apenas mapas 1 e 2 foram processados")


def verify_import(db_file_path="../data/bets.db"):
    """
    Verifica os dados importados
    """
    conn = sqlite3.connect(db_file_path)

    # Últimas 5 apostas importadas
    recent_bets = pd.read_sql(
        """
        SELECT market_name, selection_line, handicap, house_odds, roi_average, 
               fair_odds, actual_value, bet_status, actual_win
        FROM bets 
        ORDER BY id DESC 
        LIMIT 10
    """,
        conn,
    )

    conn.close()

    print(f"\n🔍 VERIFICAÇÃO:")
    print("\nÚltimas 10 apostas importadas:")
    print(recent_bets.to_string(index=False))


if __name__ == "__main__":
    csv_file = "../data/historical_bets.csv"

    print("🚀 Importando apostas históricas...")
    print("📋 Apenas colunas relevantes serão processadas")
    print("🎮 Apenas mapas 1 e 2 serão importados")

    import_historical_bets(csv_file)
    verify_import()
