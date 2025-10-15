import sqlite3

conn = sqlite3.connect("../data/lol_odds.db")
cursor = conn.cursor()

# Ver todos os mercados disponíveis
cursor.execute(
    """
    SELECT DISTINCT market_name, COUNT(*) as count
    FROM current_odds
    GROUP BY market_name
    ORDER BY count DESC
"""
)

for row in cursor.fetchall():
    print(f"{row[0]}: {row[1]} odds")

conn.close()
