import pandas as pd
import json
import pprint

# Carregue seu DataFrame
df = pd.read_csv("data_transformed.csv")  # substitua pelo caminho correto do CSV

# Dicionário com liga como chave e set de times como valor
ligas_times = {}

for _, row in df.iterrows():
    liga = row["league"]
    time1 = row["t1"]
    time2 = row["t2"]

    if liga not in ligas_times:
        ligas_times[liga] = set()

    ligas_times[liga].update([time1, time2])

# Converte os sets para listas (para salvar em JSON)
ligas_times = {liga: sorted(list(times)) for liga, times in ligas_times.items()}

# Exibir no terminal
pprint.pprint(ligas_times)

# Salvar em JSON
with open("ligas_times.json", "w", encoding="utf-8") as f:
    json.dump(ligas_times, f, ensure_ascii=False, indent=2)
