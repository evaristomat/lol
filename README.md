lol_api/
├── data/
│   ├── lol_esports.db          # Banco de dados com estatísticas de jogos
│   ├── lol_odds.db             # Banco de dados com odds das casas de apostas
│   └── bets.db                 # Banco de dados com suas apostas e resultados
├── scripts/
│   ├── db_odds_updater.py      # Atualiza as odds no banco de dados
│   ├── db_results_daily_updater.py # Atualiza resultados diários
│   ├── bet_results_updater.py  # Atualiza resultados das apostas
│   ├── get_roi_bets.py         # Analisa ROI e identifica apostas com valor
│   └── db_get_bets.py          # Escaneia e salva apostas com bom ROI
└── requirements.txt            # Dependências do projeto

Próximos Passos
Com a estrutura completa, você pode:

Executar o fluxo diário para coleta e análise de dados

Monitorar performance através dos relatórios gerados

Ajustar estratégias com base nos resultados

Expandir para outros mercados além de "Totals"

Adicionar mais casas de apostas para comparar odds
