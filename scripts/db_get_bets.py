import os
import re
import sqlite3
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from get_roi_bets import ROIAnalyzer
from scipy import stats

# Carrega variáveis de ambiente do arquivo .env
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.services.telegram_notifier import TelegramNotifier


class BetScanner:
    def __init__(self, odds_db_path: str, bets_db_path: str = "../data/bets.db"):
        self.odds_db_path = odds_db_path
        self.bets_db_path = bets_db_path
        self.analyzer = ROIAnalyzer(odds_db_path)
        self.telegram_notifier = TelegramNotifier()
        self.setup_database()

        # Cache do histórico de players (CSV)
        self.player_history_df: Optional[pd.DataFrame] = None

    def setup_database(self):
        """Cria as tabelas necessárias com estrutura expandida"""
        conn = sqlite3.connect(self.bets_db_path)
        cursor = conn.cursor()

        # Tabela de eventos com status e resultado
        cursor.execute(
            """
        CREATE TABLE IF NOT EXISTS events (
            event_id TEXT PRIMARY KEY,
            league_name TEXT,
            match_date TEXT,
            home_team TEXT,
            away_team TEXT,
            status TEXT DEFAULT 'scheduled',  -- scheduled, live, finished, canceled, postponed
            home_score INTEGER DEFAULT 0,
            away_score INTEGER DEFAULT 0,
            winner TEXT,  -- home, away, or draw
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        """
        )

        # Tabela de apostas - ADICIONANDO actual_value
        cursor.execute(
            """
        CREATE TABLE IF NOT EXISTS bets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id TEXT NOT NULL,
            market_name TEXT NOT NULL,
            selection_line TEXT NOT NULL,
            handicap REAL NOT NULL,
            house_odds REAL NOT NULL,
            roi_average REAL NOT NULL,
            fair_odds REAL NOT NULL,
            actual_value REAL,  -- Nova coluna para valor real
            bet_status TEXT DEFAULT 'pending',
            stake REAL DEFAULT 0,
            potential_win REAL DEFAULT 0,
            actual_win REAL DEFAULT 0,
            result_verified BOOLEAN DEFAULT FALSE,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (event_id) REFERENCES events (event_id) ON DELETE CASCADE,
            UNIQUE(event_id, market_name, selection_line, handicap)
        )
        """
        )

        # Tabela de resultados para histórico de verificações
        cursor.execute(
            """
        CREATE TABLE IF NOT EXISTS results_verification (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id TEXT NOT NULL,
            bet_id INTEGER NOT NULL,
            expected_result TEXT,
            actual_result TEXT,
            status TEXT,  -- correct, incorrect, void
            profit_loss REAL,
            verified_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (event_id) REFERENCES events (event_id) ON DELETE CASCADE,
            FOREIGN KEY (bet_id) REFERENCES bets (id) ON DELETE CASCADE
        )
        """
        )

        # Índices para melhor performance
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_events_status ON events(status)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_bets_status ON bets(bet_status)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_bets_event ON bets(event_id)")
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_events_date ON events(match_date)"
        )

        conn.commit()
        conn.close()

    def _load_player_history(self) -> bool:
        """
        Carrega o CSV de histórico de players (kills, deaths, assists).
        Esperado: ../data/database.csv  (ajuste se precisar)
        """
        try:
            csv_path = (
                Path(__file__).resolve().parent.parent
                / "data"
                / "database"
                / "database.csv"
            )
            if not csv_path.exists():
                print(f"⚠️ CSV de players não encontrado em: {csv_path}")
                return False

            df = pd.read_csv(csv_path, low_memory=False)
            needed_cols = [
                "playername",
                "teamname",
                "date",
                "kills",
                "deaths",
                "assists",
            ]
            if not all(c in df.columns for c in needed_cols):
                print(f"⚠️ CSV de players não contém colunas necessárias: {needed_cols}")
                return False

            df["date"] = pd.to_datetime(df["date"], errors="coerce")
            for c in ["kills", "deaths", "assists"]:
                df[c] = pd.to_numeric(df[c], errors="coerce")

            self.player_history_df = df
            return True
        except Exception as e:
            print(f"❌ Erro ao carregar CSV de players: {e}")
            return False

    @staticmethod
    def _extract_side(selection_name: str) -> Optional[str]:
        s = (selection_name or "").strip()
        if not s:
            return None
        # pega a 1ª palavra e normaliza
        head = s.split(maxsplit=1)[0].casefold()
        if head == "over":
            return "over"
        if head == "under":
            return "under"
        return None

    @staticmethod
    def _market_stat_label(market_name: str) -> str:
        """Retorna rótulo curto do stat a partir do market_name."""
        s = (market_name or "").lower()
        if "player total kills" in s:
            return "Kills"
        if "player total deaths" in s:
            return "Deaths"
        if "player total assists" in s:
            return "Assists"
        if "totals" in s:
            return "Totals"
        return "Market"

    @staticmethod
    def _extract_player(selection_name: str, candidates: List[str]) -> Optional[str]:
        """
        Encontra o nome do jogador dentro do selection_name (ex.: 'Over SkewMond').
        Tenta word-boundary e depois substring, para robustez.
        """
        low = str(selection_name).lower()
        for c in sorted(candidates, key=len, reverse=True):
            if f" {c.lower()} " in f" {low} ":
                return c
        for c in sorted(candidates, key=len, reverse=True):
            if c.lower() in low:
                return c
        return None

    def _get_player_values(
        self, player: str, team: Optional[str], stat: str, n: int = 50
    ) -> np.ndarray:
        """
        Últimos n valores do player para um stat. Se team não bater, escolhe o time com mais registros do player.
        """
        if self.player_history_df is None or self.player_history_df.empty:
            return np.array([])

        sub = self.player_history_df[self.player_history_df["playername"] == player]
        if team is not None:
            sub_team = sub[sub["teamname"] == team]
        else:
            sub_team = pd.DataFrame(columns=sub.columns)
        if sub_team.empty:
            if not sub.empty:
                counts = sub.groupby("teamname").size().sort_values(ascending=False)
                if len(counts) > 0:
                    fallback = counts.index[0]
                    sub_team = sub[sub["teamname"] == fallback]
                else:
                    sub_team = sub
        sub_team = sub_team.dropna(subset=[stat]).sort_values("date", ascending=False)
        return sub_team[stat].astype(float).values[:n]

    @staticmethod
    def _calc_window_stats(
        values: np.ndarray, handicap: float, side: str
    ) -> Optional[dict]:
        if values is None or len(values) == 0:
            return None
        mean = float(np.mean(values))
        median = float(np.median(values))
        std = float(np.std(values, ddof=0))
        cv = float((std / mean * 100.0) if mean > 0 else 0.0)
        hit_rate = (
            float(np.mean(values > handicap))
            if side == "over"
            else float(np.mean(values < handicap))
        )
        # trend
        trend = 0.0
        if len(values) >= 20:
            recent = float(np.mean(values[:10]))
            older = float(np.mean(values[10:20]))
            trend = float(((recent - older) / older * 100.0) if older > 0 else 0.0)
        return dict(
            mean=mean, median=median, std=std, cv=cv, hit_rate=hit_rate, trend=trend
        )

    @staticmethod
    def _posterior(p_prior: float, p_like: float, w_prior: float = 0.5) -> float:
        p_prior = float(np.clip(p_prior, 1e-6, 1 - 1e-6))
        p_like = float(np.clip(p_like, 1e-6, 1 - 1e-6))
        return w_prior * p_prior + (1.0 - w_prior) * p_like

    @staticmethod
    def _fair_from_p(p: float) -> float:
        p = float(np.clip(p, 1e-6, 1 - 1e-6))
        return 1.0 / p

    @staticmethod
    def _ev_percent(p_real: float, odds: float) -> float:
        p_real = float(np.clip(p_real, 1e-6, 1 - 1e-6))
        return (p_real * float(odds) - 1.0) * 100.0

    @staticmethod
    def _implied_prob(odds: float) -> float:
        p = 1.0 / max(float(odds), 1e-12)
        return float(np.clip(p, 1e-6, 1 - 1e-6))

    def analyze_event_for_player_bets(
        self, event_id: str, min_roi: float = 10
    ) -> List[Dict]:
        """
        Analisa odds de players (odds_type='player') usando APENAS o método Original:
        - Prior bruto (com vig): 1/odds
        - Likelihood: hit-rate L10/L20 (60/40)
        - Posterior: média ponderada (0.5 prior, 0.5 likelihood)
        Retorna apostas no mesmo formato do restante do pipeline.
        """
        good_bets: List[Dict] = []

        # Garantir histórico carregado
        if self.player_history_df is None:
            ok = self._load_player_history()
            if not ok:
                return good_bets

        # Info do evento (times)
        event_info = self.analyzer.get_event_info(event_id)
        if not event_info:
            return good_bets

        home_team = event_info.get("home_team")
        away_team = event_info.get("away_team")

        # Buscar odds de players
        conn = sqlite3.connect(self.odds_db_path)
        q = """
            SELECT market_name, selection_name, handicap, odds_value
            FROM current_odds
            WHERE event_id = ?
              AND odds_type = 'player'
              AND market_name IN (
                'Map 1 - Player Total Kills',
                'Map 1 - Player Total Deaths',
                'Map 1 - Player Total Assists'
              )
        """
        odds_df = pd.read_sql_query(q, conn, params=[event_id])
        conn.close()

        if odds_df.empty:
            return good_bets

        # Mapear mercado -> coluna do CSV
        stat_map = {
            "Map 1 - Player Total Kills": "kills",
            "Map 1 - Player Total Deaths": "deaths",
            "Map 1 - Player Total Assists": "assists",
        }

        # Parsing
        odds_df["handicap"] = pd.to_numeric(odds_df["handicap"], errors="coerce")
        odds_df["odds_value"] = pd.to_numeric(odds_df["odds_value"], errors="coerce")

        odds_df["side"] = odds_df["selection_name"].apply(self._extract_side)
        odds_df["stat"] = odds_df["market_name"].map(stat_map)

        candidates = self.player_history_df["playername"].dropna().unique().tolist()
        odds_df["player"] = odds_df["selection_name"].apply(
            lambda s: self._extract_player(s, candidates)
        )

        # Filtrar válidas
        dfv = odds_df[
            odds_df["handicap"].notna()
            & odds_df["odds_value"].notna()
            & odds_df["side"].notna()
            & odds_df["stat"].notna()
            & odds_df["player"].notna()
        ].copy()

        if dfv.empty:
            return good_bets

        # Processar
        for _, row in dfv.iterrows():
            player = row["player"]
            stat = row["stat"]
            side = row["side"]
            line = float(row["handicap"])
            odds = float(row["odds_value"])
            market_name = row["market_name"]
            selection_line = row["selection_name"]  # manter exatamente como está

            # Escolher time heurístico (qual tem mais registros do player)
            # tenta com home/away e escolhe aquele que tiver mais dados
            v_home = self._get_player_values(player, home_team, stat, n=1)
            v_away = self._get_player_values(player, away_team, stat, n=1)
            team_guess = home_team if len(v_home) >= len(v_away) else away_team

            values = self._get_player_values(player, team_guess, stat, n=50)
            if len(values) < 20:
                continue

            l10 = values[:10]
            l20 = values[:20]
            st10 = self._calc_window_stats(l10, line, side)
            st20 = self._calc_window_stats(l20, line, side)
            if st10 is None or st20 is None:
                continue

            # ORIGINAL:
            p_prior = self._implied_prob(odds)  # prior bruto (com vig)
            p_like = 0.6 * st10["hit_rate"] + 0.4 * st20["hit_rate"]  # hit rate L10/L20
            p_real = self._posterior(p_prior, p_like, w_prior=0.5)
            fair = self._fair_from_p(p_real)
            roi = (odds / fair - 1.0) * 100.0
            ev_pct = self._ev_percent(p_real, odds)

            if roi >= min_roi and p_real > p_prior:
                good_bets.append(
                    {
                        "event_id": event_id,
                        "market_name": market_name,
                        "selection_line": selection_line,
                        "handicap": line,
                        "house_odds": odds,
                        "roi_average": roi,  # mantém nome
                        "fair_odds": fair,  # mantém nome
                        # opcionalmente podemos gravar 'actual_value' depois
                    }
                )

        # Ordena por ROI desc e pega top 10 por segurança (mantém padrão)
        good_bets.sort(key=lambda x: x["roi_average"], reverse=True)
        return good_bets[:10]

    def update_event_status(
        self,
        event_id: str,
        status: str,
        home_score: Optional[int] = None,
        away_score: Optional[int] = None,
        winner: Optional[str] = None,
    ):
        """Atualiza o status e resultado de um evento"""
        conn = sqlite3.connect(self.bets_db_path)
        cursor = conn.cursor()

        if home_score is not None and away_score is not None:
            cursor.execute(
                """
                UPDATE events 
                SET status = ?, home_score = ?, away_score = ?, winner = ?, updated_at = CURRENT_TIMESTAMP
                WHERE event_id = ?
            """,
                (status, home_score, away_score, winner, event_id),
            )
        else:
            cursor.execute(
                """
                UPDATE events 
                SET status = ?, updated_at = CURRENT_TIMESTAMP
                WHERE event_id = ?
            """,
                (status, event_id),
            )

        conn.commit()
        conn.close()

    def _notify_new_bet(self, bets: List[Dict], stake: float = 1.0):
        """Notifica sobre múltiplas apostas do mesmo evento via Telegram de forma agrupada"""
        try:
            if not bets:
                return

            # Usa o primeiro item para obter informações do evento
            first_bet = bets[0]
            event_info = self.analyzer.get_event_info(first_bet["event_id"])

            if event_info:
                home_team = event_info.get("home_team", "Unknown")
                away_team = event_info.get("away_team", "Unknown")
                league_name = event_info.get("league_name", "Unknown")
                match_date = event_info.get("match_date", "Unknown")
            else:
                home_team = "Unknown"
                away_team = "Unknown"
                league_name = "Unknown"
                match_date = "Unknown"

            # Formatar data
            if match_date != "Unknown":
                try:
                    dt = datetime.strptime(match_date, "%Y-%m-%d %H:%M:%S")
                    formatted_date = dt.strftime("%d/%m/%Y às %H:%M")
                except Exception:
                    formatted_date = match_date
            else:
                formatted_date = "Data não disponível"

            # Agrupa por tipo de mercado
            market_groups: Dict[str, Dict[str, List[Dict]]] = {}
            for bet in bets:
                market_name = bet["market_name"]

                # Identifica o tipo de mercado (removendo informação de mapa)
                if "Map 1" in market_name:
                    base_market = market_name.replace("Map 1 - ", "")
                    map_info = "Mapa 1"
                elif "Map 2" in market_name:
                    base_market = market_name.replace("Map 2 - ", "")
                    map_info = "Mapa 2"
                else:
                    base_market = market_name
                    map_info = "Geral"

                if base_market not in market_groups:
                    market_groups[base_market] = {}
                if map_info not in market_groups[base_market]:
                    market_groups[base_market][map_info] = []

                market_groups[base_market][map_info].append(bet)

            # Helper local: adiciona sufixo (Kills/Deaths/Assists) somente para player markets
            def _player_suffix(market_name: str) -> str:
                s = (market_name or "").lower()
                if "player total kills" in s:
                    return " (Kills)"
                if "player total deaths" in s:
                    return " (Deaths)"
                if "player total assists" in s:
                    return " (Assists)"
                return ""

            # Constrói mensagem agrupada
            message = f"🎯 *Nova Aposta Encontrada!*\n\n🏆 *Liga:* {league_name}\n"
            message += f"⚔️ *Partida:* {home_team} vs {away_team}\n"
            message += f"📅 *Data:* {formatted_date}\n\n"

            for market_name, maps_data in market_groups.items():
                # Verifica se todas as apostas são iguais em ambos os mapas
                all_same = True
                if "Mapa 1" in maps_data and "Mapa 2" in maps_data:
                    map1_bets = maps_data["Mapa 1"]
                    map2_bets = maps_data["Mapa 2"]

                    if len(map1_bets) == len(map2_bets):
                        for i in range(len(map1_bets)):
                            bet1 = map1_bets[i]
                            bet2 = map2_bets[i]
                            if (
                                bet1["selection_line"] != bet2["selection_line"]
                                or bet1["handicap"] != bet2["handicap"]
                                or bet1["house_odds"] != bet2["house_odds"]
                            ):
                                all_same = False
                                break
                    else:
                        all_same = False
                else:
                    all_same = False

                if all_same:
                    # Mesmas apostas em ambos os mapas → lista TODAS (não só a primeira)
                    message += f"🗺️ *Mercado:* {market_name} (Mapa 1 & 2)\n"
                    for bet in maps_data["Mapa 1"]:
                        suffix = _player_suffix(bet["market_name"])
                        message += f"✅ *Seleção:* {bet['selection_line']} {bet['handicap']}{suffix}\n"
                        message += f"💰 *Odds:* {bet['house_odds']} | Odd Justa: {bet['fair_odds']:.2f}\n"
                        message += f"📊 *ROI:* {bet['roi_average']:.1f}%\n"
                    message += f"💵 *Stake:* {stake} unidade(s) por mapa\n\n"

                else:
                    # Apostas diferentes por mapa → mantém formato original, mas com sufixo só em player
                    message += f"📊 *Mercado:* {market_name}\n"
                    for map_name, map_bets in maps_data.items():
                        if len(map_bets) == 1:
                            # Apenas uma aposta neste mapa
                            bet = map_bets[0]
                            suffix = _player_suffix(bet["market_name"])
                            message += f"\n🗺️ *{map_name}:*\n"
                            message += f"   ✅ {bet['selection_line']} {bet['handicap']}{suffix}\n"
                            message += f"   💰 Odds: {bet['house_odds']} | Odd Justa: {bet['fair_odds']:.2f} | ROI: {bet['roi_average']:.1f}%\n"
                        else:
                            # Múltiplas apostas no mesmo mapa
                            message += f"\n🗺️ *{map_name}:*\n"
                            for bet in map_bets:
                                suffix = _player_suffix(bet["market_name"])
                                message += f"   ✅ {bet['selection_line']} {bet['handicap']}{suffix}\n"
                                message += f"   💰 Odds: {bet['house_odds']} | Odd Justa: {bet['fair_odds']:.2f} | ROI: {bet['roi_average']:.1f}%\n"

                    message += f"\n💵 *Stake:* {stake} unidade(s) por aposta\n\n"

            message += "#LoL #Bet365 #Aposta #EV+"

            # Enviar notificação
            success = self.telegram_notifier.send_message(
                message, parse_mode="Markdown"
            )
            if success:
                print(f"📤 Notificação agrupada enviada para Telegram")
            else:
                print(f"⚠️ Falha ao enviar notificação agrupada")

        except Exception as e:
            print(f"❌ Erro ao enviar notificação agrupada: {str(e)}")

    def update_bet_result(self, bet_id: int, bet_status: str, actual_win: float = 0):
        """Atualiza o resultado de uma aposta"""
        conn = sqlite3.connect(self.bets_db_path)
        cursor = conn.cursor()

        cursor.execute(
            """
            UPDATE bets 
            SET bet_status = ?, actual_win = ?, result_verified = TRUE, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
        """,
            (bet_status, actual_win, bet_id),
        )

        conn.commit()
        conn.close()

    def verify_bet_results(self, event_id: str):
        """
        Verifica os resultados das apostas para um evento finalizado
        Retorna o lucro/prejuízo total para o evento
        """
        conn = sqlite3.connect(self.bets_db_path)
        cursor = conn.cursor()

        # Busca informações do evento
        cursor.execute(
            "SELECT home_score, away_score, winner FROM events WHERE event_id = ?",
            (event_id,),
        )
        event_result = cursor.fetchone()

        if not event_result:
            conn.close()
            return 0

        home_score, away_score, winner = event_result

        # Busca todas as apostas para este evento
        cursor.execute(
            "SELECT id, market_name, selection_line, handicap, house_odds, stake FROM bets WHERE event_id = ? AND bet_status = 'pending'",
            (event_id,),
        )
        bets = cursor.fetchall()

        total_profit_loss = 0

        for bet_id, market_name, selection_line, handicap, house_odds, stake in bets:
            # Determina se a aposta foi vencedora
            bet_won = self._determine_bet_result(
                selection_line, handicap, home_score, away_score, winner, market_name
            )

            if bet_won:
                actual_win = (house_odds - 1) * stake
                self.update_bet_result(bet_id, "won", actual_win)
                total_profit_loss += actual_win

                # Registrar verificação
                cursor.execute(
                    """
                    INSERT INTO results_verification (event_id, bet_id, expected_result, actual_result, status, profit_loss)
                    VALUES (?, ?, ?, ?, ?, ?)
                """,
                    (event_id, bet_id, "win", "win", "correct", actual_win),
                )

                # Notificar resultado vencedor
                self._notify_bet_result(bet_id, "won", actual_win)
            else:
                actual_win = -stake
                self.update_bet_result(bet_id, "lost", actual_win)
                total_profit_loss += actual_win

                # Registrar verificação
                cursor.execute(
                    """
                    INSERT INTO results_verification (event_id, bet_id, expected_result, actual_result, status, profit_loss)
                    VALUES (?, ?, ?, ?, ?, ?)
                """,
                    (event_id, bet_id, "win", "loss", "incorrect", actual_win),
                )

                # Notificar resultado perdedor
                self._notify_bet_result(bet_id, "lost", actual_win)

        conn.commit()
        conn.close()
        return total_profit_loss

    def _determine_bet_result(
        self,
        selection_line: str,
        handicap: float,
        home_score: int,
        away_score: int,
        winner: str,
        market_name: str,  # Novo parâmetro
    ) -> bool:
        """
        Determina se uma aposta foi vencedora com base no resultado real
        Esta é uma implementação simplificada - adapte para seus mercados específicos
        """
        # Lógica para apostas no vencedor do mapa
        if selection_line == "Home":
            return winner == "home"
        elif selection_line == "Away":
            return winner == "away"
        elif selection_line == "Draw":
            return winner == "draw"

        # Lógica para apostas totais (ex: over/under)
        elif "Over" in selection_line:
            total_score = home_score + away_score
            return total_score > handicap
        elif "Under" in selection_line:
            total_score = home_score + away_score
            return total_score < handicap

        # Adicione lógica específica por mapa aqui se necessário
        # Por exemplo, você pode usar market_name para determinar qual mapa analisar

        return False  # Padrão: aposta perdida

    def get_future_events(self) -> List[str]:
        odds_conn = sqlite3.connect(self.odds_db_path)
        bets_conn = sqlite3.connect(self.bets_db_path)

        odds_cursor = odds_conn.cursor()
        odds_cursor.execute(
            """
            SELECT DISTINCT event_id
            FROM current_odds
            WHERE (
                (market_name LIKE '%Totals' AND odds_type IN ('map_1','map_2'))
                OR
                (odds_type = 'player' AND market_name IN (
                    'Map 1 - Player Total Kills',
                    'Map 1 - Player Total Deaths',
                    'Map 1 - Player Total Assists'
                ))
            )
            """
        )
        all_events = {row[0] for row in odds_cursor.fetchall()}

        bets_cursor = bets_conn.cursor()
        bets_cursor.execute("SELECT DISTINCT event_id FROM events")
        analyzed_events = {row[0] for row in bets_cursor.fetchall()}

        new_events = list(all_events - analyzed_events)
        odds_conn.close()
        bets_conn.close()
        return new_events

    def save_event_info(self, event_id: str, event_info: Dict):
        """Salva informações do evento"""
        conn = sqlite3.connect(self.bets_db_path)
        cursor = conn.cursor()

        cursor.execute(
            """
        INSERT OR REPLACE INTO events 
        (event_id, league_name, match_date, home_team, away_team)
        VALUES (?, ?, ?, ?, ?)
        """,
            (
                event_id,
                event_info.get("league_name"),
                event_info.get("match_date"),
                event_info.get("home_team"),
                event_info.get("away_team"),
            ),
        )

        conn.commit()
        conn.close()

    def analyze_event_for_bets(self, event_id: str, min_roi: float = 10) -> List[Dict]:
        """Analisa um evento e retorna as melhores apostas mantendo mapas separados"""
        all_good_bets = []

        # Busca e salva informações do evento
        event_info = self.analyzer.get_event_info(event_id)
        if not event_info:
            return all_good_bets

        self.save_event_info(event_id, event_info)

        team1 = event_info.get("home_team", "Team A")
        team2 = event_info.get("away_team", "Team B")

        # Processar cada mercado separadamente (TOTAlS)
        markets = ["Map 1 - Totals", "Map 2 - Totals"]
        for market in markets:
            betting_lines = self.analyzer.get_betting_lines(event_id, market)

            market_bets = []
            for line in betting_lines:
                selection = line["selection"]
                handicap = line["handicap"]
                odds = line["odds"]

                roi_team1, roi_team2, roi_average, fair_odds_average = (
                    self.analyzer.calculate_average_roi(
                        team1, team2, selection, handicap, odds
                    )
                )

                if roi_average > min_roi:
                    bet_data = {
                        "event_id": event_id,
                        "market_name": market,
                        "selection_line": selection,
                        "handicap": handicap,
                        "house_odds": odds,
                        "roi_average": roi_average,
                        "fair_odds": fair_odds_average,
                    }
                    market_bets.append(bet_data)

            market_bets.sort(key=lambda x: x["roi_average"], reverse=True)
            all_good_bets.extend(market_bets[:2])

        # ➕ NOVO: Apostas de Players (Original apenas)
        player_bets = self.analyze_event_for_player_bets(event_id, min_roi=min_roi)
        if player_bets:
            # você pode limitar, por ex., top 3 apostas de players:
            # all_good_bets.extend(player_bets[:3])
            all_good_bets.extend(player_bets)

        return all_good_bets

    def save_bets(self, bets: List[Dict], stake: float = 1.0):
        """Salva apostas no banco com stake padrão e notifica novas apostas"""
        if not bets:
            return

        conn = sqlite3.connect(self.bets_db_path)
        cursor = conn.cursor()

        bets_by_event = {}
        for bet in bets:
            event_id = bet["event_id"]
            if event_id not in bets_by_event:
                bets_by_event[event_id] = []
            bets_by_event[event_id].append(bet)

        new_bets_by_event = {}  # Para armazenar apenas as apostas novas por evento

        for event_id, event_bets in bets_by_event.items():
            new_bets_for_event = []  # Apostas novas para este evento

            for bet in event_bets:
                # Verificar se a aposta já existe para evitar duplicatas
                cursor.execute(
                    """
                    SELECT id FROM bets 
                    WHERE event_id = ? AND market_name = ? AND selection_line = ? AND handicap = ?
                    """,
                    (
                        bet["event_id"],
                        bet["market_name"],
                        bet["selection_line"],
                        bet["handicap"],
                    ),
                )
                existing_bet = cursor.fetchone()

                if existing_bet:
                    print(
                        f"⏭️ Aposta já existe: {bet['selection_line']} {bet['handicap']}"
                    )
                    continue

                potential_win = (bet["house_odds"] - 1) * stake

                cursor.execute(
                    """
                    INSERT INTO bets 
                    (event_id, market_name, selection_line, handicap, house_odds, roi_average, fair_odds, stake, potential_win)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        bet["event_id"],
                        bet["market_name"],
                        bet["selection_line"],
                        bet["handicap"],
                        bet["house_odds"],
                        bet["roi_average"],
                        bet["fair_odds"],
                        stake,
                        potential_win,
                    ),
                )

                new_bets_for_event.append(bet)

            # Se houver novas apostas para este evento, adicionar ao dicionário
            if new_bets_for_event:
                new_bets_by_event[event_id] = new_bets_for_event

        conn.commit()
        conn.close()

        # Notificar sobre as novas apostas, agrupadas por evento
        for event_id, event_bets in new_bets_by_event.items():
            self._notify_new_bet(event_bets, stake)

    def get_performance_stats(self) -> Dict:
        """Retorna estatísticas de desempenho das apostas"""
        conn = sqlite3.connect(self.bets_db_path)

        query = """
        SELECT 
            COUNT(*) as total_bets,
            SUM(CASE WHEN bet_status = 'won' THEN 1 ELSE 0 END) as won_bets,
            SUM(CASE WHEN bet_status = 'lost' THEN 1 ELSE 0 END) as lost_bets,
            SUM(CASE WHEN bet_status = 'pending' THEN 1 ELSE 0 END) as pending_bets,
            SUM(actual_win) as total_profit_loss,
            AVG(CASE WHEN bet_status != 'pending' THEN house_odds ELSE NULL END) as avg_odds,
            SUM(stake) as total_stake
        FROM bets
        """

        df = pd.read_sql_query(query, conn)
        conn.close()

        if df.empty:
            return {}

        stats = df.iloc[0].to_dict()

        # Calcular ROI
        if stats["total_stake"] > 0:
            stats["roi"] = (stats["total_profit_loss"] / stats["total_stake"]) * 100
        else:
            stats["roi"] = 0

        # Calcular taxa de acerto
        if stats["won_bets"] + stats["lost_bets"] > 0:
            stats["win_rate"] = (
                stats["won_bets"] / (stats["won_bets"] + stats["lost_bets"])
            ) * 100
        else:
            stats["win_rate"] = 0

        return stats

    def scan_all_events(self, min_roi: float = 10, stake: float = 1.0):
        """Escaneia apenas eventos novos e salva apostas ROI > min_roi%"""
        print("🔍 Iniciando scan de eventos futuros...")

        events = self.get_future_events()
        total_events = len(events)

        if total_events == 0:
            print("✅ Todos os eventos já foram analisados")
            return

        total_good_bets = 0
        print(f"📋 Encontrados {total_events} eventos NOVOS para analisar")

        all_good_bets = []

        for i, event_id in enumerate(events, 1):
            print(f"⚡ Analisando evento {i}/{total_events}: {event_id}")

            good_bets = self.analyze_event_for_bets(event_id, min_roi)

            if good_bets:
                print(f"   ✅ {len(good_bets)} apostas com ROI > {min_roi}%")
                all_good_bets.extend(good_bets)
                total_good_bets += len(good_bets)
            else:
                print(f"   ❌ Nenhuma aposta interessante")

        if all_good_bets:
            self.save_bets(all_good_bets, stake)
            print(f"\n🎯 RESULTADO: {total_good_bets} apostas salvas")
            self.show_summary()
        else:
            print("\n😔 Nenhuma aposta encontrada")

    def show_summary(self, limit: int = 10):
        """Mostra resumo das melhores apostas"""
        conn = sqlite3.connect(self.bets_db_path)

        query = """
        SELECT 
            e.event_id,
            e.league_name,
            e.match_date,
            e.home_team,
            e.away_team,
            e.status,
            b.market_name,  -- Nova coluna
            b.selection_line,
            b.handicap,
            b.house_odds,
            b.roi_average,
            b.fair_odds,
            b.bet_status,
            b.actual_win
        FROM bets b
        JOIN events e ON b.event_id = e.event_id
        ORDER BY b.roi_average DESC 
        LIMIT ?
        """

        df = pd.read_sql_query(query, conn, params=[limit])
        conn.close()

        if not df.empty:
            print(f"\n🏆 TOP {len(df)} APOSTAS:")
            print(
                "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
            )

            for _, row in df.iterrows():
                status_icon = (
                    "✅"
                    if row["bet_status"] == "won"
                    else "❌" if row["bet_status"] == "lost" else "⏳"
                )
                print(
                    f"{status_icon} {row['match_date']} | {row['league_name']} | {row['status']}"
                )
                print(f"🥊 {row['home_team']} vs {row['away_team']}")
                print(f"🗺️  {row['market_name']}")  # Nova linha para mostrar o mapa
                print(
                    f"🎯 {row['selection_line']} {row['handicap']} | ROI: {row['roi_average']:.1f}% | Odds: {row['house_odds']} → {row['fair_odds']:.2f}"
                )
                if row["bet_status"] != "pending":
                    print(
                        f"💰 Resultado: {row['bet_status']} | Lucro: {row['actual_win']:.2f}"
                    )
                print()

    def get_stats(self) -> Dict:
        """Retorna estatísticas do banco incluindo desempenho"""
        conn = sqlite3.connect(self.bets_db_path)
        cursor = conn.cursor()

        cursor.execute("SELECT COUNT(*) FROM events")
        total_events = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM bets")
        total_bets = cursor.fetchone()[0]

        cursor.execute("SELECT AVG(roi_average) FROM bets")
        avg_roi = cursor.fetchone()[0] or 0

        cursor.execute("SELECT MAX(roi_average) FROM bets")
        max_roi = cursor.fetchone()[0] or 0

        # Estatísticas de desempenho
        perf_stats = self.get_performance_stats()

        conn.close()

        return {
            "total_events": total_events,
            "total_bets": total_bets,
            "avg_roi": avg_roi,
            "max_roi": max_roi,
            "performance": perf_stats,
        }


if __name__ == "__main__":
    ODDS_DB_PATH = "../data/lol_odds.db"
    BETS_DB_PATH = "../data/bets.db"

    scanner = BetScanner(ODDS_DB_PATH, BETS_DB_PATH)

    # Opções de uso:

    # 1. Scan normal (apenas eventos novos) com ROI mínimo de 15% e stake de 1 unidade
    scanner.scan_all_events(min_roi=15, stake=1.0)

    # 2. Para forçar re-análise de todos os eventos:
    # scanner.force_rescan_all(min_roi=15, stake=1.0)

    # 3. Para limpar e começar do zero:
    # scanner.clear_database()
    # scanner.scan_all_events(min_roi=15, stake=1.0)

    # Mostra estatísticas
    stats = scanner.get_stats()
    print(f"\n📊 ESTATÍSTICAS:")
    print(f"   Eventos: {stats['total_events']}")
    print(f"   Apostas: {stats['total_bets']}")
    print(f"   ROI Médio: {stats['avg_roi']:.1f}%")
    print(f"   ROI Máximo: {stats['max_roi']:.1f}%")

    # Mostrar estatísticas de desempenho se disponíveis
    if stats["performance"]:
        perf = stats["performance"]
        print(f"\n📈 DESEMPENHO:")
        print(
            f"   Apostas: {perf['total_bets']} (⏳{perf['pending_bets']} | ✅{perf['won_bets']} | ❌{perf['lost_bets']})"
        )
        print(f"   Stake Total: {perf['total_stake']:.2f}")
        print(f"   Lucro/Prejuízo: {perf['total_profit_loss']:.2f}")
        print(f"   ROI: {perf['roi']:.1f}%")
        print(f"   Taxa de Acerto: {perf['win_rate']:.1f}%")
        print(f"   Odds Média: {perf['avg_odds']:.2f}")
