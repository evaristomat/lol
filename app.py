import streamlit as st
import sqlite3
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import numpy as np
import os
from contextlib import contextmanager
import time


def check_db_modified():
    db_path = "data/bets.db"
    if os.path.exists(db_path):
        return os.path.getmtime(db_path)
    return 0


# Configuração da página
st.set_page_config(
    page_title="Bet365 Analytics Dashboard",
    page_icon="🎯",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# Inicializar variáveis de sessão para controle de atualização
if "last_db_update" not in st.session_state:
    st.session_state.last_db_update = check_db_modified()

# Verificar se o banco foi modificado desde a última verificação
current_db_mtime = check_db_modified()
if current_db_mtime > st.session_state.last_db_update:
    # Limpar todos os caches
    st.cache_data.clear()
    st.session_state.last_db_update = current_db_mtime
    st.rerun()

# CSS personalizado
st.markdown(
    """
<style>
    .main-header {
        font-size: 3rem;
        color: #1E88E5;
        text-align: center;
        margin-bottom: 1rem;
    }
    .stButton>button {
        width: 100%;
        border-radius: 5px;
        border: none;
        background-color: #1E88E5;
        color: white;
        padding: 0.5rem;
        font-weight: bold;
    }
    .stButton>button:hover {
        background-color: #0D47A1;
        color: white;
    }
    .dataframe {
        border-radius: 10px;
        overflow: hidden;
    }
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
    }
    .stTabs [data-baseweb="tab"] {
        background-color: #f0f2f6;
        border-radius: 8px 8px 0 0;
        padding: 10px 16px;
        font-weight: bold;
        color: #333;
    }
    .stTabs [aria-selected="true"] {
        background-color: #1E88E5;
        color: white;
    }
    /* Estilizar métricas diretamente */
    div[data-testid="metric-container"] {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 10px;
        border-left: 4px solid #1E88E5;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
    /* Remover padding extra */
    .block-container {
        padding-top: 2rem !important;
    }
</style>
""",
    unsafe_allow_html=True,
)


# Conexão com o banco
@contextmanager
def get_connection():
    """Gerenciador de contexto para conexões SQLite thread-safe"""
    db_path = "data/bets.db"
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path, check_same_thread=False)
    try:
        yield conn
    finally:
        conn.close()


# Carregar dados
@st.cache_data
def load_events():
    with get_connection() as conn:
        return pd.read_sql("SELECT * FROM events", conn)

@st.cache_data
def load_bets():
    with get_connection() as conn:
        return pd.read_sql("SELECT * FROM bets", conn)

@st.cache_data
def load_pending_bets():
    with get_connection() as conn:
        return pd.read_sql("SELECT * FROM bets WHERE bet_status = 'pending'", conn)

@st.cache_data
def load_resolved_bets():
    with get_connection() as conn:
        return pd.read_sql(
            "SELECT * FROM bets WHERE bet_status IN ('win', 'loss', 'won', 'lost')", conn
        )

# Função para calcular lucro/prejuízo corretamente
def calculate_profit_loss(row):
    if row["bet_status"] in ["win", "won"]:
        return row["stake"] * (row["house_odds"] - 1)  # Lucro = stake * (odds - 1)
    else:  # loss ou lost
        return -row["stake"]  # Prejuízo = -stake


# Interface principal
def main():
    st.markdown(
        '<h1 class="main-header">🎯 Bet365 Analytics Dashboard</h1>',
        unsafe_allow_html=True,
    )

    # Carregar dados
    events_df = load_events()
    bets_df = load_bets()
    pending_bets_df = load_pending_bets()
    resolved_bets_df = load_resolved_bets()

    # Corrigir status para consistência
    if not resolved_bets_df.empty:
        resolved_bets_df["bet_status"] = resolved_bets_df["bet_status"].replace(
            {"won": "win", "lost": "loss"}
        )
        
    pending_with_events = pd.merge(
        pending_bets_df,
        events_df[
            [
                "event_id",
                "home_team",
                "away_team",
                "match_date",
                "league_name",
                "status",
            ]
        ],
        on="event_id",
        how="left",
    )

    # Layout principal - Métricas
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("📊 Total Apostas", len(bets_df))

    with col2:
        st.metric("⏳ Pendentes", len(pending_bets_df))

    with col3:
        st.metric("✅ Resolvidas", len(resolved_bets_df))

    with col4:
        if not resolved_bets_df.empty:
            # Calcular ROI geral (lucro total / total apostado * 100)
            resolved_bets_df_copy = resolved_bets_df.copy()
            resolved_bets_df_copy["Lucro_Prejuizo"] = resolved_bets_df_copy.apply(calculate_profit_loss, axis=1)
            total_profit = resolved_bets_df_copy["Lucro_Prejuizo"].sum()
            total_stake_resolved = resolved_bets_df_copy["stake"].sum()
            roi_geral = (total_profit / total_stake_resolved * 100) if total_stake_resolved > 0 else 0
            st.metric("📈 ROI Geral", f"{roi_geral:.1f}%")
        else:
            st.metric("📈 ROI Geral", "0.0%")

    # Abas principais - REORDENADAS: Resultados do Mês antes de Resultado Geral
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs(
        [
            "🎯 Apostas em Aberto",
            "🎮 Estratégia V1",
            f"📅 Resultados {datetime.now().strftime('%B %Y')}",  # Movida para antes
            "📈 Resultado Geral",  # Movida para depois
            "📋 Estatísticas",
            "📊 Todas as Apostas",
        ]
    )

    with tab1:
        show_pending_bets(pending_with_events)

    with tab2:
        show_strategy_v1()

    with tab3:  # Agora é Resultados do Mês
        show_current_month_results(resolved_bets_df, events_df)

    with tab4:  # Agora é Resultado Geral
        show_general_results(resolved_bets_df, events_df)

    with tab5:
        show_statistics(resolved_bets_df, events_df)

    with tab6:
        show_all_bets(bets_df, events_df)

    # Footer
    st.markdown("---")
    st.caption(
        f"Última atualização: {datetime.now().strftime('%d/%m/%Y %H:%M')} | "
        f"Total de eventos: {len(events_df)} | "
        f"Banco: data/bets.db ({len(bets_df)} apostas)"
    )

    # Verificar atualizações a cada 30 segundos
    if "last_update_check" not in st.session_state:
        st.session_state.last_update_check = time.time()

    if time.time() - st.session_state.last_update_check > 30:
        st.session_state.last_update_check = time.time()
        current_db_mtime = check_db_modified()
        if current_db_mtime > st.session_state.last_db_update:
            st.cache_data.clear()
            st.session_state.last_db_update = current_db_mtime
            st.rerun()


def show_pending_bets(bets_with_events):
    st.header("🎯 Apostas em Aberto")

    # Adicionar filtros de data dentro desta aba
    st.subheader("📅 Filtros de Data")

    # Botões de período rápido
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        if st.button("Hoje", key="pending_today", use_container_width=True):
            st.session_state.pending_filter_selected = "today"
    with col2:
        if st.button("Amanhã", key="pending_tomorrow", use_container_width=True):
            st.session_state.pending_filter_selected = "tomorrow"
    with col3:
        if st.button("Esta Semana", key="pending_week", use_container_width=True):
            st.session_state.pending_filter_selected = "week"
    with col4:
        if st.button("Todos", key="pending_all", use_container_width=True):
            st.session_state.pending_filter_selected = "all"

    # Inicializar session state para o filtro da aba de pendentes
    if "pending_filter_selected" not in st.session_state:
        st.session_state.pending_filter_selected = "today"  # Filtro padrão: Hoje

    # Aplicar filtros de data
    today = datetime.now().date()
    if st.session_state.pending_filter_selected == "today":
        filtered_bets = bets_with_events[
            pd.to_datetime(bets_with_events["match_date"]).dt.date == today
        ]
    elif st.session_state.pending_filter_selected == "tomorrow":
        tomorrow = today + timedelta(days=1)
        filtered_bets = bets_with_events[
            pd.to_datetime(bets_with_events["match_date"]).dt.date == tomorrow
        ]
    elif st.session_state.pending_filter_selected == "week":
        week_end = today + timedelta(days=7)
        filtered_bets = bets_with_events[
            pd.to_datetime(bets_with_events["match_date"]).dt.date.between(
                today, week_end
            )
        ]
    else:  # 'all'
        filtered_bets = bets_with_events

    if filtered_bets.empty:
        st.info("Nenhuma aposta em aberto com os filtros atuais.")
        return

    # Métricas rápidas
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        avg_roi = filtered_bets["roi_average"].mean()
        st.metric("📈 ROI Médio", f"{avg_roi:.1f}%")

    with col2:
        avg_odds = filtered_bets["house_odds"].mean()
        st.metric("🎲 Odds Média", f"{avg_odds:.2f}")

    with col3:
        total_stake = filtered_bets["stake"].sum()
        st.metric("💰 Unidades em Aberto", f"{total_stake:.0f} un.")

    with col4:
        total_potential = filtered_bets["potential_win"].sum()
        st.metric("🚀 Ganho Potencial", f"{total_potential:.2f} un.")

    # Ordenar por data (mais antigo primeiro - ordem crescente)
    filtered_bets["match_date"] = pd.to_datetime(filtered_bets["match_date"])
    sorted_bets = filtered_bets.sort_values("match_date", ascending=True).copy()

    # Formatar para exibição
    sorted_bets.loc[:, "match_date_display"] = sorted_bets["match_date"].dt.strftime(
        "%d/%m %H:%M"
    )
    sorted_bets.loc[:, "Partida"] = (
        sorted_bets["home_team"] + " vs " + sorted_bets["away_team"]
    )
    sorted_bets.loc[:, "Retorno Esperado"] = (
        sorted_bets["house_odds"] * sorted_bets["stake"]
    )

    # Certificar-se de que a coluna handicap existe
    if "handicap" not in sorted_bets.columns:
        sorted_bets["handicap"] = None

    # Exibir tabela
    display_cols = [
        "match_date_display",
        "Partida",
        "league_name",
        "market_name",
        "selection_line",
        "handicap",
        "house_odds",
        "fair_odds",
        "roi_average",
        "stake",
        "potential_win",
    ]

    st.dataframe(
        sorted_bets[display_cols],
        column_config={
            "match_date_display": "Data/Hora",
            "league_name": "Liga",
            "market_name": "Mercado",
            "selection_line": "Seleção",
            "handicap": st.column_config.NumberColumn("Linha", format="%.1f"),
            "house_odds": st.column_config.NumberColumn("Odds Casa", format="%.2f"),
            "fair_odds": st.column_config.NumberColumn("Odds Justas", format="%.2f"),
            "roi_average": st.column_config.NumberColumn("ROI (%)", format="%.1f%%"),
            "stake": st.column_config.NumberColumn("Stake", format="%.0f un."),
            "potential_win": st.column_config.NumberColumn(
                "Ganho Potencial", format="%.2f un."
            ),
        },
        hide_index=True,
        use_container_width=True,
        height=400,
    )

    # Gráficos de distribuição
    col1, col2 = st.columns(2)

    with col1:
        league_distribution = sorted_bets["league_name"].value_counts()
        fig = px.pie(
            values=league_distribution.values,
            names=league_distribution.index,
            title="Distribuição de Apostas por Liga",
            hole=0.4,
        )
        fig.update_traces(textposition="inside", textinfo="percent+label")
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        selection_distribution = sorted_bets["selection_line"].value_counts().head(10)
        fig = px.bar(
            x=selection_distribution.values,
            y=selection_distribution.index,
            orientation="h",
            title="Top 10 Seleções Mais Apostadas",
            labels={"x": "Quantidade", "y": "Seleção"},
        )
        fig.update_layout(yaxis={"categoryorder": "total ascending"})
        st.plotly_chart(fig, use_container_width=True)


def show_strategy_v1():
    """Aba para Estratégia V1 com estatísticas gerais e mensais"""
    st.header("🎮 Estratégia V1")

    # Carregar dados
    events_df = load_events()
    bets_df = load_bets()
    pending_bets_df = load_pending_bets()
    resolved_bets_df = load_resolved_bets()

    # Definir a estratégia filtrada
    estrategia_filtrada = {
        # Mercados lucrativos por mapa (apenas os com melhor performance)
        "map1": {
            "markets": [
                "under_total_kills",  # ROI: 33.2% | WR: 72.8%
                "under_total_inhibitors",  # ROI: 21.2% | WR: 61.3%
                "under_total_dragons",  # ROI: 20.5% | WR: 57.5%
                "under_total_towers",  # ROI: 15.0% | WR: 65.7%
            ]
        },
        "map2": {
            "markets": [
                "under_total_dragons",  # ROI: 42.2% | WR: 68.1% (KING)
                "over_total_towers",  # ROI: 36.3% | WR: 66.7% (KING)
                "under_total_towers",  # ROI: 18.7% | WR: 68.2% (Bom)
                "under_game_duration",  # ROI: 14.5% | WR: 62.5% (Bom)
                "over_game_duration",  # ROI: 17.6% | WR: 64.3% (Bom - mas pequena amostra)
            ]
        },
        # Ligas a EVITAR (com prejuízo ou ROI muito baixo)
        "avoid_leagues": [
            "VCS",  # ROI: -1.5% | Prejuízo
            "AL",  # ROI: -25.4% | Prejuízo
            "PRM",  # ROI: -3.4% | Prejuízo
            "NACL",  # ROI: -31.4% | Prejuízo
            "LFL",  # ROI: 4.9% | Muito baixo
            "LTA N",  # ROI: 0.3% | Muito baixo
            "LTA S",  # ROI: 7.6% | Baixo (menor performance)
        ],
        # Filtro geral de odds (aplicado a todas as apostas)
        "min_odds": 1.50,
    }

    # Mapeamento de ligas similares
    league_mapping = {
        "LOL - LCK": ["LCK", "LOL - LCK"],
        "LPL": ["LPL", "LOL - LPL Split 3"],
        "LEC": ["LEC", "LOL - LEC Summer"],
        "TCL": ["TCL", "LOL - TCL Summer"],
        "LCP": ["LCP", "LOL - LCP Season Finals"],
        "NLC": ["NLC", "LOL - NLC Summer Playoffs"],
        "LTA N": ["LTA N", "LOL - LTA North Split 3"],
        "LTA S": ["LTA S", "LOL - LTA South Split 3"],
        "ROL": ["ROL", "LOL - ROL Summer"],
        "LFL": ["LFL", "LOL - LFL Summer"],
        "EBL": ["EBL", "LOL - EBL Summer Playoffs"],
        "NACL": ["NACL", "LOL - NACL Split 2 Playoffs"],
        "LCK CL": ["LCKC", "LOL - LCK CL Rounds 3-5"],
    }

    # Adicionar filtros de data (igual à aba principal)
    st.subheader("📅 Filtros de Data")

    # Botões de período rápido
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        if st.button("Hoje", key="strategy_today", use_container_width=True):
            st.session_state.strategy_filter_selected = "today"
    with col2:
        if st.button("Amanhã", key="strategy_tomorrow", use_container_width=True):
            st.session_state.strategy_filter_selected = "tomorrow"
    with col3:
        if st.button("Próximos 7 Dias", key="strategy_next7", use_container_width=True):
            st.session_state.strategy_filter_selected = "next7"
    with col4:
        if st.button("Todos", key="strategy_all", use_container_width=True):
            st.session_state.strategy_filter_selected = "all"

    # Inicializar session state para o filtro da estratégia
    if "strategy_filter_selected" not in st.session_state:
        st.session_state.strategy_filter_selected = "today"  # Filtro padrão: Hoje

    # Filtrar eventos por data
    today = datetime.now().date()
    filtered_events = events_df.copy()

    if st.session_state.strategy_filter_selected == "today":
        filtered_events = filtered_events[
            pd.to_datetime(filtered_events["match_date"]).dt.date == today
        ]
    elif st.session_state.strategy_filter_selected == "tomorrow":
        tomorrow = today + timedelta(days=1)
        filtered_events = filtered_events[
            pd.to_datetime(filtered_events["match_date"]).dt.date == tomorrow
        ]
    elif st.session_state.strategy_filter_selected == "next7":
        next7_start = today + timedelta(days=1)
        next7_end = today + timedelta(days=7)
        filtered_events = filtered_events[
            pd.to_datetime(filtered_events["match_date"]).dt.date.between(
                next7_start, next7_end
            )
        ]
    # Para "all", não aplicar filtro de data

    # Filtrar apostas pendentes pela estratégia e data
    pending_with_events = pd.merge(
        pending_bets_df,
        events_df[
            [
                "event_id",
                "home_team",
                "away_team",
                "match_date",
                "league_name",
                "status",
            ]
        ],
        on="event_id",
        how="left",
    )

    # Aplicar filtros da estratégia
    estrategia_pending = pending_with_events[
        (~pending_with_events["league_name"].isin(estrategia_filtrada["avoid_leagues"]))
        & (pending_with_events["house_odds"] >= estrategia_filtrada["min_odds"])
    ]

    # Aplicar filtro de data (apenas apostas futuras)
    if st.session_state.strategy_filter_selected != "all":
        estrategia_pending = estrategia_pending[
            estrategia_pending["event_id"].isin(filtered_events["event_id"])
        ]

    # Filtrar apenas apostas futuras (a partir de hoje)
    estrategia_pending = estrategia_pending[
        pd.to_datetime(estrategia_pending["match_date"]).dt.date >= today
    ]

    # Verificar se temos apostas na estratégia
    if estrategia_pending.empty:
        st.info("Nenhuma aposta em aberto que corresponda à Estratégia V1.")
    else:
        # Métricas rápidas
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            avg_roi = estrategia_pending["roi_average"].mean()
            st.metric("📈 ROI Médio", f"{avg_roi:.1f}%")

        with col2:
            avg_odds = estrategia_pending["house_odds"].mean()
            st.metric("🎲 Odds Média", f"{avg_odds:.2f}")

        with col3:
            total_stake = estrategia_pending["stake"].sum()
            st.metric("💰 Unidades em Aberto", f"{total_stake:.0f} un.")

        with col4:
            total_potential = estrategia_pending["potential_win"].sum()
            st.metric("🚀 Ganho Potencial", f"{total_potential:.2f} un.")

        # Ordenar por data (mais antigo primeiro - ordem crescente)
        estrategia_pending["match_date"] = pd.to_datetime(
            estrategia_pending["match_date"]
        )
        sorted_bets = estrategia_pending.sort_values("match_date", ascending=True)

        # Formatar para exibição
        sorted_bets["match_date_display"] = sorted_bets["match_date"].dt.strftime(
            "%d/%m %H:%M"
        )
        sorted_bets["Partida"] = (
            sorted_bets["home_team"] + " vs " + sorted_bets["away_team"]
        )
        sorted_bets.loc[:, "Retorno Esperado"] = sorted_bets["house_odds"] * sorted_bets["stake"]

        # Certificar-se de que a coluna handicap existe
        if "handicap" not in sorted_bets.columns:
            sorted_bets["handicap"] = None

        # Exibir tabela
        display_cols = [
            "match_date_display",
            "Partida",
            "league_name",
            "market_name",
            "selection_line",
            "handicap",
            "house_odds",
            "fair_odds",
            "roi_average",
            "stake",
            "potential_win",
        ]

        st.dataframe(
            sorted_bets[display_cols],
            column_config={
                "match_date_display": "Data/Hora",
                "league_name": "Liga",
                "market_name": "Mercado",
                "selection_line": "Seleção",
                "handicap": st.column_config.NumberColumn("Linha", format="%.1f"),
                "house_odds": st.column_config.NumberColumn("Odds Casa", format="%.2f"),
                "fair_odds": st.column_config.NumberColumn(
                    "Odds Justas", format="%.2f"
                ),
                "roi_average": st.column_config.NumberColumn(
                    "ROI (%)", format="%.1f%%"
                ),
                "stake": st.column_config.NumberColumn("Stake", format="%.0f un."),
                "potential_win": st.column_config.NumberColumn(
                    "Ganho Potencial", format="%.2f un."
                ),
            },
            hide_index=True,
            use_container_width=True,
            height=400,
        )

        # Gráficos de distribuição
        col1, col2 = st.columns(2)

        with col1:
            league_distribution = sorted_bets["league_name"].value_counts()
            fig = px.pie(
                values=league_distribution.values,
                names=league_distribution.index,
                title="Distribuição de Apostas por Liga (Estratégia V1)",
                hole=0.4,
            )
            fig.update_traces(textposition="inside", textinfo="percent+label")
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            selection_distribution = (
                sorted_bets["selection_line"].value_counts().head(10)
            )
            fig = px.bar(
                x=selection_distribution.values,
                y=selection_distribution.index,
                orientation="h",
                title="Top 10 Seleções Mais Apostadas (Estratégia V1)",
                labels={"x": "Quantidade", "y": "Seleção"},
            )
            fig.update_layout(yaxis={"categoryorder": "total ascending"})
            st.plotly_chart(fig, use_container_width=True)

    # Adicionar estatísticas gerais e mensais
    st.markdown("---")
    st.subheader("📊 Estatísticas da Estratégia V1")

    # Filtrar apostas resolvidas pela estratégia
    if not resolved_bets_df.empty:
        # Corrigir status para consistência
        resolved_bets_df["bet_status"] = resolved_bets_df["bet_status"].replace(
            {"won": "win", "lost": "loss"}
        )

        resolved_with_events = pd.merge(
            resolved_bets_df,
            events_df[
                ["event_id", "home_team", "away_team", "match_date", "league_name"]
            ],
            on="event_id",
            how="left",
        )

        # Aplicar agrupamento de ligas
        def map_league(league_name):
            for mapped_name, league_list in league_mapping.items():
                if league_name in league_list:
                    return mapped_name
            return league_name

        resolved_with_events["league_group"] = resolved_with_events[
            "league_name"
        ].apply(map_league)

        # Aplicar filtros da estratégia às apostas resolvidas
        estrategia_resolved = resolved_with_events[
            (
                ~resolved_with_events["league_group"].isin(
                    estrategia_filtrada["avoid_leagues"]
                )
            )
            & (resolved_with_events["house_odds"] >= estrategia_filtrada["min_odds"])
        ]

        if not estrategia_resolved.empty:
            # Calcular lucro/prejuízo corretamente
            estrategia_resolved = estrategia_resolved.copy()
            estrategia_resolved.loc[:, "Lucro_Prejuizo"] = estrategia_resolved.apply(calculate_profit_loss, axis=1)

            # Estatísticas gerais
            total_stake = estrategia_resolved["stake"].sum()
            total_profit = estrategia_resolved["Lucro_Prejuizo"].sum()
            win_bets = len(
                estrategia_resolved[estrategia_resolved["bet_status"] == "win"]
            )
            total_bets = len(estrategia_resolved)
            win_rate = (win_bets / total_bets * 100) if total_bets > 0 else 0
            roi = (total_profit / total_stake * 100) if total_stake > 0 else 0

            col1, col2, col3, col4 = st.columns(4)

            with col1:
                st.metric("📊 Total Unidades Apostadas", f"{total_stake:.2f}")

            with col2:
                profit_color = "🟢" if total_profit >= 0 else "🔴"
                st.metric(f"{profit_color} Lucro/Prejuízo Total", f"{total_profit:.2f}")

            with col3:
                st.metric("🎯 Taxa de Acerto", f"{win_rate:.1f}%")

            with col4:
                roi_color = "🟢" if roi >= 0 else "🔴"
                st.metric(f"{roi_color} ROI Total", f"{roi:.1f}%")

            # Estatísticas mensais
            st.subheader("📅 Estatísticas Mensais")

            # Criar coluna de mês/ano
            estrategia_resolved["match_date"] = pd.to_datetime(
                estrategia_resolved["match_date"]
            )
            estrategia_resolved["mes_ano"] = estrategia_resolved[
                "match_date"
            ].dt.to_period("M")

            # Agrupar por mês
            monthly_stats = (
                estrategia_resolved.groupby("mes_ano", observed=False)
                .agg(
                    {
                        "stake": "sum",
                        "Lucro_Prejuizo": "sum",
                        "bet_status": lambda x: (x == "win").sum(),
                        "event_id": "count",
                    }
                )
                .reset_index()
            )

            # Calcular ROI mensal
            monthly_stats["ROI (%)"] = (
                monthly_stats["Lucro_Prejuizo"] / monthly_stats["stake"] * 100
            ).round(2)

            # Renomear colunas
            monthly_stats.columns = [
                "Mês",
                "Unidades Apostadas",
                "Lucro/Prejuízo",
                "Apostas Ganhas",
                "Total Apostas",
                "ROI (%)",
            ]
            monthly_stats["Mês"] = monthly_stats["Mês"].astype(str)

            # Calcular taxa de acerto
            monthly_stats["Taxa Acerto (%)"] = (
                monthly_stats["Apostas Ganhas"] / monthly_stats["Total Apostas"] * 100
            ).round(1)

            # Ordenar por mês (mais recente primeiro)
            monthly_stats = monthly_stats.sort_values("Mês", ascending=False)

            # Exibir apenas colunas relevantes
            st.dataframe(
                monthly_stats[
                    [
                        "Mês",
                        "Unidades Apostadas",
                        "Lucro/Prejuízo",
                        "Total Apostas",
                        "ROI (%)",
                    ]
                ],
                column_config={
                    "Mês": "Mês",
                    "Unidades Apostadas": st.column_config.NumberColumn(
                        "Unidades Apostadas", format="%.2f"
                    ),
                    "Lucro/Prejuízo": st.column_config.NumberColumn(
                        "Lucro/Prejuízo", format="%.2f"
                    ),
                    "Total Apostas": "Total Apostas",
                    "ROI (%)": st.column_config.NumberColumn(
                        "ROI (%)", format="%.2f%%"
                    ),
                },
                hide_index=True,
                use_container_width=True,
            )

            # Adicionar mais estatísticas (similar à aba de estatísticas gerais)
            st.subheader("📈 Estatísticas Detalhadas")

            # Performance por mercado
            st.markdown("**Por Mercado**")
            market_stats = estrategia_resolved.groupby("market_name").agg(
                {
                    "bet_status": lambda x: (x == "win").sum(),
                    "event_id": "count",
                    "stake": "sum",
                    "Lucro_Prejuizo": "sum",
                }
            )

            market_stats["ROI (%)"] = (
                market_stats["Lucro_Prejuizo"] / market_stats["stake"] * 100
            ).round(2)
            market_stats.columns = [
                "Apostas Ganhas",
                "Total Apostas",
                "Stake Total",
                "Lucro Total",
                "ROI (%)",
            ]
            market_stats = market_stats[
                ["Total Apostas", "Apostas Ganhas", "Lucro Total", "ROI (%)"]
            ]
            market_stats = market_stats.sort_values("Lucro Total", ascending=False)
            st.dataframe(market_stats, use_container_width=True)

            # Performance por seleção (TODAS, não apenas top 10)
            st.markdown("**Por Seleção**")
            selection_stats = estrategia_resolved.groupby("selection_line").agg(
                {
                    "bet_status": lambda x: (x == "win").sum(),
                    "event_id": "count",
                    "stake": "sum",
                    "Lucro_Prejuizo": "sum",
                }
            )

            selection_stats["ROI (%)"] = (
                selection_stats["Lucro_Prejuizo"] / selection_stats["stake"] * 100
            ).round(2)
            selection_stats.columns = [
                "Apostas Ganhas",
                "Total Apostas",
                "Stake Total",
                "Lucro Total",
                "ROI (%)",
            ]
            selection_stats = selection_stats[
                ["Total Apostas", "Apostas Ganhas", "Lucro Total", "ROI (%)"]
            ]
            selection_stats = selection_stats.sort_values(
                "Lucro Total", ascending=False
            )
            st.dataframe(selection_stats, use_container_width=True)

            # Separar estatísticas por mapa
            map1_data = estrategia_resolved[
                estrategia_resolved["market_name"] == "Map 1 - Totals"
            ].copy()
            map2_data = estrategia_resolved[
                estrategia_resolved["market_name"] == "Map 2 - Totals"
            ].copy()

            # Estatísticas gerais por mapa
            col1, col2 = st.columns(2)

            with col1:
                st.markdown("**Map 1 - Estatísticas Gerais**")
                if not map1_data.empty:
                    map1_stats = map1_data.agg(
                        {
                            "stake": "sum",
                            "Lucro_Prejuizo": "sum",
                            "bet_status": lambda x: (x == "win").sum(),
                            "event_id": "count",
                        }
                    )

                    map1_stats["ROI (%)"] = (
                        map1_stats["Lucro_Prejuizo"] / map1_stats["stake"] * 100
                    ).round(2)
                    map1_stats_df = pd.DataFrame(
                        {
                            "Metric": [
                                "Stake Total",
                                "Lucro Total",
                                "Apostas Ganhas",
                                "Total Apostas",
                                "ROI (%)",
                            ],
                            "Value": [
                                map1_stats["stake"],
                                map1_stats["Lucro_Prejuizo"],
                                map1_stats["bet_status"],
                                map1_stats["event_id"],
                                map1_stats["ROI (%)"],
                            ],
                        }
                    )
                    st.dataframe(
                        map1_stats_df.set_index("Metric"), use_container_width=True
                    )
                else:
                    st.info("Sem dados para Map 1")

            with col2:
                st.markdown("**Map 2 - Estatísticas Gerais**")
                if not map2_data.empty:
                    map2_stats = map2_data.agg(
                        {
                            "stake": "sum",
                            "Lucro_Prejuizo": "sum",
                            "bet_status": lambda x: (x == "win").sum(),
                            "event_id": "count",
                        }
                    )

                    map2_stats["ROI (%)"] = (
                        map2_stats["Lucro_Prejuizo"] / map2_stats["stake"] * 100
                    ).round(2)
                    map2_stats_df = pd.DataFrame(
                        {
                            "Metric": [
                                "Stake Total",
                                "Lucro Total",
                                "Apostas Ganhas",
                                "Total Apostas",
                                "ROI (%)",
                            ],
                            "Value": [
                                map2_stats["stake"],
                                map2_stats["Lucro_Prejuizo"],
                                map2_stats["bet_status"],
                                map2_stats["event_id"],
                                map2_stats["ROI (%)"],
                            ],
                        }
                    )
                    st.dataframe(
                        map2_stats_df.set_index("Metric"), use_container_width=True
                    )
                else:
                    st.info("Sem dados para Map 2")

            # Estatísticas de seleção por mapa
            st.markdown("**Map 1 - Por Seleção**")
            if not map1_data.empty:
                map1_selection_stats = map1_data.groupby("selection_line").agg(
                    {
                        "bet_status": lambda x: (x == "win").sum(),
                        "event_id": "count",
                        "stake": "sum",
                        "Lucro_Prejuizo": "sum",
                    }
                )

                map1_selection_stats["ROI (%)"] = (
                    map1_selection_stats["Lucro_Prejuizo"]
                    / map1_selection_stats["stake"]
                    * 100
                ).round(2)
                map1_selection_stats.columns = [
                    "Apostas Ganhas",
                    "Total Apostas",
                    "Stake Total",
                    "Lucro Total",
                    "ROI (%)",
                ]
                map1_selection_stats = map1_selection_stats[
                    ["Total Apostas", "Apostas Ganhas", "Lucro Total", "ROI (%)"]
                ]
                map1_selection_stats = map1_selection_stats.sort_values(
                    "Lucro Total", ascending=False
                )
                st.dataframe(map1_selection_stats, use_container_width=True)
            else:
                st.info("Sem dados para Map 1")

            st.markdown("**Map 2 - Por Seleção**")
            if not map2_data.empty:
                map2_selection_stats = map2_data.groupby("selection_line").agg(
                    {
                        "bet_status": lambda x: (x == "win").sum(),
                        "event_id": "count",
                        "stake": "sum",
                        "Lucro_Prejuizo": "sum",
                    }
                )

                map2_selection_stats["ROI (%)"] = (
                    map2_selection_stats["Lucro_Prejuizo"]
                    / map2_selection_stats["stake"]
                    * 100
                ).round(2)
                map2_selection_stats.columns = [
                    "Apostas Ganhas",
                    "Total Apostas",
                    "Stake Total",
                    "Lucro Total",
                    "ROI (%)",
                ]
                map2_selection_stats = map2_selection_stats[
                    ["Total Apostas", "Apostas Ganhas", "Lucro Total", "ROI (%)"]
                ]
                map2_selection_stats = map2_selection_stats.sort_values(
                    "Lucro Total", ascending=False
                )
                st.dataframe(map2_selection_stats, use_container_width=True)
            else:
                st.info("Sem dados para Map 2")

            # Performance por liga (usando agrupamento)
            st.markdown("**Por Liga (Agrupadas)**")
            league_stats = estrategia_resolved.groupby("league_group").agg(
                {
                    "bet_status": lambda x: (x == "win").sum(),
                    "event_id": "count",
                    "stake": "sum",
                    "Lucro_Prejuizo": "sum",
                }
            )

            league_stats["ROI (%)"] = (
                league_stats["Lucro_Prejuizo"] / league_stats["stake"] * 100
            ).round(2)
            league_stats.columns = [
                "Apostas Ganhas",
                "Total Apostas",
                "Stake Total",
                "Lucro Total",
                "ROI (%)",
            ]
            league_stats = league_stats[
                ["Total Apostas", "Apostas Ganhas", "Lucro Total", "ROI (%)"]
            ]
            league_stats = league_stats.sort_values("Lucro Total", ascending=False)

            st.dataframe(league_stats, use_container_width=True)

            # Performance por faixa de odds - CORRIGIR: não deve haver apostas abaixo de 1.50
            st.markdown("**Por Faixa de Odds**")
            # Verificar se há apostas abaixo do mínimo
            odds_below_min = estrategia_resolved[
                estrategia_resolved["house_odds"] < estrategia_filtrada["min_odds"]
            ]
            if not odds_below_min.empty:
                st.warning(
                    f"Encontradas {len(odds_below_min)} apostas abaixo do mínimo de {estrategia_filtrada['min_odds']}. Isso não deveria acontecer."
                )

            # Criar faixas de odds começando do mínimo da estratégia
            min_odds = estrategia_filtrada["min_odds"]
            odds_bins = [min_odds, 2.0, 3.0, 5.0, 10.0, 100]
            odds_labels = [f"{min_odds}-2.0", "2.0-3.0", "3.0-5.0", "5.0-10.0", "10.0+"]

            estrategia_resolved["Faixa Odds"] = pd.cut(
                estrategia_resolved["house_odds"],
                bins=odds_bins,
                labels=odds_labels,
            )

            odds_stats = estrategia_resolved.groupby("Faixa Odds").agg(
                {
                    "bet_status": lambda x: (x == "win").sum(),
                    "event_id": "count",
                    "stake": "sum",
                    "Lucro_Prejuizo": "sum",
                }
            )

            odds_stats["ROI (%)"] = (
                odds_stats["Lucro_Prejuizo"] / odds_stats["stake"] * 100
            ).round(2)
            odds_stats.columns = [
                "Apostas Ganhas",
                "Total Apostas",
                "Stake Total",
                "Lucro Total",
                "ROI (%)",
            ]
            odds_stats = odds_stats[
                ["Total Apostas", "Apostas Ganhas", "Lucro Total", "ROI (%)"]
            ]

            st.dataframe(odds_stats, use_container_width=True)
        else:
            st.info("Nenhuma aposta resolvida que corresponda à Estratégia V1.")
    else:
        st.info("Nenhuma aposta resolvida disponível para análise.")

def show_general_results(resolved_bets, events_df):
    """Renomeado de show_results para show_general_results"""
    st.header("📈 Resultado Geral")

    if resolved_bets.empty:
        st.info("Nenhuma aposta resolvida ainda.")
        return

    # Juntar com eventos
    results_with_events = pd.merge(
        resolved_bets,
        events_df[["event_id", "home_team", "away_team", "match_date", "league_name"]],
        on="event_id",
        how="left",
    ).copy()

    # Calcular lucro/prejuízo corretamente
    results_with_events.loc[:, "Lucro_Prejuizo"] = results_with_events.apply(calculate_profit_loss, axis=1)

    # Ordenar por data
    results_with_events["match_date"] = pd.to_datetime(
        results_with_events["match_date"]
    )
    results_with_events = results_with_events.sort_values("match_date", ascending=True)

    # TABELA DE RESULTADOS MENSAIS
    st.subheader("📅 Resultados por Mês")

    # Criar coluna de mês/ano
    results_with_events["mes_ano"] = results_with_events["match_date"].dt.to_period("M")

    # Agrupar por mês
    monthly_stats = (
        results_with_events.groupby("mes_ano", observed=False)
        .agg(
            {
                "stake": "sum",  # Unidades apostadas
                "Lucro_Prejuizo": "sum",  # Unidades de lucro
                "bet_status": lambda x: (x == "win").mean() * 100,  # Taxa acerto
            }
        )
        .reset_index()
    )

    # Calcular ROI
    monthly_stats["ROI (%)"] = (
        monthly_stats["Lucro_Prejuizo"] / monthly_stats["stake"] * 100
    ).round(2)

    # Renomear colunas e formatar
    monthly_stats.columns = [
        "Mês",
        "Unidades Apostadas",
        "Unidades de Lucro",
        "Taxa Acerto (%)",
        "ROI (%)",
    ]
    monthly_stats["Mês"] = monthly_stats["Mês"].astype(str)
    monthly_stats["Taxa Acerto (%)"] = monthly_stats["Taxa Acerto (%)"].round(1)
    monthly_stats["Unidades Apostadas"] = monthly_stats["Unidades Apostadas"].round(2)
    monthly_stats["Unidades de Lucro"] = monthly_stats["Unidades de Lucro"].round(2)

    # Ordenar por mês (mais recente primeiro)
    monthly_stats = monthly_stats.sort_values("Mês", ascending=False)

    st.dataframe(
        monthly_stats,
        column_config={
            "Mês": "Mês",
            "Unidades Apostadas": st.column_config.NumberColumn(
                "Unidades Apostadas", format="%.2f"
            ),
            "Unidades de Lucro": st.column_config.NumberColumn(
                "Unidades de Lucro", format="%.2f"
            ),
            "Taxa Acerto (%)": st.column_config.NumberColumn(
                "Taxa Acerto (%)", format="%.1f%%"
            ),
            "ROI (%)": st.column_config.NumberColumn("ROI (%)", format="%.2f%%"),
        },
        hide_index=True,
        use_container_width=True,
        height=200,
    )

    st.markdown("---")

    # Métricas de resultados gerais
    total_stake = results_with_events["stake"].sum()
    total_profit = results_with_events["Lucro_Prejuizo"].sum()
    win_rate = (
        len(results_with_events[results_with_events["bet_status"] == "win"])
        / len(results_with_events)
        * 100
    )
    roi = (total_profit / total_stake * 100) if total_stake > 0 else 0

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("📊 Unidades Apostadas", f"{total_stake:.2f}")

    with col2:
        profit_color = "🟢" if total_profit >= 0 else "🔴"
        st.metric(f"{profit_color} Lucro/Prejuízo (un)", f"{total_profit:.2f}")

    with col3:
        st.metric("🎯 Taxa de Acerto", f"{win_rate:.1f}%")

    with col4:
        roi_color = "🟢" if roi >= 0 else "🔴"
        st.metric(f"{roi_color} ROI Total", f"{roi:.1f}%")

    # Gráficos de resultados
    if len(results_with_events) > 1:
        col1, col2 = st.columns(2)

        with col1:
            # Performance por liga
            league_performance = (
                results_with_events.groupby("league_name")
                .agg(
                    {
                        "stake": "sum",
                        "Lucro_Prejuizo": "sum",
                        "bet_status": lambda x: (x == "win").sum(),
                    }
                )
                .reset_index()
            )

            league_performance["ROI"] = (
                league_performance["Lucro_Prejuizo"] / league_performance["stake"] * 100
            )

            fig = px.bar(
                league_performance,
                x="league_name",
                y="Lucro_Prejuizo",
                title="Lucro/Prejuízo por Liga (unidades)",
                color="Lucro_Prejuizo",
                color_continuous_scale="RdYlGn",
            )
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            # Distribuição de resultados
            fig = px.pie(
                results_with_events,
                names="bet_status",
                title="Distribuição de Resultados",
                color="bet_status",
                color_discrete_map={"win": "#28a745", "loss": "#dc3545"},
            )
            st.plotly_chart(fig, use_container_width=True)


def show_current_month_results(resolved_bets, events_df):
    """Nova função para mostrar resultados do mês atual"""
    current_month = datetime.now().strftime("%B %Y")
    st.header(f"📅 Resultados {current_month}")

    if resolved_bets.empty:
        st.info("Nenhuma aposta resolvida este mês.")
        return

    # Juntar com eventos
    results_with_events = pd.merge(
        resolved_bets,
        events_df[["event_id", "home_team", "away_team", "match_date", "league_name"]],
        on="event_id",
        how="left",
    )

    # Calcular lucro/prejuízo
    results_with_events["Lucro_Prejuizo"] = results_with_events.apply(
        calculate_profit_loss, axis=1
    )

    # Converter para datetime e filtrar mês atual
    results_with_events["match_date"] = pd.to_datetime(
        results_with_events["match_date"]
    )
    current_month_start = datetime.now().replace(
        day=1, hour=0, minute=0, second=0, microsecond=0
    )
    current_month_data = results_with_events[
        results_with_events["match_date"] >= current_month_start
    ]

    if current_month_data.empty:
        st.info("Nenhuma aposta resolvida este mês.")
        return

    # Criar estatísticas diárias
    current_month_data["dia"] = current_month_data["match_date"].dt.date

    daily_stats = []
    for dia in sorted(current_month_data["dia"].unique()):
        dia_data = current_month_data[current_month_data["dia"] == dia]

        unidades_apostadas = dia_data["stake"].sum()
        lucro = dia_data["Lucro_Prejuizo"].sum()
        total_apostas = len(dia_data)
        apostas_ganhas = len(dia_data[dia_data["bet_status"] == "win"])
        taxa_acerto = (apostas_ganhas / total_apostas * 100) if total_apostas > 0 else 0
        roi = (lucro / unidades_apostadas * 100) if unidades_apostadas > 0 else 0

        daily_stats.append(
            {
                "Dia": dia.strftime("%d/%m/%Y"),
                "Unidades Apostadas": round(unidades_apostadas, 2),
                "Lucro": round(lucro, 2),
                "Taxa Acerto (%)": round(taxa_acerto, 1),
                "ROI (%)": round(roi, 2),
            }
        )

    daily_df = pd.DataFrame(daily_stats)

    # Métricas do mês
    total_stake_month = current_month_data["stake"].sum()
    total_profit_month = current_month_data["Lucro_Prejuizo"].sum()
    win_rate_month = (
        (
            len(current_month_data[current_month_data["bet_status"] == "win"])
            / len(current_month_data)
            * 100
        )
        if len(current_month_data) > 0
        else 0
    )
    roi_month = (
        (total_profit_month / total_stake_month * 100) if total_stake_month > 0 else 0
    )

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("📊 Total Unidades", f"{total_stake_month:.2f}")

    with col2:
        profit_color = "🟢" if total_profit_month >= 0 else "🔴"
        st.metric(f"{profit_color} Lucro Total", f"{total_profit_month:.2f}")

    with col3:
        st.metric("🎯 Taxa de Acerto", f"{win_rate_month:.1f}%")

    with col4:
        roi_color = "🟢" if roi_month >= 0 else "🔴"
        st.metric(f"{roi_color} ROI do Mês", f"{roi_month:.1f}%")

    # Tabela diária
    st.subheader("📊 Resultados Diários")

    st.dataframe(
        daily_df,
        column_config={
            "Dia": "Dia",
            "Unidades Apostadas": st.column_config.NumberColumn(
                "Unidades Apostadas", format="%.2f"
            ),
            "Lucro": st.column_config.NumberColumn("Lucro", format="%.2f"),
            "Taxa Acerto (%)": st.column_config.NumberColumn(
                "Taxa Acerto (%)", format="%.1f%%"
            ),
            "ROI (%)": st.column_config.NumberColumn("ROI (%)", format="%.2f%%"),
        },
        hide_index=True,
        use_container_width=True,
    )

    # Gráfico de evolução do lucro acumulado no mês
    if len(daily_df) > 1:
        daily_df["Lucro Acumulado"] = daily_df["Lucro"].cumsum()

        fig = go.Figure()
        fig.add_trace(
            go.Scatter(
                x=daily_df["Dia"],
                y=daily_df["Lucro Acumulado"],
                mode="lines+markers",
                name="Lucro Acumulado",
                line=dict(
                    color="green"
                    if daily_df["Lucro Acumulado"].iloc[-1] >= 0
                    else "red",
                    width=2,
                ),
                marker=dict(size=8),
            )
        )

        fig.add_hline(y=0, line_dash="dash", line_color="gray", opacity=0.5)

        fig.update_layout(
            title=f"Evolução do Lucro Acumulado - {current_month}",
            xaxis_title="Dia",
            yaxis_title="Lucro Acumulado (un.)",
            hovermode="x unified",
            height=400,
        )

        st.plotly_chart(fig, use_container_width=True)


def show_all_bets(bets_df, events_df):
    st.header("📊 Todas as Apostas")

    # Juntar com eventos
    all_bets_with_events = pd.merge(
        bets_df,
        events_df[["event_id", "home_team", "away_team", "match_date", "league_name"]],
        on="event_id",
        how="left",
    )

    # Corrigir status para consistência
    all_bets_with_events["bet_status"] = all_bets_with_events["bet_status"].replace(
        {"won": "win", "lost": "loss"}
    )

    # Ordenar por data (mais antigo primeiro - ordem crescente)
    all_bets_with_events["match_date"] = pd.to_datetime(
        all_bets_with_events["match_date"]
    )
    all_bets_with_events = all_bets_with_events.sort_values(
        "match_date", ascending=True
    )

    all_bets_with_events["match_date_display"] = all_bets_with_events[
        "match_date"
    ].dt.strftime("%d/%m %H:%M")
    all_bets_with_events["Partida"] = (
        all_bets_with_events["home_team"] + " vs " + all_bets_with_events["away_team"]
    )

    # Certificar-se de que a coluna handicap existe
    if "handicap" not in all_bets_with_events.columns:
        all_bets_with_events["handicap"] = None

    # Exibir tabela
    display_cols = [
        "match_date_display",
        "Partida",
        "league_name",
        "market_name",
        "selection_line",
        "handicap",
        "house_odds",
        "bet_status",
        "stake",
    ]

    st.dataframe(
        all_bets_with_events[display_cols],
        column_config={
            "match_date_display": "Data/Hora",
            "league_name": "Liga",
            "market_name": "Mercado",
            "selection_line": "Seleção",
            "handicap": st.column_config.NumberColumn("Linha", format="%.1f"),
            "house_odds": "Odds",
            "bet_status": "Status",
            "stake": "Stake",
        },
        hide_index=True,
        use_container_width=True,
        height=500,
    )


def show_statistics(resolved_bets, events_df):
    st.header("📋 Estatísticas Detalhadas")

    if resolved_bets.empty:
        st.info("Nenhuma estatística disponível (apostas não resolvidas).")
        return

    # Juntar com eventos
    stats_data = pd.merge(
        resolved_bets,
        events_df[["event_id", "league_name", "home_team", "away_team", "match_date"]],
        on="event_id",
        how="left",
    )

    # Calcular lucro/prejuízo
    stats_data["Lucro_Prejuizo"] = stats_data.apply(calculate_profit_loss, axis=1)

    # Verificar se temos dados suficientes
    if len(stats_data) > 0:
        # Converter data para datetime
        stats_data["match_date"] = pd.to_datetime(stats_data["match_date"])

        # Ordenar por data
        stats_data = stats_data.sort_values("match_date")

        # Separar dados por tipo de mercado
        map1_data = stats_data[stats_data["market_name"] == "Map 1 - Totals"].copy()
        map2_data = stats_data[stats_data["market_name"] == "Map 2 - Totals"].copy()

        # Calcular lucro acumulado para cada tipo
        evolution_data = []

        # MAP 1
        if not map1_data.empty:
            map1_data["Lucro_Acumulado"] = map1_data["Lucro_Prejuizo"].cumsum()
            map1_evolution = map1_data[["match_date", "Lucro_Acumulado"]].copy()
            map1_evolution["Tipo"] = "MAP 1"
            evolution_data.append(map1_evolution)

        # MAP 2
        if not map2_data.empty:
            map2_data["Lucro_Acumulado"] = map2_data["Lucro_Prejuizo"].cumsum()
            map2_evolution = map2_data[["match_date", "Lucro_Acumulado"]].copy()
            map2_evolution["Tipo"] = "MAP 2"
            evolution_data.append(map2_evolution)

        # Total Geral (todas as apostas)
        stats_data["Lucro_Acumulado_Total"] = stats_data["Lucro_Prejuizo"].cumsum()
        total_evolution = stats_data[["match_date", "Lucro_Acumulado_Total"]].copy()
        total_evolution.columns = ["match_date", "Lucro_Acumulado"]
        total_evolution["Tipo"] = "Total Geral"
        evolution_data.append(total_evolution)

        if evolution_data:
            evolution_df = pd.concat(evolution_data)

            # Criar gráfico
            fig = px.line(
                evolution_df,
                x="match_date",
                y="Lucro_Acumulado",
                color="Tipo",
                title="Evolução da Banca por Estratégia",
                labels={
                    "match_date": "Data",
                    "Lucro_Acumulado": "Lucro Acumulado (un.)",
                },
                color_discrete_map={
                    "MAP 1": "#1E88E5",
                    "MAP 2": "#FFA726",
                    "Total Geral": "#AB47BC",
                },
            )

            fig.update_layout(
                hovermode="x unified",
                height=400,
                legend=dict(
                    orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1
                ),
            )

            # Adicionar linha no zero
            fig.add_hline(y=0, line_dash="dash", line_color="gray", opacity=0.5)

            st.plotly_chart(fig, use_container_width=True)

            # Métricas de performance por estratégia
            col1, col2, col3, col4 = st.columns(4)

            with col1:
                if not map1_data.empty:
                    lucro_map1 = map1_data["Lucro_Prejuizo"].sum()
                    st.metric(
                        "MAP 1",
                        f"{lucro_map1:.2f} un.",
                        delta=f"{len(map1_data)} apostas",
                    )

            with col2:
                if not map2_data.empty:
                    lucro_map2 = map2_data["Lucro_Prejuizo"].sum()
                    st.metric(
                        "MAP 2",
                        f"{lucro_map2:.2f} un.",
                        delta=f"{len(map2_data)} apostas",
                    )

            with col3:
                lucro_total = stats_data["Lucro_Prejuizo"].sum()
                st.metric(
                    "Total Geral",
                    f"{lucro_total:.2f} un.",
                    delta=f"{len(stats_data)} apostas",
                )
        else:
            st.info("Dados insuficientes para mostrar a evolução da banca.")
    else:
        st.info("Dados insuficientes para mostrar a evolução da banca.")

    st.markdown("---")

    # Seção 1: Estatísticas por Mercado e Seleção
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("📊 Por Mercado")
        market_stats = (
            stats_data.groupby("market_name", observed=False)
            .agg(
                {
                    "bet_status": lambda x: (x == "win").mean() * 100,
                    "stake": "count",
                    "Lucro_Prejuizo": "sum",
                }
            )
            .round(2)
        )

        market_stats.columns = ["Taxa Acerto (%)", "Total Apostas", "Lucro Total"]
        market_stats = market_stats.sort_values("Lucro Total", ascending=False)
        st.dataframe(market_stats, use_container_width=True)

    with col2:
        st.subheader("🎯 Por Seleção (Top 10)")
        selection_stats = (
            stats_data.groupby("selection_line", observed=False)
            .agg(
                {
                    "bet_status": lambda x: (x == "win").mean() * 100,
                    "stake": "count",
                    "Lucro_Prejuizo": "sum",
                }
            )
            .round(2)
        )

        selection_stats.columns = ["Taxa Acerto (%)", "Total Apostas", "Lucro Total"]
        selection_stats = selection_stats.sort_values("Lucro Total", ascending=False)
        st.dataframe(selection_stats.head(10), use_container_width=True)

    # Seção 2: Ligas e Times mais Lucrativos
    st.markdown("---")

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("🏆 Ligas Mais Lucrativas")
        league_stats = stats_data.groupby("league_name").agg(
            {
                "bet_status": lambda x: (x == "win").mean() * 100,
                "stake": ["count", "sum"],
                "Lucro_Prejuizo": "sum",
            }
        )

        league_stats.columns = [
            "Taxa Acerto (%)",
            "Total Apostas",
            "Stake Total",
            "Lucro Total",
        ]
        league_stats["ROI (%)"] = (
            league_stats["Lucro Total"] / league_stats["Stake Total"] * 100
        ).round(2)
        league_stats = league_stats.sort_values("Lucro Total", ascending=False)

        st.dataframe(
            league_stats[
                ["Taxa Acerto (%)", "Total Apostas", "Lucro Total", "ROI (%)"]
            ].round(2),
            use_container_width=True,
        )

    with col2:
        st.subheader("⭐ Top 3 Times por Liga Lucrativa")

        # Pegar as 3 ligas mais lucrativas
        top_leagues = league_stats.head(3).index.tolist()

        for league in top_leagues:
            st.write(f"**{league}**")

            # Criar coluna de time (tanto home quanto away)
            league_data = stats_data[stats_data["league_name"] == league].copy()

            # Separar apostas por time (home e away)
            home_bets = league_data.copy()
            home_bets["team"] = home_bets["home_team"]

            away_bets = league_data.copy()
            away_bets["team"] = away_bets["away_team"]

            # Combinar e agrupar por time
            all_team_bets = pd.concat([home_bets, away_bets])

            team_stats = (
                all_team_bets.groupby("team")
                .agg(
                    {
                        "bet_status": lambda x: (x == "win").mean() * 100,
                        "stake": "count",
                        "Lucro_Prejuizo": "sum",
                    }
                )
                .round(2)
            )

            team_stats.columns = ["Taxa (%)", "Apostas", "Lucro"]
            team_stats = team_stats.sort_values("Lucro", ascending=False).head(3)

            if not team_stats.empty:
                st.dataframe(team_stats, use_container_width=True, height=150)
            else:
                st.write("Sem dados suficientes")

            st.write("")  # Espaço entre ligas

    # Seção 3: Performance por Faixa de Odds
    st.markdown("---")
    st.subheader("📈 Performance por Faixa de Odds")

    # Criar faixas de odds
    stats_data["Faixa Odds"] = pd.cut(
        stats_data["house_odds"],
        bins=[0, 1.5, 2.0, 3.0, 5.0, 10.0, 100],
        labels=["1.0-1.5", "1.5-2.0", "2.0-3.0", "3.0-5.0", "5.0-10.0", "10.0+"],
    )

    odds_stats = stats_data.groupby("Faixa Odds").agg(
        {
            "bet_status": lambda x: (x == "win").mean() * 100,
            "stake": ["count", "sum"],
            "Lucro_Prejuizo": "sum",
        }
    )

    odds_stats.columns = [
        "Taxa Acerto (%)",
        "Total Apostas",
        "Stake Total",
        "Lucro Total",
    ]
    odds_stats["ROI (%)"] = (
        odds_stats["Lucro Total"] / odds_stats["Stake Total"] * 100
    ).round(2)

    st.dataframe(
        odds_stats[
            ["Taxa Acerto (%)", "Total Apostas", "Lucro Total", "ROI (%)"]
        ].round(2),
        use_container_width=True,
    )


if __name__ == "__main__":
    main()
