import streamlit as st
import sqlite3
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import numpy as np
import os

# Configuração da página
st.set_page_config(
    page_title="Bet365 Analytics Dashboard",
    page_icon="🎯",
    layout="wide",
    initial_sidebar_state="expanded",
)

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
    .metric-card {
        background-color: #f8f9fa;
        padding: 1rem;
        border-radius: 10px;
        border-left: 4px solid #1E88E5;
    }
    .positive-value {
        color: #28a745;
        font-weight: bold;
    }
    .negative-value {
        color: #dc3545;
        font-weight: bold;
    }
    .stButton>button {
        width: 100%;
        border-radius: 5px;
    }
    .today-btn {
        background-color: #1E88E5 !important;
        color: white !important;
    }
</style>
""",
    unsafe_allow_html=True,
)


# Conexão com o banco
@st.cache_resource
def init_connection():
    db_path = "data/bets.db"
    # Verificar se o diretório existe, se não, criar
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    return sqlite3.connect(db_path)


conn = init_connection()


# Carregar dados
@st.cache_data
def load_events():
    return pd.read_sql("SELECT * FROM events", conn)


@st.cache_data
def load_bets():
    return pd.read_sql("SELECT * FROM bets", conn)


@st.cache_data
def load_pending_bets():
    return pd.read_sql("SELECT * FROM bets WHERE bet_status = 'pending'", conn)


@st.cache_data
def load_resolved_bets():
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
    st.markdown("---")

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

    # Sidebar com filtros
    st.sidebar.header("📅 Filtros de Data")

    # Botões de período rápido
    col1, col2, col3 = st.sidebar.columns(3)

    with col1:
        today_btn = st.button("Hoje", key="today")
    with col2:
        tomorrow_btn = st.button("Amanhã", key="tomorrow")
    with col3:
        week_btn = st.button("Esta Semana", key="week")

    st.sidebar.markdown("---")
    st.sidebar.header("🎯 Filtros de Apostas")

    # Filtro de ROI
    min_roi = st.sidebar.slider(
        "ROI Mínimo (%)", min_value=0.0, max_value=50.0, value=5.0, step=0.5
    )

    # Filtro de odds
    min_odds = st.sidebar.slider(
        "Odds Mínimas", min_value=1.0, max_value=10.0, value=1.5, step=0.1
    )

    # Layout principal - Métricas
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.markdown('<div class="metric-card">', unsafe_allow_html=True)
        st.metric("📊 Total Apostas", len(bets_df))
        st.markdown("</div>", unsafe_allow_html=True)

    with col2:
        st.markdown('<div class="metric-card">', unsafe_allow_html=True)
        st.metric("⏳ Pendentes", len(pending_bets_df))
        st.markdown("</div>", unsafe_allow_html=True)

    with col3:
        st.markdown('<div class="metric-card">', unsafe_allow_html=True)
        st.metric("✅ Resolvidas", len(resolved_bets_df))
        st.markdown("</div>", unsafe_allow_html=True)

    with col4:
        st.markdown('<div class="metric-card">', unsafe_allow_html=True)
        if not resolved_bets_df.empty:
            win_bets = resolved_bets_df[resolved_bets_df["bet_status"] == "win"]
            win_rate = (
                len(win_bets) / len(resolved_bets_df) * 100
                if len(resolved_bets_df) > 0
                else 0
            )
            st.metric("🎯 Taxa de Acerto", f"{win_rate:.1f}%")
        else:
            st.metric("🎯 Taxa de Acerto", "0.0%")
        st.markdown("</div>", unsafe_allow_html=True)

    st.markdown("---")

    # Aplicar filtros de data
    today = datetime.now().date()
    filtered_events = events_df.copy()

    if today_btn:
        filtered_events = filtered_events[
            pd.to_datetime(filtered_events["match_date"]).dt.date == today
        ]
    elif tomorrow_btn:
        tomorrow = today + timedelta(days=1)
        filtered_events = filtered_events[
            pd.to_datetime(filtered_events["match_date"]).dt.date == tomorrow
        ]
    elif week_btn:
        week_end = today + timedelta(days=7)
        filtered_events = filtered_events[
            pd.to_datetime(filtered_events["match_date"]).dt.date.between(
                today, week_end
            )
        ]

    # Filtrar apostas pendentes
    filtered_pending_bets = pending_bets_df[
        (pending_bets_df["event_id"].isin(filtered_events["event_id"]))
        & (pending_bets_df["roi_average"] >= min_roi)
        & (pending_bets_df["house_odds"] >= min_odds)
    ]

    # Juntar com informações dos eventos
    pending_with_events = pd.merge(
        filtered_pending_bets,
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

    # Abas principais
    tab1, tab2, tab3, tab4 = st.tabs(
        [
            "🎯 Apostas em Aberto",
            "📊 Todas as Apostas",
            "📈 Resultados",
            "📋 Estatísticas",
        ]
    )

    with tab1:
        show_pending_bets(pending_with_events)

    with tab2:
        show_all_bets(bets_df, events_df)

    with tab3:
        show_results(resolved_bets_df, events_df)

    with tab4:
        show_statistics(resolved_bets_df, events_df)

    # Footer
    st.markdown("---")
    st.caption(
        f"Última atualização: {datetime.now().strftime('%d/%m/%Y %H:%M')} | "
        f"Total de eventos: {len(events_df)} | "
        f"Banco: data/bets.db ({len(bets_df)} apostas)"
    )


def show_pending_bets(bets_with_events):
    st.header("🎯 Apostas em Aberto")

    if bets_with_events.empty:
        st.info("Nenhuma aposta em aberto com os filtros atuais.")
        return

    # Métricas rápidas
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        avg_roi = bets_with_events["roi_average"].mean()
        st.metric("📈 ROI Médio", f"{avg_roi:.1f}%")

    with col2:
        avg_odds = bets_with_events["house_odds"].mean()
        st.metric("🎲 Odds Média", f"{avg_odds:.2f}")

    with col3:
        total_stake = bets_with_events["stake"].sum()
        st.metric("💰 Stake Total", f"${total_stake:.2f}")

    with col4:
        total_potential = bets_with_events["potential_win"].sum()
        st.metric("🚀 Ganho Potencial", f"${total_potential:.2f}")

    # Ordenar por ROI (prioridade) e odds
    sorted_bets = bets_with_events.sort_values(
        ["roi_average", "house_odds"], ascending=[False, False]
    )

    # Formatar para exibição
    sorted_bets["match_date"] = pd.to_datetime(sorted_bets["match_date"]).dt.strftime(
        "%d/%m %H:%M"
    )
    sorted_bets["Partida"] = (
        sorted_bets["home_team"] + " vs " + sorted_bets["away_team"]
    )
    sorted_bets["Retorno Esperado"] = sorted_bets["house_odds"] * sorted_bets["stake"]

    # Exibir tabela com coluna Linha (handicap)
    display_cols = [
        "match_date",
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
            "match_date": "Data/Hora",
            "league_name": "Liga",
            "market_name": "Mercado",
            "selection_line": "Seleção",
            "handicap": "Linha",
            "house_odds": st.column_config.NumberColumn("Odds Casa", format="%.2f"),
            "fair_odds": st.column_config.NumberColumn("Odds Justas", format="%.2f"),
            "roi_average": st.column_config.NumberColumn("ROI (%)", format="%.1f%%"),
            "stake": st.column_config.NumberColumn("Stake", format="$%.2f"),
            "potential_win": st.column_config.NumberColumn(
                "Ganho Potencial", format="$%.2f"
            ),
        },
        hide_index=True,
        use_container_width=True,
        height=400,
    )

    # Gráficos
    col1, col2 = st.columns(2)

    with col1:
        fig = px.scatter(
            sorted_bets,
            x="house_odds",
            y="roi_average",
            color="league_name",
            size="stake",
            hover_data=["Partida", "market_name", "selection_line", "handicap"],
            title="ROI vs Odds por Liga (tamanho = stake)",
        )
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        fig = px.box(
            sorted_bets,
            x="league_name",
            y="roi_average",
            title="Distribuição de ROI por Liga",
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

    all_bets_with_events["match_date"] = pd.to_datetime(
        all_bets_with_events["match_date"]
    ).dt.strftime("%d/%m %H:%M")
    all_bets_with_events["Partida"] = (
        all_bets_with_events["home_team"] + " vs " + all_bets_with_events["away_team"]
    )

    st.dataframe(
        all_bets_with_events[
            [
                "match_date",
                "Partida",
                "league_name",
                "market_name",
                "selection_line",
                "handicap",
                "house_odds",
                "bet_status",
                "stake",
            ]
        ],
        column_config={
            "match_date": "Data/Hora",
            "league_name": "Liga",
            "market_name": "Mercado",
            "selection_line": "Seleção",
            "handicap": "Linha",
            "house_odds": "Odds",
            "bet_status": "Status",
            "stake": "Stake",
        },
        hide_index=True,
        use_container_width=True,
        height=500,
    )


def show_results(resolved_bets, events_df):
    st.header("📈 Resultados das Apostas")

    if resolved_bets.empty:
        st.info("Nenhuma aposta resolvida ainda.")
        return

    # Juntar com eventos
    results_with_events = pd.merge(
        resolved_bets,
        events_df[["event_id", "home_team", "away_team", "match_date", "league_name"]],
        on="event_id",
        how="left",
    )

    # Calcular lucro/prejuízo corretamente
    results_with_events["Lucro_Prejuizo"] = results_with_events.apply(
        calculate_profit_loss, axis=1
    )

    # Métricas de resultados
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
        st.metric("💰 Stake Total", f"${total_stake:.2f}")

    with col2:
        profit_color = "🟢" if total_profit >= 0 else "🔴"
        st.metric(f"{profit_color} Lucro/Prejuízo", f"${total_profit:.2f}")

    with col3:
        st.metric("🎯 Taxa de Acerto", f"{win_rate:.1f}%")

    with col4:
        roi_color = "🟢" if roi >= 0 else "🔴"
        st.metric(f"{roi_color} ROI Total", f"{roi:.1f}%")

    # Tabela de resultados com coluna Linha
    results_with_events["match_date"] = pd.to_datetime(
        results_with_events["match_date"]
    ).dt.strftime("%d/%m %H:%M")
    results_with_events["Partida"] = (
        results_with_events["home_team"] + " vs " + results_with_events["away_team"]
    )

    st.dataframe(
        results_with_events[
            [
                "match_date",
                "Partida",
                "league_name",
                "market_name",
                "selection_line",
                "handicap",
                "house_odds",
                "bet_status",
                "stake",
                "Lucro_Prejuizo",
            ]
        ],
        column_config={
            "match_date": "Data/Hora",
            "league_name": "Liga",
            "market_name": "Mercado",
            "selection_line": "Seleção",
            "handicap": "Linha",
            "house_odds": "Odds",
            "bet_status": "Status",
            "stake": "Stake",
            "Lucro_Prejuizo": st.column_config.NumberColumn(
                "Lucro/Prej", format="$%.2f"
            ),
        },
        hide_index=True,
        use_container_width=True,
        height=400,
    )

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
                title="Lucro/Prejuízo por Liga ($)",
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


def show_statistics(resolved_bets, events_df):
    st.header("📋 Estatísticas Detalhadas")

    if resolved_bets.empty:
        st.info("Nenhuma estatística disponível (apostas não resolvidas).")
        return

    # Juntar com eventos
    stats_data = pd.merge(
        resolved_bets,
        events_df[["event_id", "league_name", "home_team", "away_team"]],
        on="event_id",
        how="left",
    )

    # Calcular lucro/prejuízo
    stats_data["Lucro_Prejuizo"] = stats_data.apply(calculate_profit_loss, axis=1)

    # Seção 1: Estatísticas por Mercado e Seleção
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("📊 Por Mercado")
        market_stats = (
            stats_data.groupby("market_name")
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
            stats_data.groupby("selection_line")
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
