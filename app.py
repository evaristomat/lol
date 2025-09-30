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
    page_title="🎮 LoL Betting Analytics",
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
    st.cache_data.clear()
    st.session_state.last_db_update = current_db_mtime
    st.rerun()

# CSS personalizado moderno inspirado no site React
st.markdown(
    """
<style>
    /* Importar fontes do Google */
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
    
    /* Reset e configurações globais */
    .main .block-container {
        padding-top: 2rem !important;
        padding-bottom: 2rem !important;
        font-family: 'Inter', sans-serif;
    }
    
    /* Header principal */
    .main-header {
        background: linear-gradient(135deg, #1e3a8a 0%, #7c3aed 50%, #3730a3 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        background-clip: text;
        font-size: 3.5rem;
        font-weight: 700;
        text-align: center;
        margin-bottom: 2rem;
        text-shadow: 0 0 30px rgba(59, 130, 246, 0.3);
    }
    
    /* Cards de métricas modernos */
    div[data-testid="metric-container"] {
        background: linear-gradient(135deg, rgba(30, 58, 138, 0.1) 0%, rgba(124, 58, 237, 0.1) 100%);
        border: 1px solid rgba(59, 130, 246, 0.2);
        border-radius: 16px;
        padding: 1.5rem;
        box-shadow: 0 8px 32px rgba(0, 0, 0, 0.1);
        backdrop-filter: blur(10px);
        transition: all 0.3s ease;
        position: relative;
        overflow: hidden;
    }
    
    div[data-testid="metric-container"]:hover {
        transform: translateY(-5px);
        box-shadow: 0 20px 40px rgba(59, 130, 246, 0.2);
        border-color: rgba(59, 130, 246, 0.4);
    }
    
    div[data-testid="metric-container"]::before {
        content: '';
        position: absolute;
        top: 0;
        left: 0;
        right: 0;
        height: 3px;
        background: linear-gradient(90deg, #3b82f6, #10b981, #f59e0b);
        opacity: 0;
        transition: opacity 0.3s ease;
    }
    
    div[data-testid="metric-container"]:hover::before {
        opacity: 1;
    }
    
    /* Valores das métricas */
    div[data-testid="metric-container"] [data-testid="metric-value"] {
        font-size: 2.5rem !important;
        font-weight: 700 !important;
        background: linear-gradient(135deg, #1e3a8a, #7c3aed);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        background-clip: text;
    }
    
    /* Labels das métricas */
    div[data-testid="metric-container"] [data-testid="metric-label"] {
        font-weight: 600 !important;
        color: #64748b !important;
        font-size: 0.9rem !important;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }
    
    /* Tabs modernos */
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
        background: rgba(30, 58, 138, 0.05);
        padding: 8px;
        border-radius: 16px;
        border: 1px solid rgba(59, 130, 246, 0.1);
    }
    
    .stTabs [data-baseweb="tab"] {
        background: transparent;
        border-radius: 12px;
        padding: 12px 24px;
        font-weight: 600;
        color: #64748b;
        border: none;
        transition: all 0.3s ease;
    }
    
    .stTabs [aria-selected="true"] {
        background: linear-gradient(135deg, #3b82f6, #1e40af);
        color: white !important;
        box-shadow: 0 4px 12px rgba(59, 130, 246, 0.3);
    }
    
    .stTabs [data-baseweb="tab"]:hover {
        background: rgba(59, 130, 246, 0.1);
        color: #3b82f6;
    }
    
    /* Botões modernos */
    .stButton > button {
        background: linear-gradient(135deg, #3b82f6, #1e40af);
        color: white;
        border: none;
        border-radius: 12px;
        padding: 0.75rem 1.5rem;
        font-weight: 600;
        font-size: 0.9rem;
        transition: all 0.3s ease;
        box-shadow: 0 4px 12px rgba(59, 130, 246, 0.2);
        position: relative;
        overflow: hidden;
    }
    
    .stButton > button:hover {
        transform: translateY(-2px);
        box-shadow: 0 8px 20px rgba(59, 130, 246, 0.3);
        background: linear-gradient(135deg, #2563eb, #1d4ed8);
    }
    
    .stButton > button:active {
        transform: translateY(0);
    }
    
    /* DataFrames modernos */
    .stDataFrame {
        border-radius: 16px;
        overflow: hidden;
        box-shadow: 0 8px 32px rgba(0, 0, 0, 0.1);
        border: 1px solid rgba(59, 130, 246, 0.1);
    }
    
    /* Headers de seção */
    .section-header {
        font-size: 1.8rem;
        font-weight: 700;
        color: #1e293b;
        margin: 2rem 0 1rem 0;
        padding-bottom: 0.5rem;
        border-bottom: 3px solid transparent;
        background: linear-gradient(90deg, #3b82f6, #10b981) padding-box,
                    linear-gradient(90deg, #3b82f6, #10b981) border-box;
        border-image: linear-gradient(90deg, #3b82f6, #10b981) 1;
    }
    
    /* Cards de conteúdo */
    .content-card {
        background: rgba(255, 255, 255, 0.8);
        border-radius: 16px;
        padding: 1.5rem;
        margin: 1rem 0;
        border: 1px solid rgba(59, 130, 246, 0.1);
        box-shadow: 0 8px 32px rgba(0, 0, 0, 0.05);
        backdrop-filter: blur(10px);
    }
    
    /* Indicadores de status */
    .status-indicator {
        display: inline-block;
        padding: 4px 12px;
        border-radius: 20px;
        font-size: 0.8rem;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }
    
    .status-win {
        background: linear-gradient(135deg, #10b981, #059669);
        color: white;
        box-shadow: 0 2px 8px rgba(16, 185, 129, 0.3);
    }
    
    .status-loss {
        background: linear-gradient(135deg, #ef4444, #dc2626);
        color: white;
        box-shadow: 0 2px 8px rgba(239, 68, 68, 0.3);
    }
    
    .status-pending {
        background: linear-gradient(135deg, #f59e0b, #d97706);
        color: white;
        box-shadow: 0 2px 8px rgba(245, 158, 11, 0.3);
    }
    
    /* Animações */
    @keyframes fadeInUp {
        from {
            opacity: 0;
            transform: translateY(30px);
        }
        to {
            opacity: 1;
            transform: translateY(0);
        }
    }
    
    .animate-fade-in {
        animation: fadeInUp 0.6s ease-out;
    }
    
    /* Gráficos */
    .plotly-graph-div {
        border-radius: 16px;
        overflow: hidden;
        box-shadow: 0 8px 32px rgba(0, 0, 0, 0.1);
        border: 1px solid rgba(59, 130, 246, 0.1);
    }
    
    /* Footer */
    .footer {
        margin-top: 3rem;
        padding: 1.5rem;
        background: linear-gradient(135deg, rgba(30, 58, 138, 0.05), rgba(124, 58, 237, 0.05));
        border-radius: 16px;
        border: 1px solid rgba(59, 130, 246, 0.1);
        text-align: center;
        color: #64748b;
        font-size: 0.9rem;
    }
    
    /* Responsividade */
    @media (max-width: 768px) {
        .main-header {
            font-size: 2.5rem;
        }
        
        div[data-testid="metric-container"] {
            padding: 1rem;
        }
        
        div[data-testid="metric-container"] [data-testid="metric-value"] {
            font-size: 2rem !important;
        }
    }
    
    /* Scrollbar personalizada */
    ::-webkit-scrollbar {
        width: 8px;
    }
    
    ::-webkit-scrollbar-track {
        background: rgba(0, 0, 0, 0.05);
        border-radius: 4px;
    }
    
    ::-webkit-scrollbar-thumb {
        background: linear-gradient(135deg, #3b82f6, #1e40af);
        border-radius: 4px;
    }
    
    ::-webkit-scrollbar-thumb:hover {
        background: linear-gradient(135deg, #2563eb, #1d4ed8);
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
            "SELECT * FROM bets WHERE bet_status IN ('win', 'loss', 'won', 'lost')",
            conn,
        )


# Função para calcular lucro/prejuízo corretamente
def calculate_profit_loss(row):
    if row["bet_status"] in ["win", "won"]:
        return row["stake"] * (row["house_odds"] - 1)
    else:
        return -row["stake"]


# Interface principal
def main():
    # Header principal com animação
    st.markdown(
        '<h1 class="main-header animate-fade-in">🎮 LoL Betting Analytics</h1>',
        unsafe_allow_html=True,
    )

    # Subtitle
    st.markdown(
        '<p style="text-align: center; color: #64748b; font-size: 1.2rem; margin-bottom: 2rem;">Dashboard Avançado de Estatísticas de Apostas em League of Legends</p>',
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

    # Layout principal - Métricas com design moderno
    st.markdown('<div class="animate-fade-in">', unsafe_allow_html=True)

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric(
            "📊 Total de Apostas",
            len(bets_df),
            help="Número total de apostas registradas no sistema",
        )

    with col2:
        st.metric(
            "⏳ Apostas Pendentes",
            len(pending_bets_df),
            help="Apostas aguardando resultado",
        )

    with col3:
        st.metric(
            "✅ Apostas Resolvidas",
            len(resolved_bets_df),
            help="Apostas com resultado definido",
        )

    with col4:
        if not resolved_bets_df.empty:
            resolved_bets_df_copy = resolved_bets_df.copy()
            resolved_bets_df_copy["Lucro_Prejuizo"] = resolved_bets_df_copy.apply(
                calculate_profit_loss, axis=1
            )
            total_profit = resolved_bets_df_copy["Lucro_Prejuizo"].sum()
            total_stake_resolved = resolved_bets_df_copy["stake"].sum()
            roi_geral = (
                (total_profit / total_stake_resolved * 100)
                if total_stake_resolved > 0
                else 0
            )

            delta_color = "normal" if roi_geral >= 0 else "inverse"
            st.metric(
                "📈 ROI Geral",
                f"{roi_geral:.1f}%",
                delta=f"{total_profit:.2f} unidades",
                help="Retorno sobre investimento geral",
            )
        else:
            st.metric("📈 ROI Geral", "0.0%", help="Retorno sobre investimento geral")

    st.markdown("</div>", unsafe_allow_html=True)

    # Abas principais com design moderno
    tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs(
        [
            "🏠 Dashboard",
            "🎯 Apostas em Aberto",
            "🎮 Estratégia V1",
            f"📅 Resultados {datetime.now().strftime('%B %Y')}",
            "📈 Resultado Geral",
            "📋 Estatísticas Avançadas",
            "➕ Nova Aposta",
        ]
    )

    with tab1:
        show_modern_dashboard(resolved_bets_df, pending_bets_df, events_df)

    with tab2:
        show_pending_bets_modern(pending_bets_df, events_df)

    with tab3:
        show_strategy_v1_modern(resolved_bets_df, pending_bets_df, events_df)

    with tab4:
        show_current_month_results_modern(resolved_bets_df, events_df)

    with tab5:
        show_general_results_modern(resolved_bets_df, events_df)

    with tab6:
        show_advanced_statistics(resolved_bets_df, events_df)

    with tab7:
        show_add_bet_form()

    # Footer moderno
    st.markdown(
        f"""
    <div class="footer">
        <p><strong>🎯 LoL Betting Analytics</strong> | Última atualização: {datetime.now().strftime("%d/%m/%Y %H:%M")}</p>
        <p>Total de eventos: {len(events_df)} | Banco: data/bets.db ({len(bets_df)} apostas)</p>
        <p>Desenvolvido com ❤️ para a comunidade de League of Legends</p>
    </div>
    """,
        unsafe_allow_html=True,
    )


def show_modern_dashboard(resolved_bets_df, pending_bets_df, events_df):
    """Dashboard principal moderno"""
    st.markdown(
        '<h2 class="section-header">🏠 Dashboard Principal</h2>', unsafe_allow_html=True
    )

    if resolved_bets_df.empty:
        st.markdown(
            """
        <div class="content-card">
            <h3>🎯 Bem-vindo ao LoL Betting Analytics!</h3>
            <p>Ainda não há dados suficientes para exibir o dashboard completo. Para começar:</p>
            <ul>
                <li>📝 Adicione suas primeiras apostas na aba "Nova Aposta"</li>
                <li>📊 Acompanhe apostas pendentes na aba "Apostas em Aberto"</li>
                <li>📈 Analise sua performance conforme os resultados chegam</li>
            </ul>
        </div>
        """,
            unsafe_allow_html=True,
        )
        return

    # Preparar dados
    resolved_with_events = pd.merge(
        resolved_bets_df,
        events_df[["event_id", "home_team", "away_team", "match_date", "league_name"]],
        on="event_id",
        how="left",
    )

    resolved_with_events["Lucro_Prejuizo"] = resolved_with_events.apply(
        calculate_profit_loss, axis=1
    )
    resolved_with_events["match_date"] = pd.to_datetime(
        resolved_with_events["match_date"]
    )

    # Métricas financeiras detalhadas
    st.markdown("### 💰 Resumo Financeiro")

    col1, col2, col3, col4 = st.columns(4)

    total_stake = resolved_with_events["stake"].sum()
    total_profit = resolved_with_events["Lucro_Prejuizo"].sum()
    total_wins = resolved_with_events[resolved_with_events["bet_status"] == "win"][
        "Lucro_Prejuizo"
    ].sum()
    total_losses = abs(
        resolved_with_events[resolved_with_events["bet_status"] == "loss"][
            "Lucro_Prejuizo"
        ].sum()
    )

    with col1:
        st.metric(
            "💵 Total Investido",
            f"{total_stake:.2f} un",
            help="Total de unidades apostadas",
        )

    with col2:
        profit_delta = "📈" if total_profit >= 0 else "📉"
        st.metric(
            "💎 Lucro Líquido",
            f"{total_profit:.2f} un",
            delta=f"{profit_delta}",
            help="Lucro total menos perdas",
        )

    with col3:
        st.metric(
            "🟢 Total Ganho",
            f"{total_wins:.2f} un",
            help="Soma de todas as apostas vencedoras",
        )

    with col4:
        st.metric(
            "🔴 Total Perdido",
            f"{total_losses:.2f} un",
            help="Soma de todas as apostas perdidas",
        )

    # Gráfico de evolução do lucro
    st.markdown("### 📈 Evolução da Banca")

    # Ordenar por data e calcular lucro acumulado
    resolved_sorted = resolved_with_events.sort_values("match_date")
    resolved_sorted["Lucro_Acumulado"] = resolved_sorted["Lucro_Prejuizo"].cumsum()

    fig = go.Figure()

    # Linha principal
    fig.add_trace(
        go.Scatter(
            x=resolved_sorted["match_date"],
            y=resolved_sorted["Lucro_Acumulado"],
            mode="lines+markers",
            name="Lucro Acumulado",
            line=dict(color="#3b82f6", width=3),
            marker=dict(size=6, color="#1e40af"),
            hovertemplate="<b>Data:</b> %{x}<br><b>Lucro Acumulado:</b> %{y:.2f} un<extra></extra>",
        )
    )

    # Linha de referência no zero
    fig.add_hline(y=0, line_dash="dash", line_color="gray", opacity=0.5)

    # Área de lucro/prejuízo
    fig.add_trace(
        go.Scatter(
            x=resolved_sorted["match_date"],
            y=resolved_sorted["Lucro_Acumulado"],
            fill="tonexty",
            fillcolor="rgba(59, 130, 246, 0.1)",
            line=dict(color="rgba(255,255,255,0)"),
            showlegend=False,
            hoverinfo="skip",
        )
    )

    fig.update_layout(
        title="Evolução do Lucro Acumulado ao Longo do Tempo",
        xaxis_title="Data",
        yaxis_title="Lucro Acumulado (unidades)",
        hovermode="x unified",
        height=400,
        template="plotly_white",
        font=dict(family="Inter, sans-serif"),
    )

    st.plotly_chart(fig, use_container_width=True)

    # Performance por liga (top 5)
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("### 🏆 Top 5 Ligas Mais Lucrativas")

        league_performance = (
            resolved_with_events.groupby("league_name")
            .agg({"Lucro_Prejuizo": "sum", "stake": "count"})
            .reset_index()
        )

        league_performance = league_performance.sort_values(
            "Lucro_Prejuizo", ascending=False
        ).head(5)

        fig = px.bar(
            league_performance,
            x="Lucro_Prejuizo",
            y="league_name",
            orientation="h",
            title="",
            color="Lucro_Prejuizo",
            color_continuous_scale="RdYlGn",
            text="Lucro_Prejuizo",
        )

        fig.update_traces(texttemplate="%{text:.2f}", textposition="outside")
        fig.update_layout(
            yaxis={"categoryorder": "total ascending"},
            height=300,
            template="plotly_white",
            font=dict(family="Inter, sans-serif"),
            showlegend=False,
        )

        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.markdown("### 📊 Distribuição de Resultados")

        result_counts = resolved_with_events["bet_status"].value_counts()

        fig = px.pie(
            values=result_counts.values,
            names=result_counts.index,
            title="",
            color_discrete_map={"win": "#10b981", "loss": "#ef4444"},
            hole=0.4,
        )

        fig.update_traces(
            textposition="inside", textinfo="percent+label", textfont_size=12
        )

        fig.update_layout(
            height=300, template="plotly_white", font=dict(family="Inter, sans-serif")
        )

        st.plotly_chart(fig, use_container_width=True)


def show_pending_bets_modern(pending_bets_df, events_df):
    """Versão moderna das apostas pendentes"""
    st.markdown(
        '<h2 class="section-header">🎯 Apostas em Aberto</h2>', unsafe_allow_html=True
    )

    if pending_bets_df.empty:
        st.markdown(
            """
        <div class="content-card">
            <h3>🎯 Nenhuma Aposta Pendente</h3>
            <p>Não há apostas aguardando resultado no momento. Que tal adicionar uma nova aposta na aba "Nova Aposta"?</p>
        </div>
        """,
            unsafe_allow_html=True,
        )
        return

    # Merge com eventos
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
    pending_with_events.dropna(subset=["match_date"], inplace=True)
    pending_with_events["match_date"] = pd.to_datetime(
        pending_with_events["match_date"]
    )

    # Filtros de data modernos
    st.markdown("### 📅 Filtros de Período")

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        if st.button("🗓️ Hoje", use_container_width=True):
            st.session_state.pending_filter = "today"
    with col2:
        if st.button("📅 Amanhã", use_container_width=True):
            st.session_state.pending_filter = "tomorrow"
    with col3:
        if st.button("📆 Esta Semana", use_container_width=True):
            st.session_state.pending_filter = "week"
    with col4:
        if st.button("🌐 Todos", use_container_width=True):
            st.session_state.pending_filter = "all"

    # Aplicar filtros
    if "pending_filter" not in st.session_state:
        st.session_state.pending_filter = "today"

    today = datetime.now().date()

    if st.session_state.pending_filter == "today":
        filtered_bets = pending_with_events[
            pending_with_events["match_date"].dt.date == today
        ]
    elif st.session_state.pending_filter == "tomorrow":
        tomorrow = today + timedelta(days=1)
        filtered_bets = pending_with_events[
            pending_with_events["match_date"].dt.date == tomorrow
        ]
    elif st.session_state.pending_filter == "week":
        week_end = today + timedelta(days=7)
        filtered_bets = pending_with_events[
            pending_with_events["match_date"].dt.date.between(today, week_end)
        ]
    else:
        filtered_bets = pending_with_events

    if filtered_bets.empty:
        st.info(f"📊 Nenhuma aposta encontrada para o período selecionado.")
        return

    # Métricas das apostas pendentes
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        avg_roi = filtered_bets["roi_average"].mean()
        st.metric("📈 ROI Médio", f"{avg_roi:.1f}%")

    with col2:
        avg_odds = filtered_bets["house_odds"].mean()
        st.metric("🎲 Odds Média", f"{avg_odds:.2f}")

    with col3:
        total_stake = filtered_bets["stake"].sum()
        st.metric("💰 Total Apostado", f"{total_stake:.0f} un")

    with col4:
        total_potential = filtered_bets["potential_win"].sum()
        st.metric("🚀 Ganho Potencial", f"{total_potential:.2f} un")

    # Tabela de apostas pendentes
    st.markdown("### 📋 Lista de Apostas Pendentes")

    # Preparar dados para exibição
    display_data = filtered_bets.copy()
    display_data["match_date"] = pd.to_datetime(display_data["match_date"])
    display_data = display_data.sort_values("match_date")

    display_data["Data/Hora"] = display_data["match_date"].dt.strftime("%d/%m %H:%M")
    display_data["Partida"] = (
        display_data["home_team"] + " vs " + display_data["away_team"]
    )
    display_data["ROI"] = display_data["roi_average"].apply(lambda x: f"{x:.1f}%")
    display_data["Odds"] = display_data["house_odds"].apply(lambda x: f"{x:.2f}")
    display_data["Stake"] = display_data["stake"].apply(lambda x: f"{x:.0f} un")
    display_data["Potencial"] = display_data["potential_win"].apply(
        lambda x: f"{x:.2f} un"
    )

    # Exibir tabela
    st.dataframe(
        display_data[
            [
                "Data/Hora",
                "Partida",
                "league_name",
                "market_name",
                "selection_line",
                "Odds",
                "ROI",
                "Stake",
                "Potencial",
            ]
        ],
        column_config={
            "league_name": "Liga",
            "market_name": "Mercado",
            "selection_line": "Seleção",
        },
        hide_index=True,
        use_container_width=True,
        height=400,
    )


def show_strategy_v1_modern(resolved_bets_df, pending_bets_df, events_df):
    """Versão moderna da Estratégia V1"""
    st.markdown(
        '<h2 class="section-header">🎮 Estratégia V1</h2>', unsafe_allow_html=True
    )

    # Definir a estratégia filtrada (mantendo a lógica original)
    estrategia_filtrada = {
        "map1": {
            "markets": [
                "under_total_kills",
                "under_total_inhibitors",
                "under_total_dragons",
                "under_total_towers",
            ]
        },
        "map2": {
            "markets": [
                "under_total_dragons",
                "over_total_towers",
                "under_total_towers",
                "under_game_duration",
                "over_game_duration",
            ]
        },
        "avoid_leagues": [
            "VCS",
            "AL",
            "PRM",
            "NACL",
            "LFL",
            "LTA N",
            "LTA S",
        ],
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

    st.markdown(
        """
    <div class="content-card">
        <h3>🎯 Sobre a Estratégia V1</h3>
        <p>A Estratégia V1 foca em mercados específicos com histórico de alta performance, evitando ligas com baixo ROI e aplicando filtros de odds mínimas.</p>
    </div>
    """,
        unsafe_allow_html=True,
    )

    # Filtros de data
    st.markdown("### 📅 Filtros de Período")

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        if st.button("🗓️ Hoje", key="strategy_today", use_container_width=True):
            st.session_state.strategy_filter = "today"
    with col2:
        if st.button("📅 Amanhã", key="strategy_tomorrow", use_container_width=True):
            st.session_state.strategy_filter = "tomorrow"
    with col3:
        if st.button(
            "📆 Próximos 7 Dias", key="strategy_next7", use_container_width=True
        ):
            st.session_state.strategy_filter = "next7"
    with col4:
        if st.button("🌐 Todos", key="strategy_all", use_container_width=True):
            st.session_state.strategy_filter = "all"

    if "strategy_filter" not in st.session_state:
        st.session_state.strategy_filter = "today"

    # Aplicar filtros de estratégia às apostas pendentes
    if not pending_bets_df.empty:
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
            (
                ~pending_with_events["league_name"].isin(
                    estrategia_filtrada["avoid_leagues"]
                )
            )
            & (pending_with_events["house_odds"] >= estrategia_filtrada["min_odds"])
        ]

        if not estrategia_pending.empty:
            st.markdown("### 🎯 Apostas Pendentes da Estratégia V1")

            # Métricas da estratégia
            col1, col2, col3, col4 = st.columns(4)

            with col1:
                avg_roi = estrategia_pending["roi_average"].mean()
                st.metric("📈 ROI Médio", f"{avg_roi:.1f}%")

            with col2:
                avg_odds = estrategia_pending["house_odds"].mean()
                st.metric("🎲 Odds Média", f"{avg_odds:.2f}")

            with col3:
                total_stake = estrategia_pending["stake"].sum()
                st.metric("💰 Total Apostado", f"{total_stake:.0f} un")

            with col4:
                total_potential = estrategia_pending["potential_win"].sum()
                st.metric("🚀 Ganho Potencial", f"{total_potential:.2f} un")
        else:
            st.info(
                "🎯 Nenhuma aposta pendente corresponde aos critérios da Estratégia V1."
            )

    # Análise histórica da estratégia
    if not resolved_bets_df.empty:
        st.markdown("### 📊 Performance Histórica da Estratégia V1")

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

        # Aplicar filtros da estratégia
        estrategia_resolved = resolved_with_events[
            (
                ~resolved_with_events["league_group"].isin(
                    estrategia_filtrada["avoid_leagues"]
                )
            )
            & (resolved_with_events["house_odds"] >= estrategia_filtrada["min_odds"])
        ]

        if not estrategia_resolved.empty:
            estrategia_resolved["Lucro_Prejuizo"] = estrategia_resolved.apply(
                calculate_profit_loss, axis=1
            )

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
                st.metric("📊 Total Apostado", f"{total_stake:.2f} un")

            with col2:
                profit_color = "🟢" if total_profit >= 0 else "🔴"
                st.metric(f"{profit_color} Lucro Total", f"{total_profit:.2f} un")

            with col3:
                st.metric("🎯 Taxa de Acerto", f"{win_rate:.1f}%")

            with col4:
                roi_color = "🟢" if roi >= 0 else "🔴"
                st.metric(f"{roi_color} ROI", f"{roi:.1f}%")
        else:
            st.info(
                "📊 Nenhuma aposta histórica corresponde aos critérios da Estratégia V1."
            )


def show_current_month_results_modern(resolved_bets_df, events_df):
    """Versão moderna dos resultados do mês"""
    current_month = datetime.now().strftime("%B %Y")
    st.markdown(
        f'<h2 class="section-header">📅 Resultados de {current_month}</h2>',
        unsafe_allow_html=True,
    )

    if resolved_bets_df.empty:
        st.markdown(
            """
        <div class="content-card">
            <h3>📅 Nenhum Resultado Este Mês</h3>
            <p>Ainda não há apostas resolvidas para este mês. Continue apostando e acompanhe seus resultados aqui!</p>
        </div>
        """,
            unsafe_allow_html=True,
        )
        return

    # Juntar com eventos
    results_with_events = pd.merge(
        resolved_bets_df,
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
        st.info(f"📊 Nenhuma aposta resolvida em {current_month}.")
        return

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
        st.metric("📊 Total Apostado", f"{total_stake_month:.2f} un")

    with col2:
        profit_color = "🟢" if total_profit_month >= 0 else "🔴"
        st.metric(f"{profit_color} Lucro do Mês", f"{total_profit_month:.2f} un")

    with col3:
        st.metric("🎯 Taxa de Acerto", f"{win_rate_month:.1f}%")

    with col4:
        roi_color = "🟢" if roi_month >= 0 else "🔴"
        st.metric(f"{roi_color} ROI do Mês", f"{roi_month:.1f}%")

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

    # Tabela diária
    st.markdown("### 📊 Resultados Diários")

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
                    width=3,
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
            template="plotly_white",
            font=dict(family="Inter, sans-serif"),
        )

        st.plotly_chart(fig, use_container_width=True)


def show_general_results_modern(resolved_bets_df, events_df):
    """Versão moderna dos resultados gerais"""
    st.markdown(
        '<h2 class="section-header">📈 Resultados Gerais</h2>', unsafe_allow_html=True
    )

    if resolved_bets_df.empty:
        st.markdown(
            """
        <div class="content-card">
            <h3>📈 Nenhum Resultado Ainda</h3>
            <p>Ainda não há apostas resolvidas para análise. Continue apostando e volte aqui para ver sua performance geral!</p>
        </div>
        """,
            unsafe_allow_html=True,
        )
        return

    # Juntar com eventos
    results_with_events = pd.merge(
        resolved_bets_df,
        events_df[["event_id", "home_team", "away_team", "match_date", "league_name"]],
        on="event_id",
        how="left",
    ).copy()

    # Calcular lucro/prejuízo corretamente
    results_with_events["Lucro_Prejuizo"] = results_with_events.apply(
        calculate_profit_loss, axis=1
    )

    # Ordenar por data
    results_with_events["match_date"] = pd.to_datetime(
        results_with_events["match_date"]
    )
    results_with_events = results_with_events.sort_values("match_date", ascending=True)

    # Criar coluna de mês/ano
    results_with_events["mes_ano"] = results_with_events["match_date"].dt.to_period("M")

    # Agrupar por mês
    monthly_stats = (
        results_with_events.groupby("mes_ano", observed=False)
        .agg(
            {
                "stake": "sum",
                "Lucro_Prejuizo": "sum",
                "bet_status": lambda x: (x == "win").mean() * 100,
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

    st.markdown("### 📅 Resultados por Mês")

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

    # Métricas de resultados gerais
    total_stake = results_with_events["stake"].sum()
    total_profit = results_with_events["Lucro_Prejuizo"].sum()
    win_rate = (
        len(results_with_events[results_with_events["bet_status"] == "win"])
        / len(results_with_events)
        * 100
    )
    roi = (total_profit / total_stake * 100) if total_stake > 0 else 0

    st.markdown("### 💰 Resumo Geral")

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("📊 Unidades Apostadas", f"{total_stake:.2f}")

    with col2:
        profit_color = "🟢" if total_profit >= 0 else "🔴"
        st.metric(f"{profit_color} Lucro/Prejuízo", f"{total_profit:.2f} un")

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


def show_advanced_statistics(resolved_bets_df, events_df):
    """Estatísticas avançadas com análises detalhadas"""
    st.markdown(
        '<h2 class="section-header">📋 Estatísticas Avançadas</h2>',
        unsafe_allow_html=True,
    )

    if resolved_bets_df.empty:
        st.markdown(
            """
        <div class="content-card">
            <h3>📋 Estatísticas Indisponíveis</h3>
            <p>Ainda não há dados suficientes para gerar estatísticas avançadas. Continue apostando para desbloquear análises detalhadas!</p>
        </div>
        """,
            unsafe_allow_html=True,
        )
        return

    # Juntar com eventos
    stats_data = pd.merge(
        resolved_bets_df,
        events_df[["event_id", "league_name", "home_team", "away_team", "match_date"]],
        on="event_id",
        how="left",
    )

    # Calcular lucro/prejuízo
    stats_data["Lucro_Prejuizo"] = stats_data.apply(calculate_profit_loss, axis=1)

    # Converter data
    stats_data["match_date"] = pd.to_datetime(stats_data["match_date"])
    stats_data = stats_data.sort_values("match_date")

    # Seção 1: Estatísticas por Mercado e Seleção
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("### 📊 Performance por Mercado")
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
        st.markdown("### 🎯 Top 10 Seleções")
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

    # Seção 2: Ligas mais Lucrativas
    st.markdown("### 🏆 Performance por Liga")

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

    # Seção 3: Performance por Faixa de Odds
    st.markdown("### 📈 Performance por Faixa de Odds")

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


def show_add_bet_form():
    """Formulário para adicionar nova aposta"""
    st.markdown(
        '<h2 class="section-header">➕ Adicionar Nova Aposta</h2>',
        unsafe_allow_html=True,
    )

    st.markdown(
        """
    <div class="content-card">
        <p>Use este formulário para registrar uma nova aposta no sistema. Todos os campos marcados com * são obrigatórios.</p>
    </div>
    """,
        unsafe_allow_html=True,
    )

    with st.form("add_bet_form", clear_on_submit=True):
        col1, col2 = st.columns(2)

        with col1:
            st.markdown("#### 🏟️ Informações da Partida")

            home_team = st.text_input("Time da Casa *", placeholder="Ex: T1")
            away_team = st.text_input("Time Visitante *", placeholder="Ex: G2 Esports")

            match_date = st.date_input(
                "Data e Hora da Partida *",
                value=datetime.now().date() + timedelta(hours=1),
                min_value=datetime.now().date(),
            )
            match_time = st.time_input("Hora da Partida *", value=datetime.now().time())
            # Combinar data e hora
            match_datetime = datetime.combine(match_date, match_time)

            league_name = st.selectbox(
                "Liga/Torneio *",
                ["LCK", "LEC", "LCS", "LPL", "Worlds", "MSI", "TCL", "LCP", "Outros"],
                index=0,
            )

        with col2:
            st.markdown("#### 🎯 Detalhes da Aposta")

            market_name = st.selectbox(
                "Mercado *",
                [
                    "Map 1 - Totals",
                    "Map 2 - Totals",
                    "Match Winner",
                    "Total Maps",
                    "First Blood",
                    "Outros",
                ],
                index=0,
            )

            selection_line = st.text_input(
                "Seleção *", placeholder="Ex: Over 25.5 Kills"
            )

            house_odds = st.number_input(
                "Odds da Casa *", min_value=1.01, value=2.00, step=0.01, format="%.2f"
            )

            stake = st.number_input(
                "Stake (unidades) *", min_value=0.1, value=1.0, step=0.1, format="%.1f"
            )

        col1, col2 = st.columns(2)

        with col1:
            fair_odds = st.number_input(
                "Odds Justas",
                min_value=1.01,
                value=house_odds,
                step=0.01,
                format="%.2f",
            )

        with col2:
            handicap = st.number_input(
                "Handicap (se aplicável)", value=0.0, step=0.5, format="%.1f"
            )

        # Calcular automaticamente
        if house_odds > 0 and stake > 0:
            potential_win = stake * house_odds
            roi_average = ((house_odds / fair_odds) - 1) * 100 if fair_odds > 0 else 0

            col1, col2 = st.columns(2)
            with col1:
                st.info(f"💰 Ganho Potencial: {potential_win:.2f} unidades")
            with col2:
                st.info(f"📈 ROI Estimado: {roi_average:.1f}%")

        submitted = st.form_submit_button(
            "🚀 Adicionar Aposta", use_container_width=True
        )

        if submitted:
            # Validação
            if not all([home_team, away_team, selection_line]):
                st.error("❌ Por favor, preencha todos os campos obrigatórios.")
                return

            try:
                # Inserir no banco de dados
                with get_connection() as conn:
                    # Primeiro, inserir o evento se não existir
                    event_id = f"{home_team}_{away_team}_{match_datetime.strftime('%Y%m%d_%H%M')}"

                    conn.execute(
                        """
                        INSERT OR REPLACE INTO events 
                        (event_id, home_team, away_team, match_date, league_name, status)
                        VALUES (?, ?, ?, ?, ?, 'scheduled')
                    """,
                        (
                            event_id,
                            home_team,
                            away_team,
                            match_datetime.isoformat(),
                            league_name,
                        ),
                    )

                    # Inserir a aposta
                    potential_win = stake * house_odds
                    roi_average = (
                        ((house_odds / fair_odds) - 1) * 100 if fair_odds > 0 else 0
                    )

                    conn.execute(
                        """
                        INSERT INTO bets 
                        (event_id, market_name, selection_line, house_odds, fair_odds, 
                         roi_average, stake, potential_win, bet_status, handicap)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?)
                    """,
                        (
                            event_id,
                            market_name,
                            selection_line,
                            house_odds,
                            fair_odds,
                            roi_average,
                            stake,
                            potential_win,
                            handicap if handicap != 0 else None,
                        ),
                    )

                    conn.commit()

                # Limpar cache e mostrar sucesso
                st.cache_data.clear()
                st.success("✅ Aposta adicionada com sucesso!")
                st.balloons()

                # Atualizar a página após um breve delay
                time.sleep(1)
                st.rerun()

            except Exception as e:
                st.error(f"❌ Erro ao adicionar aposta: {str(e)}")


if __name__ == "__main__":
    main()
