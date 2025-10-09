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
import calendar
import io


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
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
    
    .main .block-container {
        padding-top: 2rem !important;
        padding-bottom: 2rem !important;
        font-family: 'Inter', sans-serif;
    }
    
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
    
    div[data-testid="metric-container"] [data-testid="metric-value"] {
        font-size: 2.5rem !important;
        font-weight: 700 !important;
        background: linear-gradient(135deg, #1e3a8a, #7c3aed);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        background-clip: text;
    }
    
    div[data-testid="metric-container"] [data-testid="metric-label"] {
        font-weight: 600 !important;
        color: #64748b !important;
        font-size: 0.9rem !important;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }
    
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
    
    .stDataFrame {
        border-radius: 16px;
        overflow: hidden;
        box-shadow: 0 8px 32px rgba(0, 0, 0, 0.1);
        border: 1px solid rgba(59, 130, 246, 0.1);
    }
    
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
    
    .content-card {
        background: rgba(255, 255, 255, 0.8);
        border-radius: 16px;
        padding: 1.5rem;
        margin: 1rem 0;
        border: 1px solid rgba(59, 130, 246, 0.1);
        box-shadow: 0 8px 32px rgba(0, 0, 0, 0.05);
        backdrop-filter: blur(10px);
    }
    
    .plotly-graph-div {
        border-radius: 16px;
        overflow: hidden;
        box-shadow: 0 8px 32px rgba(0, 0, 0, 0.1);
        border: 1px solid rgba(59, 130, 246, 0.1);
    }
    
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


# Carregar dados sem cache para evitar problemas de tokenização
def load_events():
    with get_connection() as conn:
        return pd.read_sql("SELECT * FROM events", conn)


def load_bets():
    with get_connection() as conn:
        return pd.read_sql("SELECT * FROM bets", conn)


def load_pending_bets():
    with get_connection() as conn:
        return pd.read_sql("SELECT * FROM bets WHERE bet_status = 'pending'", conn)


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


# === FUNÇÕES DA ABA HISTÓRICO ===


def preparar_dados_para_csv(dados_filtrados):
    """Prepara os dados filtrados para exportação CSV com formatação adequada"""

    if dados_filtrados.empty:
        return pd.DataFrame()

    # Criar cópia dos dados para não modificar o original
    dados_csv = dados_filtrados.copy()

    # Formatar colunas para CSV
    dados_csv["data_aposta"] = dados_csv["match_date"].dt.strftime("%d/%m/%Y %H:%M")
    dados_csv["partida"] = dados_csv["home_team"] + " vs " + dados_csv["away_team"]
    dados_csv["liga"] = dados_csv["league_name"]
    dados_csv["mercado"] = dados_csv["market_name"]
    dados_csv["selecao"] = dados_csv["selection_line"]
    dados_csv["odds"] = dados_csv["house_odds"].round(2)
    dados_csv["stake_unidades"] = dados_csv["stake"].round(2)

    # Formatar status em português
    status_map = {
        "win": "Ganhou",
        "won": "Ganhou",
        "loss": "Perdeu",
        "lost": "Perdeu",
        "pending": "Pendente",
    }
    dados_csv["status"] = dados_csv["bet_status"].map(status_map)

    # Calcular lucro/prejuízo formatado
    def formatar_lucro_prejuizo(row):
        if row["bet_status"] in ["win", "won"]:
            return round(row["stake"] * (row["house_odds"] - 1), 2)
        elif row["bet_status"] in ["loss", "lost"]:
            return round(-row["stake"], 2)
        else:  # pending
            return 0.0

    dados_csv["lucro_prejuizo"] = dados_csv.apply(formatar_lucro_prejuizo, axis=1)

    # Calcular ROI percentual
    def calcular_roi_percent(row):
        if row["bet_status"] in ["win", "won", "loss", "lost"]:
            return round((row["lucro_prejuizo"] / row["stake"]) * 100, 1)
        else:
            return 0.0

    dados_csv["roi_percent"] = dados_csv.apply(calcular_roi_percent, axis=1)

    # Adicionar informações extras úteis
    dados_csv["potencial_ganho"] = (dados_csv["stake"] * (dados_csv["odds"] - 1)).round(
        2
    )
    dados_csv["dia_semana"] = dados_csv["match_date"].dt.strftime("%A")
    dados_csv["mes_ano"] = dados_csv["match_date"].dt.strftime("%m/%Y")

    # Traduzir dias da semana
    dias_pt = {
        "Monday": "Segunda-feira",
        "Tuesday": "Terça-feira",
        "Wednesday": "Quarta-feira",
        "Thursday": "Quinta-feira",
        "Friday": "Sexta-feira",
        "Saturday": "Sábado",
        "Sunday": "Domingo",
    }
    dados_csv["dia_semana"] = dados_csv["dia_semana"].map(dias_pt)

    # Selecionar e ordenar colunas finais para CSV
    colunas_csv = [
        "data_aposta",
        "partida",
        "liga",
        "mercado",
        "selecao",
        "odds",
        "stake_unidades",
        "status",
        "lucro_prejuizo",
        "roi_percent",
        "potencial_ganho",
        "dia_semana",
        "mes_ano",
    ]

    return dados_csv[colunas_csv]


def gerar_metadados_csv(dados_filtrados, filtros_aplicados=None):
    """Gera metadados para incluir no CSV"""

    metadados = {
        "data_exportacao": datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
        "total_registros": len(dados_filtrados),
        "periodo_inicio": dados_filtrados["match_date"].min().strftime("%d/%m/%Y")
        if not dados_filtrados.empty
        else "N/A",
        "periodo_fim": dados_filtrados["match_date"].max().strftime("%d/%m/%Y")
        if not dados_filtrados.empty
        else "N/A",
    }

    if not dados_filtrados.empty:
        # Estatísticas resumidas
        apostas_resolvidas = dados_filtrados[
            dados_filtrados["bet_status"].isin(["win", "won", "loss", "lost"])
        ]

        if not apostas_resolvidas.empty:
            lucro_total = apostas_resolvidas["Lucro_Prejuizo"].sum()
            stake_total = apostas_resolvidas["stake"].sum()
            roi_geral = (lucro_total / stake_total * 100) if stake_total > 0 else 0
            win_rate = (
                apostas_resolvidas["bet_status"].isin(["win", "won"]).sum()
                / len(apostas_resolvidas)
                * 100
            )

            metadados.update(
                {
                    "apostas_resolvidas": len(apostas_resolvidas),
                    "apostas_pendentes": len(
                        dados_filtrados[dados_filtrados["bet_status"] == "pending"]
                    ),
                    "lucro_total": round(lucro_total, 2),
                    "roi_geral_percent": round(roi_geral, 1),
                    "win_rate_percent": round(win_rate, 1),
                    "stake_total": round(stake_total, 2),
                    "odds_media": round(dados_filtrados["house_odds"].mean(), 2),
                    "mercados_unicos": dados_filtrados["market_name"].nunique(),
                    "ligas_unicas": dados_filtrados["league_name"].nunique(),
                }
            )

    if filtros_aplicados:
        metadados["filtros_aplicados"] = filtros_aplicados

    return metadados


def criar_csv_com_metadados(dados_csv, metadados):
    """Cria arquivo CSV com metadados no cabeçalho"""

    output = io.StringIO()

    # Escrever metadados como comentários
    output.write("# LoL Betting Analytics - Exportação de Histórico\n")
    output.write(f"# Data de Exportação: {metadados['data_exportacao']}\n")
    output.write(f"# Total de Registros: {metadados['total_registros']}\n")
    output.write(
        f"# Período: {metadados['periodo_inicio']} até {metadados['periodo_fim']}\n"
    )

    if "apostas_resolvidas" in metadados:
        output.write(f"# Apostas Resolvidas: {metadados['apostas_resolvidas']}\n")
        output.write(f"# Apostas Pendentes: {metadados['apostas_pendentes']}\n")
        output.write(f"# Lucro Total: {metadados['lucro_total']} unidades\n")
        output.write(f"# ROI Geral: {metadados['roi_geral_percent']}%\n")
        output.write(f"# Win Rate: {metadados['win_rate_percent']}%\n")
        output.write(f"# Stake Total: {metadados['stake_total']} unidades\n")
        output.write(f"# Odds Média: {metadados['odds_media']}\n")
        output.write(f"# Mercados Únicos: {metadados['mercados_unicos']}\n")
        output.write(f"# Ligas Únicas: {metadados['ligas_unicas']}\n")

    if "filtros_aplicados" in metadados:
        output.write(f"# Filtros Aplicados: {metadados['filtros_aplicados']}\n")

    output.write("#\n")
    output.write("# Colunas:\n")
    output.write("# data_aposta: Data e hora da aposta\n")
    output.write("# partida: Times que jogaram (Home vs Away)\n")
    output.write("# liga: Nome da liga/campeonato\n")
    output.write("# mercado: Tipo de mercado apostado\n")
    output.write("# selecao: Linha/seleção específica\n")
    output.write("# odds: Odds da casa de apostas\n")
    output.write("# stake_unidades: Valor apostado em unidades\n")
    output.write("# status: Resultado da aposta (Ganhou/Perdeu/Pendente)\n")
    output.write("# lucro_prejuizo: Lucro ou prejuízo em unidades\n")
    output.write("# roi_percent: Retorno sobre investimento em %\n")
    output.write("# potencial_ganho: Ganho potencial se a aposta for vencedora\n")
    output.write("# dia_semana: Dia da semana da partida\n")
    output.write("# mes_ano: Mês e ano da partida\n")
    output.write("#\n")

    # Escrever dados CSV
    dados_csv.to_csv(output, index=False, sep=",", decimal=".", encoding="utf-8")

    return output.getvalue()


def show_export_section(dados_filtrados, filtros_info=None):
    """Exibe seção de exportação CSV com opções avançadas"""

    st.markdown("### 📥 Exportar Dados")

    if dados_filtrados.empty:
        st.warning(
            "⚠️ Não há dados para exportar. Ajuste os filtros para incluir mais apostas."
        )
        return

    col1, col2 = st.columns([2, 1])

    with col1:
        st.markdown(
            f"""
        <div class="content-card">
            <h4>📊 Resumo da Exportação</h4>
            <p><strong>Total de apostas:</strong> {len(dados_filtrados)}</p>
            <p><strong>Período:</strong> {dados_filtrados["match_date"].min().strftime("%d/%m/%Y")} até {dados_filtrados["match_date"].max().strftime("%d/%m/%Y")}</p>
            <p><strong>Formato:</strong> CSV com metadados inclusos</p>
            <p><strong>Encoding:</strong> UTF-8 (compatível com Excel brasileiro)</p>
        </div>
        """,
            unsafe_allow_html=True,
        )

    with col2:
        # Opções de exportação
        incluir_metadados = st.checkbox(
            "📋 Incluir metadados no arquivo",
            value=True,
            help="Adiciona informações resumidas no cabeçalho do CSV",
        )

        formato_decimal = st.selectbox(
            "🔢 Separador decimal",
            options=["Ponto (.)", "Vírgula (,)"],
            index=0,
            help="Escolha o formato de decimal para compatibilidade",
        )

    # Preparar dados para exportação
    dados_csv = preparar_dados_para_csv(dados_filtrados)

    if dados_csv.empty:
        st.error("❌ Erro ao preparar dados para exportação.")
        return

    # Gerar metadados
    metadados = gerar_metadados_csv(dados_filtrados, filtros_info)

    # Criar arquivo CSV
    if incluir_metadados:
        csv_content = criar_csv_com_metadados(dados_csv, metadados)
    else:
        # CSV simples sem metadados
        if formato_decimal == "Vírgula (,)":
            csv_content = dados_csv.to_csv(
                index=False, sep=";", decimal=",", encoding="utf-8"
            )
        else:
            csv_content = dados_csv.to_csv(
                index=False, sep=",", decimal=".", encoding="utf-8"
            )

    # Ajustar formato decimal se necessário
    if formato_decimal == "Vírgula (,)" and incluir_metadados:
        # Substituir pontos por vírgulas nos números (preservando metadados)
        lines = csv_content.split("\n")
        header_end = 0
        for i, line in enumerate(lines):
            if not line.startswith("#"):
                header_end = i
                break

        # Processar apenas as linhas de dados
        for i in range(header_end, len(lines)):
            if lines[i] and not lines[i].startswith("#"):
                # Substituir separador de campo por ; e decimal por ,
                lines[i] = lines[i].replace(",", ";").replace(".", ",")

        csv_content = "\n".join(lines)

    # Nome do arquivo
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    nome_arquivo = f"lol_betting_historico_{timestamp}.csv"

    # Botão de download
    st.download_button(
        label="📥 Baixar Histórico CSV",
        data=csv_content,
        file_name=nome_arquivo,
        mime="text/csv",
        help="Clique para baixar o arquivo CSV com o histórico filtrado",
        use_container_width=True,
    )

    # Informações adicionais
    st.markdown(
        f"""
    <div style="margin-top: 1rem; padding: 1rem; background: rgba(59, 130, 246, 0.1); border-radius: 8px; border-left: 4px solid #3b82f6;">
        <h5>💡 Dicas para usar o CSV:</h5>
        <ul>
            <li><strong>Excel:</strong> Use "Dados > Obter Dados > De Arquivo > CSV" para importar corretamente</li>
            <li><strong>Google Sheets:</strong> Importe o arquivo e escolha "vírgula" como separador</li>
            <li><strong>Análise:</strong> Os metadados no cabeçalho contêm estatísticas resumidas</li>
            <li><strong>Filtros:</strong> Use as colunas 'status', 'liga' e 'mercado' para análises específicas</li>
        </ul>
    </div>
    """,
        unsafe_allow_html=True,
    )

    # Preview dos dados
    with st.expander("👀 Visualizar Preview dos Dados CSV", expanded=False):
        st.markdown("**Primeiras 10 linhas do arquivo CSV:**")
        st.dataframe(dados_csv.head(10), use_container_width=True, height=300)

        st.markdown(
            f"**Total de colunas:** {len(dados_csv.columns)} | **Total de linhas:** {len(dados_csv)}"
        )


def get_filtros_aplicados_info(periodo, status, liga, mercado, odds_range=None):
    """Gera string com informações dos filtros aplicados"""

    filtros = []

    if periodo != "Todos":
        filtros.append(f"Período: {periodo}")

    if status != "Todos":
        filtros.append(f"Status: {status}")

    if liga != "Todas":
        filtros.append(f"Liga: {liga}")

    if mercado != "Todos":
        filtros.append(f"Mercado: {mercado}")

    if odds_range:
        filtros.append(f"Odds: {odds_range[0]:.1f} - {odds_range[1]:.1f}")

    return " | ".join(filtros) if filtros else "Nenhum filtro aplicado"


def show_historico_completo(resolved_bets_df, pending_bets_df, events_df):
    """Aba de histórico completo com todas as apostas e visualizações avançadas"""
    st.markdown(
        '<h2 class="section-header">📜 Histórico Completo</h2>', unsafe_allow_html=True
    )

    # Combinar apostas resolvidas e pendentes
    all_bets_df = pd.concat([resolved_bets_df, pending_bets_df], ignore_index=True)

    if all_bets_df.empty:
        st.markdown(
            """
        <div class="content-card">
            <h3>📜 Histórico Vazio</h3>
            <p>Ainda não há apostas registradas no sistema. Comece fazendo suas primeiras apostas para ver o histórico aqui!</p>
        </div>
        """,
            unsafe_allow_html=True,
        )
        return

    # Merge com eventos para obter informações completas
    historico_completo = pd.merge(
        all_bets_df,
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

    # Limpar dados e converter datas
    historico_completo = historico_completo.dropna(subset=["match_date"])
    historico_completo["match_date"] = pd.to_datetime(historico_completo["match_date"])

    # Calcular lucro/prejuízo para apostas resolvidas
    def calculate_profit_loss_safe(row):
        if row["bet_status"] in ["win", "won"]:
            return row["stake"] * (row["house_odds"] - 1)
        elif row["bet_status"] in ["loss", "lost"]:
            return -row["stake"]
        else:  # pending
            return 0

    historico_completo["Lucro_Prejuizo"] = historico_completo.apply(
        calculate_profit_loss_safe, axis=1
    )

    # Ordenar por data (mais recente primeiro)
    historico_completo = historico_completo.sort_values("match_date", ascending=False)

    # === MÉTRICAS DE RESUMO ===
    st.markdown("### 📊 Resumo Geral do Histórico")

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        total_apostas = len(historico_completo)
        st.metric(
            "📈 Total de Apostas",
            total_apostas,
            help="Número total de apostas registradas (resolvidas + pendentes)",
        )

    with col2:
        if not historico_completo.empty:
            primeira_aposta = (
                historico_completo["match_date"].min().strftime("%d/%m/%Y")
            )
            ultima_aposta = historico_completo["match_date"].max().strftime("%d/%m/%Y")
            st.metric(
                "📅 Período de Atividade",
                f"{primeira_aposta}",
                delta=f"até {ultima_aposta}",
                help="Período entre a primeira e última aposta",
            )
        else:
            st.metric("📅 Período", "N/A")

    with col3:
        apostas_resolvidas = len(
            historico_completo[
                historico_completo["bet_status"].isin(["win", "won", "loss", "lost"])
            ]
        )
        apostas_pendentes = len(
            historico_completo[historico_completo["bet_status"] == "pending"]
        )
        st.metric(
            "✅ Resolvidas",
            apostas_resolvidas,
            delta=f"{apostas_pendentes} pendentes",
            help="Apostas com resultado definido vs pendentes",
        )

    with col4:
        if apostas_resolvidas > 0:
            lucro_total = historico_completo[
                historico_completo["bet_status"].isin(["win", "won", "loss", "lost"])
            ]["Lucro_Prejuizo"].sum()
            stake_total = historico_completo[
                historico_completo["bet_status"].isin(["win", "won", "loss", "lost"])
            ]["stake"].sum()
            roi_geral = (lucro_total / stake_total * 100) if stake_total > 0 else 0

            st.metric(
                "💎 ROI Histórico",
                f"{roi_geral:.1f}%",
                delta=f"{lucro_total:.2f} un",
                help="Retorno sobre investimento de todas as apostas resolvidas",
            )
        else:
            st.metric("💎 ROI Histórico", "0.0%")

    # === FILTROS AVANÇADOS ===
    st.markdown("### 🔍 Filtros Avançados")

    with st.expander("🎛️ Configurar Filtros", expanded=False):
        col1, col2, col3 = st.columns(3)

        with col1:
            # Filtro de período
            periodo_opcoes = [
                "Todos",
                "Hoje",
                "Última Semana",
                "Último Mês",
                "Últimos 3 Meses",
                "Personalizado",
            ]
            periodo_selecionado = st.selectbox(
                "📅 Período", periodo_opcoes, key="hist_periodo"
            )

            if periodo_selecionado == "Personalizado":
                data_inicio = st.date_input(
                    "Data Início", value=historico_completo["match_date"].min().date()
                )
                data_fim = st.date_input(
                    "Data Fim", value=historico_completo["match_date"].max().date()
                )

        with col2:
            # Filtro de status
            status_opcoes = ["Todos"] + list(historico_completo["bet_status"].unique())
            status_selecionado = st.selectbox(
                "🎯 Status", status_opcoes, key="hist_status"
            )

            # Filtro de liga
            ligas_opcoes = ["Todas"] + sorted(
                historico_completo["league_name"].dropna().unique()
            )
            liga_selecionada = st.selectbox("🏆 Liga", ligas_opcoes, key="hist_liga")

        with col3:
            # Filtro de mercado
            mercados_opcoes = ["Todos"] + sorted(
                historico_completo["market_name"].dropna().unique()
            )
            mercado_selecionado = st.selectbox(
                "📊 Mercado", mercados_opcoes, key="hist_mercado"
            )

            # Filtro de odds
            if not historico_completo["house_odds"].isna().all():
                odds_min, odds_max = st.slider(
                    "🎲 Faixa de Odds",
                    min_value=float(historico_completo["house_odds"].min()),
                    max_value=float(historico_completo["house_odds"].max()),
                    value=(
                        float(historico_completo["house_odds"].min()),
                        float(historico_completo["house_odds"].max()),
                    ),
                    step=0.1,
                    key="hist_odds",
                )

    # Aplicar filtros
    dados_filtrados = historico_completo.copy()

    # Filtro de período
    if periodo_selecionado == "Hoje":
        hoje = datetime.now().date()
        dados_filtrados = dados_filtrados[dados_filtrados["match_date"].dt.date == hoje]
    elif periodo_selecionado == "Última Semana":
        uma_semana_atras = datetime.now() - timedelta(days=7)
        dados_filtrados = dados_filtrados[
            dados_filtrados["match_date"] >= uma_semana_atras
        ]
    elif periodo_selecionado == "Último Mês":
        um_mes_atras = datetime.now() - timedelta(days=30)
        dados_filtrados = dados_filtrados[dados_filtrados["match_date"] >= um_mes_atras]
    elif periodo_selecionado == "Últimos 3 Meses":
        tres_meses_atras = datetime.now() - timedelta(days=90)
        dados_filtrados = dados_filtrados[
            dados_filtrados["match_date"] >= tres_meses_atras
        ]
    elif periodo_selecionado == "Personalizado":
        dados_filtrados = dados_filtrados[
            (dados_filtrados["match_date"].dt.date >= data_inicio)
            & (dados_filtrados["match_date"].dt.date <= data_fim)
        ]

    # Outros filtros
    if status_selecionado != "Todos":
        dados_filtrados = dados_filtrados[
            dados_filtrados["bet_status"] == status_selecionado
        ]

    if liga_selecionada != "Todas":
        dados_filtrados = dados_filtrados[
            dados_filtrados["league_name"] == liga_selecionada
        ]

    if mercado_selecionado != "Todos":
        dados_filtrados = dados_filtrados[
            dados_filtrados["market_name"] == mercado_selecionado
        ]

    if not historico_completo["house_odds"].isna().all():
        dados_filtrados = dados_filtrados[
            (dados_filtrados["house_odds"] >= odds_min)
            & (dados_filtrados["house_odds"] <= odds_max)
        ]

    # Verificar se há dados após filtros
    if dados_filtrados.empty:
        st.warning(
            "🔍 Nenhuma aposta encontrada com os filtros aplicados. Tente ajustar os critérios de busca."
        )
        return

    # === VISUALIZAÇÕES GRÁFICAS ===
    st.markdown("### 📈 Visualizações do Histórico")

    col1, col2 = st.columns(2)

    with col1:
        # Timeline de apostas
        st.markdown("#### 📅 Timeline de Apostas")

        # Agrupar por data
        timeline_data = (
            dados_filtrados.groupby(dados_filtrados["match_date"].dt.date)
            .size()
            .reset_index()
        )
        timeline_data.columns = ["Data", "Quantidade"]

        fig_timeline = px.line(
            timeline_data,
            x="Data",
            y="Quantidade",
            title="",
            markers=True,
            line_shape="spline",
        )

        fig_timeline.update_traces(
            line=dict(color="#3b82f6", width=3), marker=dict(size=8, color="#1e40af")
        )

        fig_timeline.update_layout(
            height=300,
            template="plotly_white",
            font=dict(family="Inter, sans-serif"),
            xaxis_title="Data",
            yaxis_title="Número de Apostas",
            hovermode="x unified",
        )

        st.plotly_chart(fig_timeline, use_container_width=True)

    with col2:
        # Distribuição por status
        st.markdown("#### 🎯 Distribuição por Status")

        status_counts = dados_filtrados["bet_status"].value_counts()

        # Mapear cores por status
        color_map = {
            "win": "#10b981",
            "won": "#10b981",
            "loss": "#ef4444",
            "lost": "#ef4444",
            "pending": "#f59e0b",
        }

        colors = [color_map.get(status, "#64748b") for status in status_counts.index]

        fig_status = px.pie(
            values=status_counts.values,
            names=status_counts.index,
            title="",
            hole=0.4,
            color_discrete_sequence=colors,
        )

        fig_status.update_traces(
            textposition="inside", textinfo="percent+label", textfont_size=12
        )

        fig_status.update_layout(
            height=300, template="plotly_white", font=dict(family="Inter, sans-serif")
        )

        st.plotly_chart(fig_status, use_container_width=True)

    # === HEATMAP DE ATIVIDADE ===
    st.markdown("#### 🔥 Heatmap de Atividade por Dia da Semana e Hora")

    # Preparar dados para heatmap
    dados_filtrados["dia_semana"] = dados_filtrados["match_date"].dt.day_name()
    dados_filtrados["hora"] = dados_filtrados["match_date"].dt.hour

    # Traduzir dias da semana
    dias_pt = {
        "Monday": "Segunda",
        "Tuesday": "Terça",
        "Wednesday": "Quarta",
        "Thursday": "Quinta",
        "Friday": "Sexta",
        "Saturday": "Sábado",
        "Sunday": "Domingo",
    }
    dados_filtrados["dia_semana_pt"] = dados_filtrados["dia_semana"].map(dias_pt)

    # Criar matriz para heatmap
    heatmap_data = (
        dados_filtrados.groupby(["dia_semana_pt", "hora"])
        .size()
        .reset_index(name="count")
    )
    heatmap_pivot = heatmap_data.pivot(
        index="dia_semana_pt", columns="hora", values="count"
    ).fillna(0)

    # Reordenar dias da semana
    ordem_dias = ["Segunda", "Terça", "Quarta", "Quinta", "Sexta", "Sábado", "Domingo"]
    heatmap_pivot = heatmap_pivot.reindex(ordem_dias)

    fig_heatmap = px.imshow(
        heatmap_pivot, title="", color_continuous_scale="Blues", aspect="auto"
    )

    fig_heatmap.update_layout(
        height=300,
        template="plotly_white",
        font=dict(family="Inter, sans-serif"),
        xaxis_title="Hora do Dia",
        yaxis_title="Dia da Semana",
    )

    st.plotly_chart(fig_heatmap, use_container_width=True)

    # === MÉTRICAS DOS DADOS FILTRADOS ===
    if len(dados_filtrados) != len(historico_completo):
        st.markdown("### 📊 Estatísticas dos Dados Filtrados")

        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.metric("🔍 Apostas Filtradas", len(dados_filtrados))

        with col2:
            if not dados_filtrados.empty:
                mercados_unicos = dados_filtrados["market_name"].nunique()
                st.metric("📊 Mercados Únicos", mercados_unicos)

        with col3:
            if not dados_filtrados.empty:
                ligas_unicas = dados_filtrados["league_name"].nunique()
                st.metric("🏆 Ligas Únicas", ligas_unicas)

        with col4:
            apostas_resolvidas_filtradas = len(
                dados_filtrados[
                    dados_filtrados["bet_status"].isin(["win", "won", "loss", "lost"])
                ]
            )
            if apostas_resolvidas_filtradas > 0:
                lucro_filtrado = dados_filtrados[
                    dados_filtrados["bet_status"].isin(["win", "won", "loss", "lost"])
                ]["Lucro_Prejuizo"].sum()
                st.metric("💰 Lucro Filtrado", f"{lucro_filtrado:.2f} un")

    # === SEÇÃO DE EXPORTAÇÃO ===
    filtros_info = get_filtros_aplicados_info(
        periodo_selecionado,
        status_selecionado,
        liga_selecionada,
        mercado_selecionado,
        (odds_min, odds_max)
        if not historico_completo["house_odds"].isna().all()
        else None,
    )

    show_export_section(dados_filtrados, filtros_info)

    # === TABELA DE HISTÓRICO ===
    st.markdown("### 📋 Tabela Completa do Histórico")

    # Preparar dados para exibição na tabela
    tabela_dados = dados_filtrados.copy()

    # Formatar colunas para exibição
    tabela_dados["Data/Hora"] = tabela_dados["match_date"].dt.strftime("%d/%m/%Y %H:%M")
    tabela_dados["Partida"] = (
        tabela_dados["home_team"] + " vs " + tabela_dados["away_team"]
    )
    tabela_dados["Odds"] = tabela_dados["house_odds"].apply(
        lambda x: f"{x:.2f}" if pd.notna(x) else "N/A"
    )
    tabela_dados["Stake"] = tabela_dados["stake"].apply(
        lambda x: f"{x:.0f} un" if pd.notna(x) else "N/A"
    )

    # Formatar resultado baseado no status
    def formatar_resultado(row):
        if row["bet_status"] in ["win", "won"]:
            return f"+{row['Lucro_Prejuizo']:.2f} un"
        elif row["bet_status"] in ["loss", "lost"]:
            return f"{row['Lucro_Prejuizo']:.2f} un"
        else:
            return "Pendente"

    tabela_dados["Resultado"] = tabela_dados.apply(formatar_resultado, axis=1)

    # Formatar status para exibição
    status_map = {
        "win": "✅ Ganhou",
        "won": "✅ Ganhou",
        "loss": "❌ Perdeu",
        "lost": "❌ Perdeu",
        "pending": "⏳ Pendente",
    }
    tabela_dados["Status_Formatado"] = tabela_dados["bet_status"].map(status_map)

    # Selecionar colunas para exibição
    colunas_exibicao = [
        "Data/Hora",
        "Partida",
        "league_name",
        "market_name",
        "selection_line",
        "Odds",
        "Stake",
        "Status_Formatado",
        "Resultado",
    ]

    # Configurar colunas
    config_colunas = {
        "league_name": "Liga",
        "market_name": "Mercado",
        "selection_line": "Seleção",
        "Status_Formatado": "Status",
    }

    # Exibir tabela com paginação
    st.dataframe(
        tabela_dados[colunas_exibicao],
        column_config=config_colunas,
        hide_index=True,
        use_container_width=True,
        height=500,
    )

    # Informações adicionais
    st.markdown(
        f"**📊 Exibindo {len(dados_filtrados)} de {len(historico_completo)} apostas totais**"
    )

    return dados_filtrados  # Retornar dados filtrados para uso na exportação


# === FUNÇÕES ORIGINAIS (mantidas) ===


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
                <li>📝 Adicione suas primeiras apostas</li>
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

    # Performance por liga (top 10)
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("### 🏆 Top 10 Ligas Mais Lucrativas")

        league_performance = (
            resolved_with_events.groupby("league_name")
            .agg({"Lucro_Prejuizo": "sum", "stake": "count"})
            .reset_index()
        )

        league_performance = league_performance.sort_values(
            "Lucro_Prejuizo", ascending=False
        ).head(10)

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
            <p>Não há apostas aguardando resultado no momento.</p>
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

    # Remover linhas com match_date nulo e converter para datetime
    pending_with_events = pending_with_events.dropna(subset=["match_date"])
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


def show_strategy_v1():
    """Aba para Estratégia V1 com estatísticas gerais e mensais"""
    st.markdown(
        '<h2 class="section-header">🎮 Estratégia V1</h2>', unsafe_allow_html=True
    )

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

    st.markdown(
        """
    <div class="content-card">
        <h3>🎯 Estratégia V1 - Mercados Selecionados</h3>
        <p>Esta estratégia foca nos mercados mais lucrativos identificados através de análise histórica detalhada.</p>
    </div>
    """,
        unsafe_allow_html=True,
    )

    # Exibir informações da estratégia
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("#### 🗺️ **Mapa 1 - Mercados Recomendados**")
        for market in estrategia_filtrada["map1"]["markets"]:
            st.write(f"• {market}")

        st.markdown("#### 🗺️ **Mapa 2 - Mercados Recomendados**")
        for market in estrategia_filtrada["map2"]["markets"]:
            st.write(f"• {market}")

    with col2:
        st.markdown("#### ❌ **Ligas a Evitar**")
        for league in estrategia_filtrada["avoid_leagues"]:
            st.write(f"• {league}")

        st.markdown("#### ⚙️ **Configurações**")
        st.write(f"• Odds mínimas: {estrategia_filtrada['min_odds']}")

    # Apostas em Aberto da Estratégia V1
    if not pending_bets_df.empty:
        st.markdown("### 🎯 Apostas em Aberto - Estratégia V1")

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

        # Remover linhas com match_date nulo e converter para datetime
        pending_with_events = pending_with_events.dropna(subset=["match_date"])
        pending_with_events["match_date"] = pd.to_datetime(
            pending_with_events["match_date"]
        )

        # Filtrar apostas que seguem a estratégia V1 (igual ao código original)
        strategy_pending = pending_with_events[
            (pending_with_events["house_odds"] >= estrategia_filtrada["min_odds"])
            & (
                ~pending_with_events["league_name"].isin(
                    estrategia_filtrada["avoid_leagues"]
                )
            )
        ]

        if not strategy_pending.empty:
            # Filtros de data
            st.markdown("#### 📅 Filtros de Período")

            col1, col2, col3, col4 = st.columns(4)

            with col1:
                if st.button("🗓️ Hoje", key="v1_today", use_container_width=True):
                    st.session_state.v1_filter = "today"
            with col2:
                if st.button("📅 Amanhã", key="v1_tomorrow", use_container_width=True):
                    st.session_state.v1_filter = "tomorrow"
            with col3:
                if st.button("📆 Esta Semana", key="v1_week", use_container_width=True):
                    st.session_state.v1_filter = "week"
            with col4:
                if st.button("🌐 Todos", key="v1_all", use_container_width=True):
                    st.session_state.v1_filter = "all"

            # Aplicar filtros
            if "v1_filter" not in st.session_state:
                st.session_state.v1_filter = "today"

            today = datetime.now().date()

            if st.session_state.v1_filter == "today":
                filtered_v1_bets = strategy_pending[
                    strategy_pending["match_date"].dt.date == today
                ]
            elif st.session_state.v1_filter == "tomorrow":
                tomorrow = today + timedelta(days=1)
                filtered_v1_bets = strategy_pending[
                    strategy_pending["match_date"].dt.date == tomorrow
                ]
            elif st.session_state.v1_filter == "week":
                week_end = today + timedelta(days=7)
                filtered_v1_bets = strategy_pending[
                    strategy_pending["match_date"].dt.date.between(today, week_end)
                ]
            else:
                filtered_v1_bets = strategy_pending

            if not filtered_v1_bets.empty:
                # Métricas das apostas V1 pendentes
                col1, col2, col3, col4 = st.columns(4)

                with col1:
                    st.metric("🎯 Apostas V1", len(filtered_v1_bets))

                with col2:
                    avg_odds = filtered_v1_bets["house_odds"].mean()
                    st.metric("🎲 Odds Média", f"{avg_odds:.2f}")

                with col3:
                    total_stake = filtered_v1_bets["stake"].sum()
                    st.metric("💰 Total Apostado", f"{total_stake:.0f} un")

                with col4:
                    total_potential = filtered_v1_bets["potential_win"].sum()
                    st.metric("🚀 Ganho Potencial", f"{total_potential:.2f} un")

                # Tabela de apostas V1 pendentes
                st.markdown("#### 📋 Lista de Apostas V1 Pendentes")

                # Preparar dados para exibição
                display_data = filtered_v1_bets.copy()
                display_data = display_data.sort_values("match_date")

                display_data["Data/Hora"] = display_data["match_date"].dt.strftime(
                    "%d/%m %H:%M"
                )
                display_data["Partida"] = (
                    display_data["home_team"] + " vs " + display_data["away_team"]
                )
                display_data["ROI"] = display_data["roi_average"].apply(
                    lambda x: f"{x:.1f}%"
                )
                display_data["Odds"] = display_data["house_odds"].apply(
                    lambda x: f"{x:.2f}"
                )
                display_data["Stake"] = display_data["stake"].apply(
                    lambda x: f"{x:.0f} un"
                )
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
            else:
                st.info("📊 Nenhuma aposta V1 encontrada para o período selecionado.")
        else:
            st.info("📊 Nenhuma aposta pendente segue os critérios da Estratégia V1.")
    else:
        st.info("📊 Não há apostas pendentes no momento.")


def show_current_month_results(resolved_bets_df, events_df):
    """Resultados do mês atual"""
    st.markdown(
        '<h2 class="section-header">📅 Resultados do Mês Atual</h2>',
        unsafe_allow_html=True,
    )

    if resolved_bets_df.empty:
        st.markdown(
            """
        <div class="content-card">
            <h3>📅 Sem Dados do Mês Atual</h3>
            <p>Ainda não há apostas resolvidas para este mês.</p>
        </div>
        """,
            unsafe_allow_html=True,
        )
        return

    # Merge com eventos
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

    # Filtrar apenas o mês atual
    current_month = datetime.now().month
    current_year = datetime.now().year

    current_month_data = resolved_with_events[
        (resolved_with_events["match_date"].dt.month == current_month)
        & (resolved_with_events["match_date"].dt.year == current_year)
    ]

    if current_month_data.empty:
        st.info("📊 Nenhuma aposta resolvida encontrada para este mês.")
        return

    # Métricas do mês
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("📊 Apostas do Mês", len(current_month_data))

    with col2:
        lucro_mes = current_month_data["Lucro_Prejuizo"].sum()
        st.metric("💰 Lucro do Mês", f"{lucro_mes:.2f} un")

    with col3:
        win_rate = (
            current_month_data["bet_status"].value_counts().get("win", 0)
            / len(current_month_data)
            * 100
        )
        st.metric("🎯 Win Rate", f"{win_rate:.1f}%")

    with col4:
        stake_total = current_month_data["stake"].sum()
        roi_mes = (lucro_mes / stake_total * 100) if stake_total > 0 else 0
        st.metric("📈 ROI do Mês", f"{roi_mes:.1f}%")

    # Gráfico de evolução diária
    st.markdown("### 📈 Evolução Diária do Mês")

    daily_data = (
        current_month_data.groupby(current_month_data["match_date"].dt.date)
        .agg({"Lucro_Prejuizo": "sum"})
        .reset_index()
    )
    daily_data["Lucro_Acumulado"] = daily_data["Lucro_Prejuizo"].cumsum()

    fig = go.Figure()

    fig.add_trace(
        go.Scatter(
            x=daily_data["match_date"],
            y=daily_data["Lucro_Acumulado"],
            mode="lines+markers",
            name="Lucro Acumulado",
            line=dict(color="#3b82f6", width=3),
            marker=dict(size=8, color="#1e40af"),
        )
    )

    fig.add_hline(y=0, line_dash="dash", line_color="gray", opacity=0.5)

    fig.update_layout(
        title="Evolução do Lucro Acumulado no Mês",
        xaxis_title="Data",
        yaxis_title="Lucro Acumulado (unidades)",
        height=400,
        template="plotly_white",
        font=dict(family="Inter, sans-serif"),
    )

    st.plotly_chart(fig, use_container_width=True)

    # Performance por liga no mês
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("### 🏆 Performance por Liga (Mês)")

        league_month = (
            current_month_data.groupby("league_name")
            .agg({"Lucro_Prejuizo": "sum", "stake": "count"})
            .reset_index()
        )
        league_month = league_month.sort_values("Lucro_Prejuizo", ascending=False)

        st.dataframe(
            league_month,
            column_config={
                "league_name": "Liga",
                "Lucro_Prejuizo": st.column_config.NumberColumn(
                    "Lucro", format="%.2f un"
                ),
                "stake": "Apostas",
            },
            hide_index=True,
            use_container_width=True,
        )

    with col2:
        st.markdown("### 📊 Distribuição de Mercados (Mês)")

        market_month = current_month_data["market_name"].value_counts().head(10)

        fig = px.bar(
            x=market_month.values,
            y=market_month.index,
            orientation="h",
            title="",
            color=market_month.values,
            color_continuous_scale="Blues",
        )

        fig.update_layout(
            height=300,
            template="plotly_white",
            font=dict(family="Inter, sans-serif"),
            showlegend=False,
        )

        st.plotly_chart(fig, use_container_width=True)


def show_general_results(resolved_bets_df, events_df):
    """Resultados gerais com análise mensal"""
    st.markdown(
        '<h2 class="section-header">📈 Resultados Gerais</h2>', unsafe_allow_html=True
    )

    if resolved_bets_df.empty:
        st.markdown(
            """
        <div class="content-card">
            <h3>📈 Sem Dados Gerais</h3>
            <p>Ainda não há apostas resolvidas para análise geral.</p>
        </div>
        """,
            unsafe_allow_html=True,
        )
        return

    # Merge com eventos
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

    # Análise mensal
    st.markdown("### 📅 Performance Mensal")

    resolved_with_events["Mes_Ano"] = resolved_with_events["match_date"].dt.to_period(
        "M"
    )

    monthly_performance = (
        resolved_with_events.groupby("Mes_Ano")
        .agg(
            {
                "Lucro_Prejuizo": "sum",
                "stake": ["sum", "count"],
                "bet_status": lambda x: (x == "win").mean() * 100,
            }
        )
        .reset_index()
    )

    monthly_performance.columns = [
        "Mes",
        "Lucro",
        "Stake_Total",
        "Apostas",
        "Win_Rate",
    ]
    monthly_performance["ROI"] = (
        monthly_performance["Lucro"] / monthly_performance["Stake_Total"] * 100
    )
    monthly_performance["Lucro_Acumulado"] = monthly_performance["Lucro"].cumsum()

    # Gráfico de performance mensal
    fig = go.Figure()

    # Barras de lucro mensal
    fig.add_trace(
        go.Bar(
            x=monthly_performance["Mes"].astype(str),
            y=monthly_performance["Lucro"],
            name="Lucro Mensal",
            marker_color=[
                "#10b981" if x >= 0 else "#ef4444" for x in monthly_performance["Lucro"]
            ],
            yaxis="y",
            opacity=0.7,
        )
    )

    # Linha de lucro acumulado
    fig.add_trace(
        go.Scatter(
            x=monthly_performance["Mes"].astype(str),
            y=monthly_performance["Lucro_Acumulado"],
            mode="lines+markers",
            name="Lucro Acumulado",
            line=dict(color="#3b82f6", width=3),
            marker=dict(size=8, color="#1e40af"),
            yaxis="y2",
        )
    )

    fig.update_layout(
        title={
            "text": "📈 Evolução da Performance Mensal",
            "x": 0.5,
            "font": {"size": 20, "color": "#1e293b"},
        },
        xaxis_title="Mês",
        yaxis=dict(title="Lucro Mensal (unidades)", side="left", color="#1e293b"),
        yaxis2=dict(
            title="Lucro Acumulado (unidades)",
            side="right",
            overlaying="y",
            color="#3b82f6",
        ),
        height=500,
        template="plotly_white",
        font=dict(family="Inter, sans-serif"),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        hovermode="x unified",
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
    )

    # Adicionar gradiente de fundo
    fig.update_layout(
        shapes=[
            dict(
                type="rect",
                xref="paper",
                yref="paper",
                x0=0,
                y0=0,
                x1=1,
                y1=1,
                fillcolor="rgba(59, 130, 246, 0.02)",
                layer="below",
                line_width=0,
            )
        ]
    )

    st.plotly_chart(fig, use_container_width=True)

    # Adicionar métricas de resumo mensal
    col1, col2, col3 = st.columns(3)

    with col1:
        melhor_mes = monthly_performance.loc[monthly_performance["Lucro"].idxmax()]
        st.metric(
            "🏆 Melhor Mês", str(melhor_mes["Mes"]), f"+{melhor_mes['Lucro']:.2f} un"
        )

    with col2:
        if (monthly_performance["Lucro"] < 0).any():
            pior_mes = monthly_performance.loc[monthly_performance["Lucro"].idxmin()]
            st.metric(
                "📉 Pior Mês", str(pior_mes["Mes"]), f"{pior_mes['Lucro']:.2f} un"
            )
        else:
            st.metric("📉 Pior Mês", "Nenhum", "Todos positivos! 🎉")

    with col3:
        lucro_medio = monthly_performance["Lucro"].mean()
        st.metric(
            "📊 Lucro Médio/Mês",
            f"{lucro_medio:.2f} un",
            f"ROI: {monthly_performance['ROI'].mean():.1f}%",
        )

    # Análise por mercado
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("### 🎯 Performance por Mercado")

        market_performance = (
            resolved_with_events.groupby("market_name")
            .agg(
                {
                    "Lucro_Prejuizo": "sum",
                    "stake": "count",
                    "bet_status": lambda x: (x == "win").mean() * 100,
                }
            )
            .reset_index()
        )

        market_performance.columns = ["Mercado", "Lucro", "Apostas", "Win_Rate"]
        market_performance = market_performance.sort_values(
            "Lucro", ascending=False
        ).head(10)

        st.dataframe(
            market_performance,
            column_config={
                "Lucro": st.column_config.NumberColumn("Lucro", format="%.2f un"),
                "Win_Rate": st.column_config.NumberColumn("Win Rate", format="%.1f%%"),
            },
            hide_index=True,
            use_container_width=True,
        )

    with col2:
        st.markdown("### 🏆 Performance por Liga")

        league_performance = (
            resolved_with_events.groupby("league_name")
            .agg(
                {
                    "Lucro_Prejuizo": "sum",
                    "stake": "count",
                    "bet_status": lambda x: (x == "win").mean() * 100,
                }
            )
            .reset_index()
        )

        league_performance.columns = ["Liga", "Lucro", "Apostas", "Win_Rate"]
        league_performance = league_performance.sort_values(
            "Lucro", ascending=False
        ).head(10)

        st.dataframe(
            league_performance,
            column_config={
                "Lucro": st.column_config.NumberColumn("Lucro", format="%.2f un"),
                "Win_Rate": st.column_config.NumberColumn("Win Rate", format="%.1f%%"),
            },
            hide_index=True,
            use_container_width=True,
        )


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
            <h3>📊 Dados Insuficientes</h3>
            <p>Ainda não há dados suficientes para estatísticas avançadas.</p>
        </div>
        """,
            unsafe_allow_html=True,
        )
        return

    # Merge com eventos
    resolved_with_events = pd.merge(
        resolved_bets_df,
        events_df[["event_id", "home_team", "away_team", "match_date", "league_name"]],
        on="event_id",
        how="left",
    )

    resolved_with_events["Lucro_Prejuizo"] = resolved_with_events.apply(
        calculate_profit_loss, axis=1
    )

    # 📊 Por Mercado
    st.markdown("### 📊 Por Mercado")

    market_stats = (
        resolved_with_events.groupby("market_name")
        .agg(
            {
                "Lucro_Prejuizo": ["sum", "count"],
                "bet_status": lambda x: (x == "win").mean() * 100,
                "house_odds": "mean",
                "stake": "sum",
            }
        )
        .reset_index()
    )

    market_stats.columns = [
        "Mercado",
        "Lucro",
        "Apostas",
        "Win_Rate",
        "Odds_Media",
        "Stake_Total",
    ]
    market_stats["ROI"] = market_stats["Lucro"] / market_stats["Stake_Total"] * 100
    market_stats = market_stats.sort_values("ROI", ascending=False)

    st.dataframe(
        market_stats,
        column_config={
            "Lucro": st.column_config.NumberColumn("Lucro", format="%.2f un"),
            "Win_Rate": st.column_config.NumberColumn("Win Rate", format="%.1f%%"),
            "ROI": st.column_config.NumberColumn("ROI", format="%.1f%%"),
            "Odds_Media": st.column_config.NumberColumn("Odds Média", format="%.2f"),
        },
        hide_index=True,
        use_container_width=True,
    )

    # 🎯 Por Seleção (Top 10)
    st.markdown("### 🎯 Por Seleção (Top 10)")

    selection_stats = (
        resolved_with_events.groupby("selection_line")
        .agg(
            {
                "Lucro_Prejuizo": ["sum", "count"],
                "bet_status": lambda x: (x == "win").mean() * 100,
                "stake": "sum",
            }
        )
        .reset_index()
    )

    selection_stats.columns = ["Seleção", "Lucro", "Apostas", "Win_Rate", "Stake_Total"]
    selection_stats["ROI"] = (
        selection_stats["Lucro"] / selection_stats["Stake_Total"] * 100
    )
    selection_stats = selection_stats.sort_values("ROI", ascending=False).head(10)

    st.dataframe(
        selection_stats,
        column_config={
            "Lucro": st.column_config.NumberColumn("Lucro", format="%.2f un"),
            "Win_Rate": st.column_config.NumberColumn("Win Rate", format="%.1f%%"),
            "ROI": st.column_config.NumberColumn("ROI", format="%.1f%%"),
        },
        hide_index=True,
        use_container_width=True,
    )

    # 🏆 Ligas Mais Lucrativas
    st.markdown("### 🏆 Ligas Mais Lucrativas")

    league_stats = (
        resolved_with_events.groupby("league_name")
        .agg(
            {
                "Lucro_Prejuizo": ["sum", "count"],
                "bet_status": lambda x: (x == "win").mean() * 100,
                "stake": "sum",
            }
        )
        .reset_index()
    )

    league_stats.columns = ["Liga", "Lucro", "Apostas", "Win_Rate", "Stake_Total"]
    league_stats["ROI"] = league_stats["Lucro"] / league_stats["Stake_Total"] * 100
    # Ordenar por LUCRO (não por ROI)
    league_stats = league_stats.sort_values("Lucro", ascending=False)

    # Filtrar apenas ligas lucrativas
    profitable_leagues = league_stats[league_stats["Lucro"] > 0]

    if not profitable_leagues.empty:
        st.dataframe(
            profitable_leagues,
            column_config={
                "Lucro": st.column_config.NumberColumn("Lucro", format="%.2f un"),
                "Win_Rate": st.column_config.NumberColumn("Win Rate", format="%.1f%%"),
                "ROI": st.column_config.NumberColumn("ROI", format="%.1f%%"),
            },
            hide_index=True,
            use_container_width=True,
        )

        # ⭐ Top 3 Times por Liga Lucrativa (das ligas com maior lucro)
        st.markdown("### ⭐ Top 3 Times por Liga Mais Lucrativa")

        # Pegar as 3 ligas com maior lucro
        top_profitable_leagues = profitable_leagues.head(3)

        for _, league_row in top_profitable_leagues.iterrows():
            league_name = league_row["Liga"]

            st.markdown(f"#### {league_name} (Lucro: {league_row['Lucro']:.2f} un)")

            # Filtrar apostas da liga
            league_bets = resolved_with_events[
                resolved_with_events["league_name"] == league_name
            ]

            # Analisar performance por time (tanto home quanto away)
            home_stats = (
                league_bets.groupby("home_team")
                .agg({"Lucro_Prejuizo": "sum", "stake": "count"})
                .reset_index()
            )
            home_stats.columns = ["Time", "Lucro", "Apostas"]

            away_stats = (
                league_bets.groupby("away_team")
                .agg({"Lucro_Prejuizo": "sum", "stake": "count"})
                .reset_index()
            )
            away_stats.columns = ["Time", "Lucro", "Apostas"]

            # Combinar estatísticas
            team_stats = (
                pd.concat([home_stats, away_stats]).groupby("Time").sum().reset_index()
            )
            team_stats = team_stats.sort_values("Lucro", ascending=False).head(3)

            if not team_stats.empty:
                col1, col2, col3 = st.columns(3)

                for i, (_, team_row) in enumerate(team_stats.iterrows()):
                    with [col1, col2, col3][i]:
                        st.metric(
                            f"🥇 {team_row['Time']}"
                            if i == 0
                            else f"🥈 {team_row['Time']}"
                            if i == 1
                            else f"🥉 {team_row['Time']}",
                            f"{team_row['Lucro']:.2f} un",
                            f"{team_row['Apostas']} apostas",
                        )
    else:
        st.info("📊 Nenhuma liga lucrativa encontrada nos dados atuais.")

    # 📈 Performance por Faixa de Odds
    st.markdown("### 📈 Performance por Faixa de Odds")

    # Criar faixas de odds
    resolved_with_events["Faixa_Odds"] = pd.cut(
        resolved_with_events["house_odds"],
        bins=[0, 1.5, 2.0, 2.5, 3.0, float("inf")],
        labels=["1.00-1.50", "1.51-2.00", "2.01-2.50", "2.51-3.00", "3.00+"],
    )

    odds_stats = (
        resolved_with_events.groupby("Faixa_Odds")
        .agg(
            {
                "Lucro_Prejuizo": ["sum", "count"],
                "bet_status": lambda x: (x == "win").mean() * 100,
                "stake": "sum",
            }
        )
        .reset_index()
    )

    odds_stats.columns = [
        "Faixa de Odds",
        "Lucro",
        "Apostas",
        "Win_Rate",
        "Stake_Total",
    ]
    odds_stats["ROI"] = odds_stats["Lucro"] / odds_stats["Stake_Total"] * 100

    st.dataframe(
        odds_stats,
        column_config={
            "Lucro": st.column_config.NumberColumn("Lucro", format="%.2f un"),
            "Win_Rate": st.column_config.NumberColumn("Win Rate", format="%.1f%%"),
            "ROI": st.column_config.NumberColumn("ROI", format="%.1f%%"),
        },
        hide_index=True,
        use_container_width=True,
    )


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

            st.metric(
                "📈 ROI Geral",
                f"{roi_geral:.1f}%",
                delta=f"{total_profit:.2f} unidades",
                help="Retorno sobre investimento geral",
            )
        else:
            st.metric("📈 ROI Geral", "0.0%", help="Retorno sobre investimento geral")

    st.markdown("</div>", unsafe_allow_html=True)

    # Obter nome do mês atual em português
    current_month = datetime.now().month
    current_year = datetime.now().year
    month_name = calendar.month_name[current_month]

    # Traduzir para português
    month_names_pt = {
        "January": "Janeiro",
        "February": "Fevereiro",
        "March": "Março",
        "April": "Abril",
        "May": "Maio",
        "June": "Junho",
        "July": "Julho",
        "August": "Agosto",
        "September": "Setembro",
        "October": "Outubro",
        "November": "Novembro",
        "December": "Dezembro",
    }
    month_name_pt = month_names_pt.get(month_name, month_name)

    # Abas principais com design moderno - INCLUINDO A NOVA ABA HISTÓRICO
    tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs(
        [
            "🏠 Dashboard",
            "🎯 Apostas em Aberto",
            "🎮 Estratégia V1",
            f"📅 Resultados de {month_name_pt}",
            "📈 Resultados Gerais",
            "📋 Estatísticas Avançadas",
            "📜 Histórico",  # NOVA ABA
        ]
    )

    with tab1:
        show_modern_dashboard(resolved_bets_df, pending_bets_df, events_df)

    with tab2:
        show_pending_bets_modern(pending_bets_df, events_df)

    with tab3:
        show_strategy_v1()

    with tab4:
        show_current_month_results(resolved_bets_df, events_df)

    with tab5:
        show_general_results(resolved_bets_df, events_df)

    with tab6:
        show_advanced_statistics(resolved_bets_df, events_df)

    with tab7:  # NOVA ABA HISTÓRICO
        show_historico_completo(resolved_bets_df, pending_bets_df, events_df)

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


if __name__ == "__main__":
    main()
