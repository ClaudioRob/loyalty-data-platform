import streamlit as st
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

st.set_page_config(page_title="Data Platform - Cláudio", layout="wide", page_icon="💳")

st.title("💳 Painel de Fidelidade & Detecção de Fraude")
st.markdown("---")

# --- FUNÇÕES DE CARREGAMENTO ---

@st.cache_data
def get_gold_data():
    path = "/opt/airflow/loyalty-data-platform/data/gold/kpi_category_finance.parquet"
    if os.path.exists(path):
        return pd.read_parquet(path)
    return pd.DataFrame()

@st.cache_data
def get_fraud_alerts():
    path = "/opt/airflow/loyalty-data-platform/data/gold/fraud_alerts.parquet"
    if os.path.exists(path):
        return pd.read_parquet(path)
    return pd.DataFrame()

# --- LÓGICA DO DASHBOARD ---

try:
    df = get_gold_data()
    df_fraud = get_fraud_alerts()

    # --- SEÇÃO 1: ALERTAS DE FRAUDE (Destaque no topo se houver algo) ---
    if not df_fraud.empty:
        with st.expander("⚠️ ALERTAS DE FRAUDE DETECTADOS", expanded=True):
            st.error(f"Foram identificadas {len(df_fraud)} transações com suspeita de deslocamento impossível.")
            st.dataframe(df_fraud, use_container_width=True)
        st.markdown("---")

    # --- SEÇÃO 2: KPIs FINANCEIROS ---
    if not df.empty:
        st.sidebar.header("Filtros")
        categoria = st.sidebar.multiselect("Categoria", options=df['category'].unique(), default=df['category'].unique())
        
        df_filtered = df[df['category'].isin(categoria)]

        # Métricas de Topo
        c1, c2, c3 = st.columns(3)
        total_rev = df_filtered['total_revenue'].sum()
        vol_trans = df_filtered['transaction_volume'].sum()
        ticket_medio = total_rev / vol_trans if vol_trans > 0 else 0

        c1.metric("Faturamento Total", f"€ {total_rev:,.2f}")
        c2.metric("Volume de Transações", f"{vol_trans:,}")
        c3.metric("Ticket Médio", f"€ {ticket_medio:,.2f}")

        # Gráficos
        st.subheader("📊 Faturamento por Categoria")
        st.bar_chart(df_filtered, x="category", y="total_revenue")

        st.subheader("📋 Tabela de KPIs")
        st.dataframe(df_filtered, use_container_width=True)
    else:
        st.info("Nenhum dado financeiro encontrado na camada Gold.")

except Exception as e:
    st.error(f"Erro ao carregar dados: {e}")
    st.info("Dica: Verifique os caminhos no volume do Docker.")