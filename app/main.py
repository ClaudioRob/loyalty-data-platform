import streamlit as st
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

st.set_page_config(page_title="Data Platform - Cláudio", layout="wide", page_icon="💳")

st.title("💳 Painel de Fidelidade & Detecção de Fraude")
st.markdown("---")

@st.cache_data
def get_gold_data():
    # USAR O CAMINHO ABSOLUTO DO CONTAINER
    # O Docker mapeou sua pasta 'data/gold' para este caminho abaixo:
    path = "/opt/airflow/loyalty-data-platform/data/gold/kpi_category_finance.parquet"
    
    if not os.path.exists(path):
        raise FileNotFoundError(f"Arquivo não encontrado em: {path}. Verifique se a DAG rodou com sucesso.")
        
    return pd.read_parquet(path)

try:
    df = get_gold_data()

    # Filtros na barra lateral - ATENÇÃO AOS NOMES DAS COLUNAS (category)
    st.sidebar.header("Filtros")
    # O seu Spark gerou a coluna 'category', não 'tx_category'
    categoria = st.sidebar.multiselect("Categoria", options=df['category'].unique(), default=df['category'].unique())
    
    df_filtered = df[df['category'].isin(categoria)]

    # Métricas de Topo - ATENÇÃO AOS NOMES DAS COLUNAS (total_revenue)
    c1, c2, c3 = st.columns(3)
    # O seu Spark gerou 'total_revenue', não 'total_amount'
    total_rev = df_filtered['total_revenue'].sum()
    vol_trans = df_filtered['transaction_volume'].sum()
    ticket_medio = total_rev / vol_trans if vol_trans > 0 else 0

    c1.metric("Faturamento Total", f"€ {total_rev:,.2f}")
    c2.metric("Volume de Transações", f"{vol_trans:,}")
    c3.metric("Ticket Médio", f"€ {ticket_medio:,.2f}")

    # Gráfico de Faturamento
    st.subheader("📊 Faturamento por Categoria")
    st.bar_chart(df_filtered, x="category", y="total_revenue")

    # Mostrar Tabela de Dados
    st.subheader("📋 Tabela de KPIs")
    st.dataframe(df_filtered, use_container_width=True)

except Exception as e:
    st.error(f"Erro ao carregar dados da camada Gold: {e}")
    st.info("Dica: Verifique se o script transform_silver_to_gold.py está salvando no caminho local '/opt/airflow/loyalty-data-platform/data/gold/'.")