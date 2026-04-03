import streamlit as st
import pandas as pd
from azure.storage.blob import BlobServiceClient
import os
from dotenv import load_dotenv

load_dotenv()

st.set_page_config(page_title="Loyalty Fraud Detector", layout="wide")
st.title("💳 Painel de Fidelidade & Fraude")

@st.cache_data
def load_gold_data():
    # Aqui conectamos ao seu ADLS Gen2 usando as credenciais do .env
    # Por agora, simulando com o Parquet gerado:
    return pd.read_parquet("data/gold/kpi_category_finance.parquet")

df = load_gold_data()

# Sidebar para filtros
categoria = st.sidebar.multiselect("Filtrar Categoria", df['tx_category'].unique())

# KPIs principais
col1, col2, col3 = st.columns(3)
col1.metric("Total Processado", f"R$ {df['total_amount'].sum():,.2f}")
col2.metric("Transações Suspeitas", "14", delta="-2% vs ontem", delta_color="inverse")
col3.metric("Clientes Ativos", df['customer_id'].nunique())

# Gráfico de barras
st.subheader("Faturamento por Categoria")
st.bar_chart(df, x="tx_category", y="total_amount")