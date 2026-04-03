# Plataforma de Fidelidade & Detecção de Fraude 💳🛡️

## 📋 Escopo do Projeto
Este projeto simula um ecossistema financeiro onde transações diárias de seguros, previdência e serviços bancários são processadas para alimentar um motor de recompensas. O objetivo principal é demonstrar a construção de pipelines robustos que garantam a integridade dos dados e a segurança do ecossistema através de análises de fraude e arquitetura de dados moderna.

### Objetivos Principais:
* **Ingestão Batch:** Processamento diário de grandes volumes de arquivos transacionais via PySpark.
* **Detecção de Fraude:** Implementação de camadas analíticas para identificar comportamentos anômalos (ex: deslocamento impossível ou inconsistência geográfica).
* **Arquitetura Medallion:** Enriquecimento de dados brutos (**Bronze**) para tabelas analíticas (**Silver**) e agregadores de negócio (**Gold**) utilizando o formato **Parquet**.
* **Infraestrutura como Código (IaC):** Provisionamento e automação utilizando **Terraform** no ecossistema Azure.

## 🏢 Contexto de Negócio
O sistema atua em três frentes principais:
1. **Seguros e Previdência:** Ingestão de aportes e pagamentos de prêmios.
2. **Banking & Fidelidade:** Conversão de transações de cartão e conta corrente em pontos de recompensa.
3. **Segurança Operacional:** Filtro de validação para garantir que pontos não sejam atribuídos a transações fraudulentas.

## 💾 Arquitetura de Dados (Data Sources)
O projeto consome dados sintéticos gerados para simular um ambiente de produção real:

1. **Banking Transactions (CSV):** Dados transacionais brutos contendo `tx_id`, `tx_amount`, `tx_datetime`, `tx_category`, `location_city`, `ip_address` e `device_id`.
2. **Customer Profiles (JSON):** Dados de perfil de cliente e segmentação de produtos (Seguros/Previdência), cruciais para as regras de negócio de fidelidade.

### 🛡️ Camada de Segurança e Fraude
O motor de processamento (Spark) aplica regras baseadas no canal de origem:
* **Fraude Geográfica (Física):** Validação de "deslocamento impossível" entre estabelecimentos físicos em cidades distintas em curtos intervalos de tempo.
* **Fraude de Dispositivo (Digital):** Identificação de acessos suspeitos via IP ou `device_id` não mapeado no perfil do cliente (*Account Takeover*).

## 🏗️ Infraestrutura e Camadas (Data Lake ADLS Gen2)
A infraestrutura é organizada em camadas de maturidade no Azure Data Lake Storage:

* **🟤 Bronze (Raw):** Armazenamento dos arquivos brutos (CSV/JSON) exatamente como chegam da origem.
* **⚪ Silver (Refined):** Dados limpos, com tipos convertidos (`Double`, `Timestamp`), saneamento de nulos e padronização de moedas. Armazenado em **Parquet**.
* **🟡 Gold (Business):** Tabelas agregadas e otimizadas para consumo. Exemplo: KPI de faturamento e volume de transações por categoria (`kpi_category_finance`).

![alt text](images/image-1.png)

## 📁 Status do Pipeline de Dados
- [x] **Infraestrutura:** Provisionada via Terraform (Azure ADLS Gen2).
- [x] **Ambiente Local:** Dockerizado com Java/Spark integrado ao Airflow.
- [x] **Ingestão (Bronze):** Dados brutos de transações e perfis ingeridos com sucesso.
- [x] **Transformação (Silver):** Pipeline PySpark concluído (Saneamento e Tipagem ANSI).
- [x] **Modelagem (Gold):** Geração de KPIs financeiros consolidada em Parquet.
- [x] **Orquestração:** DAG `pipeline_loyalty_medallion` operacional via Airflow Centralizado.
- [ ] **Visualização:** Dashboard minimalista em Streamlit ou Power BI (Pendente).

## 🚀 Como Executar
Para processar as camadas do Data Lake localmente apontando para o Azure:

```bash
# Refino: Bronze -> Silver
./.venv/bin/python scripts/transform_bronze_to_silver.py
```
![alt text](images/image-3.png)

```
# Agregação: Silver -> Gold
./.venv/bin/python scripts/transform_silver_to_gold.py
```
![alt text](images/image-2.png)

### ⚙️ Orquestração e Ambiente Local (Centralized Airflow)
Para suportar o ecossistema Spark e garantir a reprodutibilidade, o projeto utiliza uma infraestrutura de containers customizada no WSL2:
* **Dockerfile Customizado:** Imagem base `apache/airflow:2.10.1` estendida com **JRE 17** e utilitários de sistema (`procps`) necessários para o runtime do Spark.
* **PySpark & Conectividade:** Instalação via `requirements.txt` incluindo `pyspark==3.5.0`, `python-dotenv` e `azure-storage-blob`.
* **Gerenciamento de Segredos:** Integração com arquivos `.env` e controle de permissões de sistema de arquivos Linux (`chmod 644`).


