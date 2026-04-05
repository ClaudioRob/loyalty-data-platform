# Data Quality Strategy - Loyalty Data Platform

Este documento detalha as regras de qualidade de dados (Data Quality) implementadas no projeto **loyalty-data-platform**, utilizando o framework **Great Expectations (GX)** integrado ao processamento Spark e orquestração via Airflow.

## 1. Categorização das Regras

As validações são divididas em duas categorias principais para facilitar o diagnóstico de falhas:

* **Integridade Física (F):** Validações técnicas de estrutura, tipos de dados e restrições de banco de dados (Primary Keys, Foreign Keys, Nulls).
* **Regras Funcionais/Negócio (N):** Validações baseadas na lógica do programa de fidelidade e conformidade com os processos da empresa.

---

## 2. Matriz de Qualidade (Camada Bronze -> Silver)

O objetivo desta etapa é garantir que apenas dados íntegros sejam promovidos para a camada Silver.

| ID | Campo | Categoria | Regra (Expectation) | Ação em caso de Falha |
| :--- | :--- | :--- | :--- | :--- |
| **DQ-F01** | `transaction_id` | Física | `expect_column_values_to_not_be_null` | **Quarentena/Stop** |
| **DQ-F02** | `transaction_id` | Física | `expect_column_values_to_be_unique` | **Quarentena/Stop** |
| **DQ-F03** | `customer_id` | Física | `expect_column_values_to_not_be_null` | **Quarentena/Stop** |
| **DQ-N01** | `points_earned` | Negócio | `expect_column_values_to_be_between(min=0)` | **Warning/Log** |
| **DQ-N02** | `status` | Negócio | `expect_column_values_to_be_in_set(['processed', 'pending', 'cancelled'])` | **Quarentena/Stop** |
| **DQ-N03** | `transaction_date`| Negócio | `expect_column_values_to_be_date_between(min='2020-01-01', max='now')` | **Quarentena/Stop** |

---

## 3. Implementação Técnica

### Localização dos Componentes

* **Documentação de Regras:** `docs/DATA_QUALITY_RULES.md`
* **Definições JSON (Expectations):** `include/gx/expectations/bronze_loyalty_suite.json`
* **Script de Execução (Spark):** `scripts/spark/quality_check_bronze.py`
* **Relatórios (Data Docs):** Armazenados em `dbfs:/mnt/reports/gx/data_docs/`

### Fluxo de Execução no Pipeline

1.  **Ingestão:** O Airflow inicia a ingestão dos dados para a tabela Delta na camada **Bronze**.
2.  **Validação:** O script de qualidade é acionado, lendo o DataFrame da Bronze e aplicando a `bronze_loyalty_suite.json`.
3.  **Decisão (Gatekeeper):**
    * **Sucesso:** A DAG prossegue para a transformação da camada **Silver**.
    * **Falha Crítica:** A DAG é interrompida. O engenheiro recebe um alerta e os dados com erro ficam retidos na Bronze para análise.
4.  **Reporting:** Um relatório HTML é atualizado automaticamente para consulta visual.

---

## 4. Interpretação de Resultados e Data Docs

O pipeline utiliza o Great Expectations para gerar relatórios visuais. Após a execução do script `quality_check_bronze.py`, um portal HTML é atualizado.

### Como acessar o relatório:
1. Navegue até `gx/uncommitted/data_docs/local_site/`
2. Abra o arquivo `index.html` no seu navegador.

### Critérios de Interrupção:
* **Sucesso (Exit 0):** Todos os testes DQ-F (Físicos) e DQ-N (Negócio) passaram. O Airflow prossegue para a Camada Silver.
* **Falha (Exit 1):** Qualquer regra marcada como "Quarentena/Stop" falhou. O pipeline é interrompido para evitar a poluição da Silver com dados inconsistentes.