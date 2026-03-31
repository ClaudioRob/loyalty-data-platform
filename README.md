# Plataforma de Fidelidade & Detecção de Fraude (Data Engineering)

## 📋 Escopo do Projeto
Este projeto simula um ecossistema financeiro onde transações diárias de seguros, previdência e serviços bancários são processadas para alimentar um motor de recompensas. O objetivo principal é demonstrar a construção de pipelines robustos que garantam a integridade dos pontos acumulados e a segurança do ecossistema através de análises de fraude.

### Objetivos Principais:
* **Ingestão Batch:** Processamento diário de grandes volumes de arquivos transacionais.
* **Detecção de Fraude:** Implementação de camadas analíticas para identificar comportamentos anômalos (ex: transações duplicadas, picos de gastos ou inconsistência geográfica).
* **Transformação de Fatos:** Enriquecimento de dados brutos (Bronze) para tabelas analíticas (Silver/Gold) utilizando lógica de negócio complexa.
* **Arquitetura Moderna:** Provisionamento e orquestração utilizando práticas de IaC (Infrastructure as Code) e GitOps.

## 🏢 Contexto de Negócio
O sistema atua em três frentes principais:
1.  **Seguros e Previdência:** Ingestão de aportes e pagamentos de prêmios.
2.  **Banking & Fidelidade:** Conversão de transações de cartão e conta corrente em pontos de recompensa.
3.  **Segurança Operacional:** Filtro de validação para garantir que pontos não sejam atribuídos a transações fraudulentas.

## 💾 Arquitetura de Dados (Data Sources)

O projeto consome dados sintéticos gerados para simular um ambiente de produção real, utilizando dois formatos principais para demonstrar a versatilidade do processamento com Spark:

1. **Banking Transactions (CSV):** Dados transacionais brutos contendo informações de valores, categorias e geolocalização.
2. **Customer Profiles (JSON):** Dados de perfil de cliente e segmentação de produtos (Seguros/Previdência), cruciais para a regra de negócio de fidelidade e segurança.

### Contrato de Dados (Principais Atributos):
* **Identificadores:** `tx_id`, `cust_id`.
* **Negócio:** `tx_amount`, `tx_category`, `loyalty_tier`.
* **Segurança/Fraude:** `lat_long`, `tx_datetime`, `home_city`.