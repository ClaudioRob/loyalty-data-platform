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

## 🧪 Geração de Dados (Data Mocking)
Para fins de desenvolvimento e testes de stress, utilizamos um gerador de dados sintéticos (`scripts/data_generator.py`). 
* **Ferramenta:** Python com biblioteca `Faker`.
* **Cenários Simulados:** O gerador é configurado para criar padrões de transações que permitem validar:
    * Regras de negócio de fidelidade (pontuação por categoria).
    * Regras de segurança (transações em cidades distantes em intervalos curtos).

### 🛡️ Camada de Segurança e Fraude
O motor de processamento (Spark) aplicará regras distintas baseadas no canal de origem:

1. **Fraude Geográfica (Física):** Validação de deslocamento impossível entre estabelecimentos físicos.
2. **Fraude de Dispositivo (Digital):** Identificação de acessos suspeitos via IP ou Device ID não mapeado no perfil do cliente, comum em ataques de 'Account Takeover' em seguros e previdência.

## 🧠 Regras de Processamento (Business Logic)
O pipeline de dados foi desenhado para processar os arquivos diários seguindo as seguintes premissas:

1. **Enriquecimento:** Unificar os dados transacionais (CSV) com o perfil de produtos e fidelidade (JSON).
2. **Validação de Produtos:** Cruzar a categoria da transação com os produtos ativos do cliente (Seguros/Previdência).
3. **Motor de Fraude:**
    * **Físico:** Identificar transações em cidades diferentes em janelas de tempo impossíveis.
    * **Digital:** Identificar o uso de `device_id` compartilhados entre múltiplos perfis em curto intervalo.

    