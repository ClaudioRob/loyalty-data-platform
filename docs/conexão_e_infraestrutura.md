🌐 Conectividade e Infraestrutura: Airflow & Azure Data Lake
Este documento detalha a arquitetura de conectividade e os protocolos de comunicação implementados para integrar o Apache Airflow (Docker) ao Azure Data Lake Storage Gen2 (ADLS Gen2), garantindo um pipeline resiliente e seguro.

🏗️ Arquitetura do Processo
A conexão foi desenhada para ser independente de ambiente, utilizando o driver ABFSS (Azure Blob File System Driver) sobre a pilha do Hadoop. O motor de processamento utiliza injeção dinâmica de dependências para garantir portabilidade total entre o ambiente local (DELLWORK) e a nuvem.

🔑 Componentes de Conectividade
Protocolo ABFSS: Uso de abfss:// para garantir transações atômicas e performance otimizada em namespaces hierárquicos da Azure, superando as limitações do protocolo Blob tradicional.

Dynamic Package Resolution: O Spark foi configurado para resolver as bibliotecas delta-core e hadoop-azure em tempo de execução via Maven, eliminando erros de classe não encontrada (ClassNotFoundException).

Secret Management: Integração com python-dotenv para carregamento de chaves de acesso via variáveis de ambiente, impedindo a exposição de credenciais sensíveis no código-fonte ou em logs.

🛡️ Segurança e Governança
Para garantir a integridade dos dados e a conformidade com padrões internacionais de engenharia, foram estabelecidas três camadas de proteção:

Isolamento de Ambiente: Mapeamento de volumes no Docker para proteger o arquivo .env, garantindo que as chaves da conta de storage nunca saiam do perímetro seguro do container.

Audit Log (Delta Lake): Ativação do log de transações para permitir rastreabilidade total (Time Travel) e auditoria de cada alteração realizada no Data Lake (quem, quando e o quê).

Resiliência de Sessão: Configuração de logs em modo ERROR e políticas de retry agressivas no Airflow para mitigar falhas intermitentes de rede (Transient Failures).

📈 Otimização de Performance: Particionamento
O pipeline foi evoluído para suportar Particionamento Dinâmico na camada Gold, otimizando o consumo de dados para camadas de BI e Analytics.

📐 Estratégias Aplicadas
Partition Pruning: Partição física por dt_particao (derivada do timestamp), permitindo que o Spark ignore arquivos irrelevantes e reduza drasticamente o I/O de leitura.

Schema Enforcement: Bloqueio de gravações que não respeitem o contrato de dados estabelecido, protegendo a camada Gold de corrupção por mudanças inesperadas na origem.

Escalabilidade: Estrutura preparada para suportar crescimento volumétrico sem degradação de performance, mantendo as queries SQL eficientes mesmo com milhões de registros.