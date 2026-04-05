===============================================================================
ESTRATEGIA DE CONEXAO E INFRAESTRUTURA - LOYALTY DATA PLATFORM
===============================================================================

Este documento detalha a arquitetura de conectividade e os protocolos de 
comunicacao implementados para integrar o Apache Airflow (Docker) ao 
Azure Data Lake Storage Gen2 (ADLS Gen2).

-------------------------------------------------------------------------------
1. ARQUITETURA DE CONEXAO (CONNECTIVITY)
-------------------------------------------------------------------------------

A conexao foi desenhada para ser independente de ambiente, utilizando o driver 
ABFSS (Azure Blob File System Driver) sobre a pilha do Hadoop.

* PROTOCOLO DE COMUNICACAO (P): Uso do prefixo 'abfss://' para garantir 
  transacoes atomicas e performance otimizada em namespaces hierarquicos.
* GERENCIAMENTO DE DEPENDENCIAS (D): Implementacao de injecao dinamica via 
  Maven Packages no Spark, eliminando a fragilidade de arquivos JAR locais.

-------------------------------------------------------------------------------
2. MATRIZ DE CONFIGURACAO E CONECTIVIDADE
-------------------------------------------------------------------------------

Configuracoes aplicadas dinamicamente na inicializacao da SparkSession:

ITEM             | PARAMETRO DE CONFIGURACAO                             | TIPO
-------------------------------------------------------------------------------
Auth             | fs.azure.account.key.<account>.dfs.core.windows.net   | (P)
Driver Delta     | io.delta:delta-core_2.12:2.4.0                       | (D)
Driver Azure     | org.apache.hadoop:hadoop-azure:3.3.4                 | (D)
FileSystem       | fs.abfss.impl                                         | (P)

(P) = Protocolo / (D) = Dependencia

-------------------------------------------------------------------------------
3. SEGURANCA E GOVERNANCA NA CONEXAO
-------------------------------------------------------------------------------

Camadas estabelecidas para proteger o trafego entre DELLWORK e Azure:

A. ISOLAMENTO DE CREDENCIAIS (ENVIRONMENT ISOLATION)
   - Uso de arquivos .env protegidos e mapeados via volume no Docker.
   - Zero-Hardcoding: O codigo detecta a conta de storage dinamicamente.

B. RESILIENCIA DE SESSAO (SESSION ROBUSTNESS)
   - Dynamic Package Resolution: O Spark resolve dependencias em runtime, 
     evitando o erro 'ClassNotFoundException' em novos containers.
   - Log Level Control: Spark configurado em modo 'ERROR' para logs limpos.

-------------------------------------------------------------------------------
4. FLUXO DE EXECUCAO (HANDSHAKE)
-------------------------------------------------------------------------------

1. TRIGGER: O Airflow dispara a Task de Python no container.
2. LOAD: O script carrega as chaves do arquivo .env para a memoria.
3. RESOLVE: O Spark solicita os pacotes Delta e Hadoop ao repositorio Maven.
4. MOUNT: O Spark monta virtualmente o sistema de arquivos ABFSS.
5. EXECUTE: Escrita e leitura direta no Lake com persistencia garantida.

-------------------------------------------------------------------------------
NOTAS ADICIONAIS:
Ambiente validado em infraestrutura Docker (DELLWORK) com Spark 3.4+.
Data de atualizacao: 05/04/2026
Responsavel: Eng. de Dados (Claudio)
===============================================================================