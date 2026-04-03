# 1. Base Image (Alinhada com a versão do seu compose)
FROM apache/airflow:2.10.1

USER root

# 2. Atualização e Instalação de dependências de Sistema Operacional
# openjdk-17-jdk: Necessário para o Spark rodar no LocalExecutor
# procps: Utilitário de sistema para gestão de processos exigido pelo Airflow
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       openjdk-17-jdk \
       procps \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# 3. Configuração das Variáveis de Ambiente para o Spark
# Define o caminho do Java para que o PySpark o localize automaticamente
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
RUN export JAVA_HOME

# 4. Retorno para o usuário padrão do Airflow
# Importante para manter a segurança e compatibilidade com o AIRFLOW_UID do seu compose
USER airflow

# Nota: As bibliotecas Python (pyspark, python-dotenv, azure-storage-blob) 
# serão instaladas via a variável _PIP_ADDITIONAL_REQUIREMENTS que você já 
# definiu no seu docker-compose.yaml.