# =============================================================================
# Image Airflow du projet = image officielle Apache Airflow
#   + Java (indispensable à Spark, qui tourne dans la machine virtuelle Java)
#   + PySpark et les dépendances du pipeline
#   + dbt, installé dans un environnement Python SÉPARÉ (bonne pratique : les
#     dépendances de dbt et d'Airflow entrent souvent en conflit)
# =============================================================================
FROM apache/airflow:3.3.2-python3.12

USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends default-jre-headless procps \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/default-java

USER airflow

# Dépendances du pipeline (on fige la version d'Airflow pour que pip ne la modifie pas).
COPY requirements-airflow.txt /tmp/requirements-airflow.txt
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /tmp/requirements-airflow.txt

# dbt dans son propre environnement virtuel.
RUN python -m venv /opt/airflow/dbt-venv \
 && /opt/airflow/dbt-venv/bin/pip install --no-cache-dir "dbt-postgres~=1.11.0"
