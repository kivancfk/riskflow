# RiskFlow — Airflow image (v6)
#
# Changelog:
#   v6: added git to apt-get install (dbt requires git for `dbt deps`
#       and reports "git [ERROR]" in `dbt debug` without it).
#   v5: pinned pyspark to 3.5.6 to match apache/spark:3.5.6 cluster.
#   v4: added pip retries/timeout for PyPI flakiness.
#   v3: removed explicit pyspark pin (constraint conflict).
#   v2: switched to Airflow constraints file.
#   v1: original.

FROM apache/airflow:2.10.4-python3.11

USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        curl \
        procps \
        git && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Pip resilience: longer timeout, more retries.
ENV PIP_DEFAULT_TIMEOUT=300 \
    PIP_RETRIES=10 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

USER airflow

ARG AIRFLOW_VERSION=2.10.4
ARG PYTHON_VERSION=3.11
ARG CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# Layer 1: Airflow providers + Kafka client.
RUN pip install --no-cache-dir \
    --constraint "${CONSTRAINT_URL}" \
    "apache-airflow-providers-apache-spark" \
    "apache-airflow-providers-postgres" \
    "kafka-python==2.0.2"

# Layer 2: pin pyspark to match cluster (apache/spark:3.5.6).
# Installed WITHOUT the constraint file so we can override the
# Airflow constraint's pyspark pin (3.5.3 → 3.5.6).
RUN pip install --no-cache-dir --upgrade "pyspark==3.5.6"

# Layer 3: dbt-postgres in isolation.
RUN pip install --no-cache-dir "dbt-postgres>=1.8,<1.9"

# Layer 4: Great Expectations — exact pin for Phase 2 filesystem workflow.
RUN pip install --no-cache-dir "great-expectations==0.18.21"

# Layer 5: dev/test tools.
RUN pip install --no-cache-dir "pytest>=8.0" "pytest-cov>=5.0"
