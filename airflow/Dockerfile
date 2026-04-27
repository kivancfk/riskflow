# RiskFlow — Airflow image (v4)
#
# Changelog:
#   v4: added --retries=10 --timeout=300 to all pip installs.
#       PyPI timed out mid-download on the 317MB pyspark tarball.
#       Default pip timeout is 15s; we now wait 5 min per chunk and retry 10x.
#   v3: removed explicit pyspark pin (constraint conflict).
#   v2: switched to Airflow constraints file.
#   v1: original, suffered backtracking hell.

FROM apache/airflow:2.10.4-python3.11

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        curl \
        procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Pip resilience: longer timeout, more retries.
# Set as env vars so all four pip installs inherit them automatically.
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

# Layer 2: dbt-postgres in isolation.
RUN pip install --no-cache-dir "dbt-postgres>=1.8,<1.9"

# Layer 3: Great Expectations — loose pin, isolated layer.
RUN pip install --no-cache-dir "great-expectations>=0.18,<0.19"

# Layer 4: dev/test tools.
RUN pip install --no-cache-dir "pytest>=8.0" "pytest-cov>=5.0"
