# RiskFlow — Airflow image
#
# Built on the official Airflow image, adds:
#   - Java 17 (required for spark-submit client)
#   - apache-airflow-providers-apache-spark (SparkSubmitOperator)
#   - apache-airflow-providers-postgres
#   - dbt-postgres
#   - great-expectations
#   - pyspark (matching Spark cluster version 3.5)
#   - kafka-python (for any DAG that talks to Kafka directly)

FROM apache/airflow:2.10.4-python3.11

USER root

# Java is needed locally for spark-submit (client mode)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        curl \
        procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow

# Pinned versions matter — match Spark cluster (3.5), dbt-postgres (1.8), GE (0.18+)
RUN pip install --no-cache-dir \
    "apache-airflow-providers-apache-spark==4.11.3" \
    "apache-airflow-providers-postgres==5.13.1" \
    "pyspark==3.5.1" \
    "dbt-postgres==1.8.2" \
    "great-expectations==0.18.19" \
    "kafka-python==2.0.2" \
    "pandas==2.2.3"
