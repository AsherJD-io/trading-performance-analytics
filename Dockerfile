FROM kestra/kestra:latest

USER root

# Install system deps
RUN apt-get update && apt-get install -y \
    git \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

# Install dbt BigQuery adapter
RUN pip install --no-cache-dir dbt-bigquery

USER kestra
