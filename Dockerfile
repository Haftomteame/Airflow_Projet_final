FROM apache/airflow:3.0.5-python3.11

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq-dev gcc \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

ENV PYTHONPATH=/opt/airflow:/opt/airflow/project
