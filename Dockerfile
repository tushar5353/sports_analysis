FROM apache/airflow:3.0.6

USER root

# Copy requirements.txt and install dependencies
RUN pip install .

USER airflow

