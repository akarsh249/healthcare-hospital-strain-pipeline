FROM apache/airflow:2.9.3-python3.11

USER root
RUN apt-get update && apt-get install -y build-essential && apt-get clean

USER airflow
RUN pip install --no-cache-dir \
    boto3==1.34.162 \
    requests==2.32.3 \
    pandas==2.2.2 \
    psycopg2-binary==2.9.9
