FROM quay.io/astronomer/astro-runtime:13.1.0

RUN pip install dbt-redshift

COPY dbt/spotifyredshift /usr/local/airflow/dbt/spotifyredshift

WORKDIR /usr/local/airflow
COPY dbt-requirements.txt ./
RUN python -m virtualenv dbt_venv && source dbt_venv/bin/activate && \
    pip install --no-cache-dir -r dbt-requirements.txt && deactivate


