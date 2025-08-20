FROM quay.io/astronomer/astro-runtime:13.1.0

COPY ./dbt/spotifyredshift /Spotify-ELT/dbt/spotifyredshift
ENV DBT_PROFILES_DIR=/usr/local/airflow/.dbt
RUN pip install dbt-redshift

COPY dbt/spotifyredshift/profiles.yml /usr/local/airflow/.dbt/profiles.yml

WORKDIR /usr/local/airflow
COPY dbt-requirements.txt ./
RUN python -m virtualenv dbt_venv && source dbt_venv/bin/activate && \
    pip install --no-cache-dir -r dbt-requirements.txt && deactivate

# RUN pip install --no-cache-dir -r dbt-requirements.txt


