import requests
from base64 import b64encode
import pandas as pd  
import spotify_config as sc
import amazon_config as ac
from datetime import datetime
import json
import io
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
from cosmos import DbtTaskGroup
from cosmos.config import ProfileConfig, ProjectConfig, ExecutionConfig
from cosmos.profiles import RedshiftUserPasswordProfileMapping


# ============ SPOTIFY API FUNCTIONS ============

# Request an access token from Spotify using Client Credentials Flow
def get_spotify_token(client_id, client_secret):
    credentials = b64encode(f"{client_id}:{client_secret}".encode()).decode()
    response = requests.post(
        "https://accounts.spotify.com/api/token",
        headers={"Authorization": f"Basic {credentials}"},
        data={"grant_type": "client_credentials"}
    )
    return response.json().get("access_token")


# Build headers dynamically (ensures token is always fresh)
def get_headers():
    token = get_spotify_token(sc.client_id, sc.client_secret)
    return {"Authorization": f"Bearer {token}"}


# Retrieve Spotify artist ID from artist name
def get_artist_id(name):
    r = requests.get(f"{base_url}/search", headers=get_headers(), params={"q": name, "type": "artist", "limit": 1})
    return r.json()["artists"]["items"][0]["id"]


# Get unique album IDs and release dates for a given artist
def get_album_ids(artist_id):
    albums = []
    url = f"{base_url}/artists/{artist_id}/albums"
    params = {"include_groups": "album,single", "limit": 50}
    
    # Handle paginated responses from Spotify
    while url:
        r = requests.get(url, headers=get_headers(), params=params)
        data = r.json()
        albums += [{"id": a["id"], "release_date": a["release_date"]} for a in data["items"]]
        url = data.get("next")
        params = None  

    # Deduplicate albums by ID
    seen = set()
    unique_albums = []
    for album in albums:
        if album["id"] not in seen:
            seen.add(album["id"])
            unique_albums.append(album)
    return unique_albums

# Get tracks (songs) from each album
def get_tracks(album_ids):
    tracks = []
    for album in album_ids:
        r = requests.get(f"{base_url}/albums/{album['id']}/tracks", headers=get_headers())
        for t in r.json()["items"]:
            tracks.append({
                "name": t["name"],
                "release_date": album["release_date"],  
                "track_number": t.get("track_number"),
                "duration_ms": t.get("duration_ms"),
                "explicit": t.get("explicit"),
                "id": t.get("id")  
            })

    # Add popularity score to each track
    return add_popularity(tracks)  


# Add popularity score for tracks (Spotify requires batch requests for up to 50 IDs)
def add_popularity(tracks):
    track_ids = [t["id"] for t in tracks if t["id"]]
    id_to_track = {t["id"]: t for t in tracks if t["id"]}
    
    for i in range(0, len(track_ids), 50):
        batch_ids = track_ids[i:i+50]
        r = requests.get(f"{base_url}/tracks", headers=get_headers(), params={"ids": ",".join(batch_ids)})
        for t in r.json()["tracks"]:
            if t["id"] in id_to_track:
                id_to_track[t["id"]]["popularity"] = t.get("popularity")
    return list(id_to_track.values())


# Get all songs + metadata for a given artist name
def get_all_songs(artist_name):
    artist_id = get_artist_id(artist_name)
    album_ids = get_album_ids(artist_id)
    return get_tracks(album_ids)


# Base URL for Spotify API requests
base_url = "https://api.spotify.com/v1"


# ============ AIRFLOW TASK FUNCTIONS ============

# Main function to extract Spotify data
def spotifyMain():
    artist_names = ["Morgan Wallen", "Chris Stapleton", "Treaty Oak Revival", "Jason Aldean", "George Strait", "Parker McCollum"]
    country_songs_df = pd.DataFrame()

    # Loop through artists, collect songs, and combine into one DataFrame
    for artist in artist_names:
        songs = get_all_songs(artist)
        for s in songs:
            s["artist"] = artist  # Add artist name
        artist_df = pd.DataFrame(songs)
        country_songs_df = pd.concat([country_songs_df, artist_df], ignore_index=True)

    # Drop duplicates across artist datasets
    country_songs_df = country_songs_df.drop_duplicates(subset=["name", "release_date"])

    # Remove unwanted special characters from all string columns
    chars_to_remove = r"[\[\]\"\"\(\)\.]"
    country_songs_df = country_songs_df.replace(chars_to_remove, "", regex=True)

    return country_songs_df.to_json(orient="records")


# Upload extracted Spotify data to S3
def s3Main(**context):
    ti = context["ti"]
    country_songs_json = ti.xcom_pull(task_ids="spotifyAPI")
    df = pd.DataFrame(json.loads(country_songs_json))

    s3_hook = S3Hook(aws_conn_id="aws_conn_camden")
    # Timestamped filename with millisecond precision
    filename = f"data/spotify-api-{datetime.now().strftime('%Y%m%d%H%M%S')}-{datetime.now().microsecond//1000}.csv"

    # Write DataFrame to buffer
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)

    # Upload to S3
    s3_hook.load_string(
        string_data=csv_buffer.getvalue(),
        key=filename,
        bucket_name=ac.s3_bucket,
        replace=True
    )


# Load data from S3 into Amazon Redshift
def s3ToRedshiftMain(**context):
    copy_sql = f"""
            COPY {ac.redshift_table}
            FROM '{ac.s3_path}'
            IAM_ROLE '{ac.iam_role}'
            CSV
            IGNOREHEADER 1;
                """

    redshift_hook = RedshiftSQLHook(redshift_conn_id="redshift_conn_camden")
    redshift_hook.run(copy_sql)


# ============ DBT + AIRFLOW CONFIG ============

args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 8, 16)
}

# dbt profile configuration for Redshift
profile_config = ProfileConfig(
    profile_name="spotifyredshift",
    target_name="prod",
    profile_mapping=RedshiftUserPasswordProfileMapping(
        conn_id="redshift_conn_camden",
        profile_args={"schema": "public"},
    ),
)

# dbt project location
project_config = ProjectConfig("/usr/local/airflow/dbt/spotifyredshift")

# dbt executable path
execution_config = ExecutionConfig(dbt_executable_path="/usr/local/airflow/dbt_venv/bin/dbt")


# ============ AIRFLOW DAG ============

with DAG(
    dag_id="spotifyELT",
    default_args=args,
    catchup=False,
    schedule="45 18 * * *"   # Run daily at 18:45 UTC
) as dag:

    spotifyAPI = PythonOperator(
        task_id="spotifyAPI",
        python_callable=spotifyMain
    )

    uploadS3 = PythonOperator(
        task_id="uploadToS3",
        python_callable=s3Main
    )

    uploadRedshift = PythonOperator(
        task_id="uploadToRedshift",
        python_callable=s3ToRedshiftMain
    )

    dbtGroup = DbtTaskGroup(
        group_id="dbtSpotify",
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config
    )

    # Task dependencies
    spotifyAPI >> uploadS3 >> uploadRedshift >> dbtGroup
