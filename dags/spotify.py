import requests
from base64 import b64encode
import pandas as pd  
import spotify_config as sc
import amazon_config as ac
from datetime import datetime
import json
import io
from airflow.models.dag import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
from cosmos import DbtTaskGroup
from cosmos.config import ProfileConfig, ProjectConfig, ExecutionConfig
from cosmos.profiles import RedshiftUserPasswordProfileMapping


# Get a Spotify access token using client credentials flow
def get_spotify_token(client_id, client_secret):
    credentials = b64encode(f"{sc.client_id}:{sc.client_secret}".encode()).decode()
    response = requests.post(
        "https://accounts.spotify.com/api/token",
        headers={"Authorization": f"Basic {credentials}"},
        data={"grant_type": "client_credentials"}
    )
    token = response.json().get("access_token")
    return token

# Get Spotify artist ID by artist name
def get_artist_id(name):
    r = requests.get(f"{base_url}/search", headers=headers, params={"q": name, "type": "artist", "limit": 1})
    return r.json()["artists"]["items"][0]["id"]

# Get unique album IDs and release dates for a given artist
def get_album_ids(artist_id):
    albums = []
    url = f"{base_url}/artists/{artist_id}/albums"
    params = {"include_groups": "album,single", "limit": 50}
    while url:
        r = requests.get(url, headers=headers, params=params)
        data = r.json()
        # Collect album ID and release date for each album
        albums += [{"id": a["id"], "release_date": a["release_date"]} for a in data["items"]]
        url = data.get("next")  # Handle pagination from API
        params = None  

    # Deduplicate albums
    seen = set()
    unique_albums = []
    for album in albums:
        if album["id"] not in seen:
            seen.add(album["id"])
            unique_albums.append(album)
    return unique_albums

# Get tracks from each album along with metadata
def get_tracks(album_ids):
    tracks = []
    for album in album_ids:
        r = requests.get(f"{base_url}/albums/{album["id"]}/tracks", headers=headers)
        for t in r.json()["items"]:
            tracks.append({
                "name": t["name"],
                "release_date": album["release_date"],  
                "track_number": t.get("track_number"),
                "duration_ms": t.get("duration_ms"),
                "explicit": t.get("explicit"),
                "id": t.get("id")  
            })

    # Deduplicate tracks by name + release date
    seen = set()
    unique_tracks = []
    for track in tracks:
        key = (track["name"], track["release_date"])
        if key not in seen:
            seen.add(key)
            unique_tracks.append(track)

    return add_popularity(unique_tracks)  

# Add track popularity to each track using the tracks endpoint
def add_popularity(tracks):
    track_ids = [track["id"] for track in tracks if track["id"]]
    # Spotify API only allows up to 50 track IDs per request
    for i in range(0, len(track_ids), 50):
        batch_ids = track_ids[i:i+50]
        r = requests.get(f"{base_url}/tracks", headers=headers, params={"ids": ",".join(batch_ids)})
        for track_data in r.json()["tracks"]:
            for track in tracks:
                if track["id"] == track_data["id"]:
                    track["popularity"] = track_data.get("popularity")
    return tracks

# Get all songs and metadata for a given artist
def get_all_songs(artist_name):
    artist_id = get_artist_id(artist_name)
    album_ids = get_album_ids(artist_id)
    return get_tracks(album_ids)


base_url = "https://api.spotify.com/v1"
headers = {"Authorization": f"Bearer {get_spotify_token(sc.client_id, sc.client_secret)}"}

def spotifyMain():
    # Artists to query
    artist_names = ["Morgan Wallen"]
    # , "Luke Combs", "Riley Green", "Chris Stapleton", 
    #                 "Treaty Oak Revival", "Jason Aldean", "George Strait", "Parker McCollum"]
    country_songs_df = pd.DataFrame()

    # Loop through each artist and collect their songs
    for artist in artist_names:
        songs = get_all_songs(artist)
        for s in songs:
            s["artist"] = artist  # Add artist name to each song
        artist_df = pd.DataFrame(songs)
        country_songs_df = pd.concat([country_songs_df, artist_df], ignore_index=True)

    # Define characters to remove from text fields
    chars_to_remove = r"[\[\]\"\"\(\)\.]"

    # Remove unwanted characters from all string columns
    for col in country_songs_df.select_dtypes(include="object").columns:
        country_songs_df[col] = country_songs_df[col].str.replace(chars_to_remove, "", regex=True)

    # Return the final DataFrame
    return country_songs_df.to_json(orient="records")

def s3Main(**context):
    ti = context["ti"]
    country_songs_json = ti.xcom_pull(task_ids="spotifyAPI")
    df = pd.DataFrame(json.loads(country_songs_json))

    s3_hook = S3Hook(aws_conn_id="aws_conn_camden")
    s3_bucket = ac.s3_bucket
    filename = f"data/spotify-api-{datetime.now().strftime('%Y%m%d%H%M%S%f')[:-3]}.csv"

    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)

    s3_hook.load_string(
        string_data=csv_buffer.getvalue(),
        key=filename,
        bucket_name=s3_bucket,
        replace=True
    )

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

args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 8, 16)
}

profile_config = ProfileConfig(
    profile_name="spotifyredshift",  
    target_name="prod",              
    profile_mapping=RedshiftUserPasswordProfileMapping(
        conn_id="redshift_conn_camden"
    )
)

project_config = ProjectConfig("/Spotify-ELT/dbt/spotifyredshift")

execution_config = ExecutionConfig(dbt_executable_path="/usr/local/bin/dbt")


with DAG(dag_id="spotifyELT"
         ,default_args=args
         ,catchup=False
         ,schedule="45 18 * * *"
         ) as dag:
    spotifyAPI = PythonOperator(
        task_id="spotifyAPI",
        python_callable=spotifyMain
    )
    uploadS3 = PythonOperator(
        task_id="uploadToS3",
        python_callable=s3Main,
    )
    uploadRedshift = PythonOperator(
        task_id="uploadToRedshift",
        python_callable=s3ToRedshiftMain,
    )
    with TaskGroup(group_id="dbt_spotify") as dbtGroup:
        DbtTaskGroup(
            project_config=project_config,
            profile_config=profile_config,
            execution_config=execution_config
        )

spotifyAPI >> uploadS3 >> uploadRedshift >> dbtGroup


