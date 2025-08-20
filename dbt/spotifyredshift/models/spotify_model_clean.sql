{{ config(
    materialized='view'
) }}

select
    artist,
    extract(year from release_date) as release_year,
    count(distinct id) as number_songs,
    avg(popularity) as avg_popularity,
    avg(duration_ms) / 1000 as avg_duration_seconds
from {{ source('public', 'spotifyRaw') }}
group by 
artist,
extract(year from release_date)
