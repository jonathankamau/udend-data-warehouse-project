"""Script with executable SQL queries."""
import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create = (
    """
    CREATE TABLE IF NOT EXISTS staging_events (
        artist VARCHAR ,
        auth VARCHAR,
        first_name VARCHAR,
        gender VARCHAR,
        item_in_session INT,
        last_name VARCHAR,
        length NUMERIC,
        level VARCHAR,
        location VARCHAR,
        method VARCHAR,
        page VARCHAR,
        registration NUMERIC,
        session_id NUMERIC,
        song VARCHAR,
        status INT,
        timestamp BIGINT,
        user_agent VARCHAR,
        user_id INT)
    """
    )

staging_songs_table_create = (
    """
    CREATE TABLE IF NOT EXISTS staging_songs (
        num_songs INT,
        artist_id VARCHAR,
        artist_latitude NUMERIC,
        artist_longitude NUMERIC,
        artist_location VARCHAR,
        artist_name VARCHAR,
        song_id VARCHAR,
        title VARCHAR,
        duration NUMERIC,
        year INT)
    """
    )

songplay_table_create = (
    """
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id INT IDENTITY(0,1) PRIMARY KEY,
        start_time TIMESTAMP NOT NULL,
        user_id VARCHAR NOT NULL REFERENCES users(user_id) sortkey,
        level VARCHAR NOT NULL,
        song_id VARCHAR NOT NULL REFERENCES songs(song_id),
        artist_id VARCHAR NOT NULL REFERENCES artists(artist_id) distkey,
        session_id INT NOT NULL,
        location VARCHAR NOT NULL,
        user_agent VARCHAR NOT NULL)
    """
    )

user_table_create = (
    """
    CREATE TABLE IF NOT EXISTS users (
        user_id INT NOT NULL PRIMARY KEY,
        first_name VARCHAR NOT NULL,
        last_name VARCHAR NOT NULL,
        gender VARCHAR NOT NULL,
        level VARCHAR NOT NULL
        )
    DISTSTYLE all
    """
    )

song_table_create = (
    """
    CREATE TABLE IF NOT EXISTS songs (
        song_id VARCHAR NOT NULL PRIMARY KEY sortkey,
        title VARCHAR NOT NULL,
        artist_id VARCHAR NOT NULL REFERENCES artists(artist_id),
        year INT NOT NULL,
        duration NUMERIC NOT NULL)
    """
    )

artist_table_create = (
    """
    CREATE TABLE IF NOT EXISTS artists (
        artist_id VARCHAR NOT NULL PRIMARY KEY sortkey,
        name VARCHAR NOT NULL,
        location VARCHAR,
        latitude NUMERIC,
        longitude NUMERIC)
    """
    )

time_table_create = (
    """
    CREATE TABLE IF NOT EXISTS time (
        start_time timestamp NOT NULL PRIMARY KEY,
        hour INT NOT NULL,
        day INT NOT NULL,
        week INT NOT NULL,
        month TEXT NOT NULL,
        year INT NOT NULL,
        weekday TEXT NOT NULL)
        DISTSTYLE all
    """
    )

# STAGING TABLES

staging_events_copy = (
    "COPY staging_events FROM {} CREDENTIALS 'aws_iam_role={}' json {}"
    ).format(config.get('S3', 'LOG_DATA'),
             config.get('IAM_ROLE', 'ARN'),
             config.get('S3', 'LOG_JSONPATH'))

staging_songs_copy = (
    "COPY staging_songs FROM {} CREDENTIALS 'aws_iam_role={}' json 'auto'"
    ).format(config.get('S3', 'SONG_DATA'),
             config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = (
    """
    INSERT INTO songplays (
        start_time,
        user_id,
        level,
        song_id,
        artist_id,
        session_id,
        location,
        user_agent)
        SELECT DISTINCT TIMESTAMP 'epoch' + se.timestamp/1000 * interval
        '1 second' AS start_time,
        se.user_id, se.level, ss.song_id,
        ss.artist_id, se.session_id, se.location, se.user_agent
        FROM staging_events se
        JOIN staging_songs ss ON (se.artist = ss.artist_name)
        WHERE se.page = 'NextSong'
        AND se.user_id IS NOT NULL
        AND se.level IS NOT NULL
        AND ss.song_id IS NOT NULL
        AND ss.artist_id IS NOT NULL
        AND se.session_id IS NOT NULL
        AND se.location IS NOT NULL AND se.user_agent IS NOT NULL
    """
    )

user_table_insert = (
    """
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT user_id, first_name, last_name, gender, level
    FROM staging_events WHERE page = 'NextSong'
    """
    )

song_table_insert = (
    """
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT DISTINCT song_id, title, artist_id, year, duration
    FROM staging_songs
    """
    )

artist_table_insert = (
    """
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude,
    artist_longitude FROM staging_songs
    """
    )

time_table_insert = (
    """
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
        SELECT DISTINCT start_time,
        EXTRACT(hour from start_time),
        EXTRACT(day from start_time),
        EXTRACT(week from start_time),
        EXTRACT(month from start_time),
        EXTRACT(year from start_time),
        EXTRACT(dayofweek from start_time)
        FROM songplays
    """
    )

# QUERY LISTS

create_table_queries = [staging_events_table_create,
                        staging_songs_table_create,
                        user_table_create,
                        artist_table_create,
                        song_table_create,
                        songplay_table_create,
                        time_table_create
                        ]
drop_table_queries = [staging_events_table_drop,
                      staging_songs_table_drop,
                      songplay_table_drop,
                      user_table_drop,
                      song_table_drop,
                      artist_table_drop,
                      time_table_drop
                      ]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert,
                        time_table_insert,
                        user_table_insert,
                        song_table_insert,
                        artist_table_insert
                        ]
