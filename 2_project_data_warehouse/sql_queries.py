import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create = """
    CREATE TABLE IF NOT EXISTS staging_events (
        event_id    INTEGER IDENTITY(0,1),
        artist      VARCHAR,
        auth        VARCHAR,
        firstName   VARCHAR,
        gender      VARCHAR,
        itemInSession VARCHAR,
        lastName    VARCHAR,
        length      DECIMAL,
        level       VARCHAR,
        location    VARCHAR,
        method      VARCHAR,
        page        VARCHAR,
        registration VARCHAR,
        sessionId   INTEGER SORTKEY DISTKEY,
        song        VARCHAR,
        status      INTEGER,
        ts          BIGINT,
        userAgent   VARCHAR,
        userId      INTEGER
    );
"""

staging_songs_table_create = """
    CREATE TABLE IF NOT EXISTS staging_songs (
        num_songs           INTEGER,
        artist_id           VARCHAR SORTKEY DISTKEY,
        artist_latitude     DECIMAL,
        artist_longitude    DECIMAL,
        artist_location     VARCHAR(500),
        artist_name         VARCHAR(500),
        song_id             VARCHAR,
        title               VARCHAR(500),
        duration            DECIMAL,
        year                INTEGER
    );
"""

songplay_table_create = """
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id INTEGER IDENTITY(0,1)   NOT NULL SORTKEY,
        start_time  TIMESTAMP               NOT NULL,
        user_id     INTEGER                 NOT NULL DISTKEY,
        level       VARCHAR(50)             NOT NULL,
        song_id     VARCHAR(50)             NOT NULL,
        artist_id   VARCHAR(50)             NOT NULL,
        session_id  INTEGER                 NOT NULL,
        location    VARCHAR(100)            NULL,
        user_agent  VARCHAR(300)            NULL
    );
"""

user_table_create = """
    CREATE TABLE IF NOT EXISTS users (
        user_id     INTEGER         NOT NULL SORTKEY,
        first_name  VARCHAR(100)    NULL,
        last_name   VARCHAR(100)    NULL,
        gender      VARCHAR(10)     NULL,
        level       VARCHAR(10)     NULL
    ) diststyle all;
"""

song_table_create = """
    CREATE TABLE IF NOT EXISTS songs (
        song_id     VARCHAR(50)     NOT NULL SORTKEY,
        title       VARCHAR(500)    NOT NULL,
        artist_id   VARCHAR(50)     NOT NULL,
        year        INTEGER         NOT NULL,
        duration    DECIMAL(10)     NOT NULL
    );
"""

artist_table_create = """
    CREATE TABLE IF NOT EXISTS artists (
        artist_id   VARCHAR(50)             NOT NULL SORTKEY,
        name        VARCHAR(500)            NULL,
        location    VARCHAR(500)            NULL,
        latitude    DECIMAL(10)             NULL,
        longitude   DECIMAL(10)             NULL
    ) diststyle all;
"""

time_table_create = """
    CREATE TABLE IF NOT EXISTS time (
        start_time  TIMESTAMP   NOT NULL SORTKEY,
        hour        SMALLINT    NULL,
        day         SMALLINT    NULL,
        week        SMALLINT    NULL,
        month       SMALLINT    NULL,
        year        SMALLINT    NULL,
        weekday     SMALLINT    NULL
    ) diststyle all;
"""

# STAGING TABLES

staging_events_copy = f"""
    COPY staging_events FROM {config.get('S3', 'LOG_DATA')}
    credentials 'aws_iam_role={config.get('IAM_ROLE', 'ARN')}'
    format as json {config.get('S3', 'LOG_JSONPATH')}
    STATUPDATE ON region 'us-west-2';
"""

staging_songs_copy = f"""
    COPY staging_songs FROM {config.get('S3', 'SONG_DATA')}
    credentials 'aws_iam_role={config.get('IAM_ROLE', 'ARN')}'
    format as json 'auto'
    ACCEPTINVCHARS AS '^'
    STATUPDATE ON region 'us-west-2';
"""

# FINAL TABLES

songplay_table_insert = """
    INSERT INTO songplays (
        start_time,
        user_id,
        level,
        song_id,
        artist_id,
        session_id,
        location,
        user_agent)
    SELECT  DISTINCT TIMESTAMP 'epoch' + se.ts/1000 \
                * INTERVAL '1 second'   AS start_time,
            se.userId                   AS user_id,
            se.level                    AS level,
            ss.song_id                  AS song_id,
            ss.artist_id                AS artist_id,
            se.sessionId                AS session_id,
            se.location                 AS location,
            se.userAgent                AS user_agent
    FROM staging_events AS se
    JOIN staging_songs  AS ss ON (se.artist = ss.artist_name)
    WHERE se.page = 'NextSong';
"""

user_table_insert = """
    INSERT INTO users (
        user_id,
        first_name,
        last_name,
        gender,
        level)
    SELECT  DISTINCT se.userId  AS user_id,
            se.firstName        AS first_name,
            se.lastName         AS last_name,
            se.gender           AS gender,
            se.level            AS level
    FROM staging_events AS se
    WHERE se.page = 'NextSong';
"""

song_table_insert = """
    INSERT INTO songs (
        song_id,
        title,
        artist_id,
        year,
        duration)
    SELECT  DISTINCT ss.song_id AS song_id,
            ss.title            AS title,
            ss.artist_id        AS artist_id,
            ss.year             AS year,
            ss.duration         AS duration
    FROM staging_songs AS ss;
"""


artist_table_insert = """
    INSERT INTO artists (
        artist_id,
        name,
        location,
        latitude,
        longitude)
    SELECT  DISTINCT ss.artist_id   AS artist_id,
        ss.artist_name              AS name,
        ss.artist_location          AS location,
        ss.artist_latitude          AS latitude,
        ss.artist_longitude         AS longitude
    FROM staging_songs AS ss;
"""

time_table_insert = """
    INSERT INTO time (                  
        start_time,
        hour,
        day,
        week,
        month,
        year,
        weekday)
    SELECT  DISTINCT TIMESTAMP 'epoch' + se.ts/1000 \
                * INTERVAL '1 second'        AS start_time,
            EXTRACT(hour FROM start_time)    AS hour,
            EXTRACT(day FROM start_time)     AS day,
            EXTRACT(week FROM start_time)    AS week,
            EXTRACT(month FROM start_time)   AS month,
            EXTRACT(year FROM start_time)    AS year,
            EXTRACT(weekday FROM start_time) AS weekday
    FROM staging_events AS se
    WHERE se.page = 'NextSong';
"""

# ANALYTICAL QUERIES ON FINAL TABLES

songplays_query = """
    SELECT *
    FROM songplays
    LIMIT 10;
"""

users_query = """
    SELECT *
    FROM users
    LIMIT 10;
"""

songs_query = """
    SELECT *
    FROM songs
    LIMIT 10;
"""

artists_query = """
    SELECT *
    FROM artists
    LIMIT 10;
"""

time_query = """
    SELECT *
    FROM time
    LIMIT 10;
"""

top_users_most_songs = """
    SELECT u.user_id, u.first_name, u.last_name, COUNT(*) as song_count
    FROM songplays sp
    JOIN users u ON sp.user_id = u.user_id
    GROUP BY u.user_id, u.first_name, u.last_name
    ORDER BY song_count DESC
    LIMIT 5;
"""

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
analytic_queries = [songplays_query, users_query, songs_query, artists_query, time_query, top_users_most_songs]
