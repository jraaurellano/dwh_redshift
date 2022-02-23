import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

ARN = config.get('IAM_ROLE','ARN')

LOG_DATA = config.get('S3','LOG_DATA')
SONG_DATA = config.get('S3','SONG_DATA')
LOG_JSONPATH = config.get('S3','LOG_JSONPATH')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events (
        artist VARCHAR distkey,
        auth VARCHAR,
        firstName VARCHAR,
        gender CHAR,
        itemInSession INTEGER,
        lastName VARCHAR,
        length NUMERIC,
        level VARCHAR,
        location VARCHAR,
        method VARCHAR,
        page VARCHAR,
        registration NUMERIC,
        sessionId INTEGER,
        song VARCHAR,
        status INTEGER,
        ts BIGINT,
        userAgent VARCHAR,
        userId INTEGER
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        artist_id VARCHAR,
        artist_latitude NUMERIC,
        artist_longitude NUMERIC,
        artist_location VARCHAR,
        artist_name VARCHAR NOT NULL distkey,
        song_id VARCHAR,
        title VARCHAR NOT NULL, 
        duration NUMERIC,
        year INTEGER
    );
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id BIGINT IDENTITY(0, 1),
        start_time TIMESTAMP NOT NULL,
        user_id INT NOT NULL, 
        level VARCHAR, 
        song_id VARCHAR distkey, 
        artist_id VARCHAR, 
        session_id INT, 
        location VARCHAR, 
        user_agent VARCHAR,
        primary key(songplay_id)
    ) sortkey(start_time);
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id INTEGER,
        first_name VARCHAR, 
        last_name VARCHAR, 
        gender CHAR, 
        level VARCHAR,
        primary key(user_id)
    ) diststyle all 
    sortkey(user_id);
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id VARCHAR, 
        title VARCHAR NOT NULL, 
        artist_id VARCHAR, 
        year INTEGER, 
        duration NUMERIC,
        primary key(song_id)
    ) diststyle all
    sortkey(song_id);
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id VARCHAR, 
        name VARCHAR NOT NULL, 
        location VARCHAR, 
        latitude NUMERIC, 
        longitude NUMERIC,
        primary key(artist_id)
    ) diststyle all
    sortkey(artist_id);
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time TIMESTAMP WITHOUT TIME ZONE, 
        hour INTEGER, 
        day INTEGER, 
        week INTEGER, 
        month INTEGER, 
        year INTEGER, 
        weekday INTEGER,
        primary key(start_time)
    ) diststyle all
    sortkey(start_time);
""")
# STAGING TABLES

staging_events_copy = ("""
    copy staging_events 
    from {}
    iam_role {}
    json {}
""").format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = ("""
    copy staging_songs
    from {}
    iam_role {}
    json 'auto'
""").format(SONG_DATA, ARN)

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT timestamp 'epoch' + e.ts * interval '1 second' AS start_time,
        e.userId, 
        e.level, 
        s.song_id, 
        s.artist_id, 
        e.sessionId, 
        e.location, 
        e.userAgent
    FROM staging_songs s 
    JOIN staging_events e 
    ON s.title = e.song AND s.artist_name = e.artist
    WHERE e.userId IS NOT NULL;
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT e.userId,
        e.firstName,
        e.lastName, 
        e.gender,
        e.level
    FROM staging_events e
    WHERE e.userId IS NOT NULL;
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration) 
    SELECT DISTINCT s.song_id,
        s.title,
        s.artist_id,
        s.year,
        s.duration
    FROM staging_songs s;
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT s.artist_id,
        s.artist_name,
        s.artist_location,
        s.artist_latitude,
        s.artist_longitude
    FROM staging_songs s;
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT ts, 
        EXTRACT(HOUR FROM ts), 
        EXTRACT(DAY FROM ts), 
        EXTRACT(WEEK FROM ts), 
        EXTRACT(MONTH FROM ts), 
        EXTRACT(YEAR FROM ts), 
        EXTRACT(WEEKDAY FROM ts)
    FROM
        (SELECT (TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 Second ') as ts from staging_events)
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
