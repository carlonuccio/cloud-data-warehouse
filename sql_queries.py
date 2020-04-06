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

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events
(
artist          VARCHAR,
auth            VARCHAR, 
firstName       VARCHAR,
gender          VARCHAR,   
itemInSession   INTEGER,
lastName        VARCHAR,
length          FLOAT,
level           VARCHAR, 
location        VARCHAR,
method          VARCHAR,
page            VARCHAR,
registration    BIGINT,
sessionId       INTEGER,
song            VARCHAR,
status          INTEGER,
ts              TIMESTAMP,
userAgent       VARCHAR,
userId          INTEGER
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs
(
song_id            VARCHAR,
num_songs          INTEGER,
title              VARCHAR,
artist_name        VARCHAR,
artist_latitude    FLOAT,
year               INTEGER,
duration           FLOAT,
artist_id          VARCHAR,
artist_longitude   FLOAT,
artist_location    VARCHAR
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays
(
songplay_id     INTEGER IDENTITY(1,1) PRIMARY KEY,
start_time      TIMESTAMP,
user_id         INTEGER,
level           VARCHAR,
idSong          INTEGER,
idArtist        INTEGER,
session_id      INTEGER,
location        VARCHAR,
user_agent      VARCHAR
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users
(
idUser          INTEGER IDENTITY(1,1) PRIMARY KEY,
user_id         INTEGER,
first_name      VARCHAR,
last_name       VARCHAR,
gender          VARCHAR,
level           VARCHAR
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs
(
idSong          INTEGER IDENTITY(1,1) PRIMARY KEY,
song_id         VARCHAR NOT NULL UNIQUE,
title           VARCHAR,
artist_id       VARCHAR,
year            INTEGER,
DURATION        FLOAT
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists
(
idArtist        INTEGER IDENTITY(1,1) PRIMARY KEY,
artist_id       VARCHAR NOT NULL UNIQUE,
name            VARCHAR,
LOCATION        VARCHAR,
LATITUDE        VARCHAR,
LONGITUDE       VARCHAR
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time
(
START_TIME      TIMESTAMP PRIMARY KEY,
HOUR            INTEGER,
DAY             INTEGER,
WEEK            INTEGER,
MONTH           INTEGER,
YEAR            INTEGER,
WEEKDAY         VARCHAR
);
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events from {}
    credentials 'aws_iam_role={}'
    region 'us-west-2' format as JSON {}
    timeformat as 'epochmillisecs';
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
    copy staging_songs from {}
    credentials 'aws_iam_role={}'
    region 'us-west-2' format as JSON 'auto';
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT  DISTINCT userId     AS user_id,
            firstName           AS first_name,
            lastName            AS last_name,
            gender,
            level
    FROM staging_events
    WHERE user_id IS NOT NULL and level IS NOT NULL;
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT  DISTINCT song_id AS song_id,
                     title,
                     artist_id,
                     year,
                     duration
    FROM staging_songs
    WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT  DISTINCT artist_id  AS artist_id,
            artist_name         AS name,
            artist_location     AS location,
            artist_latitude     AS latitude,
            artist_longitude    AS longitude
    FROM staging_songs
    WHERE artist_id IS NOT NULL;
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT  DISTINCT start_time                 AS start_time,
            EXTRACT(hour FROM start_time)       AS hour,
            EXTRACT(day FROM start_time)        AS day,
            EXTRACT(week FROM start_time)       AS week,
            EXTRACT(month FROM start_time)      AS month,
            EXTRACT(year FROM start_time)       AS year,
            EXTRACT(dayofweek FROM start_time)  as weekday
    FROM staging_events;
""")


songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, idSong, idArtist, session_id, location, user_agent)
    SELECT  e.ts            AS start_time, 
            e.userId        AS user_id, 
            e.level         AS level, 
            so.idSong       AS idSong,
            a.idArtist      AS idArtist
            e.sessionId     AS session_id, 
            e.location      AS location, 
            e.userAgent     AS user_agent
    FROM staging_events e
    JOIN staging_songs  s   ON (e.song = s.title AND e.artist = s.artist_name)
    JOIN songs so on s.song_id = so.song_id
    JOIN artists a on s.artist_id = a.artist_id
    AND e.page  ==  'NextSong'
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create,
                        user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop,
                      song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert,
                        songplay_table_insert]

