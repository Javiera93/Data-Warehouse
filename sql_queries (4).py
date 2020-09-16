import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songsplay"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events(
    event_id BIGINT IDENTITY(0,1) PRIMARY KEY,
    artist_name VARCHAR(255),
    auth VARCHAR(50),
    user_first_name VARCHAR(255),
    user_gender  VARCHAR(1),
    item_in_session INTEGER,
    user_last_name VARCHAR(255),
    song_length DOUBLE PRECISION, 
    user_level VARCHAR(50),
    location VARCHAR(255),
    method VARCHAR(25),
    page VARCHAR(35),
    registration VARCHAR(50),
    session_id BIGINT,
    song_title VARCHAR(255),
    status INTEGER, 
    ts VARCHAR(50),
    user_agent TEXT,
    user_id VARCHAR(100)
    )
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs(
    song_id VARCHAR(50) PRIMARY KEY,
    num_songs INTEGER,
    artist_id VARCHAR(50),
    artist_latitude VARCHAR,
    artist_longitude VARCHAR(500),
    artist_location VARCHAR,
    artist_name VARCHAR(500),
    title VARCHAR(500),
    duration DECIMAL(9),
    year INTEGER
    )
""")

songplay_table_create = ("""
CREATE TABLE songsplay(
    songplay_id INTEGER IDENTITY(1,1) PRIMARY KEY,
    start_time TIMESTAMP,
    user_id VARCHAR,
    level VARCHAR(10),
    song_id VARCHAR(40),
    artist_id VARCHAR(50),
    session_id INTEGER,
    location VARCHAR(100),
    user_agent VARCHAR (255)
    ) 
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users(
    user_id VARCHAR PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(80),
    gender VARCHAR(10),
    level VARCHAR(10))
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs(
    song_id VARCHAR(50) PRIMARY KEY,
    title VARCHAR(500),
    artist_id VARCHAR(50),
    year INTEGER,
    duration DECIMAL(9)
    )
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists(
    artist_id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(500),
    location VARCHAR(500),
    latitude DECIMAL(9),
    longitude DECIMAL(9)
    )
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time(
    start_time TIMESTAMP PRIMARY KEY,
    hour SMALLINT,
    day SMALLINT,
    week SMALLINT,
    month SMALLINT,
    year SMALLINT,
    weekday SMALLINT
    )
""")

# STAGING TABLES

staging_events_copy = ("""copy staging_events 
                          from {}
                          credentials 'aws_iam_role={}'
                          format as json {}
                          region 'us-west-2';
""").format(config.get('S3','LOG_DATA'), config.get('IAM_ROLE', 'ARN'), config.get('S3','LOG_JSONPATH'))

staging_songs_copy = ("""copy staging_songs 
                          from {}
                          credentials 'aws_iam_role={}'
                          json 'auto'
                          region 'us-west-2';
""").format(config.get('S3','SONG_DATA'), config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songsplay (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
SELECT  
    TIMESTAMP 'epoch' + se.ts/1000 * interval '1 second' as start_time, 
    se.user_id, 
    se.user_level, 
    ss.song_id,
    ss.artist_id, 
    se.session_id,
    se.location, 
    se.user_agent
FROM staging_events se, staging_songs ss
WHERE se.page = 'NextSong' 
AND se.song_title = ss.title 
AND se.artist_name = ss.artist_name 
AND se.song_length = ss.duration
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level) 
SELECT  
    se.user_id, 
    se.user_first_name as first_name, 
    se.user_last_name as last_name,
    se.user_gender as gender, 
    se.user_level as level
FROM staging_events se
WHERE se.page = 'NextSong' 
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration) 
SELECT  
    ss.song_id, 
    ss.title, 
    ss.artist_id,
    ss.year, 
    ss.duration
FROM staging_songs ss
WHERE ss.song_id IS NOT NULL 
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude) 
SELECT  
    ss.artist_id, 
    ss.artist_name as name, 
    ss.artist_location as location,
    ss.artist_latitude as latitude, 
    ss.artist_longitude as longitude
FROM staging_songs ss
WHERE ss.artist_id IS NOT NULL 
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday) 
SELECT  
    s.start_time,
    EXTRACT(hour from start_time),
    EXTRACT(day from start_time),
    EXTRACT(week from start_time),
    EXTRACT(month from start_time),
    EXTRACT(year from start_time),
    EXTRACT(dayofweek from start_time)
FROM songsplay s
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
