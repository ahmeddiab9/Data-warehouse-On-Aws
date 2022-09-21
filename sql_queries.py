import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop    = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop     = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop          = "DROP TABLE IF EXISTS songplays ;"
user_table_drop              = "DROP TABLE IF EXISTS users "
song_table_drop              = "DROP TABLE IF EXISTS songs"
artist_table_drop            = "DROP TABLE IF EXISTS artists"
time_table_drop              = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events(
    artist          TEXT,
    auth            TEXT,
    firstName       TEXT,
    gender          TEXT,
    itemInSession   INTEGER,
    lastName        TEXT,
    length          NUMERIC,
    level           TEXT,
    location        TEXT,
    method          TEXT,
    page            TEXT,
    registration    NUMERIC,
    sessionId       INTEGER,
    song            TEXT,
    status          INTEGER,
    ts              BIGINT,
    userAgent       TEXT,
    userId          TEXT
);
""")


staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs(
    num_songs           INTEGER, 
    artist_id           TEXT, 
    artist_latitude     TEXT, 
    artist_longitude    TEXT, 
    artist_location     TEXT, 
    artist_name         TEXT, 
    song_id             TEXT, 
    title               TEXT, 
    duration            NUMERIC, 
    year                INTEGER
);
""")
songplay_table_create = ("""
        CREATE TABLE IF NOT EXISTS songplays
        (
            songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY ,
            start_time  TIMESTAMP NOT NULL SORTKEY DISTKEY,
            user_id     INTEGER NOT NULL,
            level       VARCHAR NOT NULL,
            song_id     VARCHAR,
            artist_id   VARCHAR,
            session_id  INTEGER NOT NULL,
            location    VARCHAR NOT NULL,
            user_agent  VARCHAR NOT NULL
        ) 
""")

user_table_create = ("""
        CREATE TABLE IF NOT EXISTS users
        (
            user_id    INTEGER PRIMARY KEY SORTKEY,
            first_name VARCHAR NOT NULL,
            last_name  VARCHAR NOT NULL,
            gender     CHAR NOT NULL,
            level      VARCHAR NOT NULL
        )
        diststyle all; 
""")

song_table_create = ("""
        CREATE TABLE IF NOT EXISTS songs
        (
            song_id   VARCHAR PRIMARY KEY SORTKEY,
            title     VARCHAR NOT NULL,
            artist_id VARCHAR NOT NULL,
            year      INTEGER NOT NULL,
            duration  FLOAT NOT NULL
        ) 
        diststyle all;
""")

artist_table_create = ("""
        CREATE TABLE IF NOT EXISTS artists
        (
            artist_id VARCHAR PRIMARY KEY SORTKEY,
            name      VARCHAR NOT NULL,
            location  VARCHAR NOT NULL,
            latitude  FLOAT,
            longitude FLOAT
        ) 
        diststyle all;
""")

time_table_create = ("""
        CREATE TABLE IF NOT EXISTS time
        (
            start_time TIMESTAMP PRIMARY KEY SORTKEY DISTKEY,
            hour       INTEGER NOT NULL,
            day        INTEGER NOT NULL,
            week       INTEGER NOT NULL,
            month      INTEGER NOT NULL,
            year       INTEGER NOT NULL,
            weekday    VARCHAR NOT NULL
        ) 
""")


# STAGING TABLES


staging_events_copy = ("""
copy staging_events from {}
credentials 'aws_iam_role={}'
region 'us-west-2' FORMAT AS JSON {};
""").format(config.get("S3","LOG_DATA"),config.get("IAM_ROLE","ARN"),config.get("S3","LOG_JSONPATH"))

staging_songs_copy = ("""
copy staging_songs from {}
credentials 'aws_iam_role={}'
region 'us-west-2' FORMAT AS JSON 'auto';
""").format(config.get("S3","SONG_DATA"),config.get("IAM_ROLE","ARN"))


# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays( songplay_id,start_time,user_id,level,song_id,artist_id,session_id,location,user_agent)
    SELECT DISTINCT(se.ts) AS start_time,
            se.userId AS user_id,
            se.level AS level,
            s.song_id AS song_id,
            s.artist_id AS artist_id,
            se.sessionId AS session_id,
            se.location AS location,
            se.userAgent AS user_agent
    FROM staging_events se
    JOIN staging_songs s
    ON ( se.artist=s.artist_name AND se.song = s.title )
    WHERE se.page == 'NextSong';            
""")

user_table_insert = ("""
    INSERT INTO users (user_id,first_name,last_name,gender,level)
    SELECT DISTINCT(userId) AS user_id,
           firstName AS first_name,
           lastName AS last_name,
           gender,
           level
    FROM staging_events
    WHERE userId IS NOT NULL AND page == 'NextSong';             
""")

song_table_insert = ("""
    INSERT INTO songs(song_id,title,artist_id,year,duration)
    SELECT DISTINCT(song_id) AS song_id,
            title,
            artist_id,
            year,
            duration
    FROM staging_songs
    WHERE song_id IS NOT NULL;          
""")

artist_table_insert = ("""
        INSERT INTO artists(artist_id,name,location,latitude,longitude)
        SELECT DISTINCT(artist_id) AS artist_id,
        artist_name AS name,
        artist_location AS location,
        artist_latitude AS latitude,
        artist_longitude AS longitude
        FROM staging_songs
        WHERE artist_id IS NOT NULL;
""")

time_table_insert = ("""
        INSERT INTO  time(start_time,hour,day,week,month,year,weekday)
        SELECT DISTINCT(start_time) AS start_time,
            EXTRACT(hour FROM start_time) AS hour,
            EXTRACT(day FROM start_time) AS day,
            EXTRACT(week FROM start_time) AS week,
            EXTRACT(month FROM start_time) AS month,
            EXTRACT(year FROM start_time) AS year,
            EXTRACT(dayofweek FROM start_time) AS weekday
        FROM songplays;    

""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create,
                        songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop,
                      songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert,
                        song_table_insert, artist_table_insert, time_table_insert]
