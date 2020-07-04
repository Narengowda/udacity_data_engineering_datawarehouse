import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS stage_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS stage_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplay;"
user_table_drop = "drop table if exists users;"
song_table_drop = "drop table if exists song;"
artist_table_drop = "DROP TABLE IF EXISTS artist;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE stage_events(
   artist        VARCHAR(500),
   auth          VARCHAR(50),
    firstName    VARCHAR(100),
    gender       VARCHAR(100),
   itemInSelection    INT,
   lastName        VARCHAR(100),
   length          DECIMAL ,
   level           VARCHAR(500) ,
   location        VARCHAR(500),
   method          VARCHAR(500),
   page            VARCHAR(500),
   registration    VARCHAR(500),
   sessionId       INT,
   song            VARCHAR(500),
   status          VARCHAR(500),
   ts              bigint,
   userAgent       VARCHAR(500),
   userId          INT
)
""")

staging_songs_table_create = ("""
CREATE TABLE stage_songs(
   num_songs             INT,
   artist_id             VARCHAR,
   artist_latitude       VARCHAR,
   artist_longitude      VARCHAR,
   artist_location       VARCHAR,
   artist_name           VARCHAR ,
   song_id               VARCHAR ,
   title                 VARCHAR,
   duration              decimal,
   YEAR                  INT
);
""")

songplay_table_create = ("""
CREATE TABLE songplay(
   SONGPLAY_ID  int IDENTITY(0,1) PRIMARY KEY ,
   userId       VARCHAR,
   START_TIME   VARCHAR(50),
   LEVEL        VARCHAR(100),
   SONG_ID      VARCHAR(100),
   ARTIST_ID    VARCHAR(100),
   SESSION_ID   VARCHAR(100) ,
   LOCATION     VARCHAR(100) ,
   USER_AGENT   VARCHAR(200)
);
""")

user_table_create = ("""
CREATE TABLE users(
   user_id       VARCHAR PRIMARY KEY,
   first_name    VARCHAR,
   LAST_NAME    VARCHAR,
   GENDER       VARCHAR,
   LEVEL        VARCHAR
);
""")

song_table_create = ("""
CREATE TABLE song(
   SONG_ID      VARCHAR PRIMARY KEY,
   TITLE        VARCHAR ,
   ARTIST_ID    VARCHAR,
   YEAR         INTEGER,
   DURATION     decimal
   );
""")

artist_table_create = ("""
CREATE TABLE artist(
   ARTIST_ID    VARCHAR PRIMARY KEY,
   NAME         VARCHAR ,
   LOCATION     VARCHAR,
   LATITUDE     DECIMAL,
   LONGITUDE    DECIMAL
);
""")

time_table_create = ("""
CREATE TABLE time(
   START_TIME   timestamp without time zone PRIMARY KEY,
   HOUR         INTEGER,
   DAY          VARCHAR,
   WEEK         INTEGER,
   MONTH        INTEGER,
   YEAR         INTEGER,
   WEEKDAY      BOOLEAN
);
""")

# STAGING TABLES

staging_events_copy = ("""
copy stage_events from {}
credentials 'aws_iam_role={}'
json {} compupdate off
region 'us-west-2';
""").format(config.get('S3','LOG_DATA'),config.get('IAM_ROLE','ARN'),config.get('S3','LOG_JSONPATH'))

staging_songs_copy = ("""
copy stage_songs from 's3://udacity-dend/song_data' 
credentials 'aws_iam_role={}'
json 'auto' compupdate off
region 'us-west-2';
""").format(config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES
songplay_table_insert = ("""
INSERT INTO songplay (userid, START_TIME, LEVEL, SONG_ID, ARTIST_ID, session_Id, LOCATION, user_agent)
SELECT distinct userId, ts, level, song_id, artist_id, se.sessionId, location, userAgent
  FROM stage_events se
LEFT JOIN stage_songs ss ON (se.song = ss.title AND se.artist = ss.artist_name) WHERE se.page = 'NextSong';""")

song_table_insert = ("""
INSERT INTO song (song_id, title, artist_id, YEAR, duration) select distinct  song_id, title, artist_id, YEAR, duration from stage_songs;
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level) select distinct  userId, firstName, lastName, gender, level from stage_events WHERE page = 'NextSong';
""")

artist_table_insert = ("""
INSERT INTO artist (artist_id, name, location, latitude, longitude) select distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude from stage_songs;
""")

time_table_insert = ("""
INSERT INTO time (START_TIME, HOUR, DAY, WEEK,MONTH, YEAR, WEEKDAY) SELECT timestamp 'epoch' + CAST(ts AS BIGINT)/1000 * interval '1 second' AS START_TIME,
                EXTRACT(HOUR from START_TIME) as HOUR,
                 EXTRACT(DAY from START_TIME) AS DAY,
                 EXTRACT(WEEK from START_TIME) AS WEEK,
                 EXTRACT(MONTH from START_TIME) AS MONTH,
                 EXTRACT(YEAR from START_TIME) AS YEAR,
                 EXTRACT(WEEKDAY from START_TIME) AS WEEKDAY from songplay; 
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

