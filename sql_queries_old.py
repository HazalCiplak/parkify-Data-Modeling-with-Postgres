# DROP TABLES
songplay_table_drop = "drop table if exists songplays;"
user_table_drop = "drop table if exists  users;"
song_table_drop = "drop table if exists  songs;"
artist_table_drop = "drop table if exists  artists;"
time_table_drop = "drop table if exists  time;"

# CREATE TABLES

songplay_table_create = ("""create table if not exists songplays(songplay_id varchar primary key, start_time time, user_id varchar, level varchar, song_id varchar, artist_id varchar, session_id varchar , location varchar, user_agent varchar);""")

user_table_create = ("""create table if not exists users(user_id varchar primary key, first_name varchar, last_name varchar, gender varchar, level varchar);""")

song_table_create = ("""create table if not exists songs(song_id varchar primary key, title varchar, artist_id varchar, year int, duration float);""")

artist_table_create = ("""create table if not exists artists(artist_id varchar primary key,name varchar, location varchar, latitude varchar, longitude varchar);""")

time_table_create = ("""create table if not exists time(start_time time not null, hour int, day int, week int, month int, year int, weekday varchar);""")

# INSERT RECORDS

songplay_table_insert = ("""insert into songplays(songplay_id, start_time, user_id, level, song_id, artist_id, session_id , location , user_agent) values(%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (songplay_id) DO NOTHING;""")

user_table_insert = (""" insert into users(user_id, first_name, last_name, gender, level) values(%s,%s,%s,%s,%s)  ON CONFLICT (user_id) DO NOTHING;""")

song_table_insert = ("""insert into songs(song_id, title, artist_id , year , duration ) values(%s,%s,%s,%s,%s) ON CONFLICT (song_id) DO NOTHING;""")

artist_table_insert = ("""insert into artists(artist_id, name, location, latitude, longitude) values(%s,%s,%s,%s,%s) ON CONFLICT (artist_id) DO NOTHING;""")

time_table_insert = ("""insert into time(start_time, hour, day, week, month , year , weekday) values(%s,%s,%s,%s,%s,%s,%s)""")

# FIND SONGS

song_select = (""" 
select distinct s.song_id, a.artist_id
from artists a join songs s on a.artist_id=s.artist_id
where 1=1
and (a.artist_id is not null or a.artist_id<>'')
and (s.artist_id is not null or s.artist_id<>'')
and s.title=(%s)
and a.name=(%s)
and s.duration=(%s)
""")


# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]