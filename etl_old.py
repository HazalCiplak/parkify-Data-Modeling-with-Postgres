import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
import numpy as np

def get_files(filepath):
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))
    
    return all_files

def process_song_file(cur, filepath):
    # open song file
    #song_path=filepath
    #song_files = glob.glob(song_path+"/**/*.json",recursive=True)
    #song_files=get_files(filepath)
    song_files=get_files("data/song_data")
    
    df2=pd.DataFrame()
    for file in song_files:
        df =pd.read_json(file, lines=True)
        df2 = df2.append(df,ignore_index=True)
        #print(df2)
        
        # insert song record
    df_song=df2.loc[:, ['song_id', 'title', 'artist_id', 'year', 'duration']]
    df_song=df_song.replace({np.NAN: ''}).drop_duplicates()
    #print(df_song) 
    
    for index, row in df_song.iterrows():
        song_data = (row.song_id,row.title,row.artist_id,row.year,row.duration)
        #song_data = df_song[["song_id","title","artist_id","year","duration"]].values.tolist()[0] 
        cur.execute(song_table_insert, song_data)
        #conn.commit()
        
     # insert artist record
    df_artist=df2.loc[:, ['artist_id','artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']]
    df_artist=df_artist.replace({np.NAN: ''}).drop_duplicates()
        
    for index, row in df_artist.iterrows():    
        artist_data = (row.artist_id,row.artist_name,row.artist_location, row.artist_latitude, row.artist_longitude)
        cur.execute(artist_table_insert, artist_data)
        #conn.commit('artist_table_insert')
        print()
        '''



  # insert song record
    df_song=df2.loc[:, ['song_id', 'title', 'artist_id', 'year', 'duration']]
    df_song=df_song.replace({np.NAN: ''}).drop_duplicates()
    
    for index, row in df_song.iterrows():
        song_data = tuple(df_song.values.tolist()[index])
        try:
            cur.execute(song_table_insert, song_data)
            conn.commit()
        except psycopg2.Error as e: 
            print("Error: inserting song_data")
            print(e)

    
    # insert artist record
    df_artist=df2.loc[:, ['artist_id','artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']]
    df_artist=df_artist.replace({np.NAN: ''}).drop_duplicates()

    for index, row in df_artist.iterrows():
        artist_data = tuple(df_artist.values.tolist()[index])
        try:
            cur.execute(artist_table_insert, artist_data)
            conn.commit()
        except psycopg2.Error as e: 
            print("Error: inserting artist_data")
            print(e)
 ''' 


def process_log_file(cur, filepath):
    # open log file
    #log_files =filepath
    #log_files = glob.glob(log_files+"/**/*.json",recursive=True)
    log_files=get_files("data/log_data")
    
    df2=pd.DataFrame()
    for file in log_files:
        df =pd.read_json(file, lines=True)
        df2 = df2.append(df,ignore_index=True)
        
    # filter by NextSong action
    df_filtered = df2[df2['page'] == "NextSong"]
    
    # convert timestamp column to datetime
    df_filtered['ts'] = pd.to_datetime(df_filtered.loc[:,'ts'],unit='ms')
    df_filtered.replace({np.NAN: ''}).drop_duplicates()
    t= pd.to_datetime(df_filtered.loc[:,'ts'], unit='ms')
         
   # insert time data records
    time_data = [t,t.dt.hour,t.dt.day,t.dt.week,t.dt.month,t.dt.year,t.dt.weekday]
    column_labels = ['start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday']
    time_df = pd.DataFrame(dict(zip(column_labels, time_data)))
    
    for index, row in time_df.iterrows():    
        time_data = (row.start_time,row.hour,row.day, row.week, row.month,row.year,row.weekday)
        #time_data = tuple(time_df.values.tolist()[0])
        cur.execute(time_table_insert, time_data)

        
    # load user table
    user_df = df2.loc[:,['userId','firstName', 'lastName', 'gender', 'level']]
    user_df=user_df.replace({np.NAN: ''}).drop_duplicates() 
                               
    # insert user records
    for index, row in user_df.iterrows():
        #user_data=tuple(user_df.values.tolist()[0])
        user_data=(row.userId,row.firstName,row.lastName, row.gender,row.level)
        cur.execute(user_table_insert, user_data)
    

        
    # get songid and artistid from song and artist tables
    song_select_df = df2.loc[:,['song','artist', 'length','userId','level','sessionId','location','userAgent','ts']]
    #cur.execute(song_select, (df2[0].song, df2[0].artist, df2[0].length))
    
    for index, row in song_select_df.iterrows():
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record'
        songplay_data = (index,pd.to_datetime(row.ts, unit='ms'), row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        #songplay_data = [index,t[index],int(row.userId),row.level, songid, artistid,row.sessionId,row.location,row.userAgent]
        cur.execute(songplay_table_insert, songplay_data)
        
    

def process_data(cur, conn, filepath, func):
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()