import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
import numpy as np


def process_song_file(cur, filepath):
    """
    Reads from song file and loads data into song and artist tables
    
    Parameters:
    cur: Cursor Object (for connecting to DB)
    filepath: Source data location

    Returns:
    NO returns
    """ 
    song_files=pd.read_json(filepath, lines=True)

    song_data=song_files.loc[:, ['song_id', 'title', 'artist_id', 'year', 'duration']].values[0].tolist()
    cur.execute(song_table_insert, song_data)
    
    artist_data=song_files.loc[:, ['artist_id','artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values[0].tolist()
    cur.execute(artist_table_insert, artist_data)
    

def process_log_file(cur, filepath):
    """
    Reads from log file and loads data into time, user and songplays tables
    
    Parameters:
    cur: Cursor Object (for connecting to DB)
    filepath: Source data location

    Returns:
    NO returns
    """ 
    
    log_files=pd.read_json(filepath, lines=True)
        
    # filter by NextSong action
    log_files_filtered = log_files[log_files['page'] == "NextSong"]
    
    # convert timestamp column to datetime
    log_files_filtered['ts'] = pd.to_datetime(log_files_filtered.loc[:,'ts'],unit='ms')
    t= pd.to_datetime(log_files_filtered.loc[:,'ts'], unit='ms')
         
    # insert time data records
    time_data = [t,t.dt.hour,t.dt.day,t.dt.week,t.dt.month,t.dt.year,t.dt.weekday]
    column_labels = ['start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday']
    time_df = pd.DataFrame(dict(zip(column_labels, time_data)))
    #time_df = pd.DataFrame.from_dict(dict(zip(column_labels, time_data)))
    
    for index, row in time_df.iterrows():    
        time_data = (row.start_time,row.hour,row.day, row.week, row.month,row.year,row.weekday)
        cur.execute(time_table_insert, time_data)
        
    # load user table
    user_df = log_files_filtered.loc[:,['userId','firstName', 'lastName', 'gender', 'level']]
                               
    # insert user records
    for index, row in user_df.iterrows():
        user_data=(row.userId,row.firstName,row.lastName, row.gender,row.level)
        cur.execute(user_table_insert, user_data)
  
    # get songid and artistid from song and artist tables
    song_select_df = log_files_filtered.loc[:,['song','artist', 'length','userId','level','sessionId','location','userAgent','ts']]
    
    
    for index, row in song_select_df.iterrows():
        cur.execute(song_select, (row.song, row.artist, row.length))
        
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None
            
        starttime = pd.to_datetime(row.ts,unit='ms')
        
        # insert songplay record'
        songplay_data = (index,starttime, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        
        cur.execute(songplay_table_insert, songplay_data)
        
    

def process_data(cur, conn, filepath, func):
    """
    Gets all files matching extension from directory and Iterates over data files to apply given function to each data item
    
    Parameters:
    cur: Cursor Object (for connecting to DB)
    conn: Connection
    filepath: Source data location
    func: Function being applied

    Returns:
    NO returns
    """ 

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
    """
    Main function for manage all etl process
    """ 
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()