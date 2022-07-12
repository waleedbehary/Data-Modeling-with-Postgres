import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *

def get_files(filepath):
    """
    Description:
        This function accountable to get all files within specific location and specific extension
        Arguments:
            filepath: this is the path for the current proccessing file/s
        Returns:
            Nothing
    """
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))
    
    return all_files

def process_song_file(cur, filepath):
    """
    Description:
        This function accountable to process the song_files and apply needed transformation within the returned values
        Arguments:
            cur: this is curser object
            filepath: this is the path for the current proccessing file
        Returns:
            Nothing
        
    """
    # open song file
    song_files = get_files('data/song_data')
    filepath = song_files[0]
    df = pd.read_json (filepath, lines=True)
    #df.head(10)

    # insert song record
    dfe=df[['song_id','title','artist_id','year','duration']]
    song_data = dfe.values.tolist()
    song_data = song_data[0]
    #song_data = 
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    dft=df[['artist_id','artist_name','artist_location',\
            'artist_latitude','artist_longitude']]
    artist_data = dft.values.tolist()
    artist_data = artist_data[0]
    #artist_data = 
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    Description:
        This function accountable to process the log_files and apply needed transformation within the returned values
        Arguments:
            cur: this is curser object
            filepath: this is the path for the current proccessing file
            
        Returns:
            Nothing
        
    """
    # open log file
    log_files = get_files('data/log_data')
    filepath = log_files[0]
    df = pd.read_json (filepath, lines=True)

    # filter by NextSong action
    df = df[df['page']=='NextSong']
    #df.head()

    # convert timestamp column to datetime
    df['timestamp'] = pd.to_datetime(df['ts'], unit='ms')
    df['hour'] = df['timestamp'].dt.hour
    df['day']=df['timestamp'].dt.day
    df['week_of_year'] =df['timestamp'].dt.week
    df['month'] = df['timestamp'].dt.month
    df['year'] = df['timestamp'].dt.year
    df['weekday'] = df['timestamp'].dt.weekday_name
    t = df[['timestamp','hour','day','week_of_year',
                    'month','year','weekday']]
    
    # insert time data records
   # time_data = 
    column_labels = ('TimeStamp','Hour','Day','Week Of Year'
                     ,'Month','Year','Weekday')
    #time_df =
    t=pd.to_datetime(df['ts'], unit='ms')
    time_data = [t, t.dt.hour, t.dt.day, t.dt.week, t.dt.month,
                 t.dt.year, t.dt.dayofweek]
    column_labels = ['Start Time','Hour', 'Day', 'Week', 'Month',
                     'Year', 'Weekday']
    time_df = pd.DataFrame(dict(zip(column_labels, time_data)))
    time_df.head()

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId','firstName','lastName','gender','level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (pd.to_datetime(row.ts, unit='ms'),
                        int(row.userId),
                        row.level,
                        songid,
                        artistid,
                        row.sessionId,
                        row.location,
                        row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)

def process_data(cur, conn, filepath, func):
    # get all files matching extension from directory
    """
    Description:
    - This section of the function accountable to get all files matching extension *.json from specific directory
    Arguments:
        conn: connection the database
        cur: the cursor object
        filepath: the path of files. In this project its, log data or song data file path
        func: function that transforms the data and inserts it into the database
    
    Returns:
        Nothing
    """
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    """
    Description:
        This part gets the total number of found files
    """
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    """
    Description:
        This part iterate over defined files and process the findings
    """
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """
    Description:
        - Create a connection to the databse
        - Create cursor object
        - Call function to process data from filepath 'data/song_data'
        - Call function to process data from filepath 'data/log_data'
        - Close the connection
        
        Arguments:
            conn: connection the database
            cur: the cursor object
            filepath: log data or song data file path
            func: function that transforms the data and inserts it into the database
            
            Returns:
                None
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
