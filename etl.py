import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *

def get_files(filepath):
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))
    
    return all_files


def process_song_file(cur, conn, filepath):
    """Process song file.
    
    Args:
        cur: cusor of psycopg2.
        filepath: song file path (json).
    """
    # Abrindo o arquivo
    df = pd.read_json(filepath, lines=True)

    # inserindo os dados do artista
    artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values.flatten().tolist()
    try:
        cur.execute(artist_table_insert, artist_data)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        cur.execute("ROLLBACK") 
    
    # Inserindo os dados da música
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']].values.flatten().tolist()
    try:
        cur.execute(song_table_insert, song_data)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        cur.execute("ROLLBACK")
    


def process_log_file(cur, conn, filepath):
    """Process log file.
    
    Args:
        curL cusor of psycopg2.
        filepath: log file path (json).
    """
    # Abrindo o arquivo
    df = pd.read_json(filepath, lines=True)

    # Filtrando pelo NextSong
    df = df[df.page == 'NextSong']

    # Convertendo o timestamp para datetime
    t = pd.to_datetime(df.ts, unit='ms')
    time_data = (df.ts, t.dt.hour, t.dt.day, t.dt.week, t.dt.month, t.dt.year, t.dt.weekday)
    column_labels = ('time', 'hour', 'day', 'week', 'month', 'year', 'weekday')
    time_df = pd.DataFrame({k: v.values.flatten().tolist() for k, v in zip(column_labels, time_data)})

    # insert time data records
    for i, row in time_df.iterrows():
        try:
            cur.execute(time_table_insert, list(row))
            conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            cur.execute("ROLLBACK")

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]


    # insert user records
    for i, row in user_df.iterrows():
        try:
            cur.execute(user_table_insert, row.values.flatten().tolist())
            conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            cur.execute("ROLLBACK")

    # insert songplay records
    for index, row in df.iterrows():
        try:
            # get songid and artistid from song and artist tables
            cur.execute(song_select, (row.song, row.artist, row.length))
            results = cur.fetchone()
            
            if results:
                songid, artistid = results
            else:
                songid, artistid = None, None

            # insert songplay record
            songplay_data = (row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
            cur.execute(songplay_table_insert, songplay_data)
            conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            cur.execute("ROLLBACK")


def process_data(cur, conn, filepath, func):
    """Process data.
    
    Args:
        cur: cursor of psycopg2.
        conn: connection of postgresql.
        filepath: directory path of data.
        func: function of processing.
    """
    # get all files matching extension from directory
    files = get_files(filepath)
    
    # get total number of files found
    n_files = len(files)

    # iterate over files and process
    for i in range(n_files):
        func(cur, conn, files[i])
        


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=ProjetoIB user=postgres password=postgres001 port=5400")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()