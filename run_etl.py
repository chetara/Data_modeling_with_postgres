import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *

def process_data(cur, conn, filepath, func):
    # Recursively get all JSON files
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        all_files.extend(files)

    print(f'{len(all_files)} files found in {filepath}')

    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)  # Run either process_song_file or process_log_file
        conn.commit()
        print(f'{i}/{len(all_files)} files processed.')

def process_song_file(cur, filepath):
    # Read JSON file
    df = pd.read_json(filepath, lines=True)

# Extract and insert artist data
    artist_data = df[['artist_id', 'artist_name', 'artist_location', 
                      'artist_latitude', 'artist_longitude']].values[0].tolist()
    cur.execute(artist_table_insert, artist_data)
    
    # Extract and insert song data
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0].tolist()
    cur.execute(song_table_insert, song_data)

    

def process_log_file(cur, filepath):
    df = pd.read_json(filepath, lines=True)

    # Filter by 'NextSong' actions only
    df = df[df['page'] == 'NextSong']

    # Convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')

    # Create time data records
    time_data = {
        'start_time': t,
        'hour': t.dt.hour,
        'day': t.dt.day,
        'week': t.dt.isocalendar().week,
        'month': t.dt.month,
        'year': t.dt.year,
        'weekday': t.dt.weekday
    }

    time_df = pd.DataFrame(time_data)

    # Insert into time table
    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # Load and insert user data
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # Insert songplay records (FACT TABLE)
    for index, row in df.iterrows():
        # Get song_id and artist_id from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        result = cur.fetchone()

        song_id, artist_id = result if result else (None, None)

        # Insert songplay record
        songplay_data = (
            pd.to_datetime(row.ts, unit='ms'),
            row.userId,
            row.level,
            song_id,
            artist_id,
            row.sessionId,
            row.location,
            row.userAgent
        )
        cur.execute(songplay_table_insert, songplay_data)

def main():
    # Connect to database
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=postgres password=*****")
    cur = conn.cursor()

    # Process song_data first (dimension tables)
    process_data(cur, conn, filepath='data/song_data', func=process_song_file)

    # Then process log_data (fact table + time/user dimensions)
    process_data(cur, conn, filepath= 'data/log_data', func=process_log_file)

    # Done!
    conn.close()

if __name__ == "__main__":
    main()

