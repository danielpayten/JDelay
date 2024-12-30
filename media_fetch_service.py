# The purpose of this file is to provide a service that fetches media from a given URL and stores it in a given directory.
# This will continue to run until the user stops the service.

# Optionally, we can write the details of fetched media to a sqlite database.

import m3u8
import urllib
import sqlite3
import time
import os
import atexit
import shutil

playlist_url = 'https://mediaserviceslive.akamaized.net/hls/live/2038308/triplejnsw/masterhq.m3u8'
playlist_folder = './output/'
conn = sqlite3.connect('j_playlist.db')
c = conn.cursor()

# Create output folder if it does not exist
if not os.path.exists(playlist_folder):
    os.makedirs(playlist_folder)

# Setup the function to be called when the service is stopped
def cleanup():
    # Close the database connection
    conn.close()

    # Delete the playlist file contents
    shutil.rmtree(playlist_folder)
atexit.register(cleanup)






service_start_time = time.time()

while True:

    # Try to fetch the playlist up to 3 times, waiting 5 seconds between each attempt.
    try:
        playlist = m3u8.load(playlist_url)
    except urllib.error.HTTPError as e:
        print('Failed to fetch playlist. Trying again...')
        # wait and retry after 5 seconds, up to 3 times
        for i in range(3):
            time.sleep(5)
            try:
                playlist = m3u8.load(playlist_url)
                break
            except urllib.error.HTTPError as e:
                print('Failed to fetch playlist. Trying again...')
                continue
        
        # if we still can't fetch the playlist, we exit the service
        if i == 2:
            print('Failed to fetch playlist. Exiting...')
            break

    playlist = m3u8.load(playlist_url)
    segments = playlist.segments
    segments.sort(key=lambda x: x.media_sequence)

    for i, segment in enumerate(segments):

        # Get the time of the most recent segment, which has been captured
        c.execute('''SELECT MAX(segment_start_time) FROM playlist''')
        latest_fetched_segment = c.fetchone()[0]

        if latest_fetched_segment is not None:
            # can add directly as seconds and using epoch time.
            segment_start_time = latest_fetched_segment + segment.duration
        else:
            segment_start_time = service_start_time 

        # Determine if the segment has already been fetched, if so, skip.
        c.execute('''SELECT * FROM playlist WHERE segment_number = ?''', (segment.media_sequence,))
        if c.fetchone() is not None:
            continue

        
        # Download the segment
        try:
            urllib.request.urlretrieve(segment.absolute_uri, playlist_folder + segment.uri)
        except urllib.error.HTTPError as e:
            print('Failed to download segment: ' + segment.uri + ' Trying again...')
            # wait and retry after 5 seconds, up to 3 times
            for i in range(3):
                time.sleep(5)
                try:
                    urllib.request.urlretrieve(segment.absolute_uri, "." + playlist_folder + segment.uri)
                    break
                except urllib.error.HTTPError as e:
                    print('Failed to download segment: ' + segment.uri + ' Trying again...')
                    continue
    

        # Download latest metadata
        #metadata_response = urllib.request.urlopen('''https://music.abcradio.net.au/api/v1/plays/triplej/now.json?tz=Australia%2FSydney''').read().decode('utf-8')
        

        c.execute('''INSERT INTO playlist (
                        segment_number, 
                        segment_url, 
                        segment_duration, 
                        segment_start_time, 
                        segment_end_time)
                VALUES (?,?,?,?,?)''', 
                (segment.media_sequence, 
                 segment.uri, 
                 segment.duration, 
                 segment_start_time,
                 segment_start_time + segment.duration)
                )
        conn.commit()
    # We wait 10 seconds as the segments are 10 seconds in lenght. It does not make sense to check more frequently than the segments arrive.    
    time.sleep(10)





