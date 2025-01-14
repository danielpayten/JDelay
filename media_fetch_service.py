# The purpose of this file is to provide a service that fetches media from a given URL and stores it in a given directory.
# This will continue to run until the user stops the service.

# Optionally, we can write the details of fetched media to a sqlite database.

from sys import exception
import m3u8
import urllib
import sqlite3
import time
import os
import atexit
import shutil
import logging

# Setup logging for the service
logging.basicConfig(filename='media_fetch_service.log',
                    filemode='a',
                    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    level=logging.DEBUG)



playlist_url = 'https://mediaserviceslive.akamaized.net/hls/live/2038308/triplejnsw/masterhq.m3u8'
playlist_folder = './output/'
conn = sqlite3.connect('j_playlist.db')
c = conn.cursor()

# Create output folder if it does not exist
if not os.path.exists(playlist_folder):
    logging.info('Creating output folder: ' + playlist_folder)
    os.makedirs(playlist_folder)

# Setup the function to be called when the service is stopped
def cleanup():
    # Close the database connection
    conn.close()

    # Delete the playlist file contents
    shutil.rmtree(playlist_folder)
atexit.register(cleanup)






service_start_time = time.time()
logging.info('Service started at: ' + str(service_start_time))

while True:

    # Try to fetch the playlist up to 5 times, waiting 5 seconds between each attempt.
    for attempt in range(6):
        try:
            playlist = m3u8.load(playlist_url)
        except urllib.error.HTTPError as e:
            if attempt == 5:
                print('Failed to fetch playlist after 5 attempts, exiting.')
                logging.error('Failed to fetch playlist after 5 attempts, exiting.')
                raise exception('Failed to fetch playlist after 5 attempts.')
            else:
                print('Failed to fetch playlist. Retrying...')
                logging.warning('Failed to fetch playlist. Retrying...')
                time.sleep(5)
                continue
        else:
            break

    playlist = m3u8.load(playlist_url)
    segments = playlist.segments
    segments.sort(key=lambda x: x.media_sequence)

    for i, segment in enumerate(segments):

        # Get the time of the most recent segment, which has been captured
        c.execute('''SELECT MAX(segment_start_time) FROM playlist''')
        most_recent_segment_time = c.fetchone()
        
        logging.info('Most recent segment time: ' + str(most_recent_segment_time))

        if  most_recent_segment_time[0] is not None:
            # can add directly as seconds and using epoch time.
            segment_start_time =  most_recent_segment_time[0] + segment.duration
        else:
            segment_start_time = service_start_time 

        # Determine if the segment has already been fetched, if so, skip.
        c.execute('''SELECT * FROM playlist WHERE segment_number = ?''', (segment.media_sequence,))
        
        if c.fetchone() is not None:
            # Segment has already been fetched, skip
            continue

        # Download the segment, attempting up to 5 times, otherwise skippipng the segment.
        for attempt in range(5):
            try:
                urllib.request.urlretrieve(segment.absolute_uri, playlist_folder + segment.uri)
            except urllib.error.HTTPError as e:
                print('Failed to download segment: ' + segment.uri + ' Trying again...')
                logging.warning('Failed to download segment: ' + segment.uri + ' Trying again...')
                time.sleep(5)
                continue
            else:
                break

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
        logging.info('Segment ' + str(segment.media_sequence) + ' fetched and stored in database.' + ' Segment start time: ' + str(segment_start_time) + ' Segment end time: ' + str(segment_start_time + segment.duration))
    # We wait 10 seconds as the segments are 10 seconds in lenght. It does not make sense to check more frequently than the segments arrive.    
    time.sleep(10)





