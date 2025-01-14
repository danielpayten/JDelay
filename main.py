# Note, the legal basis for this is that the media is already public and users simply time-shift the media.
# Refer to National Rugby League Investments Pty Limited v Singtel Optus Pty Ltd (2012) (Federal Court of Australia)
# The court ruled that Optus' TV Now service did not infringe copyright because it was the user, not Optus, who was responsible for making the recording.
# It does not matter that the recording is made on Optus' servers, because the user is the one who initiates the recording.


# We start the media fetch service
# We then read from the SQLite db, publishing a delayed stream
# at the intervals:
# 10 min
# 30 min
# 1,2,3,4,5... 24 hr dealyed.

import sqlite3
import time
import m3u8
import subprocess
import signal
from ctypes import cdll
import os
import logging

logging.basicConfig(filename='main.log',
                    filemode='a',
                    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    level=logging.DEBUG)



playlist_folder = './output/'
# Create output folder if it does not exist
if not os.path.exists(playlist_folder):
    os.makedirs(playlist_folder)

# Connect to the database
conn = sqlite3.connect('j_playlist.db')
c = conn.cursor()
c.execute('''DROP TABLE IF EXISTS playlist''')
c.execute('''CREATE TABLE IF NOT EXISTS playlist (segment_number INTEGER PRIMARY KEY, segment_url TEXT, segment_duration INTEGER, segment_start_time REAL, segment_end_time REAL)''')
conn.commit()


# Start the media fetch service do this on a separate thread
# python media_fetch_service.py
# kill the process in the event that this script is stopped.



# Constant taken from http://linux.die.net/include/linux/prctl.h
PR_SET_PDEATHSIG = 1

class PrCtlError(Exception):
    pass

def on_parent_exit(signame):
    """
    Return a function to be run in a child process which will trigger SIGNAME
    to be sent when the parent process dies
    """
    signum = getattr(signal, signame)
    def set_parent_exit_signal():
        # cleanup the child process when the parent dies

        # http://linux.die.net/man/2/prctl
        result = cdll['libc.so.6'].prctl(PR_SET_PDEATHSIG, signum)
        if result != 0:
            raise PrCtlError('prctl failed with error code %s' % result)

    return set_parent_exit_signal

subprocess.Popen(['python', 'media_fetch_service.py'], preexec_fn=on_parent_exit('SIGHUP'))




delays = [1,10, 30] + list(range(60, 24*60, 60))

# We now generat each of the playlists, where the requisite delay is added to the start time of each segment.
# We can only do this for delays where the maximum delay is greater than the delay.


# Create playlist
def create_playlist(delay_minutes,c,conn):
    m3u8_obj = m3u8.M3U8()
    m3u8_obj.version = 3
    # We use 10.00780 as the target duration as this is the duration of the segments in the playlist.
    m3u8_obj.target_duration = 10
    

    now = time.time()

    # We populate the new m3u8 object with the segments that are within the delay_minutes and m3u8_length
    c.execute('''
              SELECT
               * 
              FROM playlist 
              WHERE segment_start_time > ?
              ORDER BY 
                segment_number
              LIMIT 
                30
              ''',(now - delay_minutes*60,))
    segments = c.fetchall()
    conn.commit()

    # If there are no segments skip
    if len(segments) == 0:
        logging.info(f'No segments for delay: {delay_minutes}')
        logging.info('SQL Query: ' + '''
              SELECT
               *
               FROM playlist
               WHERE segment_start_time > {start_time}
               ORDER BY
                segment_number
                LIMIT
                30
                '''.format(start_time = now - delay_minutes*60))
                
        return
    first_id = segments[0][0]
    m3u8_obj.media_sequence = first_id

    
    for segment in segments:
        segment_number, segment_url, segment_duration, segment_start_time, segment_end_time = segment
        segment = m3u8.Segment()
        segment.uri = segment_url
        segment.duration = segment_duration
        m3u8_obj.add_segment(segment)
    
    # clear the existing playlist file
    if os.path.exists(playlist_folder + f'playlist_{delay_minutes}.m3u8'):
        os.remove(playlist_folder + f'playlist_{delay_minutes}.m3u8')

    # write the new playlist file with the new playlist
    with open(playlist_folder + f'playlist_{delay_minutes}.m3u8', 'w') as f:
        f.write(m3u8_obj.dumps())


while True:

    # Get the start time of the service
    c.execute('''SELECT MIN(segment_start_time) FROM playlist''')
    service_start_time = c.fetchone()[0]
    conn.commit()
    if service_start_time is None:
        time.sleep(10)
        print('Waiting for service to start...')
        continue

    # From this start time, we can determine the maximum delay that can be achieved.
    # The maximum delay is always less than 24 hrs as we delete segments older than 24 hrs.

    # We can determine the maximum delay by subtracting the current time from the start time.

    maximum_delay = time.time() - service_start_time
    print(f'Maximum delay: {maximum_delay/60/60} hrs')


    for delay in delays:
        if maximum_delay > delay*60:
            create_playlist(delay, c,conn)
        continue
    time.sleep(10)
    # We need to check if the service is still running
    # Todo: Implement a check to see if the service is still running.

# We can now serve the m3u8 and media files using a web server.



    
    