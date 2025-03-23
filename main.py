# Note, the legal basis for this is that the media is already public and users simply time-shift the media.
# Refer to National Rugby League Investments Pty Limited v Singtel Optus Pty Ltd (2012) (Federal Court of Australia)
# The court ruled that Optus' TV Now service did not infringe copyright because it was the user, not Optus, who was responsible for making the recording.
# It does not matter that the recording is made on Optus' servers, because the user is the one who initiates the recording.

import time
import m3u8
import subprocess
from pathlib import Path
import signal
import os
import logging
import atexit
from http.server import HTTPServer, SimpleHTTPRequestHandler
import threading

logging.basicConfig(filename='main.log',
                    filemode='a',
                    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    level=logging.DEBUG)

# Global variable to store the subprocess reference
ffmpeg_process = None

playlist_folder = './output/'
playlist_url = 'https://mediaserviceslive.akamaized.net/hls/live/2038308/triplejnsw/index.m3u8'



def start_ffmpeg_stream(playlist_url, output_dir,restart_id = 0):
    """Start FFmpeg process to download stream segments"""
    global ffmpeg_process
    
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    cmd = [
        'ffmpeg',
        '-i', playlist_url,
        '-hls_time', '10',
        '-strftime', '1',         # Enable timestamp in filename
        '-hls_start_number_source', 'generic',
        '-start_number', f'{str(restart_id)}',
        '-hls_flags', 'second_level_segment_index+second_level_segment_duration',
        '-hls_segment_filename', f'{output_dir}segment_%%t_%s_%%04d.ts',
        f'{output_dir}out.m3u8'  # Output pattern
    ]
    
    try:
        ffmpeg_process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True
        )
        logging.info("Started FFmpeg process")
        return ffmpeg_process
    except Exception as e:
        logging.error(f"Failed to start FFmpeg: {str(e)}")
        raise

def cleanup_ffmpeg():
    """Cleanup function to terminate the FFmpeg process"""
    global ffmpeg_process
    if ffmpeg_process:
        ffmpeg_process.terminate()
    try:
        ffmpeg_process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        logging.warning('FFmpeg process did not terminate gracefully, forcing...')
        ffmpeg_process.kill()
        

def signal_handler(signum, frame):
    """Signal handler for graceful shutdown"""
    logging.info(f'Received signal {signum}. Initiating shutdown...')
    cleanup_ffmpeg()
    exit(0)

# Register the cleanup function to run on normal program termination
atexit.register(cleanup_ffmpeg)

# Register signal handlers
signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGINT, signal_handler)




# Create output folder if it does not exist
if not os.path.exists(playlist_folder):
    os.makedirs(playlist_folder)

# We start the ffmpeg process, which will download the stream segments.
# Start time is used to calculate the maximum delay.
start_ffmpeg_stream(playlist_url, playlist_folder,0)


delays = [1,5,10, 30] + list(range(60, 24*60, 60))

# We now generate each of the playlists, where the requisite delay is added to the start time of each segment.
# We can only do this for delays where the maximum delay is greater than the delay.


class SegmentInfo:
    def __init__(self, segment_file_name, segment_id, segment_length, segment_timestamp):
        self.segment_file_name = segment_file_name
        self.segment_id = segment_id
        self.segment_length = segment_length
        self.segment_timestamp = segment_timestamp

def parse_segment_info(segment_file_name):
    # Parse the segment info from the segment file name
    segment_file_name = segment_file_name.name
    segment_id = int(segment_file_name.split('_')[3].split('.')[0])
    segment_length = float(segment_file_name.split('_')[1])/1000000
    segment_timestamp = float(segment_file_name.split('_')[2])

    return SegmentInfo(segment_file_name, segment_id, segment_length, segment_timestamp)


# Create playlist
def create_playlist(delay_minutes, playlist_folder, output_file_name):
    # Get list of segment files
    now = time.time()
    segments = Path(playlist_folder).glob('*.ts')
    segments_with_info = [parse_segment_info(segment) for segment in segments]

    segments_within_delay = filter(lambda x: x.segment_timestamp >= now - delay_minutes*60, segments_with_info)
    segments_within_delay = sorted(segments_within_delay, key=lambda x: (x.segment_timestamp, x.segment_id))


    logging.info(f'Segments within delay: {len(segments_within_delay)}')
    if len(segments_within_delay) >= 10:
        # Create m3u8 object
        m3u8_obj = m3u8.M3U8()
        m3u8_obj.version = 3
        m3u8_obj.target_duration = 10
        m3u8_obj.media_sequence = segments_within_delay[0].segment_id

        # Add the first 6 segments to the playlist
        for segment in segments_within_delay[:6]:
            m3u8_segment = m3u8.Segment()
            m3u8_segment.uri = segment.segment_file_name
            m3u8_segment.duration = segment.segment_length
            m3u8_obj.add_segment(m3u8_segment)

        # overwrite the playlist file
        with open(output_file_name, 'w') as f:
            f.write(m3u8_obj.dumps())


def get_restart_id():
    # Get the latest segment id
    segments = Path(playlist_folder).glob('*.ts')
    segments_with_info = [parse_segment_info(segment) for segment in segments]

    # If there are no segments, we start from 0
    if len(segments_with_info) == 0:
        return 0
    
    # If there are segments, we start from the next segment id
    else:
        return max([segment.segment_id for segment in segments_with_info])+1


def monitor_ffmpeg():
    """Monitor FFmpeg process and restart if needed"""
    global ffmpeg_process
    
    if ffmpeg_process is None or ffmpeg_process.poll() is not None:
        logging.warning("FFmpeg process not running, restarting...")
        
        # We need to restart the ffmpeg process with the next segment id.
        restart_id = get_restart_id()
        logging.info(f'Restarting FFmpeg with segment id: {restart_id}')
        start_ffmpeg_stream(playlist_url, playlist_folder,restart_id)
        return False
    
    # Check if new segments are being created
    try:
        # Get the latest segment file's modification time
        segments = [f for f in os.listdir(playlist_folder) if f.endswith('.ts')]
        if segments:
            latest_segment = max(segments, key=lambda x: os.path.getmtime(os.path.join(playlist_folder, x)))
            mtime = os.path.getmtime(os.path.join(playlist_folder, latest_segment))
            if time.time() - mtime > 60:  # No new segments in 60 seconds
                logging.error("No new segments created in 30 seconds, restarting FFmpeg")
                cleanup_ffmpeg()

                restart_id = get_restart_id()
                logging.info(f'Restarting FFmpeg with segment id: {restart_id}')
                start_ffmpeg_stream(playlist_url, playlist_folder,restart_id)
                return False
    except Exception as e:
        logging.error(f"Error monitoring segments: {str(e)}")
        return False
    
    return True

def parse_segment_times(playlist_folder):
    
    # Parse the segment times from the segment files
    segments = [f for f in os.listdir(playlist_folder) if f.endswith('.ts')]
    if len(segments) == 0:
        return 0
    
    segment_times = []
    for segment in segments:
        segment_times.append(float(segment.split('_')[-2]))
    
    # Calculate the maximum delay
    maximum_delay_minutes = (max(segment_times) - min(segment_times))/60
    return maximum_delay_minutes


# main loop


def main_loop():
    while True:
        try:
            monitor_ffmpeg()
            # Create playlists for different delays
            
           #  Maximum delay is calculated as the time since the first segment was created.
            maximum_delay_minutes = parse_segment_times(playlist_folder)
            logging.info(f'Maximum delay: {maximum_delay_minutes}')
            print(f'Maximum delay: {maximum_delay_minutes}')
        
            for delay_minutes in delays:
                if maximum_delay_minutes > delay_minutes:
                    logging.info(f'Creating playlist for delay: {delay_minutes}')
                    output_file_name = os.path.join(playlist_folder, f'playlist_{delay_minutes}.m3u8')
                    create_playlist(delay_minutes,playlist_folder,output_file_name)
            
            time.sleep(1)
        except Exception as e:
            logging.error(f"Error in main loop: {str(e)}")

main_loop()
    
    