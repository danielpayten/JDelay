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
import json
import sys

logging.basicConfig(filename='main.log',
                    filemode='a',
                    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    level=logging.DEBUG)

# Global variables
ffmpeg_process = None
playlist_process = None

playlist_folder = './output/'
playlist_url = 'https://mediaserviceslive.akamaized.net/hls/live/2038308/triplejnsw/index.m3u8'

delays = [2,5,10, 30] + list(range(60, 24*60, 60))

def start_playlist_creator():
    """Start the playlist creator subprocess"""
    global playlist_process
    
    try:
        playlist_process = subprocess.Popen(
            [sys.executable, 'playlist_creator.py'],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True
        )
        logging.info("Started playlist creator process")
        return playlist_process
    except Exception as e:
        logging.error(f"Failed to start playlist creator: {str(e)}")
        raise

def start_ffmpeg_stream(playlist_url, output_dir,restart_id = 0):
    """Start FFmpeg process to download stream segments"""
    global ffmpeg_process
    
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    cmd = [
        'ffmpeg',
        '-i', playlist_url,
        '-hls_time', '10',
        '-live_start_index', '0',
        '-strftime', '1',
        '-hls_start_number_source', 'generic',
        '-start_number', f'{str(restart_id)}',
        '-hls_flags', 'second_level_segment_index+second_level_segment_duration',
        # Audio encoding parameters for AAC-LC
        '-c:a', 'aac',           # Use AAC encoder
        '-b:a', '192k',          # Bitrate for AAC
        '-ar', '48000',          # Sample rate (standard for AAC)
        '-ac', '2',              # Number of channels (stereo)
        '-profile:a', 'aac_low', # Use AAC-LC profile
        '-hls_segment_filename', f'{output_dir}segment_%%t_%s_%%04d.m4a',
        # Helps with long-running processes
        '-ignore_io_errors', '1',
        f'{output_dir}out.m3u8'
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
    """Cleanup function to terminate ffmpeg process"""
    global ffmpeg_process, playlist_process
    
    if ffmpeg_process:
        ffmpeg_process.terminate()
        try:
            ffmpeg_process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            logging.warning('FFmpeg process did not terminate gracefully, forcing...')
            ffmpeg_process.kill()

def cleanup_playlist_creator():
    """Cleanup function to terminate playlist process"""
    if playlist_process:
        playlist_process.terminate()
        try:
            playlist_process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            logging.warning('Playlist creator process did not terminate gracefully, forcing...')
            playlist_process.kill()

def cleanup_processes():
    """Cleanup function to terminate all processes"""
    cleanup_ffmpeg()
    cleanup_playlist_creator()

def signal_handler(signum, frame):
    """Signal handler for graceful shutdown"""
    logging.info(f'Received signal {signum}. Initiating shutdown...')
    cleanup_processes()
    exit(0)

# Register cleanup and signal handlers
atexit.register(cleanup_processes)
signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGINT, signal_handler)

# Create output folder if it does not exist
if not os.path.exists(playlist_folder):
    os.makedirs(playlist_folder)


def get_restart_id():
    """Get the latest segment id"""
    segments = Path(playlist_folder).glob('*.m4a')

    # Get the latest segment id
    max_id = max(
        int(segment.name.split('_')[3].split('.')[0])
        for segment in segments
    )

    return max_id + 1

def monitor_ffmpeg():
    """Monitor FFmpeg process and restart if needed"""
    global ffmpeg_process
    
    if ffmpeg_process is None or ffmpeg_process.poll() is not None:
        logging.warning("FFmpeg process not running, restarting...")
        restart_id = get_restart_id()
        logging.info(f'Restarting FFmpeg with segment id: {restart_id}')
        start_ffmpeg_stream(playlist_url, playlist_folder, restart_id)
        # Wait for 10 seconds for ffmpeg to start.
        time.sleep(10)
        return False
    

    
    try:
        segments = [f for f in os.listdir(playlist_folder) if f.endswith('.m4a')]
        if segments:
            latest_segment = max(segments, key=lambda x: os.path.getmtime(os.path.join(playlist_folder, x)))
            mtime = os.path.getmtime(os.path.join(playlist_folder, latest_segment))
            if time.time() - mtime > 60:
                logging.error("No new segments created in 60 seconds, restarting FFmpeg")
                cleanup_ffmpeg()
                restart_id = get_restart_id()
                logging.info(f'Restarting FFmpeg with segment id: {restart_id}')
                start_ffmpeg_stream(playlist_url, playlist_folder, restart_id)
                return False
    except Exception as e:
        logging.error(f"Error monitoring segments: {str(e)}")
        return False
    
    return True

# main loop
def main():
    # Create output directory
    if not os.path.exists(playlist_folder):
        os.makedirs(playlist_folder)

    # Start FFmpeg process
    start_ffmpeg_stream(playlist_url, playlist_folder, 0)
    
    # Start playlist creator process
    start_playlist_creator()

    # Main monitoring loop
    while True:
        try:
            monitor_ffmpeg()
            time.sleep(1)
        except Exception as e:
            logging.error(f"Error in main loop: {str(e)}")
            time.sleep(1)

if __name__ == "__main__":
    main()
    
    