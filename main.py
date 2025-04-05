# Note, the legal basis for this is that the media is already public and users simply time-shift the media.
# Refer to National Rugby League Investments Pty Limited v Singtel Optus Pty Ltd (2012) (Federal Court of Australia)
# The court ruled that Optus' TV Now service did not infringe copyright because it was the user, not Optus, who was responsible for making the recording.
# It does not matter that the recording is made on Optus' servers, because the user is the one who initiates the recording.

from dataclasses import dataclass
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

delays_seconds = [60*x for x in ([2,5,10, 30] + list(range(60, 24*60, 60)))]
buffer_period_seconds = 1 * 60 # 1 minute
hls_length = 1.5 * 60 # 1.5 minutes

@dataclass
class PlaylistSpec:
    delay_seconds: int
    playlist_start_time: float
    playlist_file_name: str
    first_segment_id: int
    is_initalised: bool = False
    is_running: bool = False


def start_segment_downloader():
    """Start the segment downloader subprocess"""
    global segment_downloader_process
    
    try:
        segment_downloader_process = subprocess.Popen(
            [sys.executable, './segment_downloader.py'],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True
        )
        logging.info("Started segment downloader process")
        return segment_downloader_process
    except Exception as e:
        logging.error(f"Failed to start segment downloader: {str(e)}")
        raise

def cleanup_segment_downloader():
    """Cleanup function to terminate segment downloader process"""
    if segment_downloader_process:
        segment_downloader_process.terminate()
        try:
            segment_downloader_process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            logging.warning('Segment downloader process did not terminate gracefully, forcing...')
            segment_downloader_process.kill()


def start_playlist_creator(playlists_spec):
    """Start the playlist creator subprocess"""
    global playlist_process
    try:
        # Convert PlaylistSpec objects to dictionaries
        playlists_dict = [vars(spec) for spec in playlists_spec]
        env = os.environ.copy()
        env['PLAYLISTS_SPEC'] = json.dumps(playlists_dict)
        
        playlist_process = subprocess.Popen(
            [sys.executable, './playlist_creator.py'],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True
        )
        
        # Check if process started successfully
        if playlist_process.poll() is not None:
            stderr = playlist_process.stderr.read()
            logging.error(f"Playlist creator failed to start: {stderr}")
            raise Exception("Playlist creator process failed to start")
            
        logging.info("Started playlist creator process")
    except Exception as e:
        logging.error(f"Failed to start playlist creator: {str(e)}")
        raise


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
    cleanup_segment_downloader()
    cleanup_playlist_creator()
    
def check_process_health(process, process_name, folder=None, file_extension=None, timeout_seconds=60):
    """
    Check if a process is healthy and restart if necessary
    
    Args:
        process: The subprocess to check
        process_name: Name of the process for logging
        folder: Folder to check for new files (optional)
        file_extension: File extension to check for (optional)
        timeout_seconds: Timeout in seconds before considering process unhealthy
        
    Returns:
        bool: True if process is healthy, False if it needs to be restarted
    """
    # Check if process is still running
    if process.poll() is not None:
        logging.error(f"{process_name} process died unexpectedly")
        return False
    
    # If folder and file extension are provided, check for new files
    if folder and file_extension:
        current_time = time.time()
        try:
            # Get most recent file modification time
            files = [f for f in os.listdir(folder) if f.endswith(file_extension)]
            if files:
                newest_file = max(
                    files,
                    key=lambda f: os.path.getmtime(os.path.join(folder, f))
                )
                last_file_time = os.path.getmtime(os.path.join(folder, newest_file))
                
                # If no new files in the specified timeout period, consider process unhealthy
                if current_time - last_file_time > timeout_seconds:
                    logging.warning(f"No new {file_extension} files in last {timeout_seconds} seconds. {process_name} may be stuck.")
                    return False
        except Exception as e:
            logging.error(f"Error checking {process_name} files: {str(e)}")
    
    return True

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


# main loop
def main():
    start_time = time.time()
    # Create output directory
    if not os.path.exists(playlist_folder):
        os.makedirs(playlist_folder)

    start_segment_downloader()

    playlists_to_create = []
    for delay in delays_seconds:
        playlists_to_create.append(PlaylistSpec(delay, start_time, f'playlist_{delay}.m3u8', first_segment_id=None, is_initalised= False))
    
    start_playlist_creator(playlists_to_create)

    # Keep main process running and monitor subprocesses
    try:
        while True:
            # Check segment downloader health
            if not check_process_health(
                segment_downloader_process, 
                "Segment downloader", 
                playlist_folder, 
                '.aac', 
                60
            ):
                logging.warning("Restarting segment downloader...")
                cleanup_segment_downloader()
                start_segment_downloader()
                # Give segment downloader time to start and download segments.
                time.sleep(10)
            
            # Check playlist creator health
            if not check_process_health(
                playlist_process, 
                "Playlist creator",
                playlist_folder,
                '.m3u8',
                60
            ):
                logging.warning("Restarting playlist creator...")
                cleanup_playlist_creator()
                start_playlist_creator(playlists_to_create)
                
            time.sleep(1)  # Check every second
    except KeyboardInterrupt:
        logging.info("Received keyboard interrupt, shutting down...")
    finally:
        cleanup_processes()


if __name__ == "__main__":
    main()
    
    