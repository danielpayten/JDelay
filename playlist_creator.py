import time
import signal
import m3u8
from pathlib import Path
import logging
import json
import os
from typing import List

from config import (
    OUTPUT_DIR,
    BUFFER_PERIOD_SECONDS,
    HLS_LENGTH_SECONDS,
    PlaylistSpec,
    LOG_DIR,
)

LOG_DIR.mkdir(exist_ok=True)
logging.basicConfig(
    filename=LOG_DIR / "playlist_creator.log",
    filemode="a",
    format="%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.DEBUG,
)




def get_segment_infos():
    segment_info_file = OUTPUT_DIR / "segment_info.json"
    if not os.path.exists(segment_info_file):
        return None
    with open(segment_info_file, 'r') as f:
        segment_infos = json.load(f)
    return segment_infos

    
def initialise_playlist(playlist_spec: PlaylistSpec, output_folder: Path):
    segment_infos = get_segment_infos()
    segment_metadata = segment_infos['segment_metadata']

    # Convert dictionary values to list and filter
    segments_in_delay = [
        segment for segment in segment_metadata.values()
        if segment["timestamp"] - (playlist_spec.playlist_start_time - playlist_spec.delay_seconds - BUFFER_PERIOD_SECONDS) > 0
    ]

    # Get the first segment_id which meets the delay
    first_segment = min(segments_in_delay, key=lambda x: x['timestamp'])
    playlist_spec.first_segment_id = first_segment['sequence']

    playlist_spec.is_initalised = True

    return playlist_spec

def populate_playlist(playlist_spec: PlaylistSpec, segment_infos: dict):
    segment_metadata = segment_infos['segment_metadata']
    
    # This will be run every 1 second

    # We build a list of every segment, with start and end times.
    # 
    wall_time_segment_0 = segment_metadata[str(playlist_spec.first_segment_id)]

    # We need to get the segments which are in after the wall time of the first segment
    segments_in_delay = [
        segment for segment in segment_metadata.values()
        if segment['sequence'] > playlist_spec.first_segment_id
    ]

    segments_in_delay = sorted(segments_in_delay, key=lambda x: x['sequence'])

    # Build a table of segments with start and end times, with the first segment being the wall time of the first segment,
    # then recursively adding the duration of the previous segment to the start time of the next segment.
    segments_table = [
        {
            'sequence': wall_time_segment_0['sequence'],
            'start_time': wall_time_segment_0['timestamp'],
            'end_time': wall_time_segment_0['timestamp'] + wall_time_segment_0['duration'],
            'filename': wall_time_segment_0['filename'],
            'duration': wall_time_segment_0['duration']
        }
    ]
    
    for segment in segments_in_delay:
        segments_table.append({
            'sequence': segment['sequence'],
            'start_time': segments_table[-1]['end_time'],
            'end_time': segments_table[-1]['end_time'] + segment['duration'],
            'filename': segment['filename'],
            'duration': segment['duration']
        })
    
    # We need to get the segments which are in the delay

    broadcast_time = time.time() - playlist_spec.delay_seconds - BUFFER_PERIOD_SECONDS
    # We need to get the segments which are in the delay
    output_segments = [
        segment for segment in segments_table
        if segment["start_time"] <= broadcast_time <= segment["end_time"] + HLS_LENGTH_SECONDS
    ]

    # Create temporary file
    playlist_path = OUTPUT_DIR / f"playlist_{playlist_spec.delay_seconds}.m3u8"
    temp_file = playlist_path.with_suffix('.m3u8.tmp')
    
    # If there are no segments, we don't need to write a playlist
    if len(output_segments) > 0:
        try:
            # Write to temporary file
            with open(temp_file, 'w') as f:
                f.write('#EXTM3U\n')
                f.write('#EXT-X-VERSION:3\n')
                f.write('#EXT-X-TARGETDURATION:11\n')
                f.write(f'#EXT-X-MEDIA-SEQUENCE:{output_segments[0]["sequence"]}\n')
                for segment in output_segments:
                    f.write(f'#EXTINF:{segment["duration"]}\n')
                    f.write(f'{segment["filename"]}\n')
            
            # Atomic rename operation
            temp_file.replace(playlist_path)
        
        except Exception as e:
            logging.error(f"Error writing playlist: {str(e)}")
            # Clean up temp file if it exists
            if temp_file.exists():
                temp_file.unlink()


def main(playlists_spec: List[PlaylistSpec]):
    """Main function for playlist creator process. Handles SIGTERM/SIGINT for graceful shutdown."""
    spec_file = OUTPUT_DIR / "playlist_spec.json"
    shutdown_requested = False

    def request_shutdown(signum, frame):
        nonlocal shutdown_requested
        shutdown_requested = True
        logging.info("Shutdown requested (signal %s), saving state and exiting...", signum)

    signal.signal(signal.SIGTERM, request_shutdown)
    signal.signal(signal.SIGINT, request_shutdown)

    # If the playlist spec file exists, resume from where we left off (convert dicts to PlaylistSpec)
    if spec_file.exists():
        try:
            with open(spec_file, "r") as f:
                data = json.load(f)
            playlists_spec = [PlaylistSpec(**s) for s in data]
        except Exception as e:
            logging.warning("Could not load playlist_spec.json, using env/defaults: %s", e)

    specs_to_run = []
    try:
        while not shutdown_requested:
            current_time = time.time()

            # Process the list of playlists which have the delays met
            for initial_spec in playlists_spec:
                if not initial_spec.is_initalised and current_time >= initial_spec.playlist_start_time + initial_spec.delay_seconds + BUFFER_PERIOD_SECONDS:
                    initial_spec = initialise_playlist(initial_spec, OUTPUT_DIR)
                    specs_to_run.append(initial_spec)

            # If there are any playlists to run, we get the segment infos
            if (OUTPUT_DIR / "segment_info.json").exists() and len(specs_to_run) > 0:
                segment_infos = get_segment_infos()
            else:
                segment_infos = None

            if segment_infos:
                for run in specs_to_run:
                    populate_playlist(run, segment_infos)

            # Save the playlist spec so we can resume after a crash or restart
            with open(spec_file, "w") as f:
                json.dump([vars(spec) for spec in playlists_spec], f)

            time.sleep(1)
    finally:
        # Final save on shutdown so restarts pick up latest state
        try:
            with open(spec_file, "w") as f:
                json.dump([vars(spec) for spec in playlists_spec], f)
            logging.info("Saved playlist spec on shutdown")
        except Exception as e:
            logging.warning("Could not save playlist spec on shutdown: %s", e)


if __name__ == "__main__":
    # Get playlists to create from environment variable (set by main.py when spawning)
    playlists_spec = []
    if "PLAYLISTS_SPEC" in os.environ:
        playlists_spec = [PlaylistSpec(**s) for s in json.loads(os.environ["PLAYLISTS_SPEC"])]
    main(playlists_spec)
