import time
import m3u8
from pathlib import Path
import logging
import sys
from dataclasses import dataclass
from typing import List
import main


# Configure logging
logging.basicConfig(
    filename='/Users/danielpayten/Desktop/JDelay Logs/JDelay/playlist_creator.log',
    filemode='a',
    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    level=logging.DEBUG
)

playlist_folder = main.playlist_folder
delays = main.delays


class SegmentInfo:
    def __init__(self, filename: str, segment_id: int, duration: float, timestamp: float):
        self.filename = filename
        self.segment_id = segment_id
        self.duration = duration
        self.timestamp = timestamp

def parse_segment_info(segment_path: Path) -> SegmentInfo:
    """Parse segment information from filename"""
    parts = segment_path.name.split('_')
    segment_id = int(parts[3].split('.')[0])
    duration = float(parts[1]) / 1000000
    timestamp = float(parts[2])
    return SegmentInfo(segment_path.name, segment_id, duration, timestamp)

def create_playlist(delay_minutes: int, segments: List[SegmentInfo], output_file: Path):
    """Create a single playlist file"""
    segment_buffer = 10
    segment_duration = 11
    
    try:
        now = time.time()
        delay_seconds = delay_minutes * 60
        
        # Filter segments within delay window
        segments_within_delay = [
            s for s in segments 
            # We add the length of the segment buffer to the delay
            if (s.timestamp >= now - delay_seconds - (segment_buffer * segment_duration))
        ]

        if len(segments_within_delay) < 10:
            logging.info(f'Less than 10 segments to create playlist for delay: {delay_minutes}')
            # if less than 10 segments, we use the number of segments available
            segment_buffer = len(segments_within_delay)

        if len(segments_within_delay) == 0:
            logging.info(f'No segments to create playlist for delay: {delay_minutes}')
            return

        # Sort segments
        segments_within_delay.sort(key=lambda x: (x.timestamp, x.segment_id))

        # Create playlist
        m3u8_obj = m3u8.M3U8()
        m3u8_obj.version = 3
        m3u8_obj.target_duration = segment_duration
        m3u8_obj.media_sequence = segments_within_delay[0].segment_id

        # Add segments to playlist
        for segment in segments_within_delay[:segment_buffer]:
            m3u8_segment = m3u8.Segment()
            m3u8_segment.uri = segment.filename
            m3u8_segment.duration = segment.duration
            m3u8_obj.add_segment(m3u8_segment)

        # Write playlist file
        with open(output_file, 'w') as f:
            f.write(m3u8_obj.dumps())

        logging.info(f'Created playlist for delay: {delay_minutes}')

    except Exception as e:
        logging.error(f"Error creating playlist for delay {delay_minutes}: {str(e)}")

def main():
    """Main function for playlist creator process"""
    try:
        while True:
            # Get all segments
            segments = list(Path(playlist_folder).glob('*.m4a'))
            if not segments:
                time.sleep(1)
                continue

            # Parse segment information
            segment_infos = [parse_segment_info(segment) for segment in segments]
            if not segment_infos:
                time.sleep(1)
                continue

            # Calculate maximum delay
            max_delay = max(info.timestamp for info in segment_infos) - min(info.timestamp for info in segment_infos)
            max_delay_minutes = max_delay / 60

            # Create playlists for each delay
            for delay in delays:
                if max_delay_minutes > delay:
                    output_file = Path(playlist_folder) / f'playlist_{delay}.m3u8'
                    create_playlist(delay, segment_infos, output_file)

            time.sleep(1)

    except Exception as e:
        logging.error(f"Error in playlist creator: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main() 