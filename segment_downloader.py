import time
import signal
import m3u8  # type: ignore[import-untyped]
import requests  # type: ignore[import-untyped]
from pathlib import Path
import logging
import json
import os
from typing import Set, Dict, Optional, Union
from urllib.parse import urljoin
from dataclasses import dataclass
from datetime import datetime
from requests.adapters import HTTPAdapter  # type: ignore[import-untyped]
from urllib3.util import Retry  # type: ignore[import-untyped]

from config import OUTPUT_DIR, LOG_DIR

LOG_DIR.mkdir(exist_ok=True)
logging.basicConfig(
    filename=LOG_DIR / "segment_downloader.log",
    filemode="a",
    format="%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.DEBUG,
)

@dataclass
class SegmentInfo:
    url: str
    duration: float
    timestamp: float
    sequence: int
    filename: str

class SegmentDownloader:
    def __init__(self, output_dir: Optional[Union[str, Path]] = None, max_retries: int = 5, initial_backoff: float = 1.0):
        self.output_dir = Path(output_dir) if output_dir is not None else OUTPUT_DIR
        self.output_dir.mkdir(exist_ok=True)
        self.master_url = 'https://mediaserviceslive.akamaized.net/hls/live/2109456/triplejnsw/v0-221.m3u8'
        self.fetched_segments: Set[str] = set()
        self.segment_metadata: Dict[str, SegmentInfo] = {}
        self.segment_info_file = self.output_dir / 'segment_info.json'
        self.max_retries = max_retries
        self.initial_backoff = initial_backoff
        
        # Configure retry strategy
        retry_strategy = Retry(
            total=max_retries,
            backoff_factor=initial_backoff,
            status_forcelist=[500, 502, 503, 504],
        )
        self.session = requests.Session()
        self.session.mount("http://", HTTPAdapter(max_retries=retry_strategy))
        self.session.mount("https://", HTTPAdapter(max_retries=retry_strategy))
        
        self.load_segment_info()

    def load_segment_info(self):
        """Load previously fetched segment information"""
        try:
            if self.segment_info_file.exists():
                with open(self.segment_info_file, 'r') as f:
                    data = json.load(f)
                    self.fetched_segments = set(data.get('fetched_segments', []))
                    self.segment_metadata = data.get('segment_metadata', {})
                logging.info(f"Loaded {len(self.fetched_segments)} previously fetched segments")
        except Exception as e:
            logging.error(f"Error loading segment info: {str(e)}")

    def save_segment_info(self):
        """Save fetched segment information using atomic file operations"""
        try:
            # Create temporary file in the same directory
            temp_file = self.segment_info_file.with_suffix('.json.tmp')
            
            # Write to temporary file
            with open(temp_file, 'w') as f:
                json.dump({
                    'fetched_segments': list(self.fetched_segments),
                    'segment_metadata': self.segment_metadata,
                    'last_updated': datetime.now().isoformat()
                }, f)
            
            # Atomic rename operation
            temp_file.replace(self.segment_info_file)
            
        except Exception as e:
            logging.error(f"Error saving segment info: {str(e)}")
            # Clean up temp file if it exists
            if temp_file.exists():
                temp_file.unlink()

    def fetch_playlist(self, url: str) -> Optional[m3u8.M3U8]:
        """Fetch and parse a playlist with retry logic"""
        for attempt in range(self.max_retries):
            try:
                response = self.session.get(url)
                response.raise_for_status()
                return m3u8.loads(response.text)
            except Exception as e:
                if attempt == self.max_retries - 1:
                    logging.error(f"Failed to fetch playlist after {self.max_retries} attempts: {str(e)}")
                    return None
                backoff = self.initial_backoff * (2 ** attempt)  # Exponential backoff
                logging.warning(f"Attempt {attempt + 1} failed to fetch playlist. Retrying in {backoff} seconds...")
                time.sleep(backoff)

    def download_segment(self, segment_info: SegmentInfo) -> bool:
        """Download a single segment with retry logic"""
        output_path = os.path.join(self.output_dir, segment_info.filename)
        if os.path.exists(output_path):
            logging.debug(f"Segment already exists: {segment_info.filename}")
            return True

        for attempt in range(self.max_retries):
            try:
                # Add timeout to prevent hanging on slow connections
                response = self.session.get(segment_info.url, stream=True, timeout=(5, 30))
                response.raise_for_status()
                
                # Use a temporary file for atomic writes
                temp_path = output_path + '.tmp'
                try:
                    with open(temp_path, 'wb') as f:
                        for chunk in response.iter_content(chunk_size=8192):
                            if chunk:
                                f.write(chunk)
                    # Atomic rename
                    os.replace(temp_path, output_path)
                finally:
                    # Clean up temp file if something went wrong
                    if os.path.exists(temp_path):
                        os.unlink(temp_path)
                
                logging.info(f"Downloaded segment: {segment_info.filename}")
                return True
            except Exception as e:
                if attempt == self.max_retries - 1:
                    logging.error(f"Failed to download segment {segment_info.url} after {self.max_retries} attempts: {str(e)}")
                    return False
                backoff = self.initial_backoff * (2 ** attempt)  # Exponential backoff
                logging.warning(f"Attempt {attempt + 1} failed to download segment. Retrying in {backoff} seconds...")
                time.sleep(backoff)

    def process_segments(self):
        """Process and download new segments"""
        try:
            # Fetch playlist
            logging.info(f"Fetching playlist from {self.master_url}")
            playlist = self.fetch_playlist(self.master_url)
            if not playlist:
                return

            base_url = self.master_url.rsplit('/', 1)[0] + '/'
            new_segments = 0


            # Process segments
            for segment in playlist.segments:
                segment_url = urljoin(base_url, segment.uri)
                if segment_url not in self.fetched_segments:
                    # Create segment info
                    segment_info = SegmentInfo(
                        url=segment_url,
                        duration=segment.duration,
                        timestamp=segment.program_date_time.timestamp() if segment.program_date_time else time.time(),
                        sequence=segment.media_sequence,
                        filename=f"segment_{segment.media_sequence:04d}.aac"
                    )
                    logging.info(f"Attempting to download segment {segment_info.filename}")
                    if self.download_segment(segment_info):
                        logging.info(f"Downloaded segment {segment_info.filename}")
                        self.fetched_segments.add(segment_url)
                        self.segment_metadata[str(segment.media_sequence)] = {
                            'url': segment_url,
                            'duration': segment_info.duration,
                            'timestamp': segment_info.timestamp,
                            'sequence': segment_info.sequence,
                            'filename': segment_info.filename
                        }
                        new_segments += 1

            if new_segments > 0:
                logging.info(f"Downloaded {new_segments} new segments")
                self.save_segment_info()

            # Clean up old segments from memory (keep last 1000 by sequence)
            if len(self.segment_metadata) > 1000:
                sorted_sequences = sorted(self.segment_metadata.keys(), key=int)
                to_remove = sorted_sequences[:-1000]
                for seq in to_remove:
                    self.segment_metadata.pop(seq, None)
                # Also trim fetched_segments by keeping only URLs for remaining sequences
                remaining_urls = {m['url'] for m in self.segment_metadata.values()}
                self.fetched_segments &= remaining_urls

        except Exception as e:
            logging.error(f"Error processing segments: {str(e)}")

    def run(self, check_interval: int = 3):
        """Run the segment downloader. Handles SIGTERM/SIGINT for graceful shutdown."""
        shutdown_requested = False

        def request_shutdown(signum, frame):
            nonlocal shutdown_requested
            shutdown_requested = True
            logging.info("Shutdown requested (signal %s), saving state and exiting...", signum)

        signal.signal(signal.SIGTERM, request_shutdown)
        signal.signal(signal.SIGINT, request_shutdown)

        logging.info("Starting segment downloader")
        try:
            while not shutdown_requested:
                self.process_segments()
                for _ in range(check_interval):
                    if shutdown_requested:
                        break
                    time.sleep(1)
        except Exception as e:
            logging.error("Error in main loop: %s", e)
        finally:
            logging.info("Stopping segment downloader")
            self.save_segment_info()

if __name__ == "__main__":
    downloader = SegmentDownloader(output_dir=OUTPUT_DIR)
    downloader.run()
