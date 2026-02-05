"""
Shared configuration for JDelay.
Single source of truth for paths, timing, and PlaylistSpec.
"""
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

# Base paths: use project directory so paths work from any cwd
PROJECT_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = PROJECT_DIR / "output"
LOG_DIR = PROJECT_DIR / "logs"

# Timing
BUFFER_PERIOD_SECONDS = 1 * 60  # 1 minute
HLS_LENGTH_SECONDS = 1.0 * 60   # 1.0 minutes
DELAYS_SECONDS = [60 * x for x in ([2, 5, 10, 30] + list(range(60, 24 * 60, 60)))]

# HTTP server (can be overridden by env)
HTTP_PORT = int(os.environ.get("JDELAY_HTTP_PORT", "8080"))
HTTP_DIR = os.environ.get("JDELAY_HTTP_DIR", str(OUTPUT_DIR))


@dataclass
class PlaylistSpec:
    delay_seconds: int
    playlist_start_time: float
    playlist_file_name: str
    first_segment_id: Optional[int] = None
    is_initalised: bool = False
    is_running: bool = False
