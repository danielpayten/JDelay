import http.server
import socketserver
import os
from pathlib import Path

from config import HTTP_PORT, HTTP_DIR


class StreamHandler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=HTTP_DIR, **kwargs)

    def end_headers(self):
        # HLS: playlists should be fresh, segments can be cached
        path = self.path.split("?")[0]
        if path.endswith(".m3u8"):
            self.send_header("Cache-Control", "no-cache")
        elif path.endswith(".aac") or path.endswith(".ts") or path.endswith(".mp4"):
            self.send_header("Cache-Control", "max-age=300")
        super().end_headers()


def serve_content():
    with socketserver.TCPServer(("", HTTP_PORT), StreamHandler) as httpd:
        print(f"Serving files from {HTTP_DIR} at http://0.0.0.0:{HTTP_PORT}")
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("\nShutting down server...")
            httpd.shutdown()


if __name__ == "__main__":
    dir_path = Path(HTTP_DIR)
    if not dir_path.exists():
        dir_path.mkdir(parents=True)
    serve_content()
