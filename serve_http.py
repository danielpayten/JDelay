import http.server
import socketserver
import os

PORT = 8080

class StreamHandler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory="output", **kwargs)

def serve_content():
    with socketserver.TCPServer(("", PORT), StreamHandler) as httpd:
        print(f"Serving files from output/ directory at port {PORT}")
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("\nShutting down server...")
            httpd.shutdown()

if __name__ == "__main__":
    if not os.path.exists("output"):
        os.makedirs("output")
    serve_content()
