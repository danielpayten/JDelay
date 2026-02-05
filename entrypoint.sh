#!/bin/sh
set -e
# Run recorder and HTTP server; trap SIGTERM/SIGINT so both exit when container stops
python3 main.py &
MAIN_PID=$!
python3 serve_http.py &
HTTP_PID=$!
trap "kill $MAIN_PID $HTTP_PID 2>/dev/null; exit 0" TERM INT
wait
