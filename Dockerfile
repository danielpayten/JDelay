# JDelay - time-shifted HLS radio stream
# Build for Jetson Nano (ARM64): docker build --platform linux/arm64 -t jdelay .
# Build for local/amd64:  docker build -t jdelay .
FROM python:3.12-slim

# Prefer non-interactive
ENV DEBIAN_FRONTEND=noninteractive
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Application code
COPY config.py main.py playlist_creator.py segment_downloader.py segmentation.py serve_http.py ./

# Optional: override port/dir at run time
ENV JDELAY_HTTP_PORT=8080
ENV JDELAY_HTTP_DIR=/app/output

EXPOSE 8080

# Create dirs the app expects (output + logs); entrypoint runs main + HTTP server
RUN mkdir -p /app/output /app/logs

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
