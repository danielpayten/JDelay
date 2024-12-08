import m3u8
import urllib.request
from time import sleep
import os
import pandas as pd
import re
import gen_m3u8

# This script is used to get the playlist of a live stream.
# It then downloads each segment of the live stream and stores it in a folder.

# It continuously fetches the playlist every 10 seconds, updates the segment information and downloads the new segments.
# It also builds a dataframe with the segment information, including the broadcast time of each segment.

# This is a MVP, we could use Kafka to stream the segments to a database, but we will keep it simple for now.


# Initialize the playlist
def process_segment(segment):
    segment = dict(
        segment_id = re.search(r'(\d+)', segment.uri).group(0),
        segment_uri = segment.uri,
        segment_duration = segment.duration,
        segment_path = segment.uri
    )
    return segment

# Download the segments
def download_segments(segments, playlist_folder ):
    for i, segment in enumerate(segments):
        print(segment.uri)

        # If the segment is already downloaded, skip it

        if os.path.exists(segment.uri):
                continue
        else:
            print("Downloading segment")
            # Download the segment
            urllib.request.urlretrieve( playlist_folder + segment.uri, segment.uri)



def initalise_playlist(playlist_url, playlist_folder):

    segment_df = pd.DataFrame(columns=['segment_uri',
                                        'segment_duration',
                                        'segment_path',
                                        'segment_broadcast_time',],
                                        index = [])

    start_time = pd.Timestamp.now()
    playlist = m3u8.load(playlist_url) 
    print(playlist.segments)
    print(playlist.target_duration)

    segment_processed = pd.DataFrame([
        process_segment(segment)
        for segment in playlist.segments
    ])
    segment_processed.set_index('segment_id', inplace=True)

    segment_df = pd.concat([segment_df, segment_processed], axis=0)
    segment_df['segment_broadcast_time'] = start_time + pd.to_timedelta(segment_df['segment_duration'], unit='s').cumsum() - pd.to_timedelta(segment_df['segment_duration'], unit='s')


    # Download the segments
    

    download_segments(playlist.segments, playlist_folder)


    return segment_df, start_time
        
def fetch_and_download_playlist(playlist_url,playlist_folder , start_time, segment_df):


# We now continuously fetch the playlist every 10 seconds, update the segment information and download the new segments


     
    playlist = m3u8.load(playlist_url)

    # download the segments, if they are not already downloaded
    download_segments(playlist.segments, playlist_folder)

    segment_processed = [
        process_segment(segment)
        for segment in playlist.segments
    ]

    for i, segment in enumerate(playlist.segments):
        if segment_df['segment_uri'].str.contains(segment.uri).any():
             continue
        else:
            print("New segment detected")
            segment_processed = pd.DataFrame([process_segment(segment)])
            segment_processed.set_index('segment_id', inplace=True)

            segment_df = pd.concat([segment_df, segment_processed], axis=0)


    segment_df['segment_broadcast_time'] = start_time + pd.to_timedelta(segment_df['segment_duration'], unit='s').cumsum() - pd.to_timedelta(segment_df['segment_duration'], unit='s')


    return segment_df