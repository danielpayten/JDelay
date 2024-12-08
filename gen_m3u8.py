# This file uses a database of M3U8 segments to create a new M3U8 file with the same segments but with a different base URL.
import pandas as pd
import m3u8
import datetime


def generate_playlist(segment_db, delay_minutes=20, m3u8_length=2):

    # create an m3u8 object from the segment_db
    m3u8_obj = m3u8.M3U8()
    m3u8_obj.version = 3
    m3u8_obj.target_duration = 10


    now = datetime.datetime.now()

    # We populate the new m3u8 object with the segments that are within the delay_minutes and m3u8_length

    segments_filtered = segment_db[
        
        # Delay introduced in the stream
        (segment_db['segment_broadcast_time'] > now - datetime.timedelta(minutes=delay_minutes)) &
        
        # Buffer for the m3u8 file
        (segment_db['segment_broadcast_time'] < now - datetime.timedelta(minutes=delay_minutes) + datetime.timedelta(minutes=m3u8_length))

        ]

    if segments_filtered.empty:
        print("No segments to add to the playlist - Buffering...")
        return
    
    m3u8_obj.media_sequence = segments_filtered.index[0]



    # iterate over the rows of the segment_db and add each segment to the m3u8 object
    for index, row in segments_filtered.iterrows():
        
        


        segment = m3u8.Segment()
        segment.media_sequence = index
        segment.uri = row['segment_uri']
        segment.duration = row['segment_duration']
        m3u8_obj.add_segment(segment)

        

    # write the m3u8 object to a file
    with open('/output/delay_5min.m3u8', 'w', encoding="utf-8") as output_file:
        output_file.write(m3u8_obj.dumps())

