


sleep (10)


while True:

    
    playlist = m3u8.load('https://mediaserviceslive.akamaized.net/hls/live/2038308/triplejnsw/masterhq.m3u8') 
    

    



    segment_processed = [
        process_segment(segment)
        for segment in playlist.segments
    ]
    # We now have a list of segments with the uri, duration and path of the segment

    segment_df = pd.DataFrame(segment_processed)
    segment_df['segment_broadcast_time'] = current_time + segment_df['segment_duration'].cumsum() - segment_df['segment_duration']





    sleep(10)
































    # Create a new playlist with the downloaded segments
    # This playlist will have the segments with a delay
    # The delay will be 10 Minutes

    # Create a new playlist
    new_playlist = m3u8.M3U8()
    new_playlist.target_duration = playlist.target_duration
    new_playlist.media_sequence = playlist.media_sequence

    # Add the segments to the new playlist
    for i, segment in enumerate(playlist.segments):
        new_playlist.add_segment(m3u8.model.Segment(
            uri=f"segments/segment_{segment.uri}.aac",
            duration=segment.duration,
            title=segment.title,
            byterange=segment.byterange,
        )
        )


    # Save the new playlist
    new_playlist.dump('new_playlist.m3u8')

    # The new playlist can be used to play the live stream with a delay.
    # The delay will be the sum of the duration of the segments in the playlist.

    sleep(10)



playlist = m3u8.load('https://mediaserviceslive.akamaized.net/hls/live/2038308/triplejnsw/masterhq.m3u8') 
print(playlist.segments)
print(playlist.target_duration)

# Download the segments
for i, segment in enumerate(playlist.segments):
    print(segment.uri)


    # Download the segment
    # Save the segment in a folder
    urllib.request.urlretrieve( "https://mediaserviceslive.akamaized.net/hls/live/2038308/triplejnsw/" + segment.uri, f"segments/segment_{segment.uri}.aac")


# Create a new playlist with the downloaded segments
# This playlist will have the segments with a delay
# The delay will be 10 Minutes

# Create a new playlist
new_playlist = m3u8.M3U8()
new_playlist.target_duration = playlist.target_duration
new_playlist.media_sequence = playlist.media_sequence

# Add the segments to the new playlist
for i, segment in enumerate(playlist.segments):
    new_playlist.add_segment(m3u8.model.Segment(
        uri=f"segments/segment_{segment.uri}.aac",
        duration=segment.duration,
        title=segment.title,
        byterange=segment.byterange,
    )
    )


# Save the new playlist
new_playlist.dump('new_playlist.m3u8')

# The new playlist can be used to play the live stream with a delay.
# The delay will be the sum of the duration of the segments in the playlist.