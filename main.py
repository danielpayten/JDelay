
import get_playlist
import gen_m3u8
import time

playlist_url = 'https://mediaserviceslive.akamaized.net/hls/live/2038308/triplejnsw/masterhq.m3u8'
playlist_folder = '/output/'.join(playlist_url.split('/')[0:-1])+ '/'

segment_df, start_time = get_playlist.initalise_playlist(playlist_url, playlist_folder)


while True:

    segment_df = get_playlist.fetch_and_download_playlist(playlist_url,playlist_folder, start_time, segment_df)

    gen_m3u8.generate_playlist(segment_df, delay_minutes=5, m3u8_length=2)

    time.sleep(10)
