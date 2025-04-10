# JDelay #
## A radio stream delayed for your timezone
### Back Story
I've lived in the UK, away from my native Australia for years. One thing I miss is listing to the radio, in particular Australia's youth broadcaster, Triple J. While I can definitely listen to Triple in the UK, it is out of sync with my time zone. This means that when I'm up, the only option is to listen to midnight 'trance' type music, without presenters. I most enjoy listening in 'real time' and by that I mean that I would love to listen to 9 AM music at 9 AM.

### This repo
This repo time-shifts triple J or any other m3u8 radio stream and produces m3u8 playlist files for a set of delay periods.

### Things you might consider changing
1. Change to another radio station of your choice
2. Change the delay periods

## Legal basis of doing this
In order for this to work, we obviously need to record live radio and then re-play later.
This is legal.
The reason we know this is "National Rugby League Investments Pty Limited v Singtel Optus Pty Ltd (2012) - Federal Court"
In this case, the service optus built, enabled users to initiate a recording of streamed content and re-play later.
Although the specific case was overturned, the judgement makes clear that as long as its you, the user make the copy to time-shift, its fine.

See: https://classic.austlii.edu.au/au/journals/JlALawTA/2012/8.pdf

## Running
Install dependencies
1. m3u8
2. run `python3 main.py`
3. run `serve_http.py` to serve the files. Modify as appropriate for however you want to serve these files. Eg. Write something to upload to an AWS bucket.
4. allow sufficent buffer to build up Eg. Now Australia is GMT - 11, so I need to wait 11 hours before 'real time' streaming.
5. open with VLC 'localhost:8080/playlist_660.m3u8` (660 is 11hrs * 60 min)
