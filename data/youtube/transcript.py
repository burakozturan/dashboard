import requests
import pandas as pd
from datetime import datetime, timedelta
import isodate
from xml.etree import ElementTree as ET
import sqlite3
# === STEP 1: CONFIG ===
API_KEY = 'AIzaSyCyFRB09Zn2yKH6D1wCrmUCZpZuBoQoMgg'  # Replace with your actual API key
CHANNEL_ID = 'UCqnbDFdCpuN8CMEg0VuEBqA'  # New York Times Channel ID
START_DATE = "2025-05-01T00:00:00Z"
END_DATE = "2025-06-01T00:00:00Z"
MAX_RESULTS = 50

# === STEP 2: Get Uploads Playlist ID ===
def get_uploads_playlist_id(api_key, channel_id):
    url = f"https://www.googleapis.com/youtube/v3/channels?part=contentDetails&id={channel_id}&key={api_key}"
    response = requests.get(url).json()
    uploads_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    return uploads_id

# === STEP 3: Get All Videos in Date Range ===
def get_all_videos_in_range(api_key, playlist_id, start_date, end_date):
    videos = []
    next_page_token = None

    while True:
        url = f"https://www.googleapis.com/youtube/v3/playlistItems?part=snippet&playlistId={playlist_id}&maxResults={MAX_RESULTS}&key={api_key}"
        if next_page_token:
            url += f"&pageToken={next_page_token}"

        response = requests.get(url).json()
        items = response.get("items", [])

        for item in items:
            snippet = item['snippet']
            published_at = snippet['publishedAt']
            if start_date <= published_at < end_date:
                videos.append({
                    'video_id': snippet['resourceId']['videoId'],
                    'title': snippet['title'],
                    'description': snippet.get('description', ''),
                    'publishedAt': published_at
                })

            if published_at < start_date:
                return videos

        next_page_token = response.get("nextPageToken")
        if not next_page_token:
            break

    return videos

# === STEP 4: Get Durations from /videos endpoint ===
def get_video_durations(api_key, video_ids):
    durations = {}
    for i in range(0, len(video_ids), 50):
        batch_ids = video_ids[i:i+50]
        ids_string = ','.join(batch_ids)
        url = f"https://www.googleapis.com/youtube/v3/videos?part=contentDetails&id={ids_string}&key={api_key}"
        response = requests.get(url).json()
        for item in response.get("items", []):
            vid = item["id"]
            duration = item["contentDetails"]["duration"]
            durations[vid] = duration
    return durations

# === STEP 5: Convert ISO durations ===
def convert_duration_to_seconds(duration_str):
    try:
        duration = isodate.parse_duration(duration_str)
        return int(duration.total_seconds())
    except:
        return None

def format_duration(seconds):
    try:
        return str(timedelta(seconds=seconds))
    except:
        return None

# === STEP 6: Fetch YouTube Transcript ===
def fetch_transcript(video_id, lang="en"):
    url = f"https://video.google.com/timedtext?lang={lang}&v={video_id}"
    response = requests.get(url)
    if response.status_code != 200 or not response.text.strip():
        return "Transcript not available"

    root = ET.fromstring(response.text)
    transcript = " ".join([elem.text for elem in root.findall('text') if elem.text])
    return transcript if transcript else "Transcript not available"

# === STEP 7: Run All Steps ===
uploads_playlist_id = get_uploads_playlist_id(API_KEY, CHANNEL_ID)
all_videos = get_all_videos_in_range(API_KEY, uploads_playlist_id, START_DATE, END_DATE)
df_videos = pd.DataFrame(all_videos)

if not df_videos.empty:
    # Get durations
    video_ids = df_videos['video_id'].tolist()
    durations_dict = get_video_durations(API_KEY, video_ids)
    df_videos["duration_iso"] = df_videos["video_id"].map(durations_dict)
    df_videos["duration_seconds"] = df_videos["duration_iso"].apply(convert_duration_to_seconds)
    df_videos["duration_hms"] = df_videos["duration_seconds"].apply(format_duration)
    df_videos["url"] = "https://www.youtube.com/watch?v=" + df_videos["video_id"]

    # Get transcripts
    print("⏳ Fetching transcripts...")
    df_videos["transcript"] = df_videos["video_id"].apply(fetch_transcript)

    # Save to CSV
    #df_videos.to_csv("nyt_youtube_may_2025_with_transcripts.csv", index=False)
    # === STEP 8: Save to SQLite Database ===
    conn = sqlite3.connect('youtube_videos.db')  # Creates the database file if it doesn't exist

    # Save the DataFrame to a table named 'videos'
    df_videos.to_sql('videos', conn, if_exists='replace', index=False)

    conn.close()

    print("✅ Data saved to SQLite database: youtube_videos.db")
    print(f"✅ Saved {len(df_videos)} videos to nyt_youtube_may_2025_with_transcripts.csv")

    # Print sample
    for index, row in df_videos.iterrows():
        print(f"\n🎥 {row['title']} ({row['url']})")
        print(f"Transcript: {row['transcript'][:500]}...")  # Print first 500 characters
else:
    print("⚠️ No videos found in the given date range.")
