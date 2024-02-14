from googleapiclient.discovery import build
import datetime
import psycopg2

API_KEY = 'AIzaSyBwbMbfzilKXP3jlj10S_LjhKYkMVGXTFI'
DB_CONFIG = {
    "dbname": "postgres",
    "user": "postgres",
    "password": "Nokia_2730",
    "host": "localhost",
    "port": "5433"
}

CHANNEL_IDS = ['UC8butISFwT-Wl7EV0hUK0BQ', 'UCQ0jSGgYMLmRMeTE6UaPPXg', 'UCdV9tn79v3ecSDpC1AjVKaw',]  
TABLE_NAME = "channels"

def fetch_videos_from_channels(channel_ids, api_key, db_config, table_name):
    conn = psycopg2.connect(**db_config)
    youtube = build('youtube', 'v3', developerKey=api_key)
    
    with conn, conn.cursor() as cur:
        create_table(cur, table_name) 
        
        for channel_id in channel_ids:
            video_ids = get_video_ids_from_channel(youtube, channel_id)
            for video_id in video_ids:
                video_data = get_video_data(youtube, video_id)
                if video_data:
                    insert_video_data(cur, table_name, video_data)

        print("Data inserted into PostgreSQL table successfully !!!")

def create_table(cur, table_name):
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            video_id VARCHAR PRIMARY KEY,
            title VARCHAR,
            published_time TIMESTAMP,
            view_count INTEGER,
            like_count INTEGER,
            comment_count INTEGER
        )
    """)

def get_video_ids_from_channel(youtube, channel_id):
    video_ids = []
    next_page_token = None
    
    while True:
        playlist_items = youtube.playlistItems().list(
            part='contentDetails',
            playlistId=f'UU{channel_id[2:]}',  
            maxResults=3,
            pageToken=next_page_token
        ).execute()

        for item in playlist_items['items']:
            video_ids.append(item['contentDetails']['videoId'])

        next_page_token = playlist_items.get('nextPageToken')

        if not next_page_token:
            break

    return video_ids

def get_video_data(youtube, video_id):
    video_response = youtube.videos().list(
        part='snippet,statistics',
        id=video_id
    ).execute()

    if 'items' in video_response and video_response['items']:
        video_snippet = video_response['items'][0]['snippet']
        video_statistics = video_response['items'][0]['statistics']

        title = video_snippet['title']
        published_time = video_snippet['publishedAt']
        view_count = video_statistics.get('viewCount', 0)
        like_count = video_statistics.get('likeCount', 0)
        comment_count = video_statistics.get('commentCount', 0)

        published_time = datetime.datetime.strptime(published_time, "%Y-%m-%dT%H:%M:%SZ")

        return {
            'video_id': video_id,
            'title': title,
            'published_time': published_time,
            'view_count': view_count,
            'like_count': like_count,
            'comment_count': comment_count
        }

    else:
        print(f"No data found for video ID: {video_id}")
        return None

def insert_video_data(cur, table_name, video_data):
    cur.execute(f"""
        INSERT INTO {table_name} (video_id, title, published_time, view_count, like_count, comment_count)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (video_id) DO NOTHING;
    """, (video_data['video_id'], video_data['title'], video_data['published_time'], video_data['view_count'], video_data['like_count'], video_data['comment_count']))

fetch_videos_from_channels(CHANNEL_IDS, API_KEY, DB_CONFIG, TABLE_NAME)
