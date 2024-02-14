from googleapiclient.discovery import build
import datetime
import psycopg2

API_KEY = 'AIzaSyBwbMbfzilKXP3jlj10S_LjhKYkMVGXTFI'
VIDEO_IDS = ['7HGo9Jb3OXE', 'HJDwo7J8oL4','au7J8FacCK8','aGbfU4GiI9s','nLRL_NcnK-4','YZ5tOe7y9x4','0MhVkKHYUAY','YgCq0Y6Mhh0'] 
DB_CONFIG = {
    "dbname": "postgres",
    "user": "postgres",
    "password": "Nokia_2730",
    "host": "localhost",
    "port": "5433"
}

def fetch_videos(video_ids, api_key, db_config, table_name):
    conn = psycopg2.connect(**db_config)
    youtube = build('youtube', 'v3', developerKey=api_key)
    
    with conn, conn.cursor() as cur:
        for video_id in video_ids:
            video_data = get_video_data(youtube, video_id)
            if video_data:
                insert_video_data(cur, table_name, video_data)

        print("Data inserted into PostgreSQL table successfully !!!")

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
        INSERT INTO {table_name} (title, published_time, view_count, like_count, comment_count)
        VALUES (%s, %s, %s, %s, %s)
    """, (video_data['title'], video_data['published_time'], video_data['view_count'], video_data['like_count'], video_data['comment_count']))

TABLE_NAME = "ytb"
fetch_videos(VIDEO_IDS, API_KEY, DB_CONFIG, TABLE_NAME)
