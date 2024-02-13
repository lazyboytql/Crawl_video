from googleapiclient.discovery import build
import datetime

import psycopg2


api_key = 'AIzaSyBwbMbfzilKXP3jlj10S_LjhKYkMVGXTFI'
video_id = '7HGo9Jb3OXE'


conn = psycopg2.connect(
    dbname="postgres",
    user="postgres",
    password="Nokia_2730",
    host="localhost",
    port="5433"
)


cur = conn.cursor()


youtube = build('youtube', 'v3', developerKey=api_key)


video_response = youtube.videos().list(
    part='snippet,statistics',
    id=video_id
).execute()


video_snippet = video_response['items'][0]['snippet']
video_statistics = video_response['items'][0]['statistics']

title = video_snippet['title']
published_time = video_snippet['publishedAt']
view_count = video_statistics['viewCount']
like_count = video_statistics.get('likeCount', 0)
comment_count = video_statistics.get('commentCount', 0)


published_time = datetime.datetime.strptime(published_time, "%Y-%m-%dT%H:%M:%SZ")

cur.execute("""
    INSERT INTO ytb (title, published_time, view_count, like_count, comment_count)
    VALUES (%s, %s, %s, %s, %s)
""", (title, published_time, view_count, like_count, comment_count))

conn.commit()

cur.close()
conn.close()

print("Data inserted into PostgreSQL table successfully !!!")
 