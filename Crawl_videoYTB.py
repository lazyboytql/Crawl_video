from googleapiclient.discovery import build
import datetime

import psycopg2

# Google API key and YouTube video details
api_key = 'AIzaSyBwbMbfzilKXP3jlj10S_LjhKYkMVGXTFI'
video_id = '7HGo9Jb3OXE'

# Connect to PostgreSQL database
conn = psycopg2.connect(
    dbname="postgres",
    user="postgres",
    password="Nokia_2730",
    host="localhost",
    port="5433"
)

# Create a cursor object
cur = conn.cursor()

# Build YouTube service
youtube = build('youtube', 'v3', developerKey=api_key)

# Retrieve video details
video_response = youtube.videos().list(
    part='snippet,statistics',
    id=video_id
).execute()

# Extract required information from video response
video_snippet = video_response['items'][0]['snippet']
video_statistics = video_response['items'][0]['statistics']

title = video_snippet['title']
published_time = video_snippet['publishedAt']
view_count = video_statistics['viewCount']
like_count = video_statistics.get('likeCount', 0)
comment_count = video_statistics.get('commentCount', 0)

# Format published time
published_time = datetime.datetime.strptime(published_time, "%Y-%m-%dT%H:%M:%SZ")

# Insert data into PostgreSQL table
cur.execute("""
    INSERT INTO ytb (title, published_time, view_count, like_count, comment_count)
    VALUES (%s, %s, %s, %s, %s)
""", (title, published_time, view_count, like_count, comment_count))

# Commit the transaction
conn.commit()

# Close the cursor and connection
cur.close()
conn.close()

print("Data inserted into PostgreSQL table successfully.")
 