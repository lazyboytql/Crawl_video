from googleapiclient.discovery import build
import datetime
import csv


api_key = 'AIzaSyBwbMbfzilKXP3jlj10S_LjhKYkMVGXTFI'
youtube = build('youtube', 'v3', developerKey=api_key)
video_id = 'XMIadptpAHg'

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
published_time = published_time.strftime("%d/%m/%Y")


print(f"Tiêu đề: {title}")
print(f"Thời gian đăng: {published_time}")
print(f"Số lượt xem: {view_count}")
print(f"Số lượt thích: {like_count}")
print(f"Số comment: {comment_count}")


title = title.encode('utf-8').decode('utf-8')
published_time = published_time.encode('utf-8').decode('utf-8')

file_name = 'video_data.csv'
with open(file_name, mode='w', newline='', encoding='utf-8-sig') as file:
    writer = csv.writer(file)
    writer.writerow(['Tiêu đề', 'Thời gian đăng', 'Số lượt xem', 'Số lượt thích', 'Số comment'])
    writer.writerow([title, published_time, view_count, like_count, comment_count])

print(f"Data save as file {file_name}")