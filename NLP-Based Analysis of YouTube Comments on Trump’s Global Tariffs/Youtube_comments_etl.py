import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
from sqlalchemy import create_engine, Table, Column, Integer, String, Text, MetaData, DateTime, select, and_
from urllib.parse import quote_plus

# -----------------------------
# DAG Settings
# -----------------------------
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='youtube_comments_pipeline',
    default_args=default_args,
    description='Fetch and save YouTube comments',
    schedule_interval=None,
    start_date=datetime(2025, 8, 1),
    catchup=False
)

# -----------------------------
# Configs
# -----------------------------
DAG_FOLDER = os.path.dirname(os.path.abspath(__file__))
API_KEY_PATH = os.path.join(DAG_FOLDER, "api_key_youtube.txt")
with open(API_KEY_PATH, "r") as f:
    API_KEY = f.read().strip()

VIDEO_ID = '6ZFKHzKyfO4'
THREADS_URL = 'https://www.googleapis.com/youtube/v3/commentThreads'
COMMENTS_URL = 'https://www.googleapis.com/youtube/v3/comments'

# PostgreSQL Connection
POSTGRES_CONFIG = {
    "dbname": "postgres",
    "user": "postgres",
    "password": "Ff129962932#",
    "host": "postgres_container",  # Docker container name
    "port": "5432"
}
password_encoded = quote_plus(POSTGRES_CONFIG["password"])
CONN_STR = f"postgresql+psycopg2://{POSTGRES_CONFIG['user']}:{password_encoded}@{POSTGRES_CONFIG['host']}:{POSTGRES_CONFIG['port']}/{POSTGRES_CONFIG['dbname']}"
engine = create_engine(CONN_STR)
metadata = MetaData()

youtube_comments = Table(
    'youtube_comments', metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('video_id', String(50)),
    Column('author', String(100)),
    Column('comment', Text),
    Column('created_at', DateTime)
)

# -----------------------------
# Tasks
# -----------------------------
def setup_api():
    print(f"API Key: {API_KEY[:4]}*** | Video ID: {VIDEO_ID}")

def create_db():
    try:
        if not engine.dialect.has_table(engine.connect(), 'youtube_comments'):
            metadata.create_all(engine)
            print("Table created.")
        else:
            print("Table exists, skip creation.")
    except Exception as e:
        print("DB error:", e)

def fetch_comments():
    all_comments = []
    next_page_token = None
    try:
        while True:
            params = {'part': 'snippet', 'videoId': VIDEO_ID, 'maxResults': 100, 'textFormat': 'plainText', 'key': API_KEY}
            if next_page_token:
                params['pageToken'] = next_page_token

            r = requests.get(THREADS_URL, params=params)
            r.raise_for_status()
            data = r.json()
            items = data.get('items', [])
            if not items:
                break

            for item in items:
                top = item['snippet']['topLevelComment']['snippet']
                all_comments.append({"author": top['authorDisplayName'], "comment": top['textDisplay']})

                # Replies
                reply_count = item['snippet'].get('totalReplyCount', 0)
                if reply_count > 0:
                    parent_id = item['snippet']['topLevelComment']['id']
                    reply_params = {'part': 'snippet', 'parentId': parent_id, 'maxResults': 100, 'textFormat': 'plainText', 'key': API_KEY}
                    while True:
                        reply_r = requests.get(COMMENTS_URL, params=reply_params)
                        reply_r.raise_for_status()
                        reply_data = reply_r.json()
                        replies = reply_data.get('items', [])
                        for rep in replies:
                            snip = rep['snippet']
                            all_comments.append({"author": snip['authorDisplayName'], "comment": snip['textDisplay']})
                        if 'nextPageToken' in reply_data:
                            reply_params['pageToken'] = reply_data['nextPageToken']
                        else:
                            break

            next_page_token = data.get('nextPageToken')
            if not next_page_token:
                break
    except Exception as e:
        print("Fetch error:", e)
    print(f"Fetched {len(all_comments)} comments")
    return all_comments

def save_comments(ti):
    all_comments = ti.xcom_pull(task_ids='fetch_comments')
    if not all_comments:
        print("No comments fetched.")
        return

    saved_count, failed_count = 0, 0
    try:
        with engine.begin() as conn:
            for item in all_comments:
                stmt = select(youtube_comments).where(
                    and_(youtube_comments.c.video_id==VIDEO_ID, youtube_comments.c.comment==item['comment'])
                )
                if not conn.execute(stmt).first():
                    conn.execute(youtube_comments.insert(), {
                        "video_id": VIDEO_ID,
                        "author": (item['author'] or "Unknown")[:100],
                        "comment": item['comment'] or "",
                        "created_at": datetime.now()
                    })
                    saved_count += 1
    except Exception as e:
        print("Save error:", e)
        failed_count += 1

    print(f"{saved_count} comments saved. {failed_count} failed.")

# -----------------------------
# DAG Operators
# -----------------------------
setup_api_task = PythonOperator(task_id='setup_api', python_callable=setup_api, dag=dag)
create_db_task = PythonOperator(task_id='create_database', python_callable=create_db, dag=dag)
fetch_comments_task = PythonOperator(task_id='fetch_comments', python_callable=fetch_comments, dag=dag)
save_comments_task = PythonOperator(task_id='save_comments', python_callable=save_comments, dag=dag)

setup_api_task >> create_db_task >> fetch_comments_task >> save_comments_task
