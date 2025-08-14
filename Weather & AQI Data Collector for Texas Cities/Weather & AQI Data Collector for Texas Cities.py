from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import sqlite3
import logging
import os

DAG_FOLDER = os.path.dirname(os.path.abspath(__file__))
API_KEY_PATH = os.path.join(DAG_FOLDER, "api_key.txt")

CITIES = [
    {"name": "Houston", "lat": 29.7604, "lon": -95.3698},
    {"name": "San Antonio", "lat": 29.4241, "lon": -98.4936},
    {"name": "Dallas", "lat": 32.7767, "lon": -96.7970},
    {"name": "Austin", "lat": 30.2672, "lon": -97.7431},
    {"name": "Fort Worth", "lat": 32.7555, "lon": -97.3308},
    {"name": "El Paso", "lat": 31.7619, "lon": -106.4850},
    {"name": "Arlington", "lat": 32.7357, "lon": -97.1081},
    {"name": "Corpus Christi", "lat": 27.8006, "lon": -97.3964},
    {"name": "Plano", "lat": 33.0198, "lon": -96.6989},
    {"name": "Laredo", "lat": 27.5306, "lon": -99.4803}
]

DB_PATH = "/opt/airflow/weather_aqi.db"

def create_table():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS weather_aqi (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT,
            city TEXT,
            aqi INTEGER,
            weather_desc TEXT,
            temperature REAL,
            humidity INTEGER
        )
    ''')
    conn.commit()
    conn.close()

def fetch_data(ti, **kwargs):
    with open(API_KEY_PATH, "r") as f:
        api_key = f.read().strip()

    def get_air_quality(lat, lon, api_key):
        url = "https://api.openweathermap.org/data/2.5/air_pollution"
        params = {"lat": lat, "lon": lon, "appid": api_key}
        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()
        return resp.json()

    def get_weather(city, api_key):
        url = "https://api.openweathermap.org/data/2.5/weather"
        params = {"q": city, "appid": api_key, "units": "metric", "lang": "zh_tw"}
        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()
        return resp.json()

    results = []
    now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    for city_info in CITIES:
        city = city_info["name"]
        lat = city_info["lat"]
        lon = city_info["lon"]
        try:
            air = get_air_quality(lat, lon, api_key)
            weather = get_weather(city, api_key)

            if not air.get("list"):
                raise ValueError(f"No air quality data returned for {city}: {air}")

            data = {
                'timestamp': now_str,
                'city': city,
                'aqi': air['list'][0]['main']['aqi'],
                'weather_desc': weather['weather'][0]['description'],
                'temperature': weather['main']['temp'],
                'humidity': weather['main']['humidity']
            }
            results.append(data)
            logging.info(f"Data for {city} fetched: {data}")

        except Exception as e:
            logging.error(f"Error fetching data for {city}: {e}")

    ti.xcom_push(key='weather_data_list', value=results)

def insert_data(ti, **kwargs):
    data_list = ti.xcom_pull(key='weather_data_list', task_ids='fetch_data')
    if not data_list:
        raise ValueError("No data found in XCom for 'fetch_data' task.")

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    try:
        for data in data_list:
            cursor.execute('''
                INSERT INTO weather_aqi (timestamp, city, aqi, weather_desc, temperature, humidity)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (
                data['timestamp'],
                data['city'],
                data['aqi'],
                data['weather_desc'],
                data['temperature'],
                data['humidity']
            ))
        conn.commit()
    except Exception as e:
        logging.error(f"Error inserting data: {e}")
        raise
    finally:
        conn.close()

def query_data():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM weather_aqi ORDER BY id DESC LIMIT 10")
    rows = cursor.fetchall()
    logging.info(f"Last 10 records: {rows}")
    for row in rows:
        print(row)
    conn.close()

default_args = {
    "owner": "te-chun",
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}

with DAG(
    dag_id="weather_aqi_texas_cities",
    default_args=default_args,
    description="Fetch weather & AQI data for 10 Texas cities and save to SQLite",
    schedule_interval="@daily",
    start_date=datetime(2025, 8, 9),
    catchup=False
) as dag:

    create_table = PythonOperator(
        task_id="create_table",
        python_callable=create_table
    )

    fetch_data = PythonOperator(
        task_id="fetch_data",
        python_callable=fetch_data
    )

    insert_data = PythonOperator(
        task_id="insert_data",
        python_callable=insert_data
    )

    query_data = PythonOperator(
        task_id="query_data",
        python_callable=query_data
    )

    create_table >> fetch_data >> insert_data >> query_data
