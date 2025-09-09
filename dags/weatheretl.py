from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from datetime import datetime, timedelta

#London
LATITUDE = 51.5074
LONGITUDE = -0.1278
POSTGRES_CONN_ID = 'postgres_default'
API_CONN_ID = 'open_meteo_api'

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 9, 7),  
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id='weather_etl_pipeline',
    default_args=default_args,
    schedule ='@daily',
    catchup=False,
    tags=['etl', 'weather']
) as dag:  

    @task()
    def extract_weather_data():
        http_hook = HttpHook(http_conn_id="open_meteo_api", method="GET")
        endpoint = f"/v1/forecast?latitude={LATITUDE}&longitude={LONGITUDE}&current_weather=true"
        response = http_hook.run(endpoint)
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Failed to fetch weather data: {response.status_code} - {response.text}")

  
    @task()
    def transform_weather_data(weather_data):
        current_weather = weather_data['current_weather']
        transformed_data = {
            'latitude': LATITUDE,
            'longitude': LONGITUDE,
            'temperature': current_weather['temperature'],
            'windspeed': current_weather['windspeed'],
            'winddirection': current_weather['winddirection'],
            'weathercode': current_weather['weathercode'],
            'time': current_weather['time']  
        }
        return transformed_data

    @task()
    def load_weather_data(transformed_data):
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS weather_data (
                latitude FLOAT,
                longitude FLOAT,
                temperature FLOAT,
                windspeed FLOAT,
                winddirection FLOAT,
                weathercode INT,
                time TIMESTAMP,
                inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

        cursor.execute("""
            INSERT INTO weather_data (latitude, longitude, temperature, windspeed, winddirection, weathercode, time)
            VALUES (%s, %s, %s, %s, %s, %s, %s);
        """, (
            transformed_data.get("latitude"),
            transformed_data.get("longitude"),
            transformed_data.get("temperature"),
            transformed_data.get("windspeed"),
            transformed_data.get("winddirection"),
            transformed_data.get("weathercode"),
            transformed_data.get("time")
        ))

        conn.commit()
        cursor.close()
        conn.close()

    #task dependencies
    weather_data = extract_weather_data()
    transformed_data = transform_weather_data(weather_data)
    load_weather_data(transformed_data)

          

