"""Two-city Open-Meteo forecast ETL. Airflow 2.10.1 compatible."""

from datetime import timedelta

import pendulum
import requests
from airflow.datasets import Dataset
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


WEATHER_READY = Dataset("snowflake://DATA_226/RAW/WEATHER_FORECAST")
FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
DAILY_FIELDS = (
    "weather_code",
    "temperature_2m_max",
    "temperature_2m_min",
    "precipitation_sum",
)


@dag(
    dag_id="weather_forecast_two_cities",
    start_date=pendulum.datetime(2026, 10, 1, tz="America/Los_Angeles"),
    schedule="0 8 * * *",
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["weather", "snowflake", "forecast"],
)
def weather_forecast_two_cities():
    @task
    def extract_and_transform():
        # Example Airflow Variable weather_cities is documented in README.md.
        cities = Variable.get("weather_cities", deserialize_json=True)
        if not isinstance(cities, list) or len(cities) != 2:
            raise ValueError("weather_cities must be a JSON list of exactly two cities")

        seen = set()
        rows = []
        for city in cities:
            name = city["name"].strip()
            if not name or name.lower() in seen:
                raise ValueError("City names must be nonempty and distinct")
            seen.add(name.lower())
            latitude = float(city["latitude"])
            longitude = float(city["longitude"])
            response = requests.get(
                FORECAST_URL,
                params={
                    "latitude": latitude,
                    "longitude": longitude,
                    "daily": ",".join(DAILY_FIELDS),
                    "timezone": "America/Los_Angeles",
                    "forecast_days": 14,
                },
                timeout=60,
            )
            response.raise_for_status()
            payload = response.json()
            if payload.get("error"):
                raise ValueError(f"Open-Meteo error for {name}: {payload.get('reason')}")
            daily = payload["daily"]
            dates = daily["time"]
            if not dates or any(len(daily[field]) != len(dates) for field in DAILY_FIELDS):
                raise ValueError(f"Incomplete forecast series for {name}")
            for index, forecast_date in enumerate(dates):
                rows.append(
                    (
                        name,
                        latitude,
                        longitude,
                        forecast_date,
                        daily["weather_code"][index],
                        daily["temperature_2m_max"][index],
                        daily["temperature_2m_min"][index],
                        daily["precipitation_sum"][index],
                    )
                )
        return rows

    @task(outlets=[WEATHER_READY])
    def load_forecast(rows):
        if not rows:
            raise ValueError("Refusing to load an empty forecast")
        conn = SnowflakeHook(snowflake_conn_id="snowflake_acc_data220").get_conn()
        cursor = conn.cursor()
        try:
            # Snowflake DDL commits implicitly; create tables before BEGIN.
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS DATA_226.RAW.WEATHER_FORECAST (
                    CITY VARCHAR(100) NOT NULL,
                    LATITUDE NUMBER(10, 7) NOT NULL,
                    LONGITUDE NUMBER(10, 7) NOT NULL,
                    FORECAST_DATE DATE NOT NULL,
                    WEATHER_CODE INTEGER,
                    TEMPERATURE_MAX_C FLOAT,
                    TEMPERATURE_MIN_C FLOAT,
                    PRECIPITATION_MM FLOAT,
                    LAST_CHANGED_AT TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
                    CONSTRAINT WEATHER_FORECAST_PK PRIMARY KEY (CITY, FORECAST_DATE)
                )
            """)
            cursor.execute("""
                CREATE TEMPORARY TABLE DATA_226.RAW.WEATHER_FORECAST_BATCH (
                    CITY VARCHAR(100), LATITUDE NUMBER(10, 7),
                    LONGITUDE NUMBER(10, 7), FORECAST_DATE DATE,
                    WEATHER_CODE INTEGER, TEMPERATURE_MAX_C FLOAT,
                    TEMPERATURE_MIN_C FLOAT, PRECIPITATION_MM FLOAT
                )
            """)
            cursor.execute("BEGIN")
            cursor.executemany("""
                INSERT INTO DATA_226.RAW.WEATHER_FORECAST_BATCH (
                    CITY, LATITUDE, LONGITUDE, FORECAST_DATE, WEATHER_CODE,
                    TEMPERATURE_MAX_C, TEMPERATURE_MIN_C, PRECIPITATION_MM
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, rows)
            cursor.execute("""
                MERGE INTO DATA_226.RAW.WEATHER_FORECAST AS target
                USING DATA_226.RAW.WEATHER_FORECAST_BATCH AS incoming
                  ON target.CITY = incoming.CITY
                 AND target.FORECAST_DATE = incoming.FORECAST_DATE
                WHEN MATCHED AND (
                    target.LATITUDE IS DISTINCT FROM incoming.LATITUDE OR
                    target.LONGITUDE IS DISTINCT FROM incoming.LONGITUDE OR
                    target.WEATHER_CODE IS DISTINCT FROM incoming.WEATHER_CODE OR
                    target.TEMPERATURE_MAX_C IS DISTINCT FROM incoming.TEMPERATURE_MAX_C OR
                    target.TEMPERATURE_MIN_C IS DISTINCT FROM incoming.TEMPERATURE_MIN_C OR
                    target.PRECIPITATION_MM IS DISTINCT FROM incoming.PRECIPITATION_MM
                ) THEN UPDATE SET
                    LATITUDE = incoming.LATITUDE,
                    LONGITUDE = incoming.LONGITUDE,
                    WEATHER_CODE = incoming.WEATHER_CODE,
                    TEMPERATURE_MAX_C = incoming.TEMPERATURE_MAX_C,
                    TEMPERATURE_MIN_C = incoming.TEMPERATURE_MIN_C,
                    PRECIPITATION_MM = incoming.PRECIPITATION_MM,
                    LAST_CHANGED_AT = CURRENT_TIMESTAMP()
                WHEN NOT MATCHED THEN INSERT (
                    CITY, LATITUDE, LONGITUDE, FORECAST_DATE, WEATHER_CODE,
                    TEMPERATURE_MAX_C, TEMPERATURE_MIN_C, PRECIPITATION_MM,
                    LAST_CHANGED_AT
                ) VALUES (
                    incoming.CITY, incoming.LATITUDE, incoming.LONGITUDE,
                    incoming.FORECAST_DATE, incoming.WEATHER_CODE,
                    incoming.TEMPERATURE_MAX_C, incoming.TEMPERATURE_MIN_C,
                    incoming.PRECIPITATION_MM, CURRENT_TIMESTAMP()
                )
            """)
            cursor.execute("COMMIT")
            print(f"Merged {len(rows)} daily forecast rows for two cities")
        except Exception:
            cursor.execute("ROLLBACK")
            raise
        finally:
            cursor.close()
            conn.close()

    load_forecast(extract_and_transform())


weather_forecast_two_cities()
