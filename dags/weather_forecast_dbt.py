"""Run dbt when the two-city ETL succeeds; use the existing Airflow connection."""

import os
import subprocess

import pendulum
from airflow.datasets import Dataset
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook


WEATHER_READY = Dataset("snowflake://DATA_226/RAW/WEATHER_FORECAST")
DBT_ROOT = "/opt/airflow/dbt/weather_forecast"


def run_dbt(command):
    connection = BaseHook.get_connection("snowflake_acc_data220")
    extra = connection.extra_dejson
    account = extra.get("account") or connection.host
    if not account or not connection.login or not connection.password:
        raise ValueError("Snowflake Airflow connection needs account, login, and password for dbt")
    env = os.environ.copy()
    env.update(
        SNOWFLAKE_ACCOUNT=account,
        SNOWFLAKE_USER=connection.login,
        SNOWFLAKE_PASSWORD=connection.password,
        SNOWFLAKE_DATABASE=extra.get("database") or connection.schema or "DATA_226",
        SNOWFLAKE_WAREHOUSE=extra.get("warehouse") or "COMPUTE_WH",
        SNOWFLAKE_ROLE=extra.get("role") or "ACCOUNTADMIN",
    )
    subprocess.run(
        ["dbt", command, "--project-dir", DBT_ROOT, "--profiles-dir", DBT_ROOT],
        env=env,
        check=True,
    )


@dag(
    dag_id="weather_forecast_dbt",
    start_date=pendulum.datetime(2026, 10, 1, tz="America/Los_Angeles"),
    schedule=[WEATHER_READY],
    catchup=False,
    max_active_runs=1,
    tags=["weather", "dbt"],
)
def weather_forecast_dbt():
    @task
    def dbt_run():
        run_dbt("run")

    @task
    def dbt_test():
        run_dbt("test")

    @task
    def dbt_snapshot():
        run_dbt("snapshot")

    dbt_run() >> dbt_test() >> dbt_snapshot()


weather_forecast_dbt()
