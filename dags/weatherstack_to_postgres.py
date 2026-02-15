from __future__ import annotations

import os
from datetime import timedelta

import pendulum
import psycopg2
import requests
from psycopg2.extras import Json

from airflow import DAG
from airflow.operators.python import PythonOperator


DAG_ID = "weatherstack_to_postgres"
RETENTION_DAYS = 2  # <-- hanya simpan 2 hari terakhir


def _get_env(name: str, default: str | None = None, required: bool = False) -> str:
    val = os.getenv(name, default)
    if required and (val is None or str(val).strip() == ""):
        raise ValueError(f"Missing required environment variable: {name}")
    return str(val)


def _pg_conn_params() -> dict:
    return {
        "host": _get_env("PG_HOST", "localhost"),
        "port": int(_get_env("PG_PORT", "5433")),
        "dbname": _get_env("PG_DATABASE", "weatherdb"),
        "user": _get_env("PG_USER", "weather"),
        "password": _get_env("PG_PASSWORD", "weather"),
    }


CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS weather_current (
  id BIGSERIAL PRIMARY KEY,
  query_text TEXT NOT NULL,
  location_name TEXT,
  region TEXT,
  country TEXT,
  latitude DOUBLE PRECISION,
  longitude DOUBLE PRECISION,

  observation_time TEXT,
  temperature_c REAL,
  humidity REAL,
  wind_speed REAL,
  wind_dir TEXT,
  pressure REAL,
  precip REAL,
  cloudcover REAL,
  uv_index REAL,
  visibility REAL,
  is_day TEXT,

  fetched_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  raw JSONB NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_weather_current_fetched_at
ON weather_current (fetched_at DESC);
"""


def ensure_table() -> None:
    conn = psycopg2.connect(**_pg_conn_params())
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(CREATE_TABLE_SQL)
    finally:
        conn.close()


def fetch_weather_from_api(query: str, api_key: str, units: str = "m") -> dict:
    base_urls = [
        "https://api.weatherstack.com/current",
        "http://api.weatherstack.com/current",
    ]

    last_error = None
    for base_url in base_urls:
        try:
            resp = requests.get(
                base_url,
                params={"access_key": api_key, "query": query, "units": units},
                timeout=20,
            )
            data = resp.json()

            if isinstance(data, dict) and data.get("success") is False:
                err = data.get("error", {})
                raise RuntimeError(f"Weatherstack API error (code={err.get('code')}): {err.get('info')}")

            if "current" not in data:
                raise RuntimeError(f"Unexpected API response shape: {data}")

            return data

        except Exception as e:
            last_error = e
            continue

    raise RuntimeError(f"Failed calling Weatherstack API (https/http). Last error: {last_error}")


def load_to_postgres(api_payload: dict) -> None:
    request_obj = api_payload.get("request", {}) or {}
    location = api_payload.get("location", {}) or {}
    current = api_payload.get("current", {}) or {}

    row = {
        "query_text": request_obj.get("query"),
        "location_name": location.get("name"),
        "region": location.get("region"),
        "country": location.get("country"),
        "latitude": float(location["lat"]) if location.get("lat") is not None else None,
        "longitude": float(location["lon"]) if location.get("lon") is not None else None,
        "observation_time": current.get("observation_time"),
        "temperature_c": current.get("temperature"),
        "humidity": current.get("humidity"),
        "wind_speed": current.get("wind_speed"),
        "wind_dir": current.get("wind_dir"),
        "pressure": current.get("pressure"),
        "precip": current.get("precip"),
        "cloudcover": current.get("cloudcover"),
        "uv_index": current.get("uv_index"),
        "visibility": current.get("visibility"),
        "is_day": current.get("is_day"),
        "raw": api_payload,
    }

    insert_sql = """
    INSERT INTO weather_current (
      query_text, location_name, region, country, latitude, longitude,
      observation_time, temperature_c, humidity, wind_speed, wind_dir,
      pressure, precip, cloudcover, uv_index, visibility, is_day,
      raw
    ) VALUES (
      %(query_text)s, %(location_name)s, %(region)s, %(country)s, %(latitude)s, %(longitude)s,
      %(observation_time)s, %(temperature_c)s, %(humidity)s, %(wind_speed)s, %(wind_dir)s,
      %(pressure)s, %(precip)s, %(cloudcover)s, %(uv_index)s, %(visibility)s, %(is_day)s,
      %(raw)s
    );
    """

    conn = psycopg2.connect(**_pg_conn_params())
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            row["raw"] = Json(row["raw"])
            cur.execute(insert_sql, row)
    finally:
        conn.close()


def cleanup_old_rows() -> None:
    conn = psycopg2.connect(**_pg_conn_params())
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM weather_current WHERE fetched_at < now() - (%s || ' days')::interval;",
                (RETENTION_DAYS,),
            )
    finally:
        conn.close()


def extract_and_load() -> None:
    api_key = _get_env("WEATHERSTACK_API_KEY", required=True)
    query = _get_env("WEATHERSTACK_QUERY", "Jakarta")
    units = _get_env("WEATHERSTACK_UNITS", "m")

    payload = fetch_weather_from_api(query=query, api_key=api_key, units=units)
    load_to_postgres(payload)


with DAG(
    dag_id=DAG_ID,
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Jakarta"),
    schedule="*/10 * * * *",  # tetap tiap 10 menit
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "airflow", "retries": 2, "retry_delay": timedelta(minutes=1)},
    tags=["demo", "weatherstack", "postgres"],
) as dag:

    t1 = PythonOperator(task_id="ensure_table", python_callable=ensure_table)
    t2 = PythonOperator(task_id="extract_and_load", python_callable=extract_and_load)
    t3 = PythonOperator(task_id="cleanup_old_rows", python_callable=cleanup_old_rows)

    t1 >> t2 >> t3
