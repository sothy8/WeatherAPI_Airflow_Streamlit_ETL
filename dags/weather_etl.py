from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

import psycopg2
import requests
from airflow.decorators import dag, task


def _require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required env var: {name}")
    return value


def _parse_epoch_seconds(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    try:
        seconds = int(value)
    except (TypeError, ValueError):
        return None
    return datetime.fromtimestamp(seconds, tz=timezone.utc)


def _db_params() -> Dict[str, Any]:
    host = os.getenv("WEATHER_DB_HOST", "postgres")
    port = int(os.getenv("WEATHER_DB_PORT", "5432"))
    dbname = os.getenv("WEATHER_DB_NAME") or os.getenv("POSTGRES_DB", "airflow")
    user = os.getenv("WEATHER_DB_USER") or os.getenv("POSTGRES_USER", "airflow")
    password = os.getenv("WEATHER_DB_PASSWORD") or os.getenv("POSTGRES_PASSWORD", "airflow")
    return {"host": host, "port": port, "dbname": dbname, "user": user, "password": password}


def _ensure_schema_and_table(cur) -> None:
    cur.execute("CREATE SCHEMA IF NOT EXISTS weather;")
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS weather.observations (
          id BIGSERIAL PRIMARY KEY,
          fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          observed_at TIMESTAMPTZ NULL,
          location_name TEXT NOT NULL,
          region TEXT NULL,
          country TEXT NULL,
          lat DOUBLE PRECISION NULL,
          lon DOUBLE PRECISION NULL,
          temp_c DOUBLE PRECISION NULL,
          wind_kph DOUBLE PRECISION NULL,
          humidity INTEGER NULL,
          pressure_mb DOUBLE PRECISION NULL,
          precip_mm DOUBLE PRECISION NULL,
          condition_text TEXT NULL,
          is_day INTEGER NULL,
          raw JSONB NULL
        );
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_weather_observations_location_fetched_at
          ON weather.observations (location_name, fetched_at DESC);
        """
    )


@dag(
    dag_id="weatherapi_extract_transform_load",
    schedule="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
    tags=["weather", "etl"],
)
def weather_etl():
    @task
    def extract_weather() -> Dict[str, Any]:
        api_key = _require_env("WEATHER_API_KEY")
        location = os.getenv("WEATHER_LOCATION", "Phnom Penh")

        url = "https://api.weatherapi.com/v1/current.json"
        params = {"key": api_key, "q": location, "aqi": "no"}
        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()
        return resp.json()

    @task
    def transform_weather(raw: Dict[str, Any]) -> Dict[str, Any]:
        loc = raw.get("location") or {}
        current = raw.get("current") or {}
        condition = current.get("condition") or {}

        observed_at = (
            _parse_epoch_seconds(current.get("last_updated_epoch"))
            or _parse_epoch_seconds(loc.get("localtime_epoch"))
        )

        transformed = {
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "observed_at": observed_at.isoformat() if observed_at else None,
            "location_name": loc.get("name") or os.getenv("WEATHER_LOCATION", "Unknown"),
            "region": loc.get("region"),
            "country": loc.get("country"),
            "lat": loc.get("lat"),
            "lon": loc.get("lon"),
            "temp_c": current.get("temp_c"),
            "wind_kph": current.get("wind_kph"),
            "humidity": current.get("humidity"),
            "pressure_mb": current.get("pressure_mb"),
            "precip_mm": current.get("precip_mm"),
            "condition_text": condition.get("text"),
            "is_day": current.get("is_day"),
            "raw": json.dumps(raw),
        }
        return transformed

    @task
    def load_to_postgres(row: Dict[str, Any]) -> int:
        params = _db_params()
        conn = psycopg2.connect(**params)
        try:
            conn.autocommit = True
            with conn.cursor() as cur:
                _ensure_schema_and_table(cur)
                cur.execute(
                    """
                    INSERT INTO weather.observations (
                      fetched_at,
                      observed_at,
                      location_name,
                      region,
                      country,
                      lat,
                      lon,
                      temp_c,
                      wind_kph,
                      humidity,
                      pressure_mb,
                      precip_mm,
                      condition_text,
                      is_day,
                      raw
                    ) VALUES (
                      %s::timestamptz,
                      CASE WHEN %s IS NULL THEN NULL ELSE %s::timestamptz END,
                      %s,
                      %s,
                      %s,
                      %s,
                      %s,
                      %s,
                      %s,
                      %s,
                      %s,
                      %s,
                      %s,
                      %s,
                      %s::jsonb
                    )
                    RETURNING id;
                    """,
                    (
                        row.get("fetched_at"),
                        row.get("observed_at"),
                        row.get("observed_at"),
                        row.get("location_name"),
                        row.get("region"),
                        row.get("country"),
                        row.get("lat"),
                        row.get("lon"),
                        row.get("temp_c"),
                        row.get("wind_kph"),
                        row.get("humidity"),
                        row.get("pressure_mb"),
                        row.get("precip_mm"),
                        row.get("condition_text"),
                        row.get("is_day"),
                        row.get("raw"),
                    ),
                )
                new_id = cur.fetchone()[0]
                return int(new_id)
        finally:
            conn.close()

    raw = extract_weather()
    transformed = transform_weather(raw)
    load_to_postgres(transformed)


weather_etl()
