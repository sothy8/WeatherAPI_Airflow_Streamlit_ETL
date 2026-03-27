from __future__ import annotations

import json
import os
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

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


def _to_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _clamp(value: Optional[float], low: float, high: float) -> Optional[float]:
    if value is None:
        return None
    if value < low or value > high:
        return None
    return value


def _compact_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _db_params() -> Dict[str, Any]:
    host = os.getenv("WEATHER_DB_HOST", "postgres")
    port = int(os.getenv("WEATHER_DB_PORT", "5432"))
    dbname = os.getenv("WEATHER_DB_NAME") or os.getenv("POSTGRES_DB", "airflow")
    user = os.getenv("WEATHER_DB_USER") or os.getenv("POSTGRES_USER", "airflow")
    password = os.getenv("WEATHER_DB_PASSWORD") or os.getenv("POSTGRES_PASSWORD", "airflow")
    return {"host": host, "port": port, "dbname": dbname, "user": user, "password": password}


def _parse_iso_date(value: str, fallback: date) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError:
        return fallback


def _preprocess_payload(raw: Dict[str, Any]) -> Dict[str, Any]:
    loc = dict(raw.get("location") or {})
    current = dict(raw.get("current") or {})
    condition = dict(current.get("condition") or {})

    loc["name"] = _compact_text(loc.get("name")) or os.getenv("WEATHER_LOCATION", "Unknown")
    loc["region"] = _compact_text(loc.get("region"))
    loc["country"] = _compact_text(loc.get("country"))
    loc["lat"] = _clamp(_to_float(loc.get("lat")), -90.0, 90.0)
    loc["lon"] = _clamp(_to_float(loc.get("lon")), -180.0, 180.0)

    current["temp_c"] = _clamp(_to_float(current.get("temp_c")), -90.0, 65.0)
    current["wind_kph"] = _clamp(_to_float(current.get("wind_kph")), 0.0, 400.0)
    humidity = _to_int(current.get("humidity"))
    current["humidity"] = humidity if humidity is not None and 0 <= humidity <= 100 else None
    current["pressure_mb"] = _clamp(_to_float(current.get("pressure_mb")), 800.0, 1200.0)
    current["precip_mm"] = _clamp(_to_float(current.get("precip_mm")), 0.0, 500.0)
    is_day = _to_int(current.get("is_day"))
    current["is_day"] = is_day if is_day in (0, 1) else None

    condition["text"] = _compact_text(condition.get("text"))
    current["condition"] = condition

    return {
        "location": loc,
        "current": current,
    }


def _transform_cleaned_payload(cleaned: Dict[str, Any], fetched_at: Optional[datetime] = None) -> Dict[str, Any]:
    loc = cleaned.get("location") or {}
    current = cleaned.get("current") or {}
    condition = current.get("condition") or {}

    observed_at = (
        _parse_epoch_seconds(current.get("last_updated_epoch"))
        or _parse_epoch_seconds(loc.get("localtime_epoch"))
    )

    effective_fetched_at = fetched_at or datetime.now(timezone.utc)

    return {
        "fetched_at": effective_fetched_at.isoformat(),
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
        "raw": json.dumps(cleaned),
    }


def _upsert_observation(cur, row: Dict[str, Any]) -> int:
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
        ON CONFLICT (location_name, observed_at)
        DO UPDATE SET
          fetched_at = EXCLUDED.fetched_at,
          region = EXCLUDED.region,
          country = EXCLUDED.country,
          lat = EXCLUDED.lat,
          lon = EXCLUDED.lon,
          temp_c = EXCLUDED.temp_c,
          wind_kph = EXCLUDED.wind_kph,
          humidity = EXCLUDED.humidity,
          pressure_mb = EXCLUDED.pressure_mb,
          precip_mm = EXCLUDED.precip_mm,
          condition_text = EXCLUDED.condition_text,
          is_day = EXCLUDED.is_day,
          raw = EXCLUDED.raw
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
    cur.execute(
        """
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1
                FROM pg_constraint
                WHERE conname = 'uq_weather_observations_location_observed_at'
                    AND connamespace = 'weather'::regnamespace
            ) THEN
                ALTER TABLE weather.observations
                ADD CONSTRAINT uq_weather_observations_location_observed_at
                UNIQUE (location_name, observed_at);
            END IF;
        END
        $$;
        """
    )


@dag(
    dag_id="weatherapi_extract_transform_load",
    schedule="0 * * * *",
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
    def preprocess_weather(raw: Dict[str, Any]) -> Dict[str, Any]:
        return _preprocess_payload(raw)

    @task
    def transform_weather(cleaned: Dict[str, Any]) -> Dict[str, Any]:
        return _transform_cleaned_payload(cleaned)

    @task
    def load_to_postgres(row: Dict[str, Any]) -> int:
        params = _db_params()
        conn = psycopg2.connect(**params)
        try:
            conn.autocommit = True
            with conn.cursor() as cur:
                _ensure_schema_and_table(cur)
                return _upsert_observation(cur, row)
        finally:
            conn.close()

    raw = extract_weather()
    cleaned = preprocess_weather(raw)
    transformed = transform_weather(cleaned)
    load_to_postgres(transformed)


weather_etl()


@dag(
    dag_id="weatherapi_backfill_history",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
    tags=["weather", "etl", "backfill"],
)
def weather_history_backfill():
    @task
    def extract_history_rows() -> List[Dict[str, Any]]:
        api_key = _require_env("WEATHER_API_KEY")
        location = os.getenv("WEATHER_LOCATION", "Phnom Penh")

        today_utc = datetime.now(timezone.utc).date()
        start_date_raw = os.getenv("WEATHER_HISTORY_START_DATE", "2026-03-01")
        history_start = _parse_iso_date(start_date_raw, fallback=today_utc)
        history_end = today_utc
        if history_start > history_end:
            history_start = history_end

        url = "https://api.weatherapi.com/v1/history.json"
        rows: List[Dict[str, Any]] = []

        day = history_start
        while day <= history_end:
            params = {"key": api_key, "q": location, "dt": day.isoformat(), "hour": "0-23"}
            resp = requests.get(url, params=params, timeout=30)
            resp.raise_for_status()
            payload = resp.json()

            loc = payload.get("location") or {}
            forecast_days = ((payload.get("forecast") or {}).get("forecastday")) or []
            if not forecast_days:
                day += timedelta(days=1)
                continue

            hours = forecast_days[0].get("hour") or []
            for hour_item in hours:
                raw = {
                    "location": loc,
                    "current": {
                        "last_updated_epoch": hour_item.get("time_epoch"),
                        "temp_c": hour_item.get("temp_c"),
                        "wind_kph": hour_item.get("wind_kph"),
                        "humidity": hour_item.get("humidity"),
                        "pressure_mb": hour_item.get("pressure_mb"),
                        "precip_mm": hour_item.get("precip_mm"),
                        "condition": hour_item.get("condition") or {},
                        "is_day": hour_item.get("is_day"),
                    },
                }
                cleaned = _preprocess_payload(raw)
                observed_at = _parse_epoch_seconds(hour_item.get("time_epoch"))
                if observed_at is None:
                    continue
                rows.append(_transform_cleaned_payload(cleaned, fetched_at=observed_at))

            day += timedelta(days=1)

        return rows

    @task
    def load_history_rows(rows: List[Dict[str, Any]]) -> int:
        if not rows:
            return 0

        params = _db_params()
        conn = psycopg2.connect(**params)
        try:
            conn.autocommit = True
            inserted = 0
            with conn.cursor() as cur:
                _ensure_schema_and_table(cur)
                for row in rows:
                    _upsert_observation(cur, row)
                    inserted += 1
            return inserted
        finally:
            conn.close()

    history_rows = extract_history_rows()
    load_history_rows(history_rows)


weather_history_backfill()
