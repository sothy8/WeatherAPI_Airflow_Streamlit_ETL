import os

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text


def db_url() -> str:
    in_docker = os.path.exists("/.dockerenv")
    host = os.getenv("WEATHER_DB_HOST", "postgres" if in_docker else "localhost")
    port = os.getenv("WEATHER_DB_PORT", "5432" if in_docker else "5433")
    db = os.getenv("WEATHER_DB_NAME") or os.getenv("POSTGRES_DB", "airflow")
    user = os.getenv("WEATHER_DB_USER") or os.getenv("POSTGRES_USER", "airflow")
    password = os.getenv("WEATHER_DB_PASSWORD") or os.getenv("POSTGRES_PASSWORD", "airflow")
    return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"


st.set_page_config(page_title="Weather Dashboard", layout="wide")

location_filter = os.getenv("WEATHER_LOCATION", "")
st.title("Weather Dashboard")

engine = create_engine(db_url(), pool_pre_ping=True)

query = text(
    """
    SELECT
      fetched_at,
      observed_at,
      location_name,
      temp_c,
      humidity,
      wind_kph,
      pressure_mb,
      precip_mm,
      condition_text
    FROM weather.observations
    WHERE (:loc = '' OR location_name = :loc)
    ORDER BY fetched_at DESC
    LIMIT 500;
    """
)

with engine.connect() as conn:
    df = pd.read_sql(query, conn, params={"loc": location_filter})

if df.empty:
    st.info("No rows yet. Trigger the Airflow DAG to load data.")
    st.stop()

df["fetched_at"] = pd.to_datetime(df["fetched_at"], utc=True)
df = df.sort_values("fetched_at")

latest = df.iloc[-1]

c1, c2, c3, c4 = st.columns(4)

c1.metric("Temp (°C)", f"{latest['temp_c']}")
c2.metric("Humidity (%)", f"{latest['humidity']}")
c3.metric("Wind (kph)", f"{latest['wind_kph']}")
c4.metric("Pressure (mb)", f"{latest['pressure_mb']}")

st.subheader("Temperature over time")
st.line_chart(df.set_index("fetched_at")["temp_c"], height=250)

st.subheader("Humidity over time")
st.line_chart(df.set_index("fetched_at")["humidity"], height=250)

st.subheader("Wind speed over time")
st.line_chart(df.set_index("fetched_at")["wind_kph"], height=250)

st.subheader("Precipitation over time")
st.line_chart(df.set_index("fetched_at")["precip_mm"], height=250)

with st.expander("Latest rows"):
    st.dataframe(df.tail(50), use_container_width=True)
