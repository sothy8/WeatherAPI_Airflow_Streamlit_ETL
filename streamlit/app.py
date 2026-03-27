import os
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import altair as alt
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
dashboard_tz_name = os.getenv("WEATHER_DASHBOARD_TZ", "Asia/Phnom_Penh")
try:
    dashboard_tz = ZoneInfo(dashboard_tz_name)
except ZoneInfoNotFoundError:
    dashboard_tz = ZoneInfo("UTC")
    dashboard_tz_name = "UTC"

st.title("Weather Dashboard")
st.caption("Hourly weather monitoring with cleaned ETL data")

engine = create_engine(db_url(), pool_pre_ping=True)

query = text(
    """
    SELECT
      fetched_at,
      observed_at,
      location_name,
            region,
            country,
            lat,
            lon,
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
df["fetched_at_local"] = df["fetched_at"].dt.tz_convert(dashboard_tz)
df["observed_at"] = pd.to_datetime(df["observed_at"], utc=True, errors="coerce")
for col in ["temp_c", "humidity", "wind_kph", "pressure_mb", "precip_mm", "lat", "lon"]:
    df[col] = pd.to_numeric(df[col], errors="coerce")

# Keep latest fetch per location/timestamp to avoid duplicate points after upserts.
df = df.drop_duplicates(subset=["location_name", "observed_at", "fetched_at"], keep="last")
df = df.sort_values("fetched_at")

st.sidebar.header("Filters")
available_days = sorted(df["fetched_at_local"].dt.date.dropna().unique().tolist())
if not available_days:
    st.info("No valid timestamps available for daily visualization.")
    st.stop()

default_day = available_days[-1]
selected_day = st.sidebar.date_input(
    "Select day",
    value=default_day,
    min_value=available_days[0],
    max_value=available_days[-1],
)

locations = sorted([loc for loc in df["location_name"].dropna().unique().tolist() if str(loc).strip()])
if locations:
    selected_locations = st.sidebar.multiselect("Locations", options=locations, default=locations)
    if selected_locations:
        df = df[df["location_name"].isin(selected_locations)]

day_start = pd.Timestamp(selected_day).tz_localize(dashboard_tz)
day_end = day_start + pd.Timedelta(days=1)
window_start = day_start + pd.Timedelta(hours=1)
now_local = pd.Timestamp.now(tz=dashboard_tz)
window_end = min(now_local, day_end) if selected_day == now_local.date() else day_end
if window_end <= window_start:
    window_end = min(day_end, window_start + pd.Timedelta(hours=1))

df = df[(df["fetched_at_local"] >= window_start) & (df["fetched_at_local"] < window_end)]

if df.empty:
    st.info("No rows match the selected day (1 AM onward) and location filters.")
    st.stop()

latest = df.sort_values("fetched_at").iloc[-1]
st.caption(
    f"Showing data from 01:00 to {window_end.strftime('%H:%M')} {dashboard_tz_name} on {selected_day.isoformat()}"
)

c1, c2, c3, c4 = st.columns(4)

c1.metric("Temp (deg C)", f"{latest['temp_c']:.1f}" if pd.notna(latest["temp_c"]) else "N/A")
c2.metric("Humidity (%)", f"{latest['humidity']:.0f}" if pd.notna(latest["humidity"]) else "N/A")
c3.metric("Wind (kph)", f"{latest['wind_kph']:.1f}" if pd.notna(latest["wind_kph"]) else "N/A")
c4.metric("Pressure (mb)", f"{latest['pressure_mb']:.1f}" if pd.notna(latest["pressure_mb"]) else "N/A")

plot_df = df.copy()
plot_df["hour_ts"] = plot_df["fetched_at_local"].dt.floor("h")

hourly_df = (
    plot_df.groupby(["location_name", "hour_ts"], as_index=False)
    .agg(
        temp_c=("temp_c", "mean"),
        humidity=("humidity", "mean"),
        wind_kph=("wind_kph", "mean"),
        pressure_mb=("pressure_mb", "mean"),
        precip_mm=("precip_mm", "mean"),
        condition_text=("condition_text", "last"),
    )
)

hour_axis = pd.date_range(
    start=window_start.floor("h"),
    end=window_end.floor("h"),
    freq="h",
)
if hour_axis.empty:
    hour_axis = pd.DatetimeIndex([window_start.floor("h")])
locations_in_scope = sorted(
    [loc for loc in hourly_df["location_name"].dropna().unique().tolist() if str(loc).strip()]
)
if locations_in_scope:
    full_index = pd.MultiIndex.from_product(
        [locations_in_scope, hour_axis], names=["location_name", "hour_ts"]
    )
    hourly_df = (
        hourly_df.set_index(["location_name", "hour_ts"])
        .reindex(full_index)
        .reset_index()
    )

hourly_df["hour_of_day"] = hourly_df["hour_ts"].dt.hour

coverage_hours = plot_df["hour_ts"].nunique()
expected_hours = len(hour_axis)
st.caption(f"Data coverage in selected window: {coverage_hours}/{expected_hours} hourly points")

st.subheader("Temperature trend")
temp_vals = hourly_df["temp_c"].dropna()
temp_min = temp_vals.min() if not temp_vals.empty else None
temp_max = temp_vals.max() if not temp_vals.empty else None
if temp_min is not None and temp_max is not None:
    if temp_min == temp_max:
        y_domain = [temp_min - 1.0, temp_max + 1.0]
        st.info("Temperature is currently stable in this window, so the line appears flat.")
    else:
        pad = max(0.5, (temp_max - temp_min) * 0.2)
        y_domain = [temp_min - pad, temp_max + pad]
else:
    y_domain = None

temp_line = (
    alt.Chart(hourly_df)
    .mark_line(point=True)
    .encode(
        x=alt.X("hour_ts:T", title=f"Hour ({dashboard_tz_name})", scale=alt.Scale(domain=[window_start, window_end])),
        y=alt.Y("temp_c:Q", title="Temperature (deg C)", scale=alt.Scale(domain=y_domain) if y_domain else alt.Undefined),
        color=alt.Color("location_name:N", title="Location"),
        tooltip=["hour_ts:T", "location_name:N", "temp_c:Q", "condition_text:N"],
    )
)
st.altair_chart(temp_line.properties(height=280), width="stretch")

left, right = st.columns(2)

with left:
    st.subheader("Humidity area chart")
    humidity_area = (
        alt.Chart(hourly_df)
        .mark_area(opacity=0.45)
        .encode(
            x=alt.X("hour_ts:T", title=f"Hour ({dashboard_tz_name})", scale=alt.Scale(domain=[window_start, window_end])),
            y=alt.Y("humidity:Q", title="Humidity (%)"),
            color=alt.Color("location_name:N", title="Location"),
            tooltip=["hour_ts:T", "location_name:N", "humidity:Q"],
        )
        .properties(height=250)
    )
    st.altair_chart(humidity_area, width="stretch")

with right:
    st.subheader("Average precipitation by hour")
    precip_by_hour = (
        hourly_df.groupby("hour_of_day", as_index=False)["precip_mm"]
        .mean()
        .sort_values("hour_of_day")
    )
    if precip_by_hour.empty or precip_by_hour["precip_mm"].fillna(0).max() <= 0:
        st.info("No rainfall recorded on this day (all precipitation values are 0 mm).")
        sample_counts = (
            hourly_df.dropna(subset=["temp_c", "humidity", "wind_kph", "pressure_mb", "precip_mm"], how="all")
            .groupby("hour_of_day", as_index=False)
            .size()
            .rename(columns={"size": "samples"})
            .sort_values("hour_of_day")
        )
        fallback_bar = (
            alt.Chart(sample_counts)
            .mark_bar(cornerRadiusTopLeft=3, cornerRadiusTopRight=3)
            .encode(
                x=alt.X("hour_of_day:O", title="Hour of day"),
                y=alt.Y("samples:Q", title="Observations"),
                tooltip=["hour_of_day:O", "samples:Q"],
            )
            .properties(height=250)
        )
        st.altair_chart(fallback_bar, width="stretch")
    else:
        precip_bar = (
            alt.Chart(precip_by_hour)
            .mark_bar(cornerRadiusTopLeft=3, cornerRadiusTopRight=3)
            .encode(
                x=alt.X("hour_of_day:O", title="Hour of day"),
                y=alt.Y("precip_mm:Q", title="Avg precipitation (mm)"),
                tooltip=["hour_of_day:O", "precip_mm:Q"],
            )
            .properties(height=250)
        )
        st.altair_chart(precip_bar, width="stretch")

st.subheader("Wind vs pressure scatter")
scatter = (
    alt.Chart(hourly_df.dropna(subset=["wind_kph", "pressure_mb"]))
    .mark_circle(size=80, opacity=0.7)
    .encode(
        x=alt.X("wind_kph:Q", title="Wind (kph)"),
        y=alt.Y("pressure_mb:Q", title="Pressure (mb)"),
        color=alt.Color("condition_text:N", title="Condition"),
        tooltip=["hour_ts:T", "location_name:N", "wind_kph:Q", "pressure_mb:Q", "condition_text:N"],
    )
    .properties(height=280)
)
st.altair_chart(scatter, width="stretch")

map_df = plot_df.dropna(subset=["lat", "lon"])[["lat", "lon"]].tail(100)
if not map_df.empty:
    st.subheader("Observation map")
    st.map(map_df, width="stretch")

with st.expander("Latest rows"):
    st.dataframe(df.tail(50), width="stretch")
