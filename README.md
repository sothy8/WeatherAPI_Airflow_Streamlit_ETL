# WeatherAPI Airflow Streamlit ETL

This project is a data engineering pipeline for extracting, transforming, and loading (ETL) weather data using Apache Airflow, Docker Compose, and Streamlit for visualization.

## Features
- **Automated Hourly ETL**: Runs every hour (`0 * * * *`) to extract, preprocess/clean, transform, and load weather data into PostgreSQL.
- **Data Preprocessing**: Cleans and validates key fields (numeric parsing, range filtering, text normalization) before transformation and loading.
- **Upsert Loading**: Uses conflict-aware loading on `(location_name, observed_at)` so reruns update existing records instead of creating duplicates.
- **Containerized Stack**: All services (Airflow, Postgres, Redis, Streamlit) run in Docker containers for easy setup and reproducibility.
- **Weather Dashboard**: Streamlit app with richer visuals (line, area, bar, scatter, and map) plus interactive filters.

## Project Structure
```
├── dags/                # Airflow DAGs (ETL logic)
├── logs/                # Airflow logs (auto-generated, ignored by git)
├── plugins/             # Custom Airflow plugins (if any)
├── streamlit/           # Streamlit dashboard app
│   ├── app.py           # Main Streamlit app
│   ├── Dockerfile       # Streamlit container
│   └── requirements.txt # Streamlit dependencies
├── docker-compose.yaml  # Docker Compose stack definition
├── .env                 # Environment variables (API keys, DB credentials)
└── .gitignore           # Files/folders to ignore in git
```

## Prerequisites
- Docker & Docker Compose
- (Optional) Python 3.x for local development

## Quick Start
1. **Clone the repository:**
   ```sh
   git clone <your-repo-url>
   cd airflow_weather
   ```
2. **Configure environment variables:**
   - Edit `.env` to set your `WEATHER_API_KEY` and other secrets.
3. **Start the stack:**
   ```sh
   docker compose up -d
   ```
4. **Access services:**
   - Airflow UI: [http://localhost:8080](http://localhost:8080)  (login: `airflow` / `airflow`)
   - Streamlit Dashboard: [http://localhost:8501](http://localhost:8501)
   - Postgres: `localhost:5433` (user/password/db in `.env`)

## Airflow Usage
- DAGs are defined in `dags/`. The main ETL DAG is `weather_etl.py`.
- Airflow will automatically pick up new DAGs and plugins on container restart.
- Logs are stored in `logs/` (ignored by git).

## Streamlit Dashboard
- The dashboard code is in `streamlit/app.py`.
- Modify and extend the dashboard as needed.

## Stopping & Cleaning Up
- Stop all containers:
  ```sh
  docker compose down
  ```
- To remove all data (including Postgres volume):
  ```sh
  docker compose down -v
  ```

## Customization
- Add new DAGs to `dags/`.
- Add new dependencies to `streamlit/requirements.txt` or Airflow via the Dockerfile.

## License
This project is for educational purposes. See LICENSE if provided.
