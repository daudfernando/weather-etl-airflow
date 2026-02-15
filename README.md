# Weather Dashboard: End-to-End ETL Data Pipeline

An end-to-end local data pipeline that ingests weather data from the **Weatherstack API** every **10 minutes**, orchestrated by **Apache Airflow**, stored in **Postgres (Docker)**, and visualized in **Metabase**.

![Dashboard Screenshot](dashboard.png)

---

## What This Project Does

- **Extract**: Pulls current weather data from Weatherstack (API).
- **Orchestrate**: Airflow runs an ETL DAG on a schedule (`*/10 * * * *`).
- **Load**: Inserts records into Postgres.
- **Retention**: Keeps only the last **2 days** of data (auto-cleanup).
- **Visualize**: Metabase connects to Postgres to build dashboards.

---

## Tech Stack

- Apache Airflow
- Docker / Docker Compose
- PostgreSQL
- Metabase
- Python (`requests`, `psycopg2-binary`)

---

## Project Structure
.
├─ dags/
│ └─ weatherstack_to_postgres.py
├─ docker-compose.yml
├─ .env.example
├─ README.md
└─ dashboard.png


> **Important:** `.env` and `.env.airflow` are not committed for security reasons.

---

## Quick Start

### 1) Start Postgres + Metabase (Docker)

```bash
docker compose up -d

Postgres runs on localhost:5433 (host) → 5432 (container)

Metabase runs on http://localhost:3000

2) Configure Metabase (first time)

Open http://localhost:3000 and add a PostgreSQL database:

Host: postgres

Port: 5432

Database: weatherdb

Username: weather

Password: weather

Airflow Setup (Local / WSL)
1) Activate venv + set Airflow home
source .venv/bin/activate
export AIRFLOW_HOME="$PWD/airflow_home"

2) Create .env.airflow (DO NOT COMMIT)
WEATHERSTACK_API_KEY=PUT_YOUR_KEY_HERE
WEATHERSTACK_QUERY=Jakarta
WEATHERSTACK_UNITS=m

PG_HOST=localhost
PG_PORT=5433
PG_DATABASE=weatherdb
PG_USER=weather
PG_PASSWORD=weather


Load it:

set -a
source .env.airflow
set +a

3) Copy DAG & start Airflow
mkdir -p "$AIRFLOW_HOME/dags"
cp -v dags/*.py "$AIRFLOW_HOME/dags/"
airflow standalone


Airflow UI: http://localhost:8080

4) Enable and trigger the DAG
airflow dags unpause weatherstack_to_postgres
airflow dags trigger weatherstack_to_postgres

Verify Data in Postgres
docker exec -it weather_pg psql -U weather -d weatherdb

SELECT id, location_name, temperature_c, humidity, fetched_at
FROM weather_current
ORDER BY fetched_at DESC
LIMIT 10;

Security Notes

Never commit .env or .env.airflow

Use .env.example as a template

Rotate your API key if it ever gets exposed


---

## 2) Cara push README.md ke GitHub (update repo)
Dari WSL di folder repo:

```bash
cd "/mnt/c/Data Engineer/Weather/chatgpt"
git status