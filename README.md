# Multi-source Demo (Option A)

Minimal demo: multiple producers (Open-Meteo, OpenAQ, CSV) produce normalized JSON to a single Kafka topic `processed.normalized`. A TimescaleDB sink consumer writes records to TimescaleDB for Grafana visualization.

## Prereqs
- Python 3.9+ (tested)
- Docker & docker-compose (for TimescaleDB + Grafana)
- A running Kafka KRaft instance (your local Kafka). Confirm the bootstrap server (default: `localhost:9092`).

## Install
1. Clone repo and `cd multi-source-demo`.
2. Create `.env` (copy the sample .env values).
3. Install Python deps (prefer virtualenv):
```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Start TimescaleDB & Grafana (optional)
```bash
./scripts/run_local.sh
# or
docker-compose up -d
```
Timescale DB: `localhost:5432`. Grafana: `http://localhost:3000` (admin/admin by default).

## Create TimescaleDB DB (if required)
If the container started fresh, DB is created automatically. If you need to create DB manually, login:
```bash
psql -h localhost -U postgres -c "CREATE DATABASE timescale_demo;"
```

## Run the sink (Timescale writer)
Start the sink consumer (it will wait for Timescale to accept connections):
```bash
python sinks/timescale_writer.py
```

## Start producers (each in its own terminal)
- OpenMeteo:
```bash
python producers/openmeteo_producer.py
```
- OpenAQ:
```bash
python producers/openaq_producer.py
```
- CSV (sends rows once by default):
```bash
python producers/csv_producer.py
```

## Smoke-test
In another terminal:
```bash
python tests/smoke_publish.py
```
Then check Timescale for rows:
```bash
python tests/check_timescale.py
```

## Grafana
- Open `http://localhost:3000`, login (admin/admin or as configured).
- Add a PostgreSQL datasource using:
  - Host: `timescaledb:5432` (if Grafana in docker compose) or `localhost:5432` (if local)
  - DB: `timescale_demo`
  - User/Password: as in `.env`
- Import or create a dashboard that queries `measurements` table.

## Notes & next steps
- The producers currently normalize data before writing to Kafka. Later you can move normalization to ksqlDB by changing producers to write `raw.*` topics.
- The Timescale writer uses `INSERT ... ON CONFLICT` to be idempotent.
- Use `.env` to change Kafka endpoint or Timescale credentials without editing code.
