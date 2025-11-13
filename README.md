# Multi-source Version 1 

Multiple producers (Open-Meteo, OpenAQ, CSV) produce normalized JSON to a single Kafka topic `processed.normalized`. A TimescaleDB sink consumer writes records to TimescaleDB for Grafana visualization.

## Prereqs
- Python 3.9+
- Docker & docker-compose (for TimescaleDB + Grafana)
- A running Kafka KRaft instance (your local Kafka). (default: `localhost:9092`).

## Install
1. Clone repo and `cd multi-source-demo`.
2. Create `.env` (copy the sample .env values).
3. Install Python deps (prefer virtualenv):
```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

How to Run the Multi-Source Kafka Pipeline:

1. Go to Project Directory
`cd ~/multi-source-kafka`

2. Activate Virtual Environment
`source venv/bin/activate`

3. Start TimescaleDB + Grafana (Docker)
`sudo docker compose up -d
sudo docker compose ps`

4. Start the Kafka → Timescale Sink
Run this in a dedicated terminal window (it must stay open).
```
cd ~/multi-source-kafka
source venv/bin/activate
# stop old sink if running
pkill -f timescale_writer.py || true
# start sink (module mode)
python -m sinks.timescale_writer
```

Expected output:
[TimescaleWriter] Connected to TimescaleDB
[TimescaleWriter] Starting consumer loop...

5. Start All Producers (each in its own terminal)
Terminal 2 — OpenMeteo Producer
```
cd ~/multi-source-kafka
source venv/bin/activate
python -m producers.openmeteo_producer
```
Terminal 3 — OpenAQ Producer
```
cd ~/multi-source-kafka
source venv/bin/activate
python -m producers.openaq_producer
```
Terminal 4 — CSV Producer (one-off)
```
cd ~/multi-source-kafka
source venv/bin/activate
python -m producers.csv_producer
```

When running correctly, each producer prints:
Sent: {...}

The sink prints:
Upserted <source> @ <timestamp>

6. (Optional) Run Smoke Test
This validates that Kafka → TimescaleDB works.
```
cd ~/multi-source-kafka
source venv/bin/activate

python -m tests.smoke_publish
python -m tests.check_timescale
```
Expected:
Delivered to processed.normalized [...]
Sent smoke test message
Row count in measurements: <number>

7. View Real-Time Dashboard (Grafana)
Open in browser:
http://localhost:3000

Choose or create dashboards using data from the measurements table.

Stopping Everything
Stop Python processes:
```
pkill -f openmeteo_producer.py || true
pkill -f openaq_producer.py || true
pkill -f csv_producer.py || true
pkill -f timescale_writer.py || true
```
Stop Docker:
`sudo docker compose down`
