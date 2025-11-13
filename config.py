# config.py
from dotenv import load_dotenv
import os

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "processed.normalized")

OPENMETEO_LAT = float(os.getenv("OPENMETEO_LAT", "12.9716"))
OPENMETEO_LON = float(os.getenv("OPENMETEO_LON", "77.5946"))
PRODUCER_INTERVAL_SECONDS = int(os.getenv("PRODUCER_INTERVAL_SECONDS", "60"))

CSV_INPUT_FILE = os.getenv("CSV_INPUT_FILE", "data/sample_traffic.csv")
CSV_PUBLISH_ONCE = os.getenv("CSV_PUBLISH_ONCE", "true").lower() in ("1","true","yes")

# TimescaleDB
TS_HOST = os.getenv("TIMESCALE_HOST", "localhost")
TS_PORT = int(os.getenv("TIMESCALE_PORT", "5432"))
TS_DB = os.getenv("TIMESCALE_DB", "timescale_demo")
TS_USER = os.getenv("TIMESCALE_USER", "postgres")
TS_PASSWORD = os.getenv("TIMESCALE_PASSWORD", "postgres")
