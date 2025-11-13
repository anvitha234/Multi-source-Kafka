# sinks/timescale_writer.py
import json
import time
from datetime import datetime
import psycopg2
from psycopg2.extras import Json
from consumer_base import ConsumerBase
from config import TS_HOST, TS_PORT, TS_DB, TS_USER, TS_PASSWORD, KAFKA_TOPIC

TABLE_SQL = """
CREATE TABLE IF NOT EXISTS measurements (
    source TEXT NOT NULL,
    event_time TIMESTAMP WITH TIME ZONE NOT NULL,
    lat DOUBLE PRECISION,
    lon DOUBLE PRECISION,
    params JSONB,
    meta JSONB,
    PRIMARY KEY (source, event_time, lat, lon)
);
"""

CREATE_HYPERTABLE_SQL = """
SELECT create_hypertable('measurements', 'event_time', if_not_exists => TRUE);
"""

UPSERT_SQL = """
INSERT INTO measurements (source, event_time, lat, lon, params, meta)
VALUES (%s, %s, %s, %s, %s, %s)
ON CONFLICT (source, event_time, lat, lon)
DO UPDATE SET params = EXCLUDED.params, meta = EXCLUDED.meta;
"""

class TimescaleWriter:
    def __init__(self):
        self.consumer = ConsumerBase(KAFKA_TOPIC, group_id="timescale_writer_group")
        self.conn = None
        self._connect_db()
        self._prepare_table()

    def _connect_db(self):
        tries = 0
        while True:
            try:
                self.conn = psycopg2.connect(
                    host=TS_HOST, port=TS_PORT, dbname=TS_DB, user=TS_USER, password=TS_PASSWORD
                )
                self.conn.autocommit = True
                print("[TimescaleWriter] Connected to TimescaleDB")
                break
            except Exception as e:
                tries += 1
                print(f"[TimescaleWriter] DB connection failed ({tries}): {e}. Retrying in 5s...")
                time.sleep(5)

    def _prepare_table(self):
        with self.conn.cursor() as cur:
            cur.execute(TABLE_SQL)
            # create hypertable (if timescaledb installed; function exists)
            try:
                cur.execute(CREATE_HYPERTABLE_SQL)
            except Exception as e:
                # Possibly TimescaleDB not installed; ignore gracefully
                print("[TimescaleWriter] create_hypertable() might not be available:", e)

    def _parse_message(self, msg):
        # msg is a dict per schema
        source = msg.get("source")
        event_time = msg.get("event_time")
        # make sure event_time is a proper timestamp for psycopg2
        try:
            # psycopg2 handles ISO-8601 strings
            ts = event_time
        except:
            ts = datetime.utcnow().isoformat()

        loc = msg.get("location") or {}
        lat = loc.get("lat")
        lon = loc.get("lon")
        params = msg.get("params") or {}
        meta = msg.get("meta") or {}
        return source, ts, lat, lon, json.dumps(params), json.dumps(meta)

    def run(self):
        print("[TimescaleWriter] Starting consumer loop...")
        while True:
            try:
                for record in self.consumer:
                    if record is None:
                        continue
                    msg = record.value
                    try:
                        source, ts, lat, lon, params_json, meta_json = self._parse_message(msg)
                        with self.conn.cursor() as cur:
                            cur.execute(UPSERT_SQL, (source, ts, lat, lon, Json(json.loads(params_json)), Json(json.loads(meta_json))))
                            print(f"[TimescaleWriter] Upserted {source} @ {ts}")
                    except Exception as e:
                        print(f"[TimescaleWriter] Error processing message: {e}")
            except Exception as e:
                print(f"[TimescaleWriter] Consumer loop error: {e}. Reconnecting consumer in 5s...")
                time.sleep(5)

    def close(self):
        try:
            self.consumer.close()
        except:
            pass
        try:
            if self.conn:
                self.conn.close()
        except:
            pass

if __name__ == "__main__":
    w = TimescaleWriter()
    try:
        w.run()
    except KeyboardInterrupt:
        w.close()
