# producers/csv_producer.py
import time
import pandas as pd
from datetime import datetime, timezone
from producer_base import ProducerBase
from config import CSV_INPUT_FILE, CSV_PUBLISH_ONCE, KAFKA_TOPIC, PRODUCER_INTERVAL_SECONDS

class CSVProducer(ProducerBase):
    def __init__(self, input_file=CSV_INPUT_FILE, once=CSV_PUBLISH_ONCE, interval=PRODUCER_INTERVAL_SECONDS):
        super().__init__()
        self.input_file = input_file
        self.once = once
        self.interval = interval

    def row_to_message(self, row):
        # expects columns: date, lat, lon, vehicle_count (if present)
        event_time = None
        if "date" in row and pd.notna(row["date"]):
            try:
                event_time = pd.to_datetime(row["date"]).to_pydatetime().astimezone(timezone.utc).isoformat()
            except:
                event_time = datetime.now(timezone.utc).isoformat()
        else:
            event_time = datetime.now(timezone.utc).isoformat()

        lat = float(row.get("lat") or row.get("latitude") or 0.0)
        lon = float(row.get("lon") or row.get("longitude") or 0.0)
        vehicle_count = None
        if "vehicle_count" in row and pd.notna(row["vehicle_count"]):
            vehicle_count = int(row["vehicle_count"])

        msg = {
            "source": "csv_traffic",
            "event_time": event_time,
            "location": {"lat": lat, "lon": lon, "place_id": None},
            "params": {"vehicle_count": vehicle_count},
            "meta": {"orig_topic": "csv", "orig_id": None}
        }
        return msg

    def run(self):
        print(f"[CSVProducer] Reading {self.input_file} ...")
        while True:
            try:
                df = pd.read_csv(self.input_file)
                for _, row in df.iterrows():
                    msg = self.row_to_message(row)
                    self.send(KAFKA_TOPIC, msg)
                    print(f"[CSVProducer] Sent row event_time={msg['event_time']} vehicle_count={msg['params']['vehicle_count']}")
                    time.sleep(0.1)
                if self.once:
                    break
            except Exception as e:
                print(f"[CSVProducer] Error reading/sending CSV: {e}")
            if self.once:
                break
            time.sleep(self.interval)

if __name__ == "__main__":
    p = CSVProducer()
    p.run()
