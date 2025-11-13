# producers/openmeteo_producer.py
import time
import requests
from datetime import datetime, timezone
from producer_base import ProducerBase
from config import (
    OPENMETEO_LAT, OPENMETEO_LON, PRODUCER_INTERVAL_SECONDS, KAFKA_TOPIC
)

class OpenMeteoProducer(ProducerBase):
    API = "https://api.open-meteo.com/v1/forecast"

    def __init__(self, interval=PRODUCER_INTERVAL_SECONDS):
        super().__init__()
        self.interval = interval

    def build_message(self, resp_json):
        # Use current_weather if present
        cw = resp_json.get("current_weather", {})
        event_time = cw.get("time")
        if not event_time:
            event_time = datetime.now(timezone.utc).isoformat()

        msg = {
            "source": "openmeteo",
            "event_time": event_time,
            "location": {
                "lat": float(resp_json.get("latitude", OPENMETEO_LAT)),
                "lon": float(resp_json.get("longitude", OPENMETEO_LON)),
                "place_id": None
            },
            "params": {
                "temperature_c": cw.get("temperature"),
                "windspeed_mps": cw.get("windspeed"),
            },
            "meta": {
                "orig_topic": "openmeteo_api",
                "orig_id": None
            }
        }
        return msg

    def run(self):
        print("[OpenMeteoProducer] Starting poll loop...")
        while True:
            try:
                params = {
                    "latitude": OPENMETEO_LAT,
                    "longitude": OPENMETEO_LON,
                    "current_weather": "true"
                }
                r = requests.get(self.API, params=params, timeout=10)
                r.raise_for_status()
                data = r.json()
                msg = self.build_message(data)
                self.send(KAFKA_TOPIC, msg)
                print(f"[OpenMeteoProducer] Sent: {msg['event_time']} temp={msg['params']['temperature_c']}")
            except Exception as e:
                print(f"[OpenMeteoProducer] Error fetching/sending: {e}")
            time.sleep(self.interval)

if __name__ == "__main__":
    p = OpenMeteoProducer()
    p.run()
