# producers/openaq_producer.py
import time
import requests
from datetime import datetime, timezone
from producer_base import ProducerBase
from config import OPENMETEO_LAT, OPENMETEO_LON, PRODUCER_INTERVAL_SECONDS, KAFKA_TOPIC

class OpenAQProducer(ProducerBase):
    API = "https://api.openaq.org/v2/latest"

    def __init__(self, interval=PRODUCER_INTERVAL_SECONDS, radius=10000):
        super().__init__()
        self.interval = interval
        self.radius = radius

    def build_message(self, resp_json):
        results = resp_json.get("results") or []
        if not results:
            now = datetime.now(timezone.utc).isoformat()
            return {
                "source": "openaq",
                "event_time": now,
                "location": {"lat": OPENMETEO_LAT, "lon": OPENMETEO_LON, "place_id": None},
                "params": {"aq_values": None},
                "meta": {"orig_topic": "openaq_api", "orig_id": None}
            }
        # pick first station
        st = results[0]
        coords = st.get("coordinates") or {}
        measurements = st.get("measurements") or []
        # convert to dict: parameter -> value
        m = {}
        event_time = None
        for mm in measurements:
            param = mm.get("parameter")
            val = mm.get("value")
            m[param] = val
            if not event_time and mm.get("lastUpdated"):
                event_time = mm.get("lastUpdated")
        if not event_time:
            event_time = datetime.now(timezone.utc).isoformat()

        msg = {
            "source": "openaq",
            "event_time": event_time,
            "location": {
                "lat": coords.get("latitude", OPENMETEO_LAT),
                "lon": coords.get("longitude", OPENMETEO_LON),
                "place_id": st.get("location")
            },
            "params": {
                "aq_values": m
            },
            "meta": {
                "orig_topic": "openaq_api",
                "orig_id": st.get("location")
            }
        }
        return msg

    def run(self):
        print("[OpenAQProducer] Starting poll loop...")
        while True:
            try:
                params = {
                    "coordinates": f"{OPENMETEO_LAT},{OPENMETEO_LON}",
                    "radius": self.radius,
                    "limit": 1
                }
                r = requests.get(self.API, params=params, timeout=10)
                r.raise_for_status()
                data = r.json()
                msg = self.build_message(data)
                self.send(KAFKA_TOPIC, msg)
                print(f"[OpenAQProducer] Sent: {msg['event_time']} values={msg['params']['aq_values']}")
            except Exception as e:
                print(f"[OpenAQProducer] Error fetching/sending: {e}")
            time.sleep(self.interval)

if __name__ == "__main__":
    p = OpenAQProducer()
    p.run()
