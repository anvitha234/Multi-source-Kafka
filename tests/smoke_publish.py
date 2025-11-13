# tests/smoke_publish.py
from producer_base import ProducerBase
from datetime import datetime, timezone
from config import KAFKA_TOPIC

p = ProducerBase()
msg = {
    "source": "smoke_test",
    "event_time": datetime.now(timezone.utc).isoformat(),
    "location": {"lat": 12.9716, "lon": 77.5946, "place_id": "smoke"},
    "params": {"temperature_c": 30.5, "windspeed_mps": 1.2},
    "meta": {"orig_topic": "smoke", "orig_id": None}
}
p.send(KAFKA_TOPIC, msg)
print("Sent smoke test message")
