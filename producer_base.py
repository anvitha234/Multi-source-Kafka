# producer_base.py (confluent-kafka version)
import json
from confluent_kafka import Producer
from config import KAFKA_BOOTSTRAP_SERVERS


class ProducerBase:
    def __init__(self, bootstrap_servers=None):
        bs = bootstrap_servers or KAFKA_BOOTSTRAP_SERVERS
        self.producer = Producer({
            "bootstrap.servers": bs,
            "queue.buffering.max.messages": 100000
        })

    def _delivery_report(self, err, msg):
        if err is not None:
            print(f"[ProducerBase] Delivery failed: {err}")
        else:
            print(f"[ProducerBase] Delivered to {msg.topic()} [{msg.partition()}] offset {msg.offset()}")

    def send(self, topic, message):
        try:
            self.producer.produce(
                topic,
                json.dumps(message).encode("utf-8"),
                callback=self._delivery_report
            )
            self.producer.flush(5)
        except Exception as e:
            print(f"[ProducerBase] Error sending to {topic}: {e}")
