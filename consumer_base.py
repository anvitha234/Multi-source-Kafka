# consumer_base.py (confluent-kafka version)
import json
from confluent_kafka import Consumer, KafkaError
from config import KAFKA_BOOTSTRAP_SERVERS


class ConsumerBase:
    def __init__(self, topic, group_id="consumer_group", bootstrap_servers=None, auto_offset_reset="earliest"):
        bs = bootstrap_servers or KAFKA_BOOTSTRAP_SERVERS

        self.consumer = Consumer({
            "bootstrap.servers": bs,
            "group.id": group_id,
            "auto.offset.reset": auto_offset_reset,
            "enable.auto.commit": True,
        })

        self.topic = topic
        self.consumer.subscribe([topic])

    def __iter__(self):
        while True:
            msg = self.consumer.poll(timeout=1.0)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"[ConsumerBase] Error: {msg.error()}")
                continue

            try:
                decoded = json.loads(msg.value().decode("utf-8"))
            except Exception as e:
                print(f"[ConsumerBase] JSON decode error: {e}")
                continue

            class Rec:
                def __init__(self, val):
                    self.value = val

            yield Rec(decoded)

    def close(self):
        try:
            self.consumer.close()
        except Exception as e:
            print(f"[ConsumerBase] Close error: {e}")
