import json
import time
from dalga.core import DalgaClient
from dalga.adapters.kafka import DalgaKafka


class MockKafkaMessage:
    def __init__(self, value_dict):
        self._value = json.dumps(value_dict).encode("utf-8")

    def error(self):
        return None

    def value(self):
        return self._value


message_queue = [
    MockKafkaMessage({"event": "login", "user": 101, "ping": 45}),
    MockKafkaMessage({"event": "login", "user": 102, "ping": 120}),
    MockKafkaMessage({"event": "logout", "user": 101, "ping": None}),
]

dalga = DalgaClient(max_records=10)
kafka_adapter = DalgaKafka(dalga)

print("Starting Kafka Consumer Loop.")
for i in range(3):
    print(f"\n[Poll {i + 1}] Fetching micro-batch from broker.")

    batch = message_queue if i < 2 else []

    if batch:
        print(f"Consumed {len(batch)} messages. Profiling.")
        kafka_adapter.profile_batch(batch)
    else:
        print("No new messages.")

    time.sleep(1)

print("\nFlushing final telemetry.")
dalga._trigger_flush()
