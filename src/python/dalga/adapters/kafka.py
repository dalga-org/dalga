import json
import logging
from dalga.core import DalgaClient


logger = logging.getLogger("dalga")


class DalgaKafka:
    def __init__(self, client: DalgaClient):
        self.client = client

    def profile_batch(self, messages: list):
        """
        Translates a batch of confluent-kafka messages into standard Python dicts
        and flows them into the Dalga core.
        """
        parsed_batch = []
        for msg in messages:
            try:
                if msg is None or (hasattr(msg, "error") and msg.error()):
                    continue

                val = msg.value()
                if val:
                    if isinstance(val, bytes):
                        val = val.decode("utf-8")
                    parsed_batch.append(json.loads(val))

            except Exception as e:
                logger.debug(f"Dalga Kafka adapter failed to parse message: {e}")

        if parsed_batch:
            self.client.flow(parsed_batch)
