import os
import uuid
import psutil
import gc
from dalga.core import DalgaClient


def get_process_memory_mb():
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / (1024 * 1024)


def test_o1_memory_bounds():
    client = DalgaClient(max_records=500_000)

    warmup_batch = [{"user_uuid": str(uuid.uuid4()), "price": 10.5} for _ in range(10_000)]
    client.flow(warmup_batch)

    initial_memory_mb = get_process_memory_mb()

    for _ in range(20):
        stress_batch = [{"user_uuid": str(uuid.uuid4()), "price": 10.5} for _ in range(10_000)]
        client.flow(stress_batch)

    gc.collect()

    final_memory_mb = get_process_memory_mb()
    growth_mb = final_memory_mb - initial_memory_mb

    assert growth_mb < 15.0, f"Memory leaked! Grew by {growth_mb:.2f} MB"
    client.shutdown()
