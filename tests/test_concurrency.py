import concurrent.futures
import json
from dalga.core import DalgaClient


def test_thread_safety_under_heavy_load():
    client = DalgaClient(max_records=200_000)
    threads_count = 50
    rows_per_thread = 1000
    expected_total = threads_count * rows_per_thread

    def worker_task(thread_id):
        for i in range(rows_per_thread):
            client.flow({"thread_id": thread_id, "row_num": i, "event": "stress_test"})

    with concurrent.futures.ThreadPoolExecutor(max_workers=threads_count) as executor:
        futures = [executor.submit(worker_task, t_id) for t_id in range(threads_count)]
        concurrent.futures.wait(futures)

    payload = json.loads(client.profiler.flush())

    assert payload["event"]["total_count"] == expected_total
    assert payload["thread_id"]["estimated_cardinality"] == threads_count

    client.shutdown()
