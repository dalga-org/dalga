import random
import time
from dalga.core import DalgaClient


client = DalgaClient()

print("Starting simulated data stream for 5 iterations.")
for i in range(1, 6):
    num_rows = random.randint(50, 500)

    null_chance = 0.05
    if i == 4:
        print("\nSIMULATING PIPELINE FAILURE! Injecting massive nulls into 'age'.")
        null_chance = 0.80

    batch = [
        {
            "user_id": f"u{random.randint(1, 1000)}",
            "age": random.randint(18, 65) if random.random() > null_chance else None,
        }
        for _ in range(num_rows)
    ]

    client.flow(batch)
    print(f"[{i}/5] Profiled batch of {num_rows} rows.")

    time.sleep(1)

print("\nFinal Telemetry (Notice the null count spike!):")
client._trigger_flush()
