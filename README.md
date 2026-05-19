# Dalga (🌊)

[![PyPI version](https://img.shields.io/pypi/v/dalga.svg)](https://pypi.org/project/dalga/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A blazing-fast, $O(1)$ memory streaming data profiler. Written in Rust, built for Python. 

Dalga acts as an in-stream observability layer. It sits perfectly inside your existing data pipelines (Kafka, FastAPI, Pandas) and calculates statistical sketches of your data in real-time. It intercepts silent data corruption before it reaches your data warehouse, without adding latency to your workers.

## Why Dalga?

* **Zero Compute Cost:** Stop querying 5TB BigQuery tables just to find a null rate. Dalga profiles the data *before* it hits the warehouse.
* **$O(1)$ Memory Bound:** Uses 14-bit HyperLogLog+ algorithms to estimate cardinality. Whether you process 100 rows or 10 billion, Dalga uses exactly ~16KB of memory per column.
* **Non-Blocking & Safe:** Designed to "fail-open." If the telemetry fails, your pipeline keeps running.
* **Framework Agnostic:** Explicit adapters for Pandas, FastAPI, and `confluent-kafka`.

## Installation

You can install Dalga using `uv`, `pip`, or any standard package manager:

```bash
uv add dalga-core
# or
pip install dalga-core

```

## Quickstart

Dalga processes data locally and prints a beautiful terminal dashboard. (Optional: Pass an API key to send metadata to `api.dalga.dev` for historical tracking and alerting).

### 1. The Core Engine (Standard Python)

If you just have lists of dictionaries or JSON payloads, use the universal `.flow()` method.

```python
from dalga.core import DalgaClient

dalga = DalgaClient()

data = [{"user_id": 1, "status": "active"}, {"user_id": 2, "status": "banned"}]
dalga.flow(data)

```
### 🛡️ The Circuit Breaker (Data Quality Gates)

Dalga doesn't just monitor data; it actively protects your database from silent corruption. You can evaluate batches of data in isolated memory using the `Expectation` API. If a batch fails your rules, Dalga rejects it before it pollutes your global state or your data warehouse.

```python
from dalga.core import DalgaClient, Expectation

dalga = DalgaClient()

rules = [
    Expectation("price").min_value(0.0),            # No negative prices
    Expectation("user_id").to_not_be_null(),        # User ID is required
    Expectation("category").max_null_ratio(0.50)    # Category can be null up to 50%
]

batch = fetch_kafka_messages()

if dalga.validate(batch, rules):
    insert_to_snowflake(batch)
else:
    send_to_dead_letter_queue(batch)
```

### 2. The Kafka Adapter

Sit inside the consumer loop and profile high-throughput streams natively.

```python
from dalga.core import DalgaClient
from dalga.adapters.kafka import DalgaKafka

dalga = DalgaClient()
kafka_adapter = DalgaKafka(dalga)

while True:
    messages = consumer.consume(num_messages=500, timeout=1.0)
    if not messages:
        continue
        
    # Parses the raw bytes and profiles the batch instantly in Rust
    kafka_adapter.profile_batch(messages)
    
    # ... your normal DB insertion logic ...

```

### 3. The FastAPI Middleware

Monitor the shape of incoming JSON payloads without blocking the event loop.

```python
from fastapi import FastAPI
from dalga.core import DalgaClient
from dalga.adapters.fastapi import DalgaMiddleware

dalga = DalgaClient()
app = FastAPI()

# Monitor specific endpoints or all traffic
app.add_middleware(DalgaMiddleware, client=dalga, endpoints_to_monitor=["/ingest"])

```

### 4. The Pandas / Polars Adapter

Monitor batch transformations instantly using the `.dalga` namespace.

```python
import pandas as pd
from dalga.core import DalgaClient
import dalga.adapters.pandas 

dalga = DalgaClient()
df = pd.read_csv("daily_users.csv")

# Profiles the dataframe and prints a local report
df.dalga.profile(dalga)

```

## How it Works (Under the Hood)

Dalga leverages [PyO3](https://github.com/PyO3/pyo3) and Rust to handle data ingestion.

1. The Python adapters extract the batch data and pass it across the FFI boundary.
2. The Rust core calculates Min, Max, Nulls, and uses a `HyperLogLogPlus` sketch to track unique values with absolute memory bounds.
3. A background Python daemon thread flushes the lightweight metadata payload every 60 seconds (or when `max_records` is hit) using an `RLock` so your host application's threads are never blocked.

## Local Development

Dalga uses `nix`, `uv`, and `maturin` for reproducible builds.

```bash
# Enter the reproducible dev environment
nix develop

# Compile the Rust core and install into the local virtual environment
uv run maturin develop

# Run the test suite
uv run pytest tests/ -v

```
