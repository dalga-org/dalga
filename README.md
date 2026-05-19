# Dalga (🌊)

[![PyPI version](https://img.shields.io/pypi/v/dalga.svg)](https://pypi.org/project/dalga/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A blazing-fast, O(1) memory streaming data profiler. Written in Rust, built for Python.

Dalga acts as an **in-stream observability layer** and **circuit breaker**. It sits natively inside your existing data pipelines (Kafka, FastAPI, Pandas, Polars) and calculates statistical sketches of your data in real-time. It intercepts silent data corruption before it reaches your data warehouse, all without adding latency to your workers.

## Why Dalga?

* **Zero Compute Cost:** Stop querying multi-terabyte warehouse tables just to monitor null rates or cardinality. Dalga profiles the data *before* it lands.
* **Lock-Free Concurrency:** Built on a double-buffered `ArcSwap` and atomic counters. Multiple threads can hammer the engine simultaneously with zero lock contention.
* **Bounded O(1) Memory:** Utilizes 14-bit `HyperLogLogPlus` algorithms to estimate cardinality. Whether you process 100 rows or 10 billion, Dalga uses exactly ~16KB of memory per column.
* **Fail-Open & Safe:** Designed for mission-critical pipelines. If the telemetry fails, or if it encounters malformed bytes, Dalga gracefully drops the bad data and keeps your pipeline running.
* **Framework Agnostic:** First-class adapters for Pandas, Polars, FastAPI, and `confluent-kafka`.

---

## Installation

Dalga is available on PyPI. You can install it using `uv`, `pip`, or your preferred package manager:

```bash
uv add dalga-core
# or
pip install dalga-core
```

*(Note: The package name is `dalga-core`, but the import namespace is `dalga`)*

---

## Quickstart

Dalga processes data locally in an isolated Rust memory space. It automatically flushes metadata payloads in the background without blocking your host application.

### 1. The Core Engine (Batch Processing)

For standard lists of dictionaries or JSON payloads, use the universal `.flow()` method. The Rust engine natively processes batches for maximum throughput.

```python
from dalga.core import DalgaClient

dalga = DalgaClient()

# Pass batches directly to bypass Python iteration overhead
batch_data = [
    {"user_id": 1, "status": "active"}, 
    {"user_id": 2, "status": "banned"}
]
dalga.flow(batch_data)

```

### 2. The Circuit Breaker (Data Quality Gates)

Dalga actively protects your database from silent corruption. Evaluate incoming batches against strict `Expectation` rules. If a batch violates the rules, Dalga rejects it before it pollutes your global state or warehouse.

```python
from dalga.core import DalgaClient, Expectation

dalga = DalgaClient()

rules = [
    Expectation("price").min_value(0.0),            # No negative prices
    Expectation("user_id").to_not_be_null(),        # User ID is required
    Expectation("category").max_null_ratio(0.50)    # Category can be null up to 50%
]

batch = fetch_kafka_messages()

# Evaluates the batch in an isolated Rust memory space
if dalga.validate(batch, rules):
    insert_to_snowflake(batch)
else:
    send_to_dead_letter_queue(batch)

```

### 3. The Kafka Adapter

Drop Dalga directly into your consumer `poll()` loop to profile high-throughput streams natively.

```python
from dalga.core import DalgaClient
from dalga.adapters.kafka import DalgaKafka

dalga = DalgaClient(max_records=10_000)
kafka_adapter = DalgaKafka(dalga)

while True:
    messages = consumer.consume(num_messages=2000, timeout=1.0)
    if not messages:
        continue

    # Parses raw bytes and profiles the entire micro-batch instantly in Rust
    kafka_adapter.profile_batch(messages)

    # ... your normal DB insertion logic ...
```

### 4. DataFrames (Pandas & Polars)

Monitor batch transformations instantly. Importing the adapters automatically registers the `.dalga` namespace to your DataFrames.

```python
import pandas as pd
import polars as pl
from dalga.core import DalgaClient
import dalga.adapters.pandas 
import dalga.adapters.polars

dalga = DalgaClient()

# Pandas
df_pd = pd.read_csv("daily_users.csv")
df_pd.dalga.profile(dalga)

# Polars
df_pl = pl.read_parquet("daily_users.parquet")
df_pl.dalga.profile(dalga)
```

### 5. The FastAPI Middleware

Monitor the shape of incoming JSON payloads asynchronously without starving your endpoint streams.

```python
from fastapi import FastAPI
from dalga.core import DalgaClient
from dalga.adapters.fastapi import DalgaMiddleware

dalga = DalgaClient()
app = FastAPI()

# Profile specific endpoints (or all traffic) safely
app.add_middleware(DalgaMiddleware, client=dalga, endpoints_to_monitor=["/ingest"])

@app.post("/ingest")
async def ingest_data(payload: dict):
    return {"status": "success"}
```

---

## Architecture (Under the Hood)

Dalga leverages [PyO3](https://github.com/PyO3/pyo3) to bridge Python directly to a highly optimized Rust engine.

1. **Zero-Copy Extraction:** Python dictionaries are evaluated at the FFI boundary, extracting types (Int, Float, Str, Null) without expensive string parsing allocations.
2. **Lock-Free Aggregation:** The Rust core calculates Min, Max, and Nulls using atomic counters. Thread contention is virtually eliminated, even on extremely hot columns.
3. **Double-Buffered Flushes:** Background flushes utilize `ArcSwap` to atomically swap the active `DashMap` in O(1) time. Ingestion threads never block waiting for serialization.
4. **Absolute Memory Bounds:** Unique values are hashed into a `HyperLogLogPlus` sketch, ensuring memory utilization remains flat regardless of dataset size.

---

## Local Development

Dalga uses `nix`, `uv`, and `maturin` for reproducible builds.

```bash
# Enter the reproducible dev environment
nix develop

# Sync dependencies and compile the Rust extension
uv sync
uv run maturin develop --release

# Run the test suite
uv run pytest tests/ -v
```
