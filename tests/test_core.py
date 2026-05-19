import json
import pytest
from dalga.core import DalgaClient, Expectation


@pytest.fixture
def client():
    c = DalgaClient(max_records=100)
    yield c
    c.shutdown()


def test_rust_core_math_accuracy(client):
    data = [
        {"id": 1, "age": 20, "status": "active"},
        {"id": 2, "age": 25, "status": "active"},
        {"id": 3, "age": None, "status": "inactive"},
        {"id": 1, "age": 20, "status": "active"},
        {"id": 4, "age": 30, "status": "banned"},
        {"id": 5, "age": float("nan"), "status": "active"},
    ]

    client.flow(data)

    payload = json.loads(client.profiler.flush())

    assert payload["id"]["total_count"] == 6
    assert payload["id"]["null_count"] == 0
    assert payload["id"]["min_val"] == 1.0
    assert payload["id"]["max_val"] == 5.0
    assert payload["id"]["estimated_cardinality"] == 5
    assert payload["age"]["null_count"] == 2


def test_fail_open_safety(client):
    class WeirdObject:
        pass

    client.flow({"id": WeirdObject(), "name": "test"})

    payload = json.loads(client.profiler.flush())

    assert payload["id"]["total_count"] == 1
    assert payload["id"]["null_count"] == 1


def test_adaptive_batching_trigger():
    client = DalgaClient(max_records=3)

    client.flow([{"val": 1}, {"val": 2}])
    assert client._records_since_flush == 2

    client.flow({"val": 3})
    assert client._records_since_flush == 0
    client.shutdown()


def test_circuit_breaker_validation(client):
    rules = [Expectation("price").min_value(0.0), Expectation("user_id").to_not_be_null()]

    clean_batch = [{"user_id": 1, "price": 10.0}, {"user_id": 2, "price": 5.0}]
    corrupt_batch = [{"user_id": 3, "price": -5.0}, {"user_id": None, "price": 10.0}]

    assert client.validate(clean_batch, rules) is True
    assert client.validate(corrupt_batch, rules) is False

    payload = json.loads(client.profiler.flush())
    assert payload["user_id"]["total_count"] == 2
    assert payload["price"]["min_val"] == 5.0
