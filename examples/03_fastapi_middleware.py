from fastapi import FastAPI
from fastapi.testclient import TestClient
from dalga.core import DalgaClient
from dalga.adapters.fastapi import DalgaMiddleware

dalga = DalgaClient(max_records=2)

app = FastAPI()
app.add_middleware(DalgaMiddleware, client=dalga, endpoints_to_monitor=["/ingest"])


@app.post("/ingest")
async def ingest_data(payload: dict):
    return {"status": "success", "user": payload.get("user_id")}


@app.post("/health")
async def health_check(payload: dict):
    return {"status": "healthy"}


print("Spinning up FastAPI Test Client.")
client = TestClient(app)

print("Sending ignored request to /health.")
client.post("/health", json={"system_id": "sys_1", "status": "ok"})

print("Sending 3 rapid requests to /ingest (will trigger automatic flush!).")
client.post("/ingest", json={"user_id": 1, "price": 10.5, "event": "click"})
client.post("/ingest", json={"user_id": 2, "price": None, "event": "view"})
# The 2nd request above triggers a flush. This 3rd request starts the next batch.
client.post("/ingest", json={"user_id": 3, "price": 45.0, "event": "click"})
