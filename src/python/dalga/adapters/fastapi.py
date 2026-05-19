import json
import logging
from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.types import ASGIApp
from dalga.core import DalgaClient


logger = logging.getLogger("dalga")


class DalgaMiddleware(BaseHTTPMiddleware):
    def __init__(
        self, app: ASGIApp, client: DalgaClient, endpoints_to_monitor: list[str] | None = None
    ):
        super().__init__(app)
        self.client = client
        self.endpoints_to_monitor = endpoints_to_monitor or []

    async def dispatch(self, request: Request, call_next):
        body_bytes = b""
        if request.method in ["POST", "PUT", "PATCH"]:
            body_bytes = await request.body()

            async def receive():
                return {"type": "http.request", "body": body_bytes}

            request._receive = receive

        response = await call_next(request)

        try:
            if body_bytes:
                path = request.url.path
                if not self.endpoints_to_monitor or any(
                    path.startswith(e) for e in self.endpoints_to_monitor
                ):
                    data = json.loads(body_bytes)

                    self.client.flow(data)

        except Exception as e:
            logger.debug(f"Dalga middleware failed to observe: {e}")

        return response
