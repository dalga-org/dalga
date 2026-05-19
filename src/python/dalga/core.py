import atexit
import json
import logging
import threading
import time
import httpx
from .dalga import Profiler

logger = logging.getLogger("dalga")
logger.addHandler(logging.NullHandler())


class Expectation:
    def __init__(self, column: str):
        self.column = column
        self.rules: list[tuple[str, float]] = []

    def to_not_be_null(self) -> "Expectation":
        self.rules.append(("max_nulls", 0.0))
        return self

    def max_null_ratio(self, ratio: float) -> "Expectation":
        self.rules.append(("max_null_ratio", ratio))
        return self

    def min_value(self, val: float) -> "Expectation":
        self.rules.append(("min_val", val))
        return self


class DalgaClient:
    def __init__(
        self,
        api_key: str | None = None,
        base_url: str = "https://api.dalga.dev/v1/ingest",
        max_time_sec: int = 60,
        max_records: int = 10_000,
    ):
        self.profiler = Profiler()
        self.api_key = api_key
        self.base_url = base_url
        self.max_time_sec = max_time_sec
        self.max_records = max_records

        self.local_mode = api_key is None
        self._records_since_flush = 0
        self._last_flush_time = time.time()
        self._lock = threading.RLock()

        self._stop_event = threading.Event()
        self._worker_thread = threading.Thread(
            target=self._background_flush_loop, daemon=True, name="DalgaBackgroundWorker"
        )
        self._worker_thread.start()
        atexit.register(self.shutdown)

    def flow(self, data):
        try:
            if isinstance(data, dict):
                self.profiler.observe_dict(data)
                self._increment_and_check(1)

            elif isinstance(data, list):
                if not data:
                    return

                self.profiler.observe_dicts(data)
                self._increment_and_check(len(data))

        except Exception as e:
            logger.debug(f"Dalga flow failed to ingest: {e}")

    def validate(self, data: dict | list, expectations: list[Expectation]) -> bool:
        """
        The Circuit Breaker. Profiles a batch in an isolated Rust environment.
        Returns True if safe (and flows the data), False if it violates expectations.
        """
        try:
            temp_profiler = Profiler()

            if isinstance(data, dict):
                temp_profiler.observe_dict(data)
            elif isinstance(data, list):
                if not data:
                    return False
                temp_profiler.observe_dicts(data)
            else:
                return False

            stats_str = temp_profiler.flush()
            if not stats_str or stats_str == "{}":
                return True

            stats = json.loads(stats_str)

            for exp in expectations:
                col_stats = stats.get(exp.column)
                if not col_stats:
                    continue

                for rule_name, rule_val in exp.rules:
                    if rule_name == "max_nulls":
                        if col_stats.get("null_count", 0) > rule_val:
                            logger.warning(f"Dalga Breaker: {exp.column} has nulls.")
                            return False

                    elif rule_name == "max_null_ratio":
                        total = col_stats.get("total_count", 1)
                        ratio = col_stats.get("null_count", 0) / total
                        if ratio > rule_val:
                            logger.warning(
                                f"Dalga Breaker: {exp.column} null ratio {ratio:.2f} > {rule_val}."
                            )
                            return False

                    elif rule_name == "min_val":
                        min_observed = col_stats.get("min_val")
                        if min_observed is not None and min_observed < rule_val:
                            logger.warning(
                                f"Dalga Breaker: {exp.column} min {min_observed} < {rule_val}."
                            )
                            return False

            self.flow(data)
            return True

        except Exception as e:
            logger.error(f"Dalga validation failed internally: {e}")
            self.flow(data)
            return True

    def _increment_and_check(self, count: int):
        with self._lock:
            self._records_since_flush += count
            should_flush = self._records_since_flush >= self.max_records

        if should_flush:
            self._trigger_flush()

    def _background_flush_loop(self):
        while not self._stop_event.is_set():
            time.sleep(1)
            with self._lock:
                time_elapsed = time.time() - self._last_flush_time
                should_flush = (time_elapsed >= self.max_time_sec) and (
                    self._records_since_flush > 0
                )

            if should_flush:
                self._trigger_flush()

    def _trigger_flush(self):
        with self._lock:
            self._records_since_flush = 0
            self._last_flush_time = time.time()

        payload_str = self.profiler.flush()
        if payload_str == "{}" or not payload_str:
            return

        payload = json.loads(payload_str)

        if self.local_mode:
            self._print_local_report(payload)
        else:
            self._send_to_cloud(payload)

    def _send_to_cloud(self, payload: dict):
        try:
            httpx.post(
                self.base_url,
                json={"metadata": payload, "timestamp": time.time()},
                headers={"Authorization": f"Bearer {self.api_key}"},
                timeout=3.0,
            )
        except httpx.RequestError as e:
            logger.warning(f"Dalga couldn't reach API. Dropping payload. {e}")

    def _print_local_report(self, payload: dict):
        try:
            from rich.console import Console
            from rich.table import Table

            console = Console()
            table = Table(
                title="🌊 Dalga Local Stream Profile", show_header=True, header_style="bold cyan"
            )
            table.add_column("Column")
            table.add_column("Total Rows")
            table.add_column("Nulls")
            table.add_column("Unique (Est)")
            table.add_column("Min")
            table.add_column("Max")

            for col, stats in payload.items():
                table.add_row(
                    str(col),
                    str(stats.get("total_count", 0)),
                    str(stats.get("null_count", 0)),
                    str(stats.get("estimated_cardinality", 0)),
                    str(stats.get("min_val", "N/A")),
                    str(stats.get("max_val", "N/A")),
                )
            console.print(table)
        except ImportError:
            pass

    def shutdown(self):
        self._stop_event.set()
        with self._lock:
            if self._records_since_flush > 0:
                self._trigger_flush()
