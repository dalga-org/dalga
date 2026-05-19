import logging
from dalga.core import DalgaClient

logger = logging.getLogger("dalga")

try:
    import polars as pl

    @pl.api.register_dataframe_namespace("dalga")
    class DalgaPolarsAccessor:
        def __init__(self, df: pl.DataFrame):
            self._df = df

        def profile(self, client: DalgaClient):
            try:
                records = self._df.to_dicts()
                client.flow(records)
                client._trigger_flush()
            except Exception as e:
                logger.warning(f"Dalga Polars profiling failed: {e}")
            return self._df

except ImportError:
    pass
