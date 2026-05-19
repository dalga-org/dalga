import logging
from dalga.core import DalgaClient

logger = logging.getLogger("dalga")

try:
    import pandas as pd

    @pd.api.extensions.register_dataframe_accessor("dalga")
    class DalgaAccessor:
        def __init__(self, df: pd.DataFrame):
            self._df = df

        def profile(self, client: DalgaClient):
            try:
                records = self._df.to_dict(orient="records")
                client.flow(records)
                client._trigger_flush()
            except Exception as e:
                logger.warning(f"Dalga Pandas profiling failed: {e}")
            return self._df

except ImportError:
    pass
