import pandas as pd
import dalga.adapters.pandas
from dalga.core import DalgaClient


client = DalgaClient()

data = {
    "user_id": ["u1", "u2", "u3", "u4", "u5", "u1", "u2"],
    "age": [25, 30, None, 45, 22, 25, 30],
    "status": ["active", "active", "banned", None, "active", "active", "active"],
}

print("🌊 Profiling Pandas DataFrame...")
df_pd = pd.DataFrame(data)
df_pd.dalga.profile(client)

try:
    import polars as pl
    import dalga.adapters.polars  # noqa: F401

    print("\n🌊 Profiling Polars DataFrame...")
    df_pl = pl.DataFrame(data)
    df_pl.dalga.profile(client)
except ImportError:
    print("\n(Polars not installed, skipping Polars example)")

client._trigger_flush()
