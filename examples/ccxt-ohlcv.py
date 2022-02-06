import ccxt
import os
import pandas as pd
from datetime import datetime
from typing import Any, List

from tscache import TimeSeriesCache

config = {
    "apiKey": "",
    "secret": "",
    "enableRateLimit": True
}

exchange = ccxt.deribit(config)


def fetch_ohlcv(symbol: str, granularity: str, start: datetime, limit: int) -> List[Any]:
    since = int((start - datetime(1970, 1, 1)).total_seconds() * 1000)
    return exchange.fetch_ohlcv(symbol, timeframe=granularity, since=since, limit=limit)

def to_ohlcv_data_frame(data):
    df = pd.DataFrame(data, columns=['time', 'open', 'high', 'low', 'close', 'volume'])
    df.time = df.time.apply(lambda x: datetime.fromtimestamp(x / 1000))
    return df

if __name__ == "__main__":
    cache = TimeSeriesCache(os.path.join(os.path.dirname(__file__), "cache"), fetch_ohlcv, 1500)
    result = cache.query("BTC-PERPETUAL", "15m", datetime(2020, 1, 1), datetime.today())
    print("Cache saved to %s (%d points)" % (cache.basedir, len(result)))
    print(to_ohlcv_data_frame(result))
