import ccxt
import os
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


if __name__ == "__main__":
    cache = TimeSeriesCache(os.path.join(os.path.dirname(__file__), "cache"), fetch_ohlcv, 1500)
    result = cache.query("BTC-PERPETUAL", "15m", datetime(2020, 1, 1), datetime.today())
    print("Cache saved to %s (%d points)" % (cache.basedir, len(result)))
