import logging
import os
import re
from datetime import datetime, timedelta
from typing import Any, List, Callable
import msgpack

_GRANULARITY_SECONDS_MAP = {
    's': 1,
    'm': 60,
    'h': 60 * 60,
    'd': 24 * 60 * 60
}

BASE_DATE = datetime(2010, 1, 1)
FetcherFunc = Callable[[str, str, datetime, int], List[Any]]
logger = logging.getLogger('tscache')
logger.setLevel(logging.DEBUG)


def _convert_granularity_to_seconds(granularity: str) -> int:
    """Convert granularity string to seconds

    Args:
        granularity: granularity input string ^(%d+)([smhd])$.

    Returns:
        Seconds of the given granularity.

    Raises:
        ValueError: If granularity string is invalid.
    """
    r = re.match(r"^(\d+)([smhd])$", granularity)
    if r is not None and len(r.groups()) == 2:
        return int(r[1]) * _GRANULARITY_SECONDS_MAP[r[2]]

    raise ValueError("%s is not a valid granularity string")


def _calculate_index(timestamp: datetime, granularity: str) -> int:
    """Calculate index of given time stamp in time series cache

    Args:
        timestamp: given timestamp.
        granularity: granularity input string ^(%d+)([smhd])$.

    Returns:
        Index of of given time stamp in time series cache

    Raises:
        ValueError: If granularity string is invalid.
    """
    seconds = _convert_granularity_to_seconds(granularity)
    total_seconds = int((timestamp - BASE_DATE).total_seconds())
    return int(total_seconds / seconds)


def _calculate_block_index(index: int, block_size: int) -> int:
    return int(index / block_size)


def _get_block_path(basedir: str, symbol: str, granularity: str, block_index: int) -> str:
    return os.path.join(basedir, symbol, granularity, "%d" % block_index)


def _get_start_time_by_block(block_index: int, granularity: str, block_size: int) -> datetime:
    seconds = _convert_granularity_to_seconds(granularity)
    return BASE_DATE + timedelta(seconds=block_size * block_index * seconds)


def _block_path_exists(path: str) -> bool:
    return os.path.isfile(path)


def _load_block_by_path(path: str) -> List[Any]:
    logger.debug("Load cache data from %s", path)
    with open(path, "rb") as f:
        return msgpack.unpackb(f.read())


def _save_block_to_path(path: str, block: List[Any]) -> None:
    logger.debug("Save cache data to %s", path)
    dirname = os.path.dirname(path)
    if not os.path.isdir(dirname):
        os.makedirs(dirname)
        logger.debug("Create cache folder %s", dirname)

    with open(path, "wb") as f:
        f.write(msgpack.packb(block))


def _fetch_block(
        basedir: str,
        symbol: str,
        granularity: str,
        block_index: int,
        limit: int,
        block_size: int,
        fetcher: FetcherFunc,
        start_index: int = None,
        end_index: int = None
) -> List[Any]:
    logger.debug("Fetch block symbol=%s, granularity=%s, block_index=%d", symbol, granularity, block_index)
    path = _get_block_path(basedir, symbol, granularity, block_index)

    if _block_path_exists(path):
        block = _load_block_by_path(path)
    else:
        seconds = _convert_granularity_to_seconds(granularity)
        start = BASE_DATE + timedelta(seconds=block_size * block_index * seconds)

        block = []
        i = 0
        while i < block_size:
            logger.debug("Fetching remote data symbol=%s, granularity=%s, start=%s, limit=%d",
                         symbol, granularity, str(start), min(block_size - i, limit))
            block += fetcher(symbol, granularity, start, min(block_size - i, limit))

            start += timedelta(seconds=limit * seconds)
            i += limit

        _save_block_to_path(path, block)

    offset = block_size - len(block)

    start_index = 0 if start_index is None else start_index
    end_index = len(block) if start_index is None else end_index

    return block[start_index-offset:end_index-offset]


class TimeSeriesCache:
    """Time series cache.

    Cache remote time series data onto local storage divided into small blocks

    Attributes:
        basedir: Base directory of the cache.
        fetcher: Call back function for fetching remote time series data.
        limit: Number of data points which can be fetched per query as limitation of remote data source.
        block_size: Size of cache block.
    """

    def __init__(self, basedir: str, fetcher: FetcherFunc, limit: int, block_size: int = 4096):
        """Inits TimeSeriesCache with basedir, fetcher, limit and block size."""
        self.basedir = basedir
        self.fetcher = fetcher
        self.limit = limit
        self.block_size = block_size

    def query(self, symbol: str, granularity: str, start: datetime, end: datetime) -> List[Any]:
        """Query the time series data from cache. If the data doesn't exist, it will get the missing blocks
            from remote data source first then return the entire data frame"""
        start_index = _calculate_index(start, granularity)
        end_index = _calculate_index(end, granularity)
        start_block_index = _calculate_block_index(start_index, self.block_size)
        end_block_index = _calculate_block_index(end_index, self.block_size)

        results = []
        block_index = start_block_index
        while block_index <= end_block_index:
            results += _fetch_block(
                basedir=self.basedir,
                symbol=symbol,
                granularity=granularity,
                block_index=block_index,
                limit=self.limit,
                block_size=self.block_size,
                fetcher=self.fetcher,
                start_index=start_index % self.block_size if block_index == start_block_index else None,
                end_index=(end_index + 1) % self.block_size if block_index == end_block_index else None)

            block_index += 1

        return results


