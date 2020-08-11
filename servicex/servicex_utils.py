import asyncio
from typing import Dict
import functools
import logging

from .utils import _string_hash, clean_linq

_in_progress_items: Dict[str, asyncio.Event] = {}


def _wrap_in_memory_sx_cache(fn):
    '''
    Caching wrapper. This is like lru in python, but we have to fetch some more
    "interesting" data from the object we are wrapping to complete the cache key. Hence
    this implementation.

    NOTE: This is not thread safe. In fact, you'll likely get a bomb if try to
          call this from another thread and the two threads request something the
          same at the same time. This is because asyncio locks are associated with loops,
          and there is one loop per thread.
    '''
    @functools.wraps(fn)
    async def cached_version_of_fn(*args, **kwargs):
        assert len(args) == 2
        sx = args[0]
        from .servicex import ServiceXDataset
        assert isinstance(sx, ServiceXDataset)
        selection_query = args[1]
        assert isinstance(selection_query, str)

        # Is it in the local cache?
        h = _string_hash([sx._dataset, clean_linq(selection_query)])
        logger = logging.getLogger(__name__)
        if h in _in_progress_items:
            logger.debug(f"{h} - waiting for processing")
            await _in_progress_items[h].wait()
            logger.debug(f'{h} - done waiting for processing')

        # Is it already done?
        r = sx._cache.lookup_inmem(h)
        if r is not None:
            return r

        # It is not. We need to calculate it, and prevent
        # others from working on it.
        _in_progress_items[h] = asyncio.Event()
        try:
            logger.debug(f'{h} - processing request')
            result = await fn(*args, **kwargs)
            sx._cache.set_inmem(h, result)
        finally:
            logger.debug(f'{h} - done processing request')
            _in_progress_items[h].set()
            del _in_progress_items[h]

        return result

    return cached_version_of_fn
