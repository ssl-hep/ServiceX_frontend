from typing import Any, Callable, Dict, Optional, Awaitable

import aiohttp
from tqdm.auto import tqdm


class ServiceX_Exception(Exception):
    'Raised when something has gone wrong in the ServiceX remote service'
    def __init__(self, msg):
        super().__init__(self, msg)


class ServiceXFrontEndException(Exception):
    'Raised to indicate an API error in use of the servicex library'
    def __init__(self, msg):
        super().__init__(self, msg)


class _status_update_wrapper:
    '''
    Internal class to make it easier to deal with the updater
    '''
    def __init__(self, callback:
                 Optional[Callable[[Optional[int], int, int], None]] = None):
        self._total = None
        self._processed = None
        self._downloaded = None
        self._callback = callback

    def broadcast(self):
        'Send an update back to the system'
        if self._callback is not None:
            if self._total is not None \
                    or self._processed is not None \
                    or self._callback is not None:
                if self._processed is None:
                    self._processed = 0
                if self._downloaded is None:
                    self._downloaded = 0
                self._callback(self._total, self._processed, self._downloaded)

    def update(self, processed: Optional[int] = None,
               downloaded: Optional[int] = None,
               total: Optional[int] = None):
        if total is not None:
            self._total = total
        if processed is not None:
            self._processed = processed
        if downloaded is not None:
            self._downloaded = downloaded

    def inc(self, downloaded: Optional[int] = None):
        if downloaded is not None:
            if self._downloaded is None:
                self._downloaded = downloaded
            else:
                self._downloaded += downloaded


def _run_default_wrapper(t: Optional[int], p: int, d: int) -> None:
    'Place holder to run the default runner'
    assert False, 'This should never be called'


class _default_wrapper_mgr:
    'Default prorgress bar'
    def __init__(self, sample_name: Optional[str] = None):
        self._tqdm_p = tqdm(total=9e9, desc=sample_name, unit='file',
                            leave=True, dynamic_ncols=True,
                            bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}]')
        self._tqdm_d = tqdm(total=9e9, desc="        Downloaded", unit='file',
                            leave=True, dynamic_ncols=True,
                            bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}]')

    def _update_bar(self, bar: tqdm, total: Optional[int], num: int):
        if total is not None:
            if bar.total != total:
                # There is a bug in the tqm library if running in a notebook
                # See https://github.com/tqdm/tqdm/issues/688
                # This forces us to do the reset, sadly.
                old_num = bar.n  # type: int
                bar.reset(total)
                bar.update(old_num)
        bar.update(num - bar.n)
        bar.refresh()

        if total is not None and num == total:
            bar.close()

    def update(self, total: Optional[int], processed: int, downloaded: int):
        self._update_bar(self._tqdm_p, total, processed)
        self._update_bar(self._tqdm_d, total, downloaded)


async def _submit_or_lookup_transform(client: aiohttp.ClientSession,
                                      servicex_endpoint: str,
                                      json_query: Dict[str, Any]) -> str:
    '''
    Submit a transform, or look it up in our local query database
    '''
    async with client.post(f'{servicex_endpoint}/transformation', json=json_query) as response:
        # TODO: Make sure to throw the correct type of exception
        r = await response.json()
        if response.status != 200:
            raise ServiceX_Exception('ServiceX rejected the transformation request: '
                                     f'({response.status}){r}')
        return r["request_id"]
