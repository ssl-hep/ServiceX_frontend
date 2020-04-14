from typing import Callable, Dict, Optional
import os
import tempfile
from hashlib import blake2b
from pathlib import Path
import re

import aiohttp
from tqdm.auto import tqdm

# Where shall we store files by default when we pull them down?
default_file_cache_name = os.path.join(tempfile.gettempdir(), 'servicex')


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


# Changes in the json that won't affect the result
_json_keys_to_ignore_for_hash = ['workers']


async def _submit_or_lookup_transform(client: aiohttp.ClientSession,
                                      servicex_endpoint: str,
                                      json_query: Dict[str, str]) -> str:
    '''
    Submit a transform, or look it up in our local query database
    '''
    # Check the cache
    hasher = blake2b(digest_size=20)
    for k, v in json_query.items():
        if k not in _json_keys_to_ignore_for_hash:
            hasher.update(k.encode())
            hasher.update(str(v).encode())
    hash = hasher.hexdigest()

    hash_file = Path(default_file_cache_name) / 'request-cache' / hash
    if hash_file.exists():
        with hash_file.open('r') as r:
            return r.readline().strip()

    # Make the query.
    async with client.post(f'{servicex_endpoint}/transformation', json=json_query) as response:
        r = await response.json()
        if response.status != 200:
            raise ServiceX_Exception('ServiceX rejected the transformation request: '
                                     f'({response.status}){r}')
        req_id = r["request_id"]

        hash_file.parent.mkdir(parents=True, exist_ok=True)
        with hash_file.open('w') as w:
            w.write(f'{req_id}\n')
            # In case humans come poking around
            w.write(str(json_query))
            w.write('\n')

    return req_id


def _clean_linq(linq: str) -> str:
    '''
    Noramlize the variables in a linq expression. Should make the
    linq expression more easily comparable even if the algorithm that
    generates the underlying variable numbers changes.

    # TODO: Assumes a form with "exx" as the names. Clearly, this can't work.
    # This needs mods.
    '''
    all_uses = re.findall('e[0-9]+', linq)
    index = 0
    used = []
    mapping = {}
    for v in all_uses:
        if v not in used:
            used.append(v)
            new_var = f'a{index}'
            index += 1
            mapping[v] = new_var

    if len(mapping) == 0:
        return linq

    max_len = max([len(k) for k in mapping.keys()])
    for l in range(max_len, 0, -1):
        for k in mapping.keys():
            if len(k) == l:
                linq = linq.replace(k, mapping[k])

    return linq
