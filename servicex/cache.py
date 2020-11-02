import json
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
from contextlib import contextmanager

from .utils import ServiceXException, _query_cache_hash, sanitize_filename

_ignore_cache = False

# Make sure that generated download path names are below this to avoid os errors
MAX_PATH_LEN = 235


@contextmanager
def ignore_cache():
    '''This will cause all caches to be ignored while it is invoked:

    ```
    with ignore_cache():
        ServiceXDataset(...).get_data...()
    ```

    If you want to do this globally, you can just use the `__enter__()` method.
    This is probably the only way to do this accross cells in a notebook.

    ```
    i = ignore_cache()
    i.__enter__()
    ... Query code, jupyter notebook cells, etc. go here
    i.__exit(None, None, None)
    ```

    Note:

    - The only time the cache is checked is when the query is actually made, not when
      the servicex dataset object is created!
    - Calls to this can be safely nested.
    - Note that calling this doesn't clear the cache or delete anything. It
      just prevents the cache lookup from working while it is in effect.
    '''
    global _ignore_cache
    old_value = _ignore_cache
    _ignore_cache = True
    yield
    _ignore_cache = old_value


class Cache:
    '''
    Caching for all data returns from the system. It provides both in-memory
    and on-disk cache.

    TODO: Rename this to be an adaptor, unifying how we name things
    '''
    _in_memory_cache = {}

    @classmethod
    def reset_cache(cls):
        'Reset the internal cache, usually used for testing'
        cls._in_memory_cache = {}

    def __init__(self, cache_path: Path, ignore_cache: bool = False):
        '''
        Create the cache object

        Arguments:

            cache_path          The path to the cache directory. Only sub-directories
                                will be created in this path.
            ignore_cache        If true, then always ignore the cache for any queries
                                against this dataset.
        '''
        self._path = cache_path
        self._ignore_cache = ignore_cache

    @property
    def path(self) -> Path:
        'Return root path of cache directory'
        return self._path

    @contextmanager
    def ignore_cache(self):
        '''Ignore the cache as long as we are held. Supports nesting.
        '''
        old_ignore = self._ignore_cache
        self._ignore_cache = True
        yield
        self._ignore_cache = old_ignore

    def _query_cache_file(self, json: Dict[str, str]) -> Path:
        'Return the query cache file'
        h = _query_cache_hash(json)
        return self._path / 'query_cache' / h

    def _query_status_cache_file(self, request_id: str) -> Path:
        'Return the query cache file'
        return self._path / 'query_cache_status' / request_id

    def _files_cache_file(self, id: str) -> Path:
        'Return the file that contains the list of files'
        return self._path / 'file_list_cache' / id

    def lookup_query(self, json: Dict[str, str]) -> Optional[str]:
        global _ignore_cache
        if _ignore_cache or self._ignore_cache:
            return None

        f = self._query_cache_file(json)
        if not f.exists():
            return None

        with f.open('r') as i:
            return i.readline().strip()

    def set_query(self, json: Dict[str, str], v: str):
        f = self._query_cache_file(json)
        f.parent.mkdir(parents=True, exist_ok=True)
        with f.open('w') as o:
            o.write(f'{v}\n')

    def set_query_status(self, query_info: Dict[str, str]):
        '''Cache a query status (json dict)

        Args:
            query_info (Dict[str, str]): The info we should cache. Must contain `request_id`.
        '''
        assert 'request_id' in query_info, \
            "Internal error - request_id should always be part of info returned"
        f = self._query_status_cache_file(query_info['request_id'])
        f.parent.mkdir(parents=True, exist_ok=True)
        with f.open('w') as o:
            json.dump(query_info, o)

    def lookup_query_status(self, request_id: str) -> Dict[str, str]:
        '''Returns the info from the last time the query status was cached.

        Args:
            request_id (str): Request id we should look up.

        '''
        f = self._query_status_cache_file(request_id)
        if not f.exists():
            raise ServiceXException(f'Not cache information for query {request_id}')
        with f.open('r') as o:
            return json.load(o)

    def remove_query(self, json: Dict[str, str]):
        f = self._query_cache_file(json)
        if f.exists():
            f.unlink()

    def set_files(self, id: str, files: List[Tuple[str, str]]):
        f = self._files_cache_file(id)
        f.parent.mkdir(parents=True, exist_ok=True)
        with f.open('w') as o:
            json.dump(files, o)

    def lookup_files(self, id: str) -> Optional[List[Tuple[str, str]]]:
        f = self._files_cache_file(id)
        if not f.exists():
            return None
        with f.open('r') as i:
            return json.load(i)

    def set_inmem(self, id: str, v: Any):
        self._in_memory_cache[id] = v

    def lookup_inmem(self, id: str) -> Optional[Any]:
        global _ignore_cache
        if _ignore_cache or self._ignore_cache:
            return None

        if id not in self._in_memory_cache:
            return None
        return self._in_memory_cache[id]

    def data_file_location(self, request_id: str, data_name: str) -> Path:
        '''
        Return the path to the file that should be written out for this
        data_name. This is where the output file should get stored.
        Truncate the leftmost characters from filenames to avoid throwing a
        OSError: [Errno 63] File name too long error Assume that the most unique part of
        the name is the right hand side
        '''
        parent = self._path / 'data' / request_id
        parent.mkdir(parents=True, exist_ok=True)
        sanitized = sanitize_filename(data_name)
        return parent / sanitized[-1 * (MAX_PATH_LEN - len(parent.name)):]
