import json
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any

from .utils import _query_cache_hash


class Cache:
    '''
    Caching for all data returns from the system. It provides both in-memory
    and on-disk cache.
    '''
    _in_memory_cache = {}

    @classmethod
    def reset_cache(cls):
        'Reset the internal cache, usually used for testing'
        cls._in_memory_cache = {}

    def __init__(self, cache_path: Path):
        '''
        Create the cache object

        Arguments:

            cache_path          The path to the cache directory. Only sub-directories
                                will be created in this path.
        '''
        self._path = cache_path

    def _query_cache_file(self, json: Dict[str, str]) -> Path:
        'Return the query cache file'
        h = _query_cache_hash(json)
        return self._path / 'query_cache' / h

    def _files_cache_file(self, id: str) -> Path:
        'Return the file that contains the list of files'
        return self._path / 'file_list_cache' / id

    def lookup_query(self, json: Dict[str, str]) -> Optional[str]:
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
        if id not in self._in_memory_cache:
            return None
        return self._in_memory_cache[id]
