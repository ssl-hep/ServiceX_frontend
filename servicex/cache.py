import hashlib
import json
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

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


# Track where we should write out an analysis cache (and search up-stream).
# This contains the directory of the file.
_g_analysis_cache_location: Optional[Path] = None
_g_analysis_cache_filename: str = 'servicex_query_cache.json'

# List of queries we know to be bad from this run
_g_bad_query_cache_ids: Set[str] = set()


def reset_local_query_cache():
    '''Used to reset the analysis cache location. Normally called only
    during testing.
    '''
    global _g_analysis_cache_location
    _g_analysis_cache_location = None
    global _g_analysis_cache_filename
    _g_analysis_cache_filename = 'servicex_query_cache.json'
    global _g_bad_query_cache_ids
    _g_bad_query_cache_ids = set()


reset_local_query_cache()


def update_local_query_cache(analysis_cache: Optional[Path] = None):
    '''Record analysis query hashes in an analysis cache.

    If this routine is not called, the current directoy is searched for an
    analysis cache file. If found, it participates in the query lookup.

    After this rouinte is called, then when a query is made or looked up, an
    analysis cache file in the local directory is updated with new query request
    id's.

    This will allow one user to send a file to another user, along with the
    servicex backend in `servicex.yaml` allow them to fetch the same data.
    (or share on a similar machine).

    Args:
        analysis_cache (Optional[Path], optional): The directory or filename of
        the analysis cache file. If `None` defaults to the file `servicex_query_cache.json` in
        the local directory. If only a directory is passed, then the `servicex_query_cache.json`
        in that directory is used. Defaults to None.
    '''
    file_path = Path('.') if analysis_cache is None \
        else analysis_cache if analysis_cache.is_dir() \
        else analysis_cache.parent

    name = 'servicex_query_cache.json' if (analysis_cache is None or analysis_cache.is_dir()) \
        else analysis_cache.name

    global _g_analysis_cache_filename
    global _g_analysis_cache_location
    if _g_analysis_cache_location is not None \
            and _g_analysis_cache_filename != name:
        raise ServiceXException('Updating local query cache called twice, with '
                                f'{_g_analysis_cache_filename} and {name}.')
    _g_analysis_cache_filename = name

    if _g_analysis_cache_location is not None \
            and _g_analysis_cache_location != file_path:
        raise ServiceXException('Updating local query cache called twice, with '
                                f'{_g_analysis_cache_location} and {file_path}.')
    _g_analysis_cache_location = file_path


class Cache:
    '''
    Caching for all data returns from the system. It provides both in-memory
    and on-disk cache.

    TODO: Rename this to be an adaptor, unifying how we name things
    '''
    _in_memory_cache = {}

    @classmethod
    def reset_cache(cls):  # # pragma: no cover
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
            hash = _query_cache_hash(json)
            if hash in _g_bad_query_cache_ids:
                return None
            return self._lookup_analysis_query_cache(hash)

        with f.open('r') as i:
            request_id = i.readline().strip()

        self._write_analysis_query_cache(json, request_id)

        return request_id

    def set_query(self, json: Dict[str, str], v: str):
        '''Associate a query with a request-id.

        A hash is taken of the query.

        Args:
            json (Dict[str, str]): The query JSON
            v (str): The `request-id`
        '''
        f = self._query_cache_file(json)
        f.parent.mkdir(parents=True, exist_ok=True)
        with f.open('w') as o:
            o.write(f'{v}\n')

        self._write_analysis_query_cache(json, v)

    def remove_query(self, json: Dict[str, Any]):
        '''Remove the query from our local and analysis caches

        Args:
            json (Dict[str, Any]): The query to remove
        '''
        f = self._query_cache_file(json)
        if f.exists():
            f.unlink()
        self._remove_from_analysis_cache(_query_cache_hash(json))

    def _write_analysis_query_cache(self, query_info: Dict[str, str], request_id: str):
        '''Write out a local analysis query hash-request-id assocaition.

        Args:
            query_info (Dict[str, str]): The JSON of the request
            request_id (str): The `request-id`
        '''
        if _g_analysis_cache_location is None:
            return

        q_file = _g_analysis_cache_location / _g_analysis_cache_filename
        analysis_cache = {}
        if q_file.exists():
            with q_file.open('r') as input:
                analysis_cache = json.load(input)

        analysis_cache[_query_cache_hash(query_info)] = request_id

        if not q_file.parent.exists():
            q_file.parent.mkdir()
        with q_file.open('w') as output:
            json.dump(analysis_cache, output)

    def _remove_from_analysis_cache(self, query_hash: str):
        '''Remove an item from the analysis cache if we are writing to it!

        Args:
            query_hash (str): The hash we will remove
        '''
        if _g_analysis_cache_location is None:
            _g_bad_query_cache_ids.add(query_hash)
            return

        cache_contents, cache_file = self._find_analysis_cached_query(query_hash)
        if cache_contents is not None:
            del cache_contents[query_hash]
            assert cache_file is not None
            with cache_file.open('w') as output:
                json.dump(cache_contents, output)

    def _lookup_analysis_query_cache(self, query_hash: str,
                                     filename: Optional[str] = None,
                                     location: Optional[Path] = None) \
            -> Optional[str]:
        '''Look at all possible query caches for this query.

        If `location` is `None`, then start from the global location searching for a query file.
        If `location` is specified, check that directory.
        In both cases, if the query hash isn't found, then move up one directory and try again.

        `filename` is the name of the file we should be looking for. If `None` default to the
        global.

        Args:
            query_hash (str): The hash of the query we need to lookup.
            filename (Optional[str]): The name fo the file that contains the cache. If not
            specified then defaults to the global.
            location (Optional[Path]): Directory to start searching in. If not specified then
            defaults to the global. If that isn't specified, defaults to the current directory.

        Returns:
            (Optional[str]): The return hash of what we need to look up
        '''
        cache_contents, _ = self._find_analysis_cached_query(query_hash)
        if cache_contents is not None:
            return cache_contents[query_hash]
        return None

    def _find_analysis_cached_query(self, query_hash: str,
                                    filename: Optional[str] = None,
                                    location: Optional[Path] = None) \
            -> Tuple[Optional[Dict[str, str]], Optional[Path]]:
        '''Returns the contents of an analysis cache file and the file that contains
        a query hash

        Args:
            has (str): The hash of the query we are to find

        Returns:
            Tuple[Dict[str, str], Path]: The contents of the file and the path to the
            analysis cache that contains the hash. `None` if the query was not found
        '''
        # Get arguments setup
        c_filename = filename if filename is not None else _g_analysis_cache_filename
        c_location = location if location is not None \
            else _g_analysis_cache_location if _g_analysis_cache_location is not None \
            else Path('.')

        # If the cache is here, then see if it has a hit
        cache_file = c_location / c_filename
        if cache_file.exists():
            with cache_file.open('r') as input:
                analysis_cache = json.load(input)
                if query_hash in analysis_cache:
                    return (analysis_cache, cache_file)

        # Recurse one up.
        if len(c_location.parts) <= 1:
            return None, None
        return self._find_analysis_cached_query(query_hash, c_filename, c_location.parent)

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
            raise ServiceXException(f'No cache information for query {request_id}')
        with f.open('r') as o:
            return json.load(o)

    def query_status_exists(self, request_id: str) -> bool:
        """Returns true if the query status file exists on the local machine.

        Args:
            request_id (str): The request-id to look up

        Returns:
            bool: True if present, false otherwise.
        """
        return self._query_status_cache_file(request_id).exists()

    def set_files(self, id: str, files: List[Tuple[str, Path]]):
        """Cache the files for this request

        Note: We do check to make sure all the files exist

        Args:
            id (str): The request-id
            files (List[Tuple[str, Path]]): the minio buck name and local file paths
        """
        # Make sure they exist
        assert all(f.exists() for _, f in files)
        f = self._files_cache_file(id)
        f.parent.mkdir(parents=True, exist_ok=True)
        with f.open('w') as o:
            json.dump([(n, str(f)) for n, f in files], o)

    def lookup_files(self, id: str) -> Optional[List[Tuple[str, Path]]]:
        """Return a list of files in the cache for a request id.

        - Returns None if there is nothing in the cache
        - Returns None if any of the files are missing

        Args:
            id (str): Request-id we are looking up

        Returns:
            Optional[List[Tuple[str, Path]]]: List of minio-bucket to local file mappings
        """
        f = self._files_cache_file(id)
        if not f.exists():
            return None
        with f.open('r') as i:
            list_of_cached_files = [(n, Path(p)) for n, p in json.load(i)]
        if all(f.exists() for _, f in list_of_cached_files):
            return list_of_cached_files
        return None

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
        OSError: [Errno 63] File name too long error. Use a hash string to
        make sure that the file names remain unique.
        '''
        parent = self._path / 'data' / request_id
        parent.mkdir(parents=True, exist_ok=True)
        sanitized = sanitize_filename(data_name)
        if (len(sanitized) + len(parent.name)) > MAX_PATH_LEN:
            hash = hashlib.md5(sanitized.encode())
            hash_string = hash.hexdigest()
            max_len = MAX_PATH_LEN - len(parent.name) - len(hash_string) - 1
            sanitized = f'{hash_string}-{sanitized[len(sanitized)-max_len:]}'

        return parent / sanitized
