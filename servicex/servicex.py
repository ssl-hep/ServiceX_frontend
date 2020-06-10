# Main front end interface
from abc import abstractmethod
import asyncio
import functools
import logging
from pathlib import Path
from typing import (
    Any,
    Awaitable,
    Callable,
    Dict,
    List,
    Optional,
    Tuple,
    Union,
)
from typing import Iterator
import urllib

import aiohttp
import awkward
from make_it_sync import make_sync
from minio import Minio
import numpy as np
import pandas as pd

from .data_conversions import _convert_root_to_pandas, _convert_root_to_awkward
from .servicex_remote import (
    _download_file,
    _get_transform_status,
    _result_object_list,
    _submit_query,
)
from .utils import (
    ServiceXException,
    ServiceXUnknownRequestID,
    _default_wrapper_mgr,
    _run_default_wrapper,
    _status_update_wrapper,
    _string_hash,
)
from .cache import cache

# TODO: Make sure clean_linq is properly used, somewhere.

# Number of seconds to wait between polling servicex for the status of a transform job
# while waiting for it to finish.
servicex_status_poll_time = 5.0


class ServiceXABC:
    '''
    Abstract base class for accessing the ServiceX front-end for a particular dataset. This does
    have some implementations, but not a full set (hence why it isn't an ABC).

    A light weight, mostly immutable, base class that holds basic configuration information for use
    with ServiceX file access, including the dataset name. Subclasses implement the various access
    methods. Note that not all methods may be accessible!
    '''

    def __init__(self,
                 dataset: str,
                 image: str = 'sslhep/servicex_func_adl_xaod_transformer:v0.4',
                 storage_directory: Optional[str] = None,
                 file_name_func: Optional[Callable[[str, str], Path]] = None,
                 max_workers: int = 20,
                 status_callback: Optional[Callable[[Optional[int], int, int, int], None]]
                 = _run_default_wrapper):
        '''
        Create and configure a ServiceX object for a dataset.

        Arguments
            dataset             Name of a dataset from which queries will be selected.
            service_endpoint    Where the ServiceX web API is found
            image               Name of transformer image to use to transform the data
            storage_directory   Location to cache data that comes back from ServiceX. Data can
                                be reused between invocations.
            file_name_func      Allows for unique naming of the files that come back. Rarely used.
            max_workers         Maximum number of transformers to run simultaneously on ServiceX.
            status_callback     Callback to update client on status of the query. See Notes.


        Notes:
            The `status_callback` argument, by default, uses the `tqdm` library to render progress
            bars in a terminal window or a graphic in a Jupyter notebook (with proper jupyter
            extensions installed). If `status_callback` is specified as None, no updates will be
            rendered. A custom callback function can also be specified which takes `(total_files,
            transformed, downloaded, skipped)` as an argument. The `total_files` parameter may be
            `None` until the system knows how many files need to be processed (and some files can
            even be completed before that is known).
        '''
        self._dataset = dataset
        self._image = image
        self._max_workers = max_workers

        # Normalize how we do the status updates
        if status_callback is _run_default_wrapper:
            t = _default_wrapper_mgr(dataset)
            status_callback = t.update
        self._notifier = _status_update_wrapper(status_callback)

        # Normalize how we do the files
        if file_name_func is None:
            if storage_directory is None:
                def file_name(req_id: str, minio_name: str):
                    import servicex.utils as sx
                    return sx.default_file_cache_name / req_id / sanitize_filename(minio_name)
                self._file_name_func = file_name
            else:
                def file_name(req_id: str, minio_name: str):
                    assert storage_directory is not None
                    return Path(storage_directory) / sanitize_filename(minio_name)
                self._file_name_func = file_name
        else:
            if storage_directory is not None:
                raise ServiceXException('Can only specify `storage_directory` or `file_name_func`'
                                        ' when creating Servicex, not both.')
            self._file_name_func = file_name_func

    @property
    def dataset(self):
        return self._dataset

    @abstractmethod
    async def get_data_rootfiles_async(self, selection_query: str) -> List[Path]:
        '''
        Fetch query data from ServiceX matching `selection_query` and return it as
        a list of root files. The files are uniquely ordered (the same query will always
        return the same order).

        Arguments:
            selection_query     The `qastle` string specifying the data to be queried

        Returns:
            root_files          The list of root files
        '''
        raise NotImplementedError()

    @abstractmethod
    async def get_data_pandas_df_async(self, selection_query: str) -> pd.DataFrame:
        '''
        Fetch query data from ServiceX matching `selection_query` and return it as
        a pandas dataframe. The data is uniquely ordered (the same query will always
        return the same order).

        Arguments:
            selection_query     The `qastle` string specifying the data to be queried

        Returns:
            df                  The pandas dataframe

        Exceptions:
            xxx                 If the data is not the correct shape (e.g. a flat,
                                rectangular table).
        '''
        raise NotImplementedError()

    @abstractmethod
    async def get_data_awkward_async(self, selection_query: str) \
            -> Dict[bytes, Union[awkward.JaggedArray, np.ndarray]]:
        '''
        Fetch query data from ServiceX matching `selection_query` and return it as
        dictionary of awkward arrays, an entry for each column. The data is uniquely
        ordered (the same query will always return the same order).

        Arguments:
            selection_query     The `qastle` string specifying the data to be queried

        Returns:
            a                   Dictionary of jagged arrays (as needed), one for each
                                column. The dictionary keys are `bytes` to support possible
                                unicode characters.
        '''
        raise NotImplementedError()

    @abstractmethod
    async def get_data_parquet_async(self, selection_query: str) -> List[Path]:
        '''
        Fetch query data from ServiceX matching `selection_query` and return it as
        a list of parquet files. The files are uniquely ordered (the same query will always
        return the same order).

        Arguments:
            selection_query     The `qastle` string specifying the data to be queried

        Returns:
            root_files          The list of parquet files
        '''
        raise NotImplementedError()

    # Define the synchronous versions of the async methods for easy of use

    get_data_rootfiles = make_sync(get_data_rootfiles_async)
    get_data_pandas_df = make_sync(get_data_pandas_df_async)
    get_data_awkward = make_sync(get_data_awkward_async)
    get_data_parquet = make_sync(get_data_parquet_async)

    def ignore_cache(self):
        '''
        A context manager for use in a with statement that will cause all calls
        to `get_data` routines to ignore any local caching. This will likely force
        ServiceX to re-run the query. Only queries against this dataset will ignore
        the cache.
        '''
        return None


def ignore_cache():
    '''
        A context manager for use in a with statement that will cause all calls
        to `get_data` routines to ignore any local caching. This will likely force
        ServiceX to re-run the query. Only queries against this dataset will ignore
        the cache.
    '''
    pass


_in_progress_items: Dict[str, asyncio.Event] = {}


def _wrap_inmem_cache(fn):
    '''
    Wrap a ServiceX function that is getting data so that we can
    use the internal cache, if the item exists, and bypass everything.

    NOTE: This is not thread safe. In fact, you'll likely get a bomb if try to
          call this from another thread and the two threads request something the
          same at the same time because asyncio locks are associated with loops,
          and there is one loop per thread.
    '''
    @functools.wraps(fn)
    async def cached_version_of_fn(*args, **kwargs):
        assert len(args) == 2
        sx = args[0]
        assert isinstance(sx, ServiceX)
        selection_query = args[1]
        assert isinstance(selection_query, str)

        # Is it in the local cache?
        h = _string_hash([sx._dataset, selection_query])
        if h in _in_progress_items:
            await _in_progress_items[h].wait()

        # Is it already done?
        r = sx._cache.lookup_inmem(h)
        if r is not None:
            return r

        # It is not. We need to calculate it, and prevent
        # others from working on it.
        _in_progress_items[h] = asyncio.Event()
        try:
            result = await fn(*args, **kwargs)
            sx._cache.set_inmem(h, result)
        finally:
            _in_progress_items[h].set()
            del _in_progress_items[h]

        return result

    return cached_version_of_fn


class ServiceX(ServiceXABC):
    '''
    ServiceX on the web.
    '''
    def __init__(self,
                 dataset: str,
                 service_endpoint: str = 'http://localhost:5000/servicex',
                 image: str = 'sslhep/servicex_func_adl_xaod_transformer:v0.4',
                 storage_directory: Optional[str] = None,
                 file_name_func: Optional[Callable[[str, str], Path]] = None,
                 max_workers: int = 20,
                 status_callback: Optional[Callable[[Optional[int], int, int, int], None]]
                 = _run_default_wrapper):
        ServiceXABC.__init__(self, dataset, image, storage_directory, file_name_func,
                             max_workers, status_callback)
        self._endpoint = service_endpoint
        from servicex.utils import default_file_cache_name
        self._cache = cache(default_file_cache_name)

    @property
    def endpoint(self):
        return self._endpoint

    @functools.wraps(ServiceXABC.get_data_rootfiles_async)
    @_wrap_inmem_cache
    async def get_data_rootfiles_async(self, selection_query: str):
        return await self._file_return(selection_query, 'root-file')

    @functools.wraps(ServiceXABC.get_data_parquet_async)
    @_wrap_inmem_cache
    async def get_data_parquet_async(self, selection_query: str):
        return await self._file_return(selection_query, 'parquet')

    @functools.wraps(ServiceXABC.get_data_pandas_df_async)
    @_wrap_inmem_cache
    async def get_data_pandas_df_async(self, selection_query: str):
        import pandas as pd
        return pd.concat(await self._data_return(selection_query, _convert_root_to_pandas))

    @functools.wraps(ServiceXABC.get_data_awkward_async)
    @_wrap_inmem_cache
    async def get_data_awkward_async(self, selection_query: str):
        import awkward
        all_data = await self._data_return(selection_query, _convert_root_to_awkward)
        col_names = all_data[0].keys()
        return {c: awkward.concatenate([ar[c] for ar in all_data]) for c in col_names}

    # Define the synchronous versions of the async methods for easy of use
    # TODO: Why do these have to be repeated?
    get_data_rootfiles = make_sync(get_data_rootfiles_async)
    get_data_pandas_df = make_sync(get_data_pandas_df_async)
    get_data_awkward = make_sync(get_data_awkward_async)
    get_data_parquet = make_sync(get_data_parquet_async)

    async def _data_return(self, selection_query: str,
                           converter: Callable[[Path], Awaitable[Any]]):
        # Get all the files
        as_files = (f async for f in self._get_files(selection_query, 'root-file'))

        # Convert them to the proper format
        as_data = ((f[0], asyncio.ensure_future(converter(await f[1])))
                   async for f in as_files)

        # Finally, we need them in the proper order so we append them
        # all together
        all_data = {f[0]: await f[1] async for f in as_data}
        ordered_data = [all_data[k] for k in sorted(all_data)]

        return ordered_data

    async def _file_return(self, selection_query: str, data_format: str):
        '''
        Internal routine to refactor any query that will return a list of files (as opposed
        to in-memory data).
        '''
        all = [f async for f in self._get_files(selection_query, data_format)]
        all_dict = {f[0]: await f[1] for f in all}
        return [all_dict[k] for k in sorted(all_dict)]

    def _build_json_query(self, selection_query: str, data_type: str) -> Dict[str, str]:
        '''
        Returns a list of locally written files for a given selection query.

        Arguments:
            selection_query         The query to be send into the ServiceX API
            data_type               What is the output data type (parquet, root-file, etc.)

        Notes:
            - Internal routine.
        '''
        json_query: Dict[str, str] = {
            "did": self._dataset,
            "selection": selection_query,
            "image": self._image,
            "result-destination": "object-store",
            "result-format": 'parquet' if data_type == 'parquet' else "root-file",
            "chunk-size": '1000',
            "workers": str(self._max_workers)
        }

        logging.getLogger(__name__).debug(f'JSON to be sent to servicex: {str(json_query)}')

        return json_query

    def _minio_client(self):
        '''
        Create the minio client
        '''
        end_point_parse = urllib.parse.urlparse(self._endpoint)  # type: ignore
        minio_endpoint = f'{end_point_parse.hostname}:9000'

        minio_client = Minio(minio_endpoint,
                             access_key='miniouser',
                             secret_key='leftfoot1',
                             secure=False)
        return minio_client

    async def _get_status_loop(self, client: aiohttp.ClientSession,
                               request_id: str,
                               downloader: _result_object_list):
        '''
        Run the status loop, file scans each time a new file is finished.

        Arguments:

            client          `aiohttp` client session for web access
            request_id      Request for this query
            downloader      Download scanner object

        Returns:

            None            Done when no more files are left.

        Note:

            - Raise the `_RetryException` if this is the first query and ServiceX does not
              know about the exception.
        '''
        done = False
        last_processed = 0
        try:
            while not done:
                remaining, processed, failed = await _get_transform_status(client, self._endpoint,
                                                                           request_id)
                done = remaining is not None and remaining == 0
                if processed != last_processed:
                    last_processed = processed
                    downloader.trigger_scan()

                self._notifier.update(processed=processed, failed=failed, remaining=remaining)
                self._notifier.broadcast()

                if not done:
                    await asyncio.sleep(servicex_status_poll_time)
        finally:
            downloader.shutdown()

    async def _get_files(self, selection_query: str, data_type: str) \
            -> Iterator[Tuple[str, Awaitable[Path]]]:
        '''
        Return a list of files from servicex as they have been downloaded to this machine. The
        return type is an awaitable that will yield the path to the file.

        Arguments:

            selection_query             The query string to send to ServiceX
            data_type                   The type of data that we want to come back.

        Returns
            Awaitable[Path]             An awaitable that is a path. When it completes, the
                                        path will be valid and point to an existing file.
                                        This is returned this way so a number of downloads can run
                                        simultaneously.
        '''
        query = self._build_json_query(selection_query, data_type)
        request_id = self._cache.lookup_query(query)

        async with aiohttp.ClientSession() as client:
            if request_id is None:
                request_id = await _submit_query(client, self._endpoint, query)
                self._cache.set_query(query, request_id)

            cached_files = self._cache.lookup_files(request_id)
            if cached_files is not None:
                self._notifier.update(processed=len(cached_files), remaining=0, failed=0)
                loop = asyncio.get_event_loop()
                for f, p in cached_files:
                    self._notifier.inc(downloaded=1)
                    path_future = loop.create_future()
                    path_future.set_result(Path(p))
                    yield f, path_future
            else:
                minio = self._minio_client()
                results_from_query = _result_object_list(minio, request_id)

                # Problem - if this throws, then we want to get out of this, and we will be stuck
                # waiting below too.
                r_loop = asyncio.ensure_future(self._get_status_loop(client, request_id,
                                                                     results_from_query))

                file_object_list = []
                async for f in results_from_query.files():
                    copy_to_path = self._file_name_func(request_id, f)

                    async def do_wait(final_path):
                        assert request_id is not None
                        await _download_file(minio, request_id, f, final_path)
                        self._notifier.inc(downloaded=1)
                        self._notifier.broadcast()
                        return final_path
                    file_object_list.append((f, str(copy_to_path)))
                    yield f, do_wait(copy_to_path)

                # TODO: Pull this out to make sure this works correctly at a high level.
                # This is torture code.
                retry_me = False
                try:
                    await r_loop
                except ServiceXUnknownRequestID:
                    retry_me = True

                if retry_me:
                    self._cache.remove_query(query)
                    async for r in self._get_files(selection_query, data_type):
                        yield r

                # If we got here, then the request finished.
                self._cache.set_files(request_id, file_object_list)

                # Now that data has been moved back here, lets make sure there were no failed
                # files.
                if self._notifier.failed > 0:
                    # Take back the cache line
                    self._cache.remove_query(query)
                    raise ServiceXException(f'ServiceX failed to transform '
                                            f'{self._notifier.failed}'
                                            ' files - data incomplete.')


def sanitize_filename(fname: str):
    'No matter the string given, make it an acceptable filename'
    return fname.replace('*', '_') \
                .replace(';', '_') \
                .replace(':', '_')
