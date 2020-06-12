# Main front end interface
import asyncio
import functools
import logging
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple
from typing import Iterator
import urllib

import aiohttp
from make_it_sync import make_sync
from minio import Minio
from backoff import on_exception
import backoff

from .cache import cache
from .data_conversions import _convert_root_to_awkward, _convert_root_to_pandas
from .servicex_remote import (
    _download_file,
    _get_transform_status,
    _result_object_list,
    _submit_query,
)
from .servicexabc import ServiceXABC
from .utils import (
    ServiceXException,
    ServiceXUnknownRequestID,
    _run_default_wrapper,
)
from .servicex_utils import _wrap_in_memory_sx_cache

# TODO: Make sure clean_linq is properly used, somewhere.

# Number of seconds to wait between polling servicex for the status of a transform job
# while waiting for it to finish.
servicex_status_poll_time = 5.0


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
    @_wrap_in_memory_sx_cache
    async def get_data_rootfiles_async(self, selection_query: str):
        return await self._file_return(selection_query, 'root-file')

    @functools.wraps(ServiceXABC.get_data_parquet_async)
    @_wrap_in_memory_sx_cache
    async def get_data_parquet_async(self, selection_query: str):
        return await self._file_return(selection_query, 'parquet')

    @functools.wraps(ServiceXABC.get_data_pandas_df_async)
    @_wrap_in_memory_sx_cache
    async def get_data_pandas_df_async(self, selection_query: str):
        import pandas as pd
        return pd.concat(await self._data_return(selection_query, _convert_root_to_pandas))

    @functools.wraps(ServiceXABC.get_data_awkward_async)
    @_wrap_in_memory_sx_cache
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

    async def _file_return(self, selection_query: str, data_format: str):
        '''
        Given a query, return the list of files, in a unique order, that hold
        the data for the query.

        For certian types of exceptions, the queries will be repeated. For example,
        if `ServiceX` indicates that it was restarted in the middle of the query, then
        the query will be re-submitted.

        Arguments:

            selection_query     `qastle` data that makes up the selection request.
            data_format         The file-based data format (root or parquet)

        Returns:

            data                Data converted to the "proper" format, depending
                                on the converter call.
        '''
        async def convert_to_file(f: Path) -> Path:
            return f

        return await self._data_return(selection_query, convert_to_file, data_format)

    @on_exception(backoff.constant, ServiceXUnknownRequestID, interval=0.1, max_tries=3)
    async def _data_return(self, selection_query: str,
                           converter: Callable[[Path], Awaitable[Any]],
                           data_format: str = 'root-file'):
        '''
        Given a query, return the data, in a unique order, that hold
        the data for the query.

        For certian types of exceptions, the queries will be repeated. For example,
        if `ServiceX` indicates that it was restarted in the middle of the query, then
        the query will be re-submitted.

        Arguments:

            selection_query     `qastle` data that makes up the selection request.
            converter           A `Callable` that will convert the data returned from
                                `ServiceX` as a set of files.

        Returns:

            data                Data converted to the "proper" format, depending
                                on the converter call.
        '''
        # Get all the files
        as_files = (f async for f in self._get_files(selection_query, data_format))  # type: ignore

        # Convert them to the proper format
        as_data = ((f[0], asyncio.ensure_future(converter(await f[1])))
                   async for f in as_files)  # type: ignore

        # Finally, we need them in the proper order so we append them
        # all together
        all_data = {f[0]: await f[1] async for f in as_data}  # type: ignore
        ordered_data = [all_data[k] for k in sorted(all_data)]

        return ordered_data

    async def _get_files(self, selection_query: str, data_type: str) \
            -> Iterator[Tuple[str, Awaitable[Path]]]:
        '''
        Return a list of files from servicex as they have been downloaded to this machine. The
        return type is an awaitable that will yield the path to the file.

        For certian types of `ServiceX` failures we will automatically attempt a few retries:

            - When `ServiceX` forgets the query. This sometimes happens when a user submits a
              query, and then disconnects from the network, `ServiceX` is restarted, and then the
              user attempts to download the files from that "no-longer-existing" request.

        Up to 3 re-tries are attempted automatically.

        Arguments:

            selection_query             The query string to send to ServiceX
            data_type                   The type of data that we want to come back.

        Returns
            Awaitable[Path]             An awaitable that is a path. When it completes, the
                                        path will be valid and point to an existing file.
                                        This is returned this way so a number of downloads can run
                                        simultaneously.
        '''
        # TODO: Notifier is a per-query item, not a per dataset, needs to be created
        #       for each query, not a single one.
        query = self._build_json_query(selection_query, data_type)

        async with aiohttp.ClientSession() as client:

            # Get a request id - which might be cached, but if not, submit it.
            request_id = await self._get_request_id(client, query)

            # Look up the cache, and then fetch an iterator going thorugh the results
            # from either servicex or the cache, depending.
            cached_files = self._cache.lookup_files(request_id)
            fetched_files_seq = self._get_cached_files(cached_files) if cached_files is not None \
                else self._get_files_from_servicex(request_id, client, query)

            # Reflect the files back up a level.
            async for r in fetched_files_seq:
                yield r

    async def _get_request_id(self, client: aiohttp.ClientSession, query: Dict[str, Any]):
        '''
        For this query, fetch the request id. If we have it cached, use that. Otherwise, query
        ServiceX for a enw one (and cache it for later use).
        '''
        request_id = self._cache.lookup_query(query)
        if request_id is None:
            request_id = await _submit_query(client, self._endpoint, query)
            self._cache.set_query(query, request_id)
        return request_id

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

    async def _get_cached_files(self, cached_files: List[Tuple[str, str]]):
        '''
        Return the list of files as an iterator that we have pulled from the cache
        '''
        self._notifier.update(processed=len(cached_files), remaining=0, failed=0)
        loop = asyncio.get_event_loop()
        for f, p in cached_files:
            self._notifier.inc(downloaded=1)
            path_future = loop.create_future()
            path_future.set_result(Path(p))
            yield f, path_future

    async def _get_files_from_servicex(self, request_id: str,
                                       client: aiohttp.ClientSession,
                                       query: Dict[str, str]):
        '''
        Fetch query result files from `servicex`. Given the `request_id` we will download
        files as they become available. We also coordinate caching.
        '''
        try:
            minio = self._minio_client()
            results_from_query = _result_object_list(minio, request_id)

            # Problem - if this throws, then we want to get out of this, and we will be stuck
            # waiting below too.
            r_loop = asyncio.ensure_future(self._get_status_loop(client, request_id,
                                                                 results_from_query))

            try:
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
            finally:
                await r_loop

            # Now that data has been moved back here, lets make sure there were no failed
            # files. If there were, then we need to mark this whole transform as
            # having failed, and remove any trace of it in our caches so that if the user
            # wants to re-try, then can do it as they see fit.
            if self._notifier.failed > 0:
                self._cache.remove_query(query)
                raise ServiceXException(f'ServiceX failed to transform '
                                        f'{self._notifier.failed}'
                                        ' files - data incomplete.')

            # If we got here, then the request finished! Cache the results! Woo!
            self._cache.set_files(request_id, file_object_list)

        except ServiceXUnknownRequestID:
            # If servicex can't find this query, then we need to forget about it locally
            # too.
            self._cache.remove_query(query)

            raise

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
