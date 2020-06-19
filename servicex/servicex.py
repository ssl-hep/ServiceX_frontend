# Main front end interface
import asyncio
import functools
import logging
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple
import urllib

import aiohttp
from backoff import on_exception
import backoff

from typing import AsyncIterator

from .cache import cache
from .data_conversions import _convert_root_to_awkward, _convert_root_to_pandas
from .minio_adaptor import MinioAdaptor, find_new_bucket_files
from .servicex_adaptor import ServiceXAdaptor, transform_status_stream, watch_transform_status
from .servicex_utils import _wrap_in_memory_sx_cache
from .servicexabc import ServiceXABC
from .utils import (
    ServiceXUnknownRequestID,
    StatusUpdateFactory,
    _run_default_wrapper,
    _status_update_wrapper,
    stream_transform_updates,
    stream_inc_update
)


class ServiceX(ServiceXABC):
    '''
    ServiceX on the web.
    '''
    def __init__(self,
                 dataset: str,
                 servicex_adaptor: ServiceXAdaptor,
                 minio_adaptor: MinioAdaptor = None,
                 image: str = 'sslhep/servicex_func_adl_xaod_transformer:v0.4',
                 storage_directory: Optional[str] = None,
                 file_name_func: Optional[Callable[[str, str], Path]] = None,
                 max_workers: int = 20,
                 status_callback_factory: StatusUpdateFactory = _run_default_wrapper):
        ServiceXABC.__init__(self, dataset, image, storage_directory, file_name_func,
                             max_workers, status_callback_factory)
        self._servicex_adaptor = servicex_adaptor

        if not minio_adaptor:
            end_point_parse = \
                urllib.parse.urlparse(self._servicex_adaptor._endpoint)  # type: ignore
            minio_endpoint = f'{end_point_parse.hostname}:9000'
            self._minio_adaptor = MinioAdaptor(minio_endpoint, 'miniouser', 'leftfoot1')
        else:
            self._minio_adaptor = minio_adaptor

        from servicex.utils import default_file_cache_name
        self._cache = cache(default_file_cache_name)

    @functools.wraps(ServiceXABC.get_data_rootfiles_async, updated=())
    @_wrap_in_memory_sx_cache
    async def get_data_rootfiles_async(self, selection_query: str) -> List[Path]:
        return await self._file_return(selection_query, 'root-file')

    @functools.wraps(ServiceXABC.get_data_parquet_async, updated=())
    @_wrap_in_memory_sx_cache
    async def get_data_parquet_async(self, selection_query: str):
        return await self._file_return(selection_query, 'parquet')

    @functools.wraps(ServiceXABC.get_data_pandas_df_async, updated=())
    @_wrap_in_memory_sx_cache
    async def get_data_pandas_df_async(self, selection_query: str):
        import pandas as pd
        return pd.concat(await self._data_return(selection_query, _convert_root_to_pandas))

    @functools.wraps(ServiceXABC.get_data_awkward_async, updated=())
    @_wrap_in_memory_sx_cache
    async def get_data_awkward_async(self, selection_query: str):
        import awkward
        all_data = await self._data_return(selection_query, _convert_root_to_awkward)
        col_names = all_data[0].keys()
        return {c: awkward.concatenate([ar[c] for ar in all_data]) for c in col_names}

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
        # Get a notifier to update anyone who wants to listen.
        notifier = self._create_notifier()

        # Get all the files
        as_files = \
            (f async for f in
             self._get_files(selection_query, data_format, notifier))

        # Convert them to the proper format
        as_data = ((f[0], asyncio.ensure_future(converter(await f[1])))
                   async for f in as_files)  # type: ignore

        # Finally, we need them in the proper order so we append them
        # all together
        all_data = {f[0]: await f[1] async for f in as_data}  # type: ignore
        ordered_data = [all_data[k] for k in sorted(all_data)]

        return ordered_data

    async def _get_files(self, selection_query: str, data_type: str,
                         notifier: _status_update_wrapper) \
            -> AsyncIterator[Tuple[str, Awaitable[Path]]]:
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
            notifier                    Status callback to let our progress be advertised

        Returns
            Awaitable[Path]             An awaitable that is a path. When it completes, the
                                        path will be valid and point to an existing file.
                                        This is returned this way so a number of downloads can run
                                        simultaneously.
        '''
        query = self._build_json_query(selection_query, data_type)

        async with aiohttp.ClientSession() as client:

            # Get a request id - which might be cached, but if not, submit it.
            request_id = await self._get_request_id(client, query)

            # Look up the cache, and then fetch an iterator going thorugh the results
            # from either servicex or the cache, depending.
            cached_files = self._cache.lookup_files(request_id)
            stream_local_files = self._get_cached_files(cached_files, notifier) \
                if cached_files is not None \
                else self._get_files_from_servicex(request_id, client, query, notifier)

            # Reflect the files back up a level.
            async for r in stream_local_files:
                yield r

    async def _get_request_id(self, client: aiohttp.ClientSession, query: Dict[str, Any]):
        '''
        For this query, fetch the request id. If we have it cached, use that. Otherwise, query
        ServiceX for a enw one (and cache it for later use).
        '''
        request_id = self._cache.lookup_query(query)
        if request_id is None:
            request_id = await self._servicex_adaptor.submit_query(client, query)
            self._cache.set_query(query, request_id)
        return request_id

    async def _get_cached_files(self, cached_files: List[Tuple[str, str]],
                                notifier: _status_update_wrapper):
        '''
        Return the list of files as an iterator that we have pulled from the cache
        '''
        notifier.update(processed=len(cached_files), remaining=0, failed=0)
        loop = asyncio.get_event_loop()
        for f, p in cached_files:
            path_future = loop.create_future()
            path_future.set_result(Path(p))
            notifier.inc(downloaded=1)
            yield f, path_future

    async def _download_a_file(self, stream: AsyncIterator[str],
                               request_id: str,
                               notifier: _status_update_wrapper) \
            -> AsyncIterator[Tuple[str, Awaitable[Path]]]:
        '''
        Given an object name and request id, fetch the data locally from the minio bucket.
        The copy can take a while, so send it off to another thread - don't pause queuing up other
        files to download.
        '''
        async def do_copy(final_path):
            assert request_id is not None
            await self._minio_adaptor.download_file(request_id, f, final_path)
            notifier.inc(downloaded=1)
            notifier.broadcast()
            return final_path

        file_object_list = []
        async for f in stream:
            copy_to_path = self._file_name_func(request_id, f)
            file_object_list.append((f, str(copy_to_path)))

            yield f, do_copy(copy_to_path)

        self._cache.set_files(request_id, file_object_list)

    async def _get_files_from_servicex(self, request_id: str,
                                       client: aiohttp.ClientSession,
                                       query: Dict[str, str],
                                       notifier: _status_update_wrapper):
        '''
        Fetch query result files from `servicex`. Given the `request_id` we will download
        files as they become available. We also coordinate caching.
        '''
        # Setup the status sequence from servicex
        stream_status = transform_status_stream(self._servicex_adaptor, client, request_id)
        stream_watched = watch_transform_status(stream_status)
        stream_notified = stream_transform_updates(stream_watched, notifier)

        # Next, download the files as they are found (and return them):
        stream_new_files = find_new_bucket_files(self._minio_adaptor, request_id,
                                                 stream_notified)
        stream_downloaded = self._download_a_file(stream_new_files, request_id, notifier)

        # Return the files to anyone that wants them!

        async for info in stream_downloaded:
            yield info

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
