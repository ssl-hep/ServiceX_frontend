import asyncio
import functools
import logging
import time
from datetime import timedelta
from pathlib import Path
from typing import (Any, AsyncGenerator, AsyncIterator, Awaitable, Callable,
                    Dict, List, Optional, Tuple, Union)

import aiohttp
import backoff
from backoff import on_exception

from servicex.servicex_config import ServiceXConfigAdaptor

from .cache import Cache
from .data_conversions import DataConverterAdaptor
from .minio_adaptor import (MinioAdaptor, MinioAdaptorFactory,
                            find_new_bucket_files)
from .servicex_adaptor import (ServiceXAdaptor, transform_status_stream,
                               trap_servicex_failures)
from .servicex_utils import _wrap_in_memory_sx_cache
from .servicexabc import ServiceXABC
from .utils import (ServiceXException, ServiceXFailedFileTransform,
                    ServiceXFatalTransformException,
                    ServiceXUnknownDataRequestID, ServiceXUnknownRequestID,
                    StatusUpdateFactory, _run_default_wrapper,
                    _status_update_wrapper, default_client_session,
                    get_configured_cache_path, log_adaptor,
                    stream_status_updates, stream_unique_updates_only)


class StreamInfoBase:
    '''Contains base information about results that are streamed back from
    ServiceX.
    '''
    def __init__(self, file: str):
        self._file = file

    @property
    def file(self) -> str:
        '''Returns the ServiceX filename

        This filename is unique in the dataset, and will be the same accross different queries
        against the dataset. It can be used as a key to sort results.

        Notes:

        - May contains non-file system characters

        Returns:
            str: servicex filename
        '''
        return self._file


class StreamInfoUrl(StreamInfoBase):
    '''Contains information about results that are streamed back from ServiceX.
    Used when a URL to access the data directly from ServiceX is requested.
    '''
    def __init__(self, file: str, url: str, bucket: str):
        super().__init__(file)
        self._url = url
        self._bucket = bucket

    @property
    def url(self) -> str:
        '''URL that can can be used to stream data back from ServiceX.

        Returns:
            str: The URL of the transformed data for this file.
        '''
        return self._url

    @property
    def bucket(self) -> str:
        '''Returns the buck name - unique and constant accross transformations.
        Can be used to order the results

        Returns:
            str: The bucket name as produced by ServiceX
        '''
        return self._bucket


class StreamInfoPath(StreamInfoBase):
    '''Contains information about results that are streamed back from ServiceX.
    Used when the user has requested streaming, but copying the file locally first.
    '''
    def __init__(self, file: str, path: Path):
        super().__init__(file)
        self._path = path

    @property
    def path(self) -> Path:
        '''Path the the local file of ServiceX data that represents this query.

        Returns:
            Path: The path object that points to the data requested.
        '''
        return self._path


class StreamInfoData(StreamInfoBase):
    '''Contains information about results that are streamed back from ServiceX.
    Used when data (`pandas` or `awkward`) is requested. The data is downloaded from
    ServiceX, converted into the requested format, and then streamed to the user in these
    chunks. There is a single chunk per file.
    '''
    def __init__(self, file: str, data: Any):
        super().__init__(file)
        self._data = data

    @property
    def data(self) -> Any:
        '''The `pandas.DataFrame` or `awkward` array return

        Returns:
            Any: The ServiceX transformed data for this file.
        '''
        return self._data


class ServiceXDataset(ServiceXABC):
    '''
    Used to access an instance of ServiceX at an end point on the internet. Support convieration
    by configuration object `config_adaptor` or by creating the adaptors defined in the `__init__`
    function.
    '''
    def __init__(self,
                 dataset: str,
                 backend_type: Optional[str] = None,
                 image: str = None,
                 max_workers: int = 20,
                 servicex_adaptor: ServiceXAdaptor = None,
                 minio_adaptor: Union[MinioAdaptor, MinioAdaptorFactory] = None,
                 cache_adaptor: Optional[Cache] = None,
                 status_callback_factory: Optional[StatusUpdateFactory] = _run_default_wrapper,
                 local_log: log_adaptor = None,
                 session_generator: Callable[[], Awaitable[aiohttp.ClientSession]] = None,
                 config_adaptor: Optional[ServiceXConfigAdaptor] = None,
                 data_convert_adaptor: Optional[DataConverterAdaptor] = None,
                 ignore_cache: bool = False):
        '''
        Create and configure a ServiceX object for a dataset.

        Arguments

            dataset                     Name of a dataset from which queries will be selected.
            backend_type                The type of backend. Used only if we need to find an
                                        end-point. If we do not have a `servicex_adaptor` then this
                                        will default to xaod, unless you have any endpoint listed
                                        in your servicex file. It will default to best match there,
                                        in that case.
            image                       Name of transformer image to use to transform the data. If
                                        left as default, `None`, then the default image for the
                                        ServiceX backend will be used.
            max_workers                 Maximum number of transformers to run simultaneously on
                                        ServiceX.
            servicex_adaptor            Object to control communication with the servicex instance
                                        at a particular ip address with certian login credentials.
                                        Will be configured via the `config_adaptor` by default.
            minio_adaptor               Object to control communication with the minio servicex
                                        instance.
            cache_adaptor               Runs the caching for data and queries that are sent up and
                                        down.
            status_callback_factory     Factory to create a status notification callback for each
                                        query. One is created per query.
            local_log                   Log adaptor for logging.
            session_generator           If you want to control the `ClientSession` object that
                                        is used for callbacks. Otherwise a single one for all
                                        `servicex` queries is used.
            config_adaptor              Control how configuration options are read from the
                                        a configuration file (e.g. servicex.yaml).
            data_convert_adaptor        Manages conversions between root and parquet and `pandas`
                                        and `awkward`, including default settings for expected
                                        datatypes from the backend.
            ignore_cache                Always ignore the cache on any query for this dataset. This
                                        is only meaningful if no cache adaptor is provided.
                                        Defaults to false - the cache is used if possible.

        Notes:

            -  The `status_callback` argument, by default, uses the `tqdm` library to render
               progress bars in a terminal window or a graphic in a Jupyter notebook (with proper
               jupyter extensions installed). If `status_callback` is specified as None, no
               updates will be rendered. A custom callback function can also be specified which
               takes `(total_files, transformed, downloaded, skipped)` as an argument. The
               `total_files` parameter may be `None` until the system knows how many files need to
               be processed (and some files can even be completed before that is known).
            -  The full description of calling parameters is recorded in the local cache, including
               things like `image` name and tag.
        '''
        ServiceXABC.__init__(self, dataset, image, max_workers,
                             status_callback_factory,
                             )

        # Get the local settings
        config = config_adaptor if config_adaptor is not None \
            else ServiceXConfigAdaptor()

        # Establish the cache that will store all our queries
        self._cache = Cache(get_configured_cache_path(config.settings), ignore_cache) \
            if cache_adaptor is None \
            else cache_adaptor

        if not servicex_adaptor:
            # Given servicex adaptor is none, this should be ok. Fixes type checkers
            end_point, token = config.get_servicex_adaptor_config(backend_type)
            servicex_adaptor = ServiceXAdaptor(end_point, token)
        self._servicex_adaptor = servicex_adaptor

        if not minio_adaptor:
            self._minio_adaptor = MinioAdaptorFactory()
        else:
            if isinstance(minio_adaptor, MinioAdaptor):
                self._minio_adaptor = MinioAdaptorFactory(always_return=minio_adaptor)
            else:
                self._minio_adaptor = minio_adaptor

        self._log = log_adaptor() if local_log is None else local_log

        self._session_generator = session_generator if session_generator is not None \
            else default_client_session

        self._converter = data_convert_adaptor if data_convert_adaptor is not None \
            else DataConverterAdaptor(config.get_default_returned_datatype(backend_type))

    def ignore_cache(self):
        '''Return a context manager that, as long as it is held, will cause any queries against just
        this dataset to ignore any locally cached data.

        Returns:
            ContextManager: As long as this is held, the local query cache will be ignored.
        '''
        return self._cache.ignore_cache()

    @functools.wraps(ServiceXABC.get_data_rootfiles_async, updated=())
    @_wrap_in_memory_sx_cache
    async def get_data_rootfiles_async(self, selection_query: str) -> List[Path]:
        return await self._file_return(selection_query, 'root-file')

    async def get_data_rootfiles_stream(self, selection_query: str) \
            -> AsyncIterator[StreamInfoPath]:
        '''Returns, as an async iterator, each completed batch of work from Servicex.
        The `StreamInfoPath` contains a path where downstream consumers can directly
        access the data.

        Args:
            selection_query (str): The `qastle` query for the data to retreive.

        Yields:
            AsyncIterator[StreamInfoPath]: As ServiceX completes the data, and it is downloaded
                                           to the local machine, the async iterator returns
                                           a `StreamInfoPath` which can be used to access the
                                           file locally.
        '''
        async for f_info in \
                self._stream_local_files(selection_query, 'root-files'):  # type: ignore
            yield f_info

    @functools.wraps(ServiceXABC.get_data_parquet_async, updated=())
    @_wrap_in_memory_sx_cache
    async def get_data_parquet_async(self, selection_query: str) -> List[Path]:
        return await self._file_return(selection_query, 'parquet')

    async def get_data_parquet_stream(self, selection_query: str) \
            -> AsyncIterator[StreamInfoPath]:
        '''Returns, as an async iterator, each completed batch of work from Servicex.
        The `StreamInfoPath` contains a path where downstream consumers can directly
        access the data.

        Args:
            selection_query (str): The `qastle` query for the data to retreive.

        Yields:
            AsyncIterator[StreamInfoPath]: As ServiceX completes the data, and it is downloaded
                                           to the local machine, the async iterator returns
                                           a `StreamInfoPath` which can be used to access the
                                           file locally.
        '''
        async for f_info in self._stream_local_files(selection_query, 'parquet'):  # type: ignore
            yield f_info

    @functools.wraps(ServiceXABC.get_data_pandas_df_async, updated=())
    @_wrap_in_memory_sx_cache
    async def get_data_pandas_df_async(self, selection_query: str):
        return self._converter.combine_pandas(await self._data_return(
            selection_query, lambda f: self._converter.convert_to_pandas(f)))

    @functools.wraps(ServiceXABC.get_data_awkward_async, updated=())
    @_wrap_in_memory_sx_cache
    async def get_data_awkward_async(self, selection_query: str):
        return self._converter.combine_awkward(await self._data_return(
            selection_query, lambda f: self._converter.convert_to_awkward(f)))

    async def get_data_awkward_stream(self, selection_query: str) \
            -> AsyncGenerator[StreamInfoData, None]:
        '''Returns, as an async iterator, each completed batch of work from Servicex
        as a separate `awkward` array. The data is returned in a `StreamInfoData` object.

        Args:
            selection_query (str): The `qastle` query for the data to retreive.

        Yields:
            AsyncIterator[StreamInfoData]: As ServiceX completes the data, and it is downloaded
                                           to the local machine, the async iterator returns
                                           a `StreamInfoData` which can be used to access the
                                           data that has been loaded from the file.
        '''
        async for a in self._stream_return(selection_query,
                                           lambda f: self._converter.convert_to_awkward(f)):
            yield a

    async def get_data_pandas_stream(self, selection_query: str) \
            -> AsyncGenerator[StreamInfoData, None]:
        '''Returns, as an async iterator, each completed batch of work from Servicex
        as a separate `pandas.DataFrame` array. The data is returned in a `StreamInfoData` object.

        Args:
            selection_query (str): The `qastle` query for the data to retreive.

        Yields:
            AsyncIterator[StreamInfoData]: As ServiceX completes the data, and it is downloaded
                                           to the local machine, the async iterator returns
                                           a `StreamInfoData` which can be used to access the
                                           data that has been loaded from the file.
        '''
        async for a in self._stream_return(selection_query,
                                           lambda f: self._converter.convert_to_pandas(f)):
            yield a

    async def get_data_rootfiles_url_stream(self, selection_query: str) \
            -> AsyncIterator[StreamInfoUrl]:
        '''Returns, as an async iterator, each completed batch of work from ServiceX.
        The data that comes back includes a `url` that can be accessed to download the
        data.

        Args:
            selection_query (str): The ServiceX Selection
        '''
        async for f_info in \
                self._stream_url_buckets(selection_query, 'root-files'):  # type: ignore
            yield f_info

    async def get_data_parquet_url_stream(self, selection_query: str) \
            -> AsyncIterator[StreamInfoUrl]:
        '''Returns, as an async iterator, each of the files from the minio bucket,
        as the files are added there.

        Args:
            selection_query (str): The ServiceX Selection
        '''
        async for f_info in self._stream_url_buckets(selection_query, 'parquet'):  # type: ignore
            yield f_info

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
    @on_exception(backoff.constant, ServiceXUnknownDataRequestID, interval=0.1, max_tries=2)
    async def _stream_url_buckets(self, selection_query: str, data_format: str) \
            -> AsyncGenerator[StreamInfoUrl, None]:
        '''Get a list of files back for a request

        Args:
            selection_query (str): The selection query we are to do
            data_format (str): The requested file format

        Yields:
            AsyncIterator[Dict[str, str]]: A tuple of the minio bucket and file in that bucket.
                                           The dict will have entries for:
                                             bucket: The minio bucket name
                                             file: the completed file in the bucket
        '''
        query = self._build_json_query(selection_query, data_format)

        async with aiohttp.ClientSession() as client:

            # Get a request id - which might be cached, but if not, submit it.
            request_id = await self._get_request_id(client, query)

            # Get the minio adaptor we are going to use for downloading.
            minio_adaptor = self._minio_adaptor \
                .from_best(self._cache.lookup_query_status(request_id))

            # Look up the cache, and then fetch an iterator going thorugh the results
            # from either servicex or the cache, depending.
            try:
                notifier = self._create_notifier(False)
                minio_files = self._get_minio_bucket_files_from_servicex(request_id, client,
                                                                         minio_adaptor, notifier)

                # Reflect the files back up a level.
                async for r in minio_files:
                    yield StreamInfoUrl(r, minio_adaptor.get_access_url(request_id, r), request_id)

                # Cache the final status
                await self._update_query_status(client, request_id)

            except ServiceXUnknownRequestID as e:
                self._cache.remove_query(query)
                raise ServiceXUnknownDataRequestID('Expected the ServiceX backend to know about '
                                                   f'query {request_id}. It did not. Cleared local'
                                                   'cache. Please resubmit to trigger a new '
                                                   'query.') from e

            except ServiceXFatalTransformException as e:
                transform_status = await self._servicex_adaptor.get_query_status(client,
                                                                                 request_id)
                raise ServiceXFatalTransformException(
                    f'ServiceX Fatal Error: {transform_status["failure-info"]}') from e

            except ServiceXFailedFileTransform as e:
                self._cache.remove_query(query)
                await self._servicex_adaptor.dump_query_errors(client, request_id)
                raise ServiceXException(f'Failed to transform all files in {request_id}') from e

    async def _data_return(self, selection_query: str,
                           converter: Callable[[Path], Awaitable[Any]],
                           data_format: str = 'root-file') -> List[Any]:
        '''Given a query, return the data, in a unique order, that hold
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
        all_data = {
            f.file: f.data
            async for f in self._stream_return(selection_query, converter, data_format)
        }

        # Convert them to the proper format

        # Finally, we need them in the proper order so we append them
        # all together
        ordered_data = [all_data[k] for k in sorted(all_data.keys())]

        return ordered_data

    async def _stream_return(self, selection_query: str,
                             converter: Callable[[Path], Awaitable[Any]],
                             data_format: str = 'root-file') -> AsyncIterator[StreamInfoData]:
        '''Given a query, return the data, in the order it arrives back
        converted as appropriate.

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
        as_data = (StreamInfoData(f.file, await asyncio.ensure_future(converter(f.path)))
                   async for f in
                   self._stream_local_files(selection_query, data_format))  # type: ignore

        async for r in as_data:
            yield r

    @on_exception(backoff.constant, ServiceXUnknownRequestID, interval=0.1, max_tries=3)
    async def _stream_local_files(self, selection_query: str,
                                  data_format: str = 'root-file') \
            -> AsyncGenerator[StreamInfoPath, None]:
        '''
        Given a query, return the data as a list of paths pointing to local files
        that contain the results of the query. This is an async generator, and files
        are returned as they arrive.

        For certian types of exceptions, the queries will be repeated. For example,
        if `ServiceX` indicates that it was restarted in the middle of the query, then
        the query will be re-submitted.

        Arguments:

            selection_query     `qastle` data that makes up the selection request.

        Returns:

            data                Data converted to the "proper" format, depending
                                on the converter call.
        '''
        # Get a notifier to update anyone who wants to listen.
        notifier = self._create_notifier(True)

        # Get all the files
        as_files = \
            (f async for f in
             self._get_files(selection_query, data_format, notifier))  # type: ignore

        async for name, a_path in as_files:
            yield StreamInfoPath(name, Path(await a_path))

    @on_exception(backoff.constant, ServiceXUnknownDataRequestID, interval=0.1, max_tries=2)
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

            # Get the minio adaptor we are going to use for downloading.
            minio_adaptor = self._minio_adaptor \
                .from_best(self._cache.lookup_query_status(request_id))

            # Look up the cache, and then fetch an iterator going thorugh the results
            # from either servicex or the cache, depending.
            try:
                cached_files = self._cache.lookup_files(request_id)
                stream_local_files = self._get_cached_files(cached_files, notifier) \
                    if cached_files is not None \
                    else self._get_files_from_servicex(request_id, client, minio_adaptor, notifier)

                # Reflect the files back up a level.
                async for r in stream_local_files:
                    yield r

                # Cache the final status
                if cached_files is None:
                    await self._update_query_status(client, request_id)

            except ServiceXUnknownRequestID as e:
                self._cache.remove_query(query)
                raise ServiceXUnknownDataRequestID('Expected the ServiceX backend to know about '
                                                   f'query {request_id}. It did not. Cleared local'
                                                   ' cache. Please resubmit to trigger a new '
                                                   'query.') from e

            except ServiceXFatalTransformException as e:
                transform_status = await self._servicex_adaptor.get_query_status(client,
                                                                                 request_id)
                raise ServiceXFatalTransformException(
                    f'ServiceX Fatal Error: {transform_status["failure-info"]}') from e

            except ServiceXFailedFileTransform as e:
                self._cache.remove_query(query)
                await self._servicex_adaptor.dump_query_errors(client, request_id)
                raise ServiceXException(f'Failed to transform all files in {request_id}') from e

    async def _get_request_id(self, client: aiohttp.ClientSession, query: Dict[str, Any]) -> str:
        '''
        For this query, fetch the request id. If we have it cached, use that. Otherwise, query
        ServiceX for a enw one (and cache it for later use).
        '''
        request_id = self._cache.lookup_query(query)
        if request_id is None:
            request_info = await self._servicex_adaptor.submit_query(client, query)
            request_id = request_info['request_id']
            self._cache.set_query(query, request_id)
            await self._update_query_status(client, request_id)
        return request_id

    async def _update_query_status(self, client: aiohttp.ClientSession,
                                   request_id: str):
        '''Fetch the status from servicex and cache it locally, over
        writing what was there before.

        Args:
            request_id (str): Request id of the status to fetch and cache.
        '''
        info = await self._servicex_adaptor.get_query_status(client, request_id)
        self._cache.set_query_status(info)

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
                               minio_adaptor: MinioAdaptor,
                               notifier: _status_update_wrapper) \
            -> AsyncIterator[Tuple[str, Awaitable[Path]]]:
        '''
        Given an object name and request id, fetch the data locally from the minio bucket.
        The copy can take a while, so send it off to another thread - don't pause queuing up other
        files to download.
        '''

        async def do_copy(final_path):
            assert request_id is not None
            await minio_adaptor.download_file(request_id, f, final_path)
            notifier.inc(downloaded=1)
            notifier.broadcast()
            return final_path

        file_object_list = []
        async for f in stream:
            copy_to_path = self._cache.data_file_location(request_id, f)
            file_object_list.append((f, str(copy_to_path)))

            yield f, do_copy(copy_to_path)

        self._cache.set_files(request_id, file_object_list)

    async def _get_files_from_servicex(self, request_id: str,
                                       client: aiohttp.ClientSession,
                                       minio_adaptor: MinioAdaptor,
                                       notifier: _status_update_wrapper):
        '''
        Fetch query result files from `servicex`. Given the `request_id` we will download
        files as they become available. We also coordinate caching.
        '''
        start_time = time.monotonic()
        good = True
        try:

            # Get the stream of minio bucket new files.
            stream_new_object = self._get_minio_bucket_files_from_servicex(
                request_id, client, minio_adaptor, notifier
            )

            # Next, download the files as they are found (and return them):
            stream_downloaded = self._download_a_file(stream_new_object, request_id,
                                                      minio_adaptor, notifier)

            # Return the files to anyone that wants them!

            async for info in stream_downloaded:
                yield info

        except Exception:
            good = False
            raise

        finally:
            end_time = time.monotonic()
            run_time = timedelta(seconds=end_time - start_time)
            logging.getLogger(__name__).info(f'Running servicex query for '
                                             f'{request_id} took {run_time}')
            self._log.write_query_log(request_id, notifier.total, notifier.failed,
                                      run_time, good, self._cache.path)

    async def _get_minio_bucket_files_from_servicex(self, request_id: str,
                                                    client: aiohttp.ClientSession,
                                                    minio_adaptor: MinioAdaptor,
                                                    notifier: _status_update_wrapper) \
            -> AsyncIterator[str]:
        '''Create an async stream of `minio` bucket/filenames from a request id.

        Args:
            request_id (str): The request id that we should be polling for updates.
            client (aiohttp.ClientSession): The client connection to make API queries on
            minio_adaptor (MinioAdaptor): The minio adaptor we can use to connect to the minio
                                          bucket for new items.
            notifier (_status_update_wrapper): Allows us to send updates of progress
                                               back to the user

        Yields:
            [type]: Returns xxx and yyy.
        '''
        start_time = time.monotonic()
        try:

            # Setup the status sequence from servicex
            stream_status = transform_status_stream(self._servicex_adaptor, client, request_id)
            stream_notified = stream_status_updates(stream_status, notifier)
            stream_watched = trap_servicex_failures(stream_notified)
            stream_unique = stream_unique_updates_only(stream_watched)

            # Next, download the files as they are found (and return them):
            stream_new_object = find_new_bucket_files(minio_adaptor, request_id,
                                                      stream_unique)

            # Return the minio information.
            async for info in stream_new_object:
                yield info

        finally:
            end_time = time.monotonic()
            run_time = timedelta(seconds=end_time - start_time)
            logging.getLogger(__name__).info(f'Running servicex query for '
                                             f'{request_id} took {run_time} (no files downloaded)')

    def _build_json_query(self, selection_query: str, data_type: str) -> Dict[str, str]:
        '''
        Returns a list of locally written files for a given selection query.

        Arguments:
            selection_query         The query to be send into the ServiceX API
            data_type               What is the output data type (parquet, root-file, etc.)

        Notes:
            - Internal routine.
        '''
        # Items that must always be present
        json_query: Dict[str, str] = {
            "did": self._dataset,
            "selection": selection_query,
            "result-destination": "object-store",
            "result-format": 'parquet' if data_type == 'parquet' else "root-file",
            "chunk-size": '1000',
            "workers": str(self._max_workers)
        }

        # Optional items
        if self._image is not None:
            json_query['image'] = self._image

        logging.getLogger(__name__).debug(f'JSON to be sent to servicex: {str(json_query)}')

        return json_query
