# Main front end interface
from abc import abstractmethod
import asyncio
from concurrent.futures import ThreadPoolExecutor
import functools
import logging
import os
from pathlib import Path
import tempfile
from typing import (
    Any,
    Awaitable,
    Callable,
    Dict,
    Generator,
    Iterable,
    List,
    Optional,
    Tuple,
    Union,
    Callable
)
from typing import Iterator
import urllib

import aiohttp
import awkward
from make_it_sync import make_sync
from minio import Minio, ResponseError
import numpy as np
import pandas as pd
from retry import retry
import uproot

from .data_conversions import _convert_root_to_pandas, _convert_root_to_awkward
from .servicex_remote import (
    _download_file,
    _get_transform_status,
    _result_object_list,
    _submit_query,
)
from .utils import (
    ServiceXFrontEndException,
    ServiceX_Exception,
    _default_wrapper_mgr,
    _file_object_cache_filename,
    _file_object_cache_filename_temp,
    _query_is_cached,
    _run_default_wrapper,
    _status_update_wrapper,
    _submit_or_lookup_transform,
    clean_linq,
)

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
        self._status_callback = status_callback

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

    @property
    def endpoint(self):
        return self._endpoint

    @functools.wraps(ServiceXABC.get_data_rootfiles_async)
    async def get_data_rootfiles_async(self, selection_query: str):
        return await self._file_return(selection_query, 'root-file')

    @functools.wraps(ServiceXABC.get_data_parquet_async)
    async def get_data_parquet_async(self, selection_query: str):
        return await self._file_return(selection_query, 'parquet')

    @functools.wraps(ServiceXABC.get_data_pandas_df_async)
    async def get_data_pandas_df_async(self, selection_query: str):
        import pandas as pd
        return pd.concat(await self._data_return(selection_query, _convert_root_to_pandas))

    @functools.wraps(ServiceXABC.get_data_awkward_async)
    async def get_data_awkward_async(self, selection_query: str):
        import awkward
        all_data = await self._data_return(selection_query, _convert_root_to_awkward)
        col_names = all_data[0].keys()
        return {c: awkward.concatenate([ar[c] for ar in all_data]) for c in col_names}

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

        async with aiohttp.ClientSession() as client:
            request_id = await _submit_query(client, self._endpoint, query)

            minio = self._minio_client()
            results_from_query = _result_object_list(minio, request_id)

            # Problem - if this throws, then we want to get out of this, and we will be stuck
            # waiting below too.
            r_loop = asyncio.ensure_future(self._get_status_loop(client, request_id,
                                                                 results_from_query))

            async for f in results_from_query.files():
                copy_to_path = self._file_name_func(request_id, f)

                async def do_wait(final_path):
                    await _download_file(minio, request_id, f, final_path)
                    return copy_to_path
                yield f, do_wait(copy_to_path)

            await r_loop

# ##### Below here is old (but working!) code. For reviewing API please ignore.

    # ls = load_cached_file_list(request_id)
    # if ls is not None:
    #     return 0, sum(1 for _ in ls), 0


def sanitize_filename(fname: str):
    'No matter the string given, make it an acceptable filename'
    return fname.replace('*', '_') \
                .replace(';', '_') \
                .replace(':', '_')


# Threadpool on which downloads occur. This is because the current minio library
# uses blocking http requests, so we can't use asyncio to interleave them.
_download_executor = ThreadPoolExecutor(max_workers=5)


# def _download_file(minio_client: Minio, request_id: str, bucket_fname: str,
#                    file_name_func: Callable[[str, str], str],
#                    redownload_files: bool) -> Tuple[str, str]:
#     '''
#     Download a single file to a local temp file from the minio object store, and return
#     its location.
#     '''
#     local_filepath = file_name_func(request_id, bucket_fname)

#     # Can we re-use something that is there?
#     if (not redownload_files) and (os.path.exists(local_filepath)):
#         return (bucket_fname, local_filepath)

#     # Make sure there is a place to write the output file.
#     dir = os.path.dirname(local_filepath)
#     Path(dir).mkdir(parents=True, exist_ok=True)

#     # We are going to build a temp file, and download it from there.
#     temp_local_filepath = f'{local_filepath}.temp'
#     minio_client.fget_object(request_id, bucket_fname, temp_local_filepath)
#     os.replace(temp_local_filepath, local_filepath)

#     # Done, notify anyone that wants to update progress.
#     return (bucket_fname, local_filepath)


def load_cached_file_list(request_id: str) -> Optional[Iterator[str]]:
    '''
    If the request finished and all files were downloaded, return the list of files.

    Arguments:

        request_id          The request id for the query

    Returns
        None                No query cached
        List[str]           List of the names of the files that came back.
    '''
    p = _file_object_cache_filename(request_id)
    p.parent.mkdir(parents=True, exist_ok=True)
    if p.exists():
        return filter(lambda l: len(l) > 0, p.read_text().splitlines())
    else:
        return None


@retry(delay=1, tries=10, exceptions=ResponseError)
def protected_list_objects(client: Minio, request_id: str) -> Iterable[str]:
    '''
    Returns a list of files that are currently available for
    this request. Will pull the list from the disk cache if possible.
    '''
    # Try to fetch from cache
    cached_files = load_cached_file_list(request_id)
    if cached_files is not None:
        return cached_files

    # Go out to actually request the list
    listing = [f.object_name for f in client.list_objects(request_id)]

    # Write out the a temp cache file, which will be renamed once we are
    # sure the full set is downloaded.
    _file_object_cache_filename_temp(request_id) \
        .write_text(os.linesep.join(listing))

    return listing


def _post_process_data(data_type: str, filepath: Tuple[str, str],
                       notifier: _status_update_wrapper):
    '''
    Post-process the data and return the "appropriate" type
    '''
    try:
        if data_type == 'root-file' or data_type == 'parquet':
            return filepath

        # All other types require loading things into uproot first.
        f_in = uproot.open(filepath[1])
        try:
            r = f_in[f_in.keys()[0]]
            if data_type == 'pandas':
                return (filepath[0], r.pandas.df())
            elif data_type == 'awkward':
                return (filepath[0], r.arrays())
            else:
                raise ServiceXFrontEndException(f'Internal coding error - {data_type} '
                                                'should not be known.')
        finally:
            f_in._context.source.close()
    finally:
        notifier.inc(downloaded=1)
        notifier.broadcast()


async def _download_new_files(files_queued: Iterable[str], end_point: str,
                              request_id: str,
                              data_type: str,
                              file_name_func: Callable[[str, str], str],
                              redownload_files: bool,
                              notifier: _status_update_wrapper) -> Dict[str, Any]:
    '''
    Look at the minio bucket to see if there are any new file items written there. If so, then
    trigger a download.

    Arguments:
        files_queued        List of files already downloading or downloaded
        end_point           Where we can reach minio
        request_id          Request we are trying to access and get data for
        data_type           What is the final data type?
        file_name_func      Where we should stash the files.
        redownload_files    If present, then redownload the file anyway

    Returns:
        futures     Futures for all files that are pending downloads
    '''
    # We need to assume where the minio port is and go from there.
    end_point_parse = urllib.parse.urlparse(end_point)
    minio_endpoint = f'{end_point_parse.hostname}:9000'

    minio_client = Minio(minio_endpoint,
                         access_key='miniouser',
                         secret_key='leftfoot1',
                         secure=False)

    files = protected_list_objects(minio_client, request_id)
    new_files = [fname for fname in files if fname not in files_queued]

    # Submit in a thread pool so they can run and block concurrently (minio doesn't have
    # an async interface), and then do any post-processing needed. We write this as a function
    # because it is easier to see what is going on.
    def do_download_and_post(fname: str):
        data = _download_file(minio_client, request_id, fname, file_name_func,
                              redownload_files)
        return _post_process_data(data_type, data, notifier)

    futures = {fname: asyncio.wrap_future(_download_executor.submit(do_download_and_post, fname))
               for fname in new_files}
    return futures


async def get_data_cache_calc(request_id: str,
                              client: aiohttp.ClientSession,
                              servicex_endpoint: str,
                              notifier: _status_update_wrapper,
                              data_type: str,
                              file_name_func: Callable[[str, str], str],
                              redownload_files: bool) \
        -> Tuple[bool, Optional[Union[pd.DataFrame, Dict[bytes, np.ndarray], List[str]]]]:
    '''
    Returns:
        retry       True if we should restart the query from scratch
        result      If retry is false, the result, otherwise None.
    '''
    # Sit here waiting for the results to come in. In case there are missing items
    # in the minio stream, we will avoid counting that. That should be an explicit error taken
    # care of further on down in the code.
    done = False
    files_downloading = {}
    last_files_processed = 0
    first = True
    while not done:
        try:
            if not first:
                await asyncio.sleep(servicex_status_poll_time)
            files_remaining, files_processed, files_failed = \
                await _get_transform_status(client, servicex_endpoint, request_id)
            first = False
        except Exception as e:
            if 'error 500' in str(e):
                if first:
                    return True, None
            raise

        notifier.update(processed=files_processed)
        if files_failed is not None:
            notifier.update(failed=files_failed)
        if files_remaining is not None:
            t = files_remaining + files_processed \
                + (files_failed if files_failed is not None else 0)
            notifier.update(total=t)
        notifier.broadcast()

        if files_processed > last_files_processed:
            new_downloads = await _download_new_files(files_downloading.keys(),
                                                      servicex_endpoint, request_id,
                                                      data_type, file_name_func,
                                                      redownload_files, notifier)
            files_downloading.update(new_downloads)
            last_files_processed = files_processed

        done = (files_remaining is not None) and files_remaining == 0

    # Now, wait for all of them to complete so we can stich the files together.
    all_file_info = list(await asyncio.gather(*files_downloading.values()))

    # If any files have failed, then no need to get down anything from this request!
    # We did need to wait for everything to finish, otherwise downloads will continue,
    # files will be locked, and, in general, things will be hard to deal with outside
    # of this function when we return.
    if notifier.failed > 0:
        raise ServiceX_Exception(f'ServiceX failed to transform {notifier.failed} '
                                 f'out of {notifier.total} files.')

    # We have the complete list of files to cache. So lets rename them.
    # TODO: Caching logic is too spread out.
    t_cache_file = _file_object_cache_filename_temp(request_id)
    if t_cache_file.exists():
        t_cache_file.rename(_file_object_cache_filename(request_id))

    # return the result, sorting to keep the data in order.
    assert len(all_file_info) > 0, \
        'Internal error: no error from ServiceX, but also no files back either!'
    all_file_info.sort(key=lambda k: k[0])
    all_files = [v[1] for v in all_file_info]

    if data_type == 'root-file' or data_type == 'parquet':
        return False, list(all_files)

    # We need to shift the files to another format.
    if len(all_files) == 1:
        return False, all_files[0]
    else:
        if data_type == 'pandas':
            r = pd.concat(all_files)
            assert isinstance(r, pd.DataFrame)
            return False, r
        elif data_type == 'awkward':
            col_names = all_files[0].keys()
            return False, {c: awkward.concatenate([ar[c] for ar in all_files]) for c in col_names}
        else:
            raise ServiceXFrontEndException(f'Internal programming error - {data_type} should '
                                            'not be unknown.')


# Cache the data that is coming back.
_data_cache \
    = {}

# Locks a query incase two identical queries come in at the
# same time.
_query_locks = {}
_query_locker = asyncio.Lock()


class weak_holder:
    'Holder to hold all types of objects - weakref cannot hold lists, etc'
    def __init__(self, o):
        self.o = o


async def get_data_async(selection_query: str, dataset: str,
                         servicex_endpoint: str = 'http://localhost:5000/servicex',
                         data_type: str = 'pandas',
                         image: str = 'sslhep/servicex_func_adl_xaod_transformer:v0.4',
                         max_workers: int = 20,
                         storage_directory: Optional[str] = None,
                         file_name_func: Callable[[str, str], str] = None,
                         redownload_files: bool = False,
                         use_cache: bool = True,
                         status_callback: Optional[Callable[[Optional[int], int, int, int], None]]
                         = _run_default_wrapper) \
        -> Union[pd.DataFrame, Dict[bytes, np.ndarray], List[str]]:
    '''
    Return data from a query with data sets.

    Arguments:
        selection_query     `qastle` string that specifies what columns to extract, how to format
                            them, and how to format them.
        dataset             Dataset (DID) to run the query against.
        service_endpoint    The URL where the instance of ServivceX we are querying lives
        data_type           How should the data come back? 'pandas', 'awkward', and 'root-file'
                            are the only legal values. Defaults to 'pandas'
        image               ServiceX image that should run this.
        max_workers         Max number of workers that will run to process this request.
        storage_directory   Location where files should be downloaded. If None is specified then
                            they are stored in the machine's temp directory.
        file_name_func      Returns a path where the file should be written. The path must be
                            fully qualified (`storage_directory` must not be set if this is used).
                            See notes on how to use this lambda.
        redownload_files    If true, even if the file is already in the requested location,
                            re-download it.
        status_callback     If specified, called as we grab files and download them with updates.
                            Called with arguments TotalFiles, Processed By ServiceX, Downloaded
                            locally. TotalFiles might change over time as more files are found by
                            servicex.
        use_cache           Use the local query cache. If false it will force a re-run, and the
                            result of the run will overwrite what is currently in the cache.

    Returns:
        df                  Depends on the `data_type` that has been requested:
                            `data_type == 'pandas'` a single in-memory pandas.DataFrame
                            `data_type == 'awkward'` a single dict of JaggedArrays
                            `data_type == 'root-file'` List of paths to root files. You
                                are responsible for deleting them when done.

    ## Notes

    - There are combinations of image name and and `data_type` and `selection_query` that
      do not work together. That logic is resolved at the `ServiceX` backend, and if the
      parameters do not match, an exception will be thrown in your code.
    - the `max_workers` parameter is currently translated into the actual number of workers
        to process this request. As the `ServiceX` back-end evolves this will be come the max
        number of workers.
    - This is the python `async` interface (see python documentation on `await` and
        `async`). It should be used if you plan to make more than one simultaneous query
        to the system.
    - The `file_name_func` function takes two arguments, the first is the request-id and
      the second is the full minio object name.
        - The object name may contain ':', ';', and '*' and perhaps other characters that
          are not allowed on your filesystem.
        - The file path does not need to have been created - `os.mkdir` will be run on the
          filepath.
        - It will almost certainly be the case that the call-back for this function occurs
          on a different thread than you made the initial call to `get_data_async`. Make sure
          your code is thread safe!
        - The filename should be safe in the sense that a ".downloading" can be appended to
          the end of the string without causing any trouble.
    '''
    # Parameter clean up, API safety checking
    if (data_type != 'pandas') \
            and (data_type != 'awkward') \
            and (data_type != 'parquet') \
            and (data_type != 'root-file'):
        raise ServiceXFrontEndException('Unknown return type.')

    if (storage_directory is not None) and (file_name_func is not None):
        raise Exception("You may only specify `storage_direcotry` or `file_name_func`, not both.")

    if status_callback is _run_default_wrapper:
        t = _default_wrapper_mgr(dataset)
        status_callback = t.update
    notifier = _status_update_wrapper(status_callback)

    selection_query = clean_linq(selection_query)

    # Normalize how we do the files
    if file_name_func is None:
        if storage_directory is None:
            def file_name(req_id: str, minio_name: str):
                import servicex.utils as sx
                return os.path.join(sx.default_file_cache_name, req_id,
                                    sanitize_filename(minio_name))
            file_name_func = file_name
        else:
            def file_name(req_id: str, minio_name: str):
                assert storage_directory is not None
                return os.path.join(tempfile.gettempdir(), storage_directory,
                                    sanitize_filename(minio_name))
            file_name_func = file_name

    # Build the query, get a request ID back.
    json_query: Dict[str, str] = {
        "did": dataset,
        "selection": selection_query,
        "image": image,
        "result-destination": "object-store",
        "result-format": 'parquet' if data_type == 'parquet' else "root-file",
        "chunk-size": '1000',
        "workers": str(max_workers)
    }

    # Log this
    logging.getLogger(__name__).debug(f'JSON to be sent to servicex: {str(json_query)}')

    # Start the async context manager. We should use only one for the whole app, however,
    # that just isn't going to work here. The advantage is better handling of connections.
    # TODO: Option to pass in the connection pool?
    async with aiohttp.ClientSession() as client:
        done = False
        while not done:
            notifier.reset()
            cached_query = use_cache and _query_is_cached(json_query)
            request_id = await _submit_or_lookup_transform(client, servicex_endpoint,
                                                           use_cache, json_query)

            # If we get a query while we are processing this query, we should
            # wait.

            global _query_locks, _query_locker
            async with _query_locker:
                if request_id not in _query_locks:
                    _query_locks[request_id] = asyncio.Lock()
                q_lock = _query_locks[request_id]

            async with q_lock:
                # We have this in memory - so return it!
                global _data_cache
                result = _data_cache.get(request_id, None)
                if result is not None:
                    return result.o
                else:
                    # Run this as we do not have it in the cache
                    retry = False
                    r = None
                    try:
                        retry, r = await get_data_cache_calc(request_id,
                                                             client,
                                                             servicex_endpoint,
                                                             notifier,
                                                             data_type,
                                                             file_name_func,
                                                             redownload_files)
                    except ServiceX_Exception as sx_e:
                        if cached_query and "failed to transform" in str(sx_e):
                            retry = True
                        else:
                            raise

                    if not retry:
                        assert r is not None, 'Internal error'
                        result = weak_holder(r)
                        _data_cache[request_id] = result
                        return r
                    else:
                        # Retry - so force us not to use the cache
                        use_cache = False

    assert False, 'Internal programming error - should not have gotten here!'


get_data = make_sync(get_data_async)
