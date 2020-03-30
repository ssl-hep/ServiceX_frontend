# Main front end interface
import asyncio
from concurrent.futures import ThreadPoolExecutor
import os
import tempfile
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union, Callable
import urllib

import aiohttp
import awkward
from minio import Minio, ResponseError
import numpy as np
import pandas as pd
from retry import retry
import uproot


# Number of seconds to wait between polling servicex for the status of a transform job
# while waiting for it to finish.
servicex_status_poll_time = 5.0


class ServiceX_Exception(Exception):
    'Raised when something has gone wrong in the ServiceX remote service'
    def __init__(self, msg):
        super().__init__(self, msg)


class ServiceXFrontEndException(Exception):
    'Raised to indicate an API error in use of the servicex library'
    def __init__(self, msg):
        super().__init__(self, msg)


async def _get_transform_status(client: aiohttp.ClientSession, endpoint: str,
                                request_id: str) -> Tuple[Optional[int], int]:
    '''
    Internal routine that queries for the current stat of things. We expect the following things
    to come back:
        - files-processed
        - files-remaining
        - files-skipped
        - request-id
        - stats

    Arguments:
        endpoint            Web API address where servicex lives
        request_id          The id of the request to check up on

    Returns
        files_remaining     How many files remain to be processed. None if the number has not yet
                            been determined
        file_processed      How many files have been successfully processed by the system.
    '''
    async with client.get(f'{endpoint}/transformation/{request_id}/status') as response:
        if response.status != 200:
            raise ServiceX_Exception(f'Unable to get transformation status '
                                     f' - http error {response.status}')
        info = await response.json()
        files_remaining = None \
            if (('files-remaining' not in info) or (info['files-remaining'] is None)) \
            else int(info['files-remaining'])
        files_processed = int(info['files-processed'])
        return files_remaining, files_processed


def santize_filename(fname: str):
    'No matter the string given, make it an acceptable filename'
    return fname.replace('*', '_') \
                .replace(';', '_') \
                .replace(':', '_')


# Threadpool on which downloads occur. This is because the current minio library
# uses blocking http requests, so we can't use asyncio to interleave them.
_download_executor = ThreadPoolExecutor(max_workers=5)


def _download_file(minio_client: Minio, request_id: str, bucket_fname: str,
                   file_name_func: Callable[[str, str], str]) -> str:
    '''
    Download a single file to a local temp file from the minio object store, and return
    its location.
    '''
    local_filepath = file_name_func(request_id, bucket_fname)
    dir = os.path.dirname(local_filepath)
    if not os.path.exists(dir):
        os.mkdir(dir)
    minio_client.fget_object(request_id, bucket_fname, local_filepath)

    return local_filepath


@retry(delay=1, tries=10, exceptions=ResponseError)
def protected_list_objects(client: Minio, request_id: str):
    '''
    Return a list of objects in a minio bucket. We've seen some failures here in
    the real world, so protect this with a retry.
    '''
    return client.list_objects(request_id)


async def _post_process_data(data_type: str, filepath: str):
    '''
    Post-process the data and return the "appropriate" type
    '''
    # Load it into uproot and get the first and only key out of it.
    # Make sure to release the lock on the file once we've extracted
    # the data!
    f_in = uproot.open(filepath)
    try:
        r = f_in[f_in.keys()[0]]
        if data_type == 'pandas':
            return r.pandas.df()
        elif data_type == 'awkward':
            return r.arrays()
        elif data_type == 'root-file':
            return filepath
        else:
            raise ServiceXFrontEndException(f'Internal coding error - {data_type} '
                                            'should not be known.')
    finally:
        f_in._context.source.close()


async def _download_new_files(files_queued: Iterable[str], end_point: str,
                              request_id: str,
                              data_type: str,
                              file_name_func: Callable[[str, str], str]) -> Dict[str, Any]:
    '''
    Look at the minio bucket to see if there are any new file items written there. If so, then
    trigger a download.

    Arguments:
        files_queued        List of files already downloading or downloaded
        end_point           Where we can reach minio
        request_id          Request we are trying to access and get data for
        data_type           What is the final data type?
        storage_directory   Where we should stash the files. If None, then use temp.

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

    files = list([f.object_name for f in protected_list_objects(minio_client, request_id)])  \
        # type: List[str]
    new_files = [fname for fname in files if fname not in files_queued]

    # Submit in a thread pool so they can run and block concurrently (minio doesn't have
    # an async interface), and then do any post-processing needed.
    futures = {fname: _post_process_data(
        data_type,
        await asyncio.wrap_future(_download_executor.submit(_download_file, minio_client,
                                                            request_id, fname, file_name_func)))
               for fname in new_files}
    return futures


async def get_data_async(selection_query: str, datasets: Union[str, List[str]],
                         servicex_endpoint: str = 'http://localhost:5000/servicex',
                         data_type: str = 'pandas',
                         image: str = 'sslhep/servicex_xaod_cpp_transformer:v0.2',
                         max_workers: int = 20,
                         storage_directory: Optional[str] = None,
                         file_name_func: Callable[[str, str], str] = None) \
        -> Union[pd.DataFrame, Dict[bytes, np.ndarray]]:
    '''
    Return data from a query with data sets.

    Arguments:
        selection_query     `qastle` string that specifies what columnes to extract, how to format
                            them, and how to format them.
        datasets            Dataset or datasets to run the query against.
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
        `async`). It should be used if you plan to make more than one simultanious query
        to the system.
    - The `file_name_func` function takes two arguments, the first is the request-id and
      the second is the full minio object name.
        - The object name may contain ':', ';', and '*' and perhaps other characters that
          are not allowed on your filesystem.
        - The file path does not need to have been created - `os.mkdir` will be run on the
          filepath.
        - It will almost certianly be the case that the call-back for this function occurs
          on a different thread than you made the initial call to `get_data_async`. Make sure
          your code is thread safe!
        - The filename should be safe in the sense that a ".downloading" can be appended to
          the end of the string without causing any trouble.
    '''
    # Parameter clean up
    if isinstance(datasets, str):
        datasets = [datasets]
    assert len(datasets) == 1

    if (data_type != 'pandas') and (data_type != 'awkward')and (data_type != 'root-file'):
        raise ServiceXFrontEndException('Unknown return type.')

    if (storage_directory is not None) and (file_name_func is not None):
        raise Exception("You may only specify `storage_direcotry` or `file_name_func`, not both.")

    # Normalize how we do the files
    if file_name_func is None:
        if storage_directory is None:
            def file_name(req_id: str, minio_name: str):
                return os.path.join(tempfile.gettempdir(), req_id,
                                    santize_filename(minio_name))
            file_name_func = file_name
        else:
            def file_name(req_id: str, minio_name: str):
                return os.path.join(tempfile.gettempdir(), storage_directory,
                                    santize_filename(minio_name))
            file_name_func = file_name

    # Build the query, get a request ID back.
    json_query = {
        "did": datasets[0],
        "selection": selection_query,
        "image": image,
        "result-destination": "object-store",
        "result-format": "root-file",
        "chunk-size": 1000,
        "workers": max_workers
    }

    # Start the async context manager. We should use only one for the whole app, however,
    # that just isn't going to work here. The advantage is better handling of connections.
    # TODO: Option to pass in the connectino pool?
    async with aiohttp.ClientSession() as client:
        async with client.post(f'{servicex_endpoint}/transformation', json=json_query) as response:
            # TODO: Make sure to throw the correct type of exception
            r = await response.json()
            if response.status != 200:
                raise ServiceX_Exception('ServiceX rejected the transformation request: '
                                         f'({response.status}){r}')
            request_id = r["request_id"]

        # Sit here waiting for the results to come in. In case there are missing items
        # in the minio stream, we will avoid counting that. That should be an explicit error taken
        # care of further on down in the code.
        done = False
        files_downloading = {}
        last_files_processed = 0
        while not done:
            await asyncio.sleep(servicex_status_poll_time)
            files_remaining, files_processed = await _get_transform_status(client,
                                                                           servicex_endpoint,
                                                                           request_id)
            if files_processed != last_files_processed:
                new_downloads = await _download_new_files(files_downloading.keys(),
                                                          servicex_endpoint, request_id,
                                                          data_type, file_name_func)
                files_downloading.update(new_downloads)
                last_files_processed = files_processed

            done = (files_remaining is not None) and files_remaining == 0

        # Now, wait for all of them to complete so we can stich the files together.
        all_files = await asyncio.gather(*files_downloading.values())

        # return the result
        assert len(all_files) > 0

        if data_type == 'root-file':
            return all_files

        # We need to shift the files to another format.
        if len(all_files) == 1:
            return all_files[0]
        else:
            if data_type == 'pandas':
                r = pd.concat(all_files)
                assert isinstance(r, pd.DataFrame)
                return r
            elif data_type == 'awkward':
                col_names = all_files[0].keys()
                return {c: awkward.concatenate([ar[c] for ar in all_files]) for c in col_names}
            else:
                raise ServiceXFrontEndException(f'Internal programming error - {data_type} should '
                                                'not be unknown.')


def get_data(selection_query: str, datasets: Union[str, List[str]],
             servicex_endpoint: str = 'http://localhost:5000/servicex',
             data_type: str = 'pandas',
             image: str = 'sslhep/servicex_xaod_cpp_transformer:v0.2',
             max_workers: int = 20) \
        -> Union[pd.DataFrame, Dict[bytes, np.ndarray]]:
    '''
    Return data from a query with data sets.

    Arguments:
        selection_query     `qastle` string that specifies what columnes to extract, how to format
                            them, and how to format them.
        datasets            Dataset or datasets to run the query against.
        service_endpoint    The URL where the instance of ServivceX we are querying lives
        data_type           How should the data come back? 'pandas' and 'awkward' are the only
                            legal values. Defaults to 'pandas'
        image               ServiceX image that should run this.
        max_workers         Max number of workers that will run to process this request.

    Returns:
        df                  Pandas DataFrame that contains the resulting flat data, or an awkward
                            array. Everything is in memory.

    ## Notes

        - the `max_workers` parameter is currently translated into the actual number of workers
          to process this request. As the `ServiceX` back-end evolves this will be come the max
          number of workers.
    '''
    loop = asyncio.get_event_loop()
    if not loop.is_running():
        r = get_data_async(selection_query, datasets, servicex_endpoint, image=image,
                           max_workers=max_workers)
        return loop.run_until_complete(r)
    else:
        def get_data_wrapper(*args, **kwargs):
            # New thread - get the loop.
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            assert not loop.is_running()
            try:
                return loop.run_until_complete(get_data_async(*args, **kwargs))
            finally:
                pass

        exector = ThreadPoolExecutor(max_workers=1)
        future = exector.submit(get_data_wrapper, selection_query, datasets,
                                servicex_endpoint, image=image,
                                max_workers=max_workers)

        return future.result()
