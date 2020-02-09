# Main frontend interface
import asyncio
from concurrent.futures import ThreadPoolExecutor
import os
import tempfile
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union
import urllib

import aiohttp
from minio import Minio
import nest_asyncio
import numpy as np
import pandas as pd
import uproot


# Number of seconds to wait between polling servicex for the status of a transform job
# while waiting for it to finish.
servicex_status_poll_time = 5.0


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
            raise BaseException(f'Unable to get transformation status '
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


def _download_file(minio_client: Minio, request_id: str, bucket_fname: str, data_type: str) \
        -> pd.DataFrame:
    '''
    Download a single file to a local temp file from the minio object store
    '''
    local_filename = santize_filename(bucket_fname)
    local_filepath = os.path.join(tempfile.gettempdir(), local_filename)
    # TODO: clean up these temporary files when done?
    minio_client.fget_object(request_id, bucket_fname, local_filepath)

    # Load it into uproot and get the first and only key out of it.
    f_in = uproot.open(local_filepath)
    try:
        r = f_in[f_in.keys()[0]]
        if data_type == 'pandas':
            return r.pandas.df()
        elif data_type == 'awkward':
            return r.arrays()
        else:
            raise BaseException(f'Internal coding error - {data_type} should not be known.')
    finally:
        f_in._context.source.close()


async def _download_new_files(files_queued: Iterable[str], end_point: str,
                              request_id: str,
                              data_type: str) -> Dict[str, Any]:
    '''
    Get the list of files in a minio bucket and download any files we've not already started. We
    queue them up, and return a list of the futures that point to the files when they
    are downloaded.
    '''
    # We need to assume where the minio port is and go from there.
    end_point_parse = urllib.parse.urlparse(end_point)
    minio_endpoint = f'{end_point_parse.hostname}:9000'

    minio_client = Minio(minio_endpoint,
                         access_key='miniouser',
                         secret_key='leftfoot1',
                         secure=False)

    files = list([f.object_name for f in minio_client.list_objects(request_id)])  # type: List[str]
    new_files = [fname for fname in files if fname not in files_queued]

    # Submit in a thread pool so they can run and block concurrently.
    futures = {fname: asyncio.wrap_future(_download_executor.submit(_download_file, minio_client,
                                          request_id, fname, data_type))
               for fname in new_files}
    return futures


async def get_data_async(selection_query: str, datasets: Union[str, List[str]],
                         servicex_endpoint: str = 'http://localhost:5000/servicex',
                         data_type: str = 'pandas') \
        -> Union[pd.DataFrame, Dict[bytes, np.ndarray]]:
    '''
    Return data from a query with data sets

    Arguments:
        selection_query     `qastle` string that specifies what columnes to extract, how to format
                            them, and how to format them.
        datasets            Dataset or datasets to run the query against.
        service_endpoint    The URL where the instance of ServivceX we are querying lives
        data_type           How should the data come back? 'pandas' and 'awkward' are the only
                            legal values. Defaults to 'pandas'

    Returns:
        df                  Pandas DataFrame that contains the resulting flat data, or an awkward
                            array. Everything is in memory.
    '''
    # Parameter clean up
    if isinstance(datasets, str):
        datasets = [datasets]
    assert len(datasets) == 1

    if (data_type != 'pandas') and (data_type != 'awkward'):
        raise BaseException('Unknown return type.')

    # Build the query, get a request ID back.
    image = "sslhep/servicex_xaod_cpp_transformer:v0.2"
    json_query = {
        "did": datasets[0],
        "selection": selection_query,
        "image": image,
        "result-destination": "object-store",
        "result-format": "root-file",
        "chunk-size": 1000,
        "workers": 5
    }

    # Start the async context manager. We should use only one for the whole app, however,
    # that just isn't going to work here. The advantage is better handling of connections.
    # TODO: Option to pass in the connectino pool?
    async with aiohttp.ClientSession() as client:
        async with client.post(f'{servicex_endpoint}/transformation', data=json_query) as response:
            # TODO: Make sure to throw the correct type of exception
            assert response.status == 200
            request_id = (await response.json())["request_id"]

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
                                                          data_type)
                files_downloading.update(new_downloads)
                last_files_processed = files_processed

            done = (files_remaining is not None) and files_remaining == 0

        # Now, wait for all of them to complete so we can stich the files together.
        all_files = await asyncio.gather(*files_downloading.values())

        # return the result
        assert len(all_files) > 0
        if len(all_files) == 1:
            return all_files[0]
        else:
            if data_type == 'pandas':
                r = pd.concat(all_files)
                assert isinstance(r, pd.DataFrame)
                return r
            elif data_type == 'awkward':
                col_names = all_files[0].keys()
                return {c: np.concatenate([ar[c] for ar in all_files]) for c in col_names}
            else:
                raise BaseException(f'Internal programming error - {data_type} should not be'
                                    ' unknown.')


def get_data(selection_query: str, datasets: Union[str, List[str]],
             servicex_endpoint: str = 'http://localhost:5000/servicex',
             data_type: str = 'pandas') -> Union[pd.DataFrame, Dict[bytes, np.ndarray]]:
    '''
    Return data from a query with data sets

    Arguments:
        selection_query     `qastle` string that specifies what columnes to extract, how to format
                            them, and how to format them.
        datasets            Dataset or datasets to run the query against.
        service_endpoint    The URL where the instance of ServivceX we are querying lives
        data_type           How should the data come back? 'pandas' and 'awkward' are the only
                            legal values. Defaults to 'pandas'

    Returns:
        df                  Pandas DataFrame that contains the resulting flat data.
    '''
    nest_asyncio.apply()
    loop = asyncio.get_event_loop()
    return loop.run_until_complete(get_data_async(selection_query, datasets, servicex_endpoint))
