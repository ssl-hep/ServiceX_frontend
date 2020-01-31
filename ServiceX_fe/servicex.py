# Main frontend interface
import pandas as pd
from typing import Union, List, Tuple, Optional, Dict, Any, Iterable
import aiohttp
import asyncio


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
        info = await response.json()
        files_remaining = None if (('files-remaining' not in info) or
                                   (info['files-remaining'] is None)) \
            else int(info['files-remaining'])
        files_processed = int(info['files-processed'])
        return files_remaining, files_processed


async def _download_new_files(files_queued: Iterable[str], endpoint: str,
                              request_id: str) -> Dict[str, Any]:
    return {}


async def get_data(selection_query: str, datasets: Union[str, List[str]],
                   servicex_endpoint: str = 'http://localhost:5000/servicex') -> pd.DataFrame:
    '''
    Return data from a query with data sets

    Arguments:
        selection_query     `qastle` string that specifies what columnes to extract, how to format
                            them, and how to format them.
        datasets            Dataset or datasets to run the query against.
        service_endpoint    The URL where the instance of ServivceX we are querying lives

    Returns:
        df                  Pandas DataFrame that contains the resulting flat data.
    '''
    if isinstance(datasets, str):
        datasets = [datasets]
    assert len(datasets) == 1

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
            await asyncio.sleep(5.0)
            files_remaining, files_processed = await _get_transform_status(client,
                                                                           servicex_endpoint,
                                                                           request_id)
            if files_processed != last_files_processed:
                new_downloads = await _download_new_files(files_downloading.keys(),
                                                          servicex_endpoint, request_id)
                files_downloading.update(new_downloads)
                last_files_processed = files_processed

            done = (files_remaining is not None) and files_remaining == 0

        # Now, wait for all of them to complete so we can stich the files together.

        # return the result
        return pd.DataFrame()
