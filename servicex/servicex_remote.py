import asyncio
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Optional, Tuple, List, Dict

import aiohttp
from minio import Minio, ResponseError
from retry import retry

from .utils import ServiceXException


# Low level routines for interacting with a ServiceX instance via the WebAPI

async def _get_transform_status(client: aiohttp.ClientSession, endpoint: str,
                                request_id: str) -> Tuple[Optional[int], int, Optional[int]]:
    '''
    Internal routine that queries for the current stat of things. We expect the following things
    to come back:
        - files-processed
        - files-remaining
        - files-skipped
        - request-id
        - stats

    If the transform has already completed, we return data from cache.

    Arguments:

        endpoint            Web API address where servicex lives
        request_id         The id of the request to check up on

    Returns:

        files_remaining     How many files remain to be processed. None if the number has not yet
                            been determined
        files_processed     How many files have been successfully processed by the system.
        files_failed        Number of files that were skipped
    '''
    # Make the actual query
    async with client.get(f'{endpoint}/transformation/{request_id}/status') as response:
        if response.status != 200:
            raise ServiceXException(f'Unable to get transformation status '
                                     f' - http error {response.status}')
        info = await response.json()
        files_remaining = None \
            if (('files-remaining' not in info) or (info['files-remaining'] is None)) \
            else int(info['files-remaining'])
        files_failed = None \
            if (('files-skipped' not in info) or (info['files-skipped'] is None)) \
            else int(info['files-skipped'])
        files_processed = int(info['files-processed'])
        return files_remaining, files_processed, files_failed


# Threadpool on which downloads occur. This is because the current minio library
# uses blocking http requests, so we can't use asyncio to interleave them.
_download_executor = ThreadPoolExecutor(max_workers=5)


async def _download_file(minio_client: Minio, request_id: str, bucket_fname: str,
                         output_file: Path) -> None:
    '''
    Download a single file to a local temp file.

    Arguments:
        minio_client        Open and authenticated minio client
        request_id          The id of the request we are going after
        bucket_fname        The fname of the bucket
        output_file         Filename where we should write this file.

    Notes:
        - Download to a temp file that is renamed at the end so that a partially
          downloaded file is not mistaken as a full one
        - Run with async, despite minio not being async.
    '''
    # Make sure the output directory exists
    output_file.parent.mkdir(parents=True, exist_ok=True)

    # We are going to build a temp file, and download it from there.
    def do_copy() -> None:
        temp_file = output_file.parent.with_name(f'{output_file.name}.temp')
        try:
            minio_client.fget_object(request_id, bucket_fname, str(temp_file))
            temp_file.rename(output_file)
        except Exception as e:
            raise ServiceXException(f'Failed to copy minio bucket {bucket_fname} from request '
                                     f'{request_id} to {output_file}') from e

    # If the file exists, we don't need to do anything.
    if output_file.exists():
        return

    # Do the copy, which might take a while, on a separate thread.
    return await asyncio.wrap_future(_download_executor.submit(do_copy))


@retry(delay=1, tries=10, exceptions=ResponseError)
def _protected_list_objects(client: Minio, request_id: str) -> List[str]:
    '''
    Returns the list of files that are Minio has stored in this particular
    bucket as an iterable.

    Arguments:
        client          The authenticated Minio client object
        request_id      The index we can look up.

    Returns:
        Iterable[str]   List of the filenames in this key in minio

    Note:
        Despite being a http request, this is a sync request and will hang while the
        request is made.
    '''
    return [f.object_name for f in client.list_objects(request_id)]


class _result_object_list:
    '''
    Will poll the minio bucket each time it's event is triggered for a particular
    request id. It will return an async stream of new files until it is shut off.
    '''
    def __init__(self, client: Minio, request_id: str):
        self._client = client
        self._req_id = request_id
        self._event = asyncio.Event()
        self._trigger_done = False

    def trigger_scan(self):
        'Trigger a scan of the minio to look for new items in the bucket'
        self._event.set()

    def shutdown(self):
        '''
        Initiate shutdown - a last check is performed and any new files found are
        routed.
        '''
        self._trigger_done = True
        self._event.set()

    async def files(self):
        '''
        Returns an awaitable sequence of files that come back from Minio. Each file
        is only returned once (as you would expect). Use `trigger_scan` to trigger
        a polling of `minio`.
        '''
        seen = []
        done = False
        done_counter = 1
        while not done:
            if not self._trigger_done:
                await self._event.wait()
                self._event.clear()
            if not done:
                files = _protected_list_objects(self._client, self._req_id)
                for f in files:
                    if f not in seen:
                        seen.append(f)
                        yield f

            # Make sure to go around one last time to pick up any stragglers.
            if done_counter == 0:
                done = True
            if self._trigger_done:
                done_counter -= 1


async def _submit_query(client: aiohttp.ClientSession,
                        servicex_endpoint: str,
                        json_query: Dict[str, str]) -> str:
    '''
    Submit a query to ServiceX, and return a request ID
    '''
    async with client.post(f'{servicex_endpoint}/transformation', json=json_query) as response:
        r = await response.json()
        if response.status != 200:
            raise ServiceXException('ServiceX rejected the transformation request: '
                                     f'({response.status}){r}')
        req_id = r["request_id"]

        return req_id
