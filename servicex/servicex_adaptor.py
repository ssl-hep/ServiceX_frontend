import asyncio
from datetime import datetime
from typing import AsyncIterator, Dict, Optional, Tuple
import logging

import aiohttp
from google.auth import jwt

from .utils import (
    ServiceXException,
    ServiceXFailedFileTransform,
    ServiceXFatalTransformException,
    ServiceXUnknownRequestID,
    TransformTuple,
)

# Number of seconds to wait between polling servicex for the status of a transform job
# while waiting for it to finish.
servicex_status_poll_time = 5.0


# Low level routines for interacting with a ServiceX instance via the WebAPI
class ServiceXAdaptor:
    def __init__(self, endpoint, refresh_token=None):
        '''
        Authenticated access to ServiceX
        '''
        self._endpoint = endpoint
        self._token = None
        self._refresh_token = refresh_token

    async def _get_token(self, client: aiohttp.ClientSession):
        url = f'{self._endpoint}/token/refresh'
        headers = {'Authorization': f'Bearer {self._refresh_token}'}
        async with client.post(url, headers=headers, json=None) as response:
            status = response.status
            if status == 200:
                j = await response.json()
                self._token = j['access_token']
            else:
                raise ServiceXException(f'ServiceX access token request rejected: {status}')

    async def _get_authorization(self, client: aiohttp.ClientSession):
        if not self._refresh_token:
            return {}
        now = datetime.utcnow().timestamp()
        if not self._token or jwt.decode(self._token, verify=False)['exp'] - now < 0:
            await self._get_token(client)
        return {'Authorization': f'Bearer {self._token}'}

    async def submit_query(self, client: aiohttp.ClientSession,
                           json_query: Dict[str, str]) -> Dict[str, str]:
        """
        Submit a query to ServiceX, and return a request ID
        """

        headers = await self._get_authorization(client)

        async with client.post(f'{self._endpoint}/servicex/transformation',
                               headers=headers, json=json_query) as response:
            status = response.status
            if status != 200:
                # This was an error at ServiceX, bubble it up so code above us can
                # handle as needed.
                t = await response.text()
                raise ServiceXException('ServiceX rejected the transformation request: '
                                        f'({status}){t}')

            r = await response.json()
            return r

    async def get_query_status(self, client: aiohttp.ClientSession,
                               request_id: str) -> Dict[str, str]:
        '''Returns the full query information from the endpoint.

        Args:
            client (aiohttp.ClientSession): Client session on which to make the request.
            request_id (str): The request id to return the tranform status

        Raises:
            ServiceXException: If we fail to find the information.

        Returns:
            Dict[str, str]: The JSON dictionary of information returned from ServiceX
        '''
        headers = await self._get_authorization(client)

        async with client.get(f'{self._endpoint}/servicex/transformation/{request_id}',
                              headers=headers) as response:
            status = response.status
            if status != 200:
                # This was an error at ServiceX, bubble it up so code above us can
                # handle as needed.
                t = await response.text()
                raise ServiceXException('ServiceX rejected the transformation status fetch: '
                                        f'({status}){t}')

            r = await response.json()
            return r

    async def dump_query_errors(self, client: aiohttp.ClientSession,
                                request_id: str):
        '''Dumps to the logging system any error messages we find from ServiceX.

        Args:
            client (aiohttp.ClientSession): Client along which to send queries.
            request_id (str): Fetch all errors from there.
        '''

        headers = await self._get_authorization(client)
        async with client.get(f'{self._endpoint}/servicex/transformation/{request_id}/errors',
                              headers=headers) as response:
            status = response.status
            if status != 200:
                t = await response.text()
                if "Request not found" in t:
                    raise ServiceXUnknownRequestID(f'Unable to get errors for request {request_id}'
                                                   f': {status} - {t}')
                else:
                    raise ServiceXException(f'Failed to get request errors for {request_id}: '
                                            f'{status} - {t}')

            # Dump the messages out to the logger if there are any!
            # Keep the output under control - only dump the first 20 errors (see #188)
            errors = (await response.json())["errors"]
            log = logging.getLogger(__name__)
            log.warning(f'Transform {request_id} had {len(errors)} errors:')
            for e in errors[:20]:
                log.warning(f'  Error transforming file: {e["file"]}')
                for ln in e["info"].split('\n'):
                    log.warning(f'  -> {ln}')

    @staticmethod
    def _get_transform_stat(info: Dict[str, str], stat_name: str) -> Optional[int]:
        'Return the info from a servicex status reply, protecting against bad internet returns'
        return None \
            if ((stat_name not in info) or (info[stat_name] is None)) \
            else int(info[stat_name])

    async def get_transform_status(self, client: aiohttp.ClientSession, request_id: str) -> \
            Tuple[Optional[int], int, Optional[int]]:
        """
        Internal routine that queries for the current stat of things. We expect the
        following things to come back:
            - files-processed
            - files-remaining
            - files-skipped
            - request-id
            - stats

        If the transform has already completed, we return data from cache.

        Arguments:

            endpoint            Web API address where servicex lives
            request_id          The id of the request to check up on

        Raises:

            ServiceXException   If the status returns `Fatal`.

        Returns:

            files_remaining     How many files remain to be processed. None if the number
                                has not yet been determined
            files_processed     How many files have been successfully processed
                                by the system.
            files_failed        Number of files that were skipped
        """
        headers = await self._get_authorization(client)

        # Make the actual query
        async with client.get(
                f'{self._endpoint}/servicex/transformation/{request_id}/status',
                headers=headers) as response:
            status = response.status
            if status != 200:
                raise ServiceXUnknownRequestID(f'Unable to get transform status '
                                               f'for request id {request_id}'
                                               f' - http error {status}')
            info = await response.json()
            logging.getLogger(__name__).debug(f'Status response for {request_id}: {info}')

            if 'status' in info and info['status'] == 'Fatal':
                raise ServiceXFatalTransformException(f'Transform status for {request_id}'
                                                      ' is marked "Fatal".')

            files_remaining = self._get_transform_stat(info, 'files-remaining')
            files_failed = self._get_transform_stat(info, 'files-skipped')
            files_processed = self._get_transform_stat(info, 'files-processed')

            assert files_processed is not None

            return files_remaining, files_processed, files_failed


async def transform_status_stream(sa: ServiceXAdaptor, client: aiohttp.ClientSession,
                                  request_id: str) \
        -> AsyncIterator[TransformTuple]:
    '''
    Returns an async stream of `(files-remaining, files_processed, files_failed)` until the
    servicex `request_id` request is finished, against the servicex instance located at
    `sa`.

    Arguments:

        sa                  The servicex low level adaptor
        client              An async http function we can call and use
        request_id          The request id for this request

    Returns:

        remaining, processed, skipped     Returns an async stream triple of the
                                          status numbers. Every time we find something
                                          we send it on.

    Note:
    '''
    done = False
    while not done:
        next_processed = await sa.get_transform_status(client, request_id)
        remaining, _, _ = next_processed
        done = remaining is not None and remaining == 0
        yield next_processed

        if not done:
            await asyncio.sleep(servicex_status_poll_time)


async def trap_servicex_failures(stream: AsyncIterator[TransformTuple]) \
        -> AsyncIterator[TransformTuple]:
    '''
    Looks for any failed files. If it catches one, it will remember it and throw once the stream
    is done. This allows all the files to come down first.
    '''
    async for p in stream:
        remain, processed, did_fail = p
        if did_fail is not None and did_fail != 0:
            raise ServiceXFailedFileTransform(f'ServiceX failed to transform {did_fail} '
                                              f'files - data incomplete (remaining: {remain}, '
                                              f'processed: {processed}).')

        yield p
