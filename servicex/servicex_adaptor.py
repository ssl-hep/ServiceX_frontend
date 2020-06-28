import asyncio
from datetime import datetime
from typing import AsyncIterator, Dict, Optional, Tuple

import aiohttp
from google.auth import jwt

from .utils import (
    ServiceXException,
    ServiceXFailedFileTransform,
    ServiceXUnknownRequestID,
    TransformTuple,
)

# Number of seconds to wait between polling servicex for the status of a transform job
# while waiting for it to finish.
servicex_status_poll_time = 5.0


# Low level routines for interacting with a ServiceX instance via the WebAPI
class ServiceXAdaptor:
    def __init__(self, endpoint, username=None, password=None):
        '''
        Authenticated access to ServiceX
        '''
        self._endpoint = endpoint
        self._username = username
        self._password = password

        self._token = None
        self._refresh_token = None

    async def _login(self, client: aiohttp.ClientSession):
        url = f'{self._endpoint}/login'
        async with client.post(url, json={
            'username': self._username,
            'password': self._password
        }) as response:
            status = response.status
            if status == 200:
                j = await response.json()
                self._token = j['access_token']
                self._refresh_token = j['refresh_token']
            else:
                raise ServiceXException(f'ServiceX login request rejected: {status}')

    async def _get_authorization(self, client: aiohttp.ClientSession):
        if self._username:
            now = datetime.utcnow().timestamp()
            if not self._token or jwt.decode(self._token, verify=False)['exp'] - now < 0:
                await self._login(client)
            return {
                'Authorization': f'Bearer {self._token}'
            }
        else:
            return {}

    async def submit_query(self, client: aiohttp.ClientSession,
                           json_query: Dict[str, str]) -> str:
        """
        Submit a query to ServiceX, and return a request ID
        """

        headers = await self._get_authorization(client)

        async with client.post(f'{self._endpoint}/servicex/transformation',
                               headers=headers, json=json_query) as response:
            r = await response.json()
            status = response.status
            if status != 200:
                # This was an error at ServiceX, bubble it up so code above us can
                # handle as needed.
                raise ServiceXException('ServiceX rejected the transformation request: '
                                        f'({status}){r}')
            req_id = r["request_id"]

            return req_id

    @staticmethod
    def _get_transform_stat(info: Dict[str, str], stat_name: str):
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
            request_id         The id of the request to check up on

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
    '''
    done = False
    last_processed = None
    while not done:
        next_processed = await sa.get_transform_status(client, request_id)
        remaining, _, _ = next_processed
        done = remaining is not None and remaining == 0
        if next_processed != last_processed:
            last_processed = next_processed
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
        _, _, did_fail = p
        if did_fail is not None and did_fail != 0:
            raise ServiceXFailedFileTransform(f'ServiceX failed to transform {did_fail} '
                                              'files - data incomplete.')

        yield p
