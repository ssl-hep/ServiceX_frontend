# Copyright (c) 2019, IRIS-HEP
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# * Redistributions of source code must retain the above copyright notice, this
#   list of conditions and the following disclaimer.
#
# * Redistributions in binary form must reproduce the above copyright notice,
#   this list of conditions and the following disclaimer in the documentation
#   and/or other materials provided with the distribution.
#
# * Neither the name of the copyright holder nor the names of its
#   contributors may be used to endorse or promote products derived from
#   this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
import asyncio
from concurrent.futures.thread import ThreadPoolExecutor
from pathlib import Path
from typing import AsyncIterator, Any

import backoff
from backoff import on_exception
from minio import Minio, ResponseError

from .utils import ServiceXException


class MinioAdaptor:
    def __init__(self, mino_endpoint, access_key='miniouser', secretkey='leftfoot1'):
        self._endpoint = mino_endpoint
        self._access_key = access_key
        self._secretkey = secretkey

        self._client = Minio(self._endpoint,
                             access_key=self._access_key,
                             secret_key=self._secretkey,
                             secure=False)

        # Threadpool on which downloads occur. This is because the current minio library
        # uses blocking http requests, so we can't use asyncio to interleave them.
        self._download_executor = ThreadPoolExecutor(max_workers=5)

    @on_exception(backoff.constant, ResponseError, interval=0.1)
    def get_files(self, request_id):
        return [f.object_name for f in self._client.list_objects(request_id)]

    async def download_file(self,
                            request_id: str,
                            bucket_fname: str,
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
                self._client.fget_object(request_id, bucket_fname, str(temp_file))
                temp_file.rename(output_file)
            except Exception as e:
                raise ServiceXException(
                    f'Failed to copy minio bucket {bucket_fname} from request '
                    f'{request_id} to {output_file}') from e

        # If the file exists, we don't need to do anything.
        if output_file.exists():
            return

        # Do the copy, which might take a while, on a separate thread.
        return await asyncio.wrap_future(self._download_executor.submit(do_copy))


async def find_new_bucket_files(adaptor: MinioAdaptor,
                                request_id: str,
                                update: AsyncIterator[Any]) -> AsyncIterator[str]:
    '''
    Each time we get something from the async iterator, check to see if
    there are any files present.
    '''
    seen = []
    async for _ in update:
        # Sadly, this is blocking, and so may hold things up
        files = adaptor.get_files(request_id)

        # If there are new files, pass them on
        for f in files:
            if f not in seen:
                seen.append(f)
                yield f


class ResultObjectList:
    """
    Will poll the minio bucket each time it's event is triggered for a particular
    request id. It will return an async stream of new files until it is shut off.
    """
    def __init__(self, adaptor: MinioAdaptor, request_id: str):
        self._adaptor = adaptor
        self._req_id = request_id
        self._event = asyncio.Event()
        self._trigger_done = False

    def trigger_scan(self):
        'Trigger a scan of the minio to look for new items in the bucket'
        self._event.set()

    def shutdown(self):
        """
        Initiate shutdown - a last check is performed and any new files found are
        routed.
        """
        self._trigger_done = True
        self._event.set()

    async def files(self):
        """
        Returns an awaitable sequence of files that come back from Minio. Each file
        is only returned once (as you would expect). Use `trigger_scan` to trigger
        a polling of `minio`.
        """
        seen = []
        done = False
        done_counter = 1
        while not done:
            if not self._trigger_done:
                await self._event.wait()
                self._event.clear()
            if not done:
                files = self._adaptor.get_files(self._req_id)
                for f in files:
                    if f not in seen:
                        seen.append(f)
                        yield f

            # Make sure to go around one last time to pick up any stragglers.
            if done_counter <= 0:
                done = True
            if self._trigger_done:
                done_counter -= 1
