# Copyright (c) 2022, IRIS-HEP
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
import logging
import os
import ssl
import sys
from datetime import datetime
from typing import Optional, Dict, List

import httpcore
import httpx
from google.auth import jwt

from servicex.models import TransformRequest, TransformStatus


class AuthorizationError(BaseException):
    pass


class ServiceXError(BaseException):
    pass


class ServiceXAdapter:
    def __init__(self, url: str, refresh_token: Optional[str] = None,
                 httpx_client: httpx.AsyncClient = None,
                 semaphore: asyncio.Semaphore = None):
        self.logger = logging.getLogger(__name__)

        self.url = url
        self.refresh_token = refresh_token
        self.token = None
        self.client = httpx_client
        assert semaphore
        self.semaphore = semaphore if semaphore is not None else asyncio.Semaphore(5)

    async def _get_token(self):
        async def retry_get_token(url, headers):
            for _ in range(3):
                try:
                    r = await self.client.post(url=url, headers=headers)
                    return r
                except (httpx.ReadTimeout, httpcore.ConnectTimeout,
                        httpx.ConnectTimeout,
                        ssl.SSLWantReadError):
                    self.logger.warning("Timeout getting token - will retry")
            raise AuthorizationError("Failed to get token")

        url = f"{self.url}/token/refresh"
        headers = {"Authorization": f"Bearer {self.refresh_token}"}

        r = await retry_get_token(url, headers)
        if r.status_code == 200:
            self.token = r.json()['access_token']
        else:
            raise AuthorizationError(
                f"ServiceX access token request rejected: {r.status_code}"
            )

    @staticmethod
    def _get_bearer_token_file():
        bearer_token_file = os.environ.get('BEARER_TOKEN_FILE')
        bearer_token = None
        if bearer_token_file:
            with open(bearer_token_file, "r") as f:
                bearer_token = f.read().strip()
        return bearer_token

    async def _get_authorization(self) -> Dict[str, str]:
        bearer_token = self._get_bearer_token_file()

        if bearer_token:
            self.token = bearer_token
        if not bearer_token and not self.refresh_token:
            return {}

        now = datetime.utcnow().timestamp()
        if not self.token or \
                float(jwt.decode(self.token, verify=False)["exp"]) - now < 0:
            await self._get_token()
        return {"Authorization": f"Bearer {self.token}"}

    async def get_transforms(self) -> List[TransformStatus]:
        async with self.semaphore:
            headers = await self._get_authorization()
            r = await self.client.get(url=f"{self.url}/servicex/transformation",
                                      headers=headers)
            if r.status_code == 401:
                raise AuthorizationError(f"Not authorized to access serviceX at {self.url}")

            statuses = [TransformStatus(**status) for status in r.json()['requests']]
        return statuses

    async def get_code_generators(self):
        async with self.semaphore:
            r = await self.client.get(url=f"{self.url}/multiple-codegen-list")

            if r.status_code == 403:
                raise AuthorizationError(
                    f"Not authorized to access serviceX at {self.url}")
        return r.json()

    async def submit_transform(self, transform_request: TransformRequest):
        async def retrying_post(url, headers, json):
            for _ in range(3):
                try:
                    r = await self.client.post(url=url, headers=headers, json=json)
                    return r
                except (httpx.ReadTimeout, httpcore.ConnectTimeout,
                        httpx.ConnectTimeout,
                        ssl.SSLWantReadError):
                    self.logger.warning("Timeout submitting transform - will retry")
            raise ServiceXError("Timed out submitting transform")

        async with self.semaphore:
            headers = await self._get_authorization()
            r = await retrying_post(f"{self.url}/servicex/transformation",
                                    headers,
                                    transform_request.dict(by_alias=True, exclude_none=True))
            if r.status_code == 401:
                raise AuthorizationError(
                    f"Not authorized to access serviceX at {self.url}")
            elif r.status_code == 400:
                raise ValueError(f"Invalid transform request: {r.json()['message']}")
        return r.json()['request_id']

    async def get_transform_status(self, request_id: str) -> TransformStatus:
        async with self.semaphore:
            headers = await self._get_authorization()
            try:
                r = await self.client.get(url=f"{self.url}/servicex/transformation/{request_id}",
                                          headers=headers)
                if r.status_code == 401:
                    raise AuthorizationError(f"Not authorized to access serviceX at {self.url}")
                if r.status_code == 404:
                    raise ValueError(f"Transform ID {request_id} not found")
                status = TransformStatus(**r.json())
                return status
            except (httpx.ReadTimeout, httpcore.ConnectTimeout, ssl.SSLWantReadError) as e:
                self.logger.warning(
                    "Timeout getting transform status - will retry at next poll interval",
                    extra={'requestId': request_id}
                )
            return None
