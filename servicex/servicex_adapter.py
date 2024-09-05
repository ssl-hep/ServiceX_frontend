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
import os
from datetime import datetime
from typing import Optional, Dict, List

import httpx
from aiohttp_retry import RetryClient, ExponentialRetry
from google.auth import jwt

from servicex.models import TransformRequest, TransformStatus


class AuthorizationError(BaseException):
    pass


class ServiceXAdapter:
    def __init__(self, url: str, refresh_token: Optional[str] = None):
        self.url = url
        self.refresh_token = refresh_token
        self.token = None

    async def _get_token(self):
        url = f"{self.url}/token/refresh"
        headers = {"Authorization": f"Bearer {self.refresh_token}"}
        async with RetryClient() as client:
            async with client.post(url, headers=headers, json=None) as r:
                if r.status == 200:
                    o = await r.json()
                    self.token = o['access_token']
                else:
                    raise AuthorizationError(
                        f"ServiceX access token request rejected: {r.status}"
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
        if self.token:
            return {"Authorization": f"Bearer {self.token}"}
        else:
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
        headers = await self._get_authorization()
        retry_options = ExponentialRetry(attempts=3, start_timeout=10)
        async with RetryClient(retry_options=retry_options) as client:
            async with client.get(url=f"{self.url}/servicex/transformation",
                                  headers=headers) as r:
                if r.status == 401:
                    raise AuthorizationError(f"Not authorized to access serviceX at {self.url}")
                o = await r.json()
                statuses = [TransformStatus(**status) for status in o['requests']]
            return statuses

    def get_code_generators(self):
        with httpx.Client() as client:
            r = client.get(url=f"{self.url}/multiple-codegen-list")

            if r.status_code == 403:
                raise AuthorizationError(
                    f"Not authorized to access serviceX at {self.url}")
            return r.json()

    async def submit_transform(self, transform_request: TransformRequest):
        headers = await self._get_authorization()
        retry_options = ExponentialRetry(attempts=3, start_timeout=30)
        async with RetryClient(retry_options=retry_options) as client:
            async with client.post(url=f"{self.url}/servicex/transformation",
                                   headers=headers,
                                   json=transform_request.model_dump(by_alias=True,
                                                                     exclude_none=True)) as r:
                if r.status == 401:
                    raise AuthorizationError(
                        f"Not authorized to access serviceX at {self.url}")
                elif r.status == 400:
                    o = await r.json()
                    raise ValueError(f"Invalid transform request: {o.get('message')}")
                elif r.status > 400:
                    o = await r.json()
                    error_message = o.get('message', str(r))
                    raise RuntimeError("ServiceX WebAPI Error during transformation "
                                       f"submission: {r.status} - {error_message}")
                else:
                    o = await r.json()
                    return o['request_id']

    async def get_transform_status(self, request_id: str) -> TransformStatus:
        headers = await self._get_authorization()
        retry_options = ExponentialRetry(attempts=5, start_timeout=10)
        async with RetryClient(retry_options=retry_options) as client:
            async with client.get(url=f"{self.url}/servicex/transformation/{request_id}",
                                  headers=headers) as r:
                if r.status == 401:
                    raise AuthorizationError(f"Not authorized to access serviceX at {self.url}")
                if r.status == 404:
                    raise ValueError(f"Transform ID {request_id} not found")
                o = await r.json()
                return TransformStatus(**o)
