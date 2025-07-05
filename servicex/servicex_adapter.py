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
import time
import datetime
from typing import Optional, Dict, List
from dataclasses import dataclass

import httpx
from httpx import AsyncClient, Response
from json import JSONDecodeError
from httpx_retries import RetryTransport, Retry
from google.auth import jwt
from tenacity import (
    AsyncRetrying,
    stop_after_attempt,
    wait_fixed,
    retry_if_not_exception_type,
)

from servicex.models import (
    TransformRequest,
    TransformStatus,
    CachedDataset,
    ServiceXInfo,
)


class AuthorizationError(BaseException):
    pass


@dataclass
class ServiceXFile:
    created_at: datetime.datetime
    filename: str
    total_bytes: int


async def _extract_message(r: Response):
    try:
        o = r.json()
        error_message = o.get("message", str(r))
    except JSONDecodeError:
        error_message = r.text
    return error_message


class ServiceXAdapter:
    def __init__(self, url: str, refresh_token: Optional[str] = None):
        self.url = url
        self.refresh_token = refresh_token
        self.token = None

        # interact with _servicex_info via get_servicex_info
        self._servicex_info: Optional[ServiceXInfo] = None
        self._sample_title_limit: Optional[int] = None

    async def _get_token(self):
        url = f"{self.url}/token/refresh"
        headers = {"Authorization": f"Bearer {self.refresh_token}"}
        async with AsyncClient() as client:
            r = await client.post(url, headers=headers, json=None)
            if r.status_code == 200:
                o = r.json()
                self.token = o["access_token"]
            else:
                raise AuthorizationError(
                    f"ServiceX access token request rejected [{r.status_code} {r.reason_phrase}]"
                )

    @staticmethod
    def _get_bearer_token_file():
        bearer_token_file = os.environ.get("BEARER_TOKEN_FILE")
        bearer_token = None
        if bearer_token_file:
            with open(bearer_token_file, "r") as f:
                bearer_token = f.read().strip()
        return bearer_token

    @staticmethod
    def _get_token_expiration(token) -> int:
        decoded_token = jwt.decode(token, verify=False)
        if "exp" not in decoded_token:
            raise RuntimeError(
                "Authentication token does not have expiration set. "
                f"Token data: {decoded_token}"
            )
        return decoded_token["exp"]

    async def _get_authorization(self, force_reauth: bool = False) -> Dict[str, str]:
        now = time.time()
        if (
            self.token
            and self._get_token_expiration(self.token) - now > 60
            and not force_reauth
        ):
            # if less than one minute validity, renew
            return {"Authorization": f"Bearer {self.token}"}
        else:
            bearer_token = self._get_bearer_token_file()

            if bearer_token:
                self.token = bearer_token
            if not bearer_token and not self.refresh_token:
                return {}

            if (
                not self.token
                or force_reauth
                or self._get_token_expiration(self.token) - now < 60
            ):
                await self._get_token()
            return {"Authorization": f"Bearer {self.token}"}

    async def get_servicex_info(self) -> ServiceXInfo:
        if self._servicex_info:
            return self._servicex_info

        headers = await self._get_authorization()
        retry_options = Retry(total=3, backoff_factor=10)
        async with AsyncClient(transport=RetryTransport(retry=retry_options)) as client:
            r = await client.get(url=f"{self.url}/servicex", headers=headers)
            if r.status_code == 401:
                raise AuthorizationError(
                    f"Not authorized to access serviceX at {self.url}"
                )
            elif r.status_code > 400:
                error_message = await _extract_message(r)
                raise RuntimeError(
                    "ServiceX WebAPI Error during transformation "
                    f"submission: {r.status_code} - {error_message}"
                )
            servicex_info = r.json()
            self._servicex_info = ServiceXInfo(**servicex_info)
            return self._servicex_info

    async def get_servicex_capabilities(self) -> List[str]:
        return (await self.get_servicex_info()).capabilities

    async def get_servicex_sample_title_limit(self) -> Optional[int]:
        # check if the capability is defined
        capabilities = await self.get_servicex_capabilities()
        for capability in capabilities:
            if capability.startswith("long_sample_titles_"):
                try:
                    # hope capability is of the form long_sample_titles_NNNN
                    return int(capability[19:])
                except ValueError:
                    raise RuntimeError(
                        "Unable to determine allowed sample title length\n"
                        f"Server capability is: {capability}"
                    )

        return None

    async def get_transforms(self) -> List[TransformStatus]:
        headers = await self._get_authorization()
        retry_options = Retry(total=3, backoff_factor=10)
        async with AsyncClient(transport=RetryTransport(retry=retry_options)) as client:
            r = await client.get(
                url=f"{self.url}/servicex/transformation", headers=headers
            )
            if r.status_code == 401:
                raise AuthorizationError(
                    f"Not authorized to access serviceX at {self.url}"
                )
            elif r.status_code > 400:
                error_message = await _extract_message(r)
                raise RuntimeError(
                    "ServiceX WebAPI Error during transformation "
                    f"status retrieval: {r.status_code} - {error_message}"
                )
            o = r.json()
            statuses = [TransformStatus(**status) for status in o["requests"]]
            return statuses

    def get_code_generators(self):
        with httpx.Client() as client:
            r = client.get(url=f"{self.url}/multiple-codegen-list")

            if r.status_code == 403:
                raise AuthorizationError(
                    f"Not authorized to access serviceX at {self.url}"
                )
            return r.json()

    async def get_datasets(
        self, did_finder=None, show_deleted=False
    ) -> List[CachedDataset]:
        headers = await self._get_authorization()
        params = {"did-finder": did_finder} if did_finder else {}
        if show_deleted:
            params["show-deleted"] = True

        async with AsyncClient() as session:
            r = await session.get(
                headers=headers, url=f"{self.url}/servicex/datasets", params=params
            )
            if r.status_code == 403:
                raise AuthorizationError(
                    f"Not authorized to access serviceX at {self.url}"
                )
            elif r.status_code != 200:
                msg = await _extract_message(r)
                raise RuntimeError(f"Failed to get datasets: {r.status_code} - {msg}")

            result = r.json()

            datasets = [CachedDataset(**d) for d in result["datasets"]]
            return datasets

    async def get_dataset(self, dataset_id=None) -> CachedDataset:
        headers = await self._get_authorization()
        path_template = "/servicex/datasets/{dataset_id}"
        url = self.url + path_template.format(dataset_id=dataset_id)
        async with AsyncClient() as session:
            r = await session.get(headers=headers, url=url)
            if r.status_code == 403:
                raise AuthorizationError(
                    f"Not authorized to access serviceX at {self.url}"
                )
            elif r.status_code == 404:
                raise ValueError(f"Dataset {dataset_id} not found")
            elif r.status_code != 200:
                msg = await _extract_message(r)
                raise RuntimeError(f"Failed to get dataset {dataset_id} - {msg}")
            result = r.json()

        dataset = CachedDataset(**result)
        return dataset

    async def delete_dataset(self, dataset_id=None) -> bool:
        headers = await self._get_authorization()
        path_template = "/servicex/datasets/{dataset_id}"
        url = self.url + path_template.format(dataset_id=dataset_id)

        async with AsyncClient() as session:
            r = await session.delete(headers=headers, url=url)
            if r.status_code == 403:
                raise AuthorizationError(
                    f"Not authorized to access serviceX at {self.url}"
                )
            elif r.status_code == 404:
                raise ValueError(f"Dataset {dataset_id} not found")
            elif r.status_code != 200:
                msg = await _extract_message(r)
                raise RuntimeError(f"Failed to delete dataset {dataset_id} - {msg}")
            result = r.json()
            return result["stale"]

    async def delete_transform(self, transform_id=None):
        headers = await self._get_authorization()
        path_template = f"/servicex/transformation/{transform_id}"
        url = self.url + path_template.format(transform_id=transform_id)

        async with AsyncClient() as session:
            r = await session.delete(headers=headers, url=url)
            if r.status_code == 403:
                raise AuthorizationError(
                    f"Not authorized to access serviceX at {self.url}"
                )
            elif r.status_code == 404:
                raise ValueError(f"Transform {transform_id} not found")
            elif r.status_code != 200:
                msg = await _extract_message(r)
                raise RuntimeError(f"Failed to delete transform {transform_id} - {msg}")

    async def get_transformation_results(
        self, request_id: str, later_than: Optional[datetime.datetime] = None
    ):
        if (
            "poll_local_transformation_results"
            not in await self.get_servicex_capabilities()
        ):
            raise ValueError("ServiceX capabilities not found")

        headers = await self._get_authorization()
        url = self.url + f"/servicex/transformation/{request_id}/results"
        params = {}
        if later_than:
            params["later_than"] = later_than.isoformat()

        async with AsyncClient() as session:
            r = await session.get(headers=headers, url=url, params=params)
            if r.status_code == 403:
                raise AuthorizationError(
                    f"Not authorized to access serviceX at {self.url}"
                )

            if r.status_code == 404:
                raise ValueError(f"Request {request_id} not found")

            if r.status_code != 200:
                msg = await _extract_message(r)
                raise RuntimeError(f"Failed with message: {msg}")

            data = r.json()
            response = list()
            for result in data.get("results", []):
                if result["transform_status"] == "success":
                    _file = ServiceXFile(
                        filename=result["s3-object-name"],
                        created_at=datetime.datetime.fromisoformat(
                            result["created_at"]
                        ).replace(tzinfo=datetime.timezone.utc),
                        total_bytes=result["total-bytes"],
                    )
                    response.append(_file)
            return response

    async def cancel_transform(self, transform_id=None):
        headers = await self._get_authorization()
        path_template = f"/servicex/transformation/{transform_id}/cancel"
        url = self.url + path_template.format(transform_id=transform_id)

        async with AsyncClient() as session:
            r = await session.get(headers=headers, url=url)
            if r.status_code == 403:
                raise AuthorizationError(
                    f"Not authorized to access serviceX at {self.url}"
                )
            elif r.status_code == 404:
                raise ValueError(f"Transform {transform_id} not found")
            elif r.status_code != 200:
                msg = await _extract_message(r)
                raise RuntimeError(f"Failed to cancel transform {transform_id} - {msg}")

    async def submit_transform(self, transform_request: TransformRequest) -> str:
        headers = await self._get_authorization()
        retry_options = Retry(total=3, backoff_factor=30)
        async with AsyncClient(transport=RetryTransport(retry=retry_options)) as client:
            r = await client.post(
                url=f"{self.url}/servicex/transformation",
                headers=headers,
                json=transform_request.model_dump(by_alias=True, exclude_none=True),
            )
            if r.status_code == 401:
                raise AuthorizationError(
                    f"Not authorized to access serviceX at {self.url}"
                )
            elif r.status_code == 400:
                message = await _extract_message(r)
                raise ValueError(f"Invalid transform request: {message}")
            elif r.status_code > 400:
                error_message = await _extract_message(r)
                raise RuntimeError(
                    "ServiceX WebAPI Error during transformation "
                    f"submission: {r.status_code} - {error_message}"
                )
            else:
                o = r.json()
                return o["request_id"]

    async def get_transform_status(self, request_id: str) -> TransformStatus:
        headers = await self._get_authorization()
        retry_options = Retry(total=5, backoff_factor=3)
        async with AsyncClient(transport=RetryTransport(retry=retry_options)) as client:
            try:
                async for attempt in AsyncRetrying(
                    retry=retry_if_not_exception_type(ValueError),
                    stop=stop_after_attempt(3),
                    wait=wait_fixed(3),
                    reraise=True,
                ):
                    with attempt:
                        r = await client.get(
                            url=f"{self.url}/servicex/" f"transformation/{request_id}",
                            headers=headers,
                        )
                        if r.status_code == 401:
                            # perhaps we just ran out of auth validity the last time?
                            # refetch auth then raise an error for retry
                            headers = await self._get_authorization(True)
                            raise AuthorizationError(
                                f"Not authorized to access serviceX at {self.url}"
                            )
                        if r.status_code == 404:
                            raise ValueError(f"Transform ID {request_id} not found")
                        elif r.status_code > 400:
                            error_message = await _extract_message(r)
                            raise RuntimeError(
                                "ServiceX WebAPI Error during transformation: "
                                f"{r.status_code} - {error_message}"
                            )
                        o = r.json()
                        return TransformStatus(**o)
            except RuntimeError as e:
                raise RuntimeError(
                    "ServiceX WebAPI Error " f"while getting transform status: {e}"
                )
        raise RuntimeError(
            "ServiceX WebAPI: unable to retrieve transform status"
        )  # pragma: no cover
