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
import asyncio
from typing import Optional, Dict, List, Any, TypeVar, Callable, cast
from functools import wraps

from aiohttp import ClientSession
import httpx
from aiohttp_retry import RetryClient, ExponentialRetry, ClientResponse
from aiohttp import ContentTypeError
from google.auth import jwt
from tenacity import (
    AsyncRetrying,
    stop_after_attempt,
    wait_fixed,
    retry_if_not_exception_type,
)

from servicex.models import TransformRequest, TransformStatus, CachedDataset

T = TypeVar('T')

def requires_resource(resource_name: str) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """
    Decorator to check if a specific API resource is available on the server before executing the method.

    Args:
        resource_name: The name of the resource that needs to be available

    Returns:
        A decorator function that wraps around class methods

    Raises:
        ResourceNotAvailableError: If the required resource is not available on the server
    """

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        # Determine if function is async at decoration time (not runtime)
        is_async = asyncio.iscoroutinefunction(func)
        func_name = func.__name__

        # Class-level cache for sync method resources
        sync_cache_key = f'_sync_resources_for_{resource_name}'

        if is_async:
            @wraps(func)
            async def async_wrapper(self, *args: Any, **kwargs: Any) -> T:
                # Get resources and check availability in one operation
                if resource_name not in await self.get_resources():
                    raise ResourceNotAvailableError(
                        f"Resource '{resource_name}' required for '{func_name}' is unavailable"
                    )
                return await func(self, *args, **kwargs)

            return cast(Callable[..., T], async_wrapper)
        else:
            @wraps(func)
            def sync_wrapper(self, *args: Any, **kwargs: Any) -> T:
                # Initialize class-level cache attributes if needed
                cls = self.__class__
                if not hasattr(cls, sync_cache_key):
                    setattr(cls, sync_cache_key, (None, 0))  # (resources, timestamp)

                cache_ttl = getattr(self, '_resources_cache_ttl', 300)
                cached_resources, timestamp = getattr(cls, sync_cache_key)
                current_time = time.time()

                # Check if cache needs refresh
                if cached_resources is None or (current_time - timestamp) >= cache_ttl:
                    loop = asyncio.new_event_loop()
                    try:
                        cached_resources = loop.run_until_complete(self.get_resources())
                        setattr(cls, sync_cache_key, (cached_resources, current_time))
                    finally:
                        loop.close()

                # Check resource availability
                if resource_name not in cached_resources:
                    raise ResourceNotAvailableError(
                        f"Resource '{resource_name}' required for '{func_name}' is unavailable"
                    )

                return func(self, *args, **kwargs)

            return cast(Callable[..., T], sync_wrapper)

    return decorator


class ResourceNotAvailableError(Exception):
    """Exception raised when a required resource is not available on the server."""
    pass

class AuthorizationError(BaseException):
    pass


async def _extract_message(r: ClientResponse):
    try:
        o = await r.json()
        error_message = o.get("message", str(r))
    except ContentTypeError:
        error_message = await r.text()
    return error_message


class ServiceXAdapter:
    def __init__(self, url: str, refresh_token: Optional[str] = None):
        self.url = url
        self.refresh_token = refresh_token
        self.token = None

        self._available_resources: Optional[Dict[str, Any]] = None
        self._resources_last_updated: Optional[float] = None
        self._resources_cache_ttl = 60*5

    async def get_resources(self) -> Dict[str, Any]:
        """
        Fetches the list of available resources from the server.
        Caches the result for 5 minutes to avoid excessive API calls.

        Returns:
            A dictionary of available resources with their properties
        """
        current_time = time.time()

        # Return cached resources if they exist and are not expired
        if (self._available_resources is not None and
                self._resources_last_updated is not None and
                current_time - self._resources_last_updated < self._resources_cache_ttl):
            return self._available_resources

        # Fetch resources from server
        headers = await self._get_authorization()
        async with ClientSession() as session:
            async with session.get(
                    headers=headers, url=f"{self.url}/servicex/resources"
            ) as r:
                if r.status == 403:
                    raise AuthorizationError(
                        f"Not authorized to access serviceX at {self.url}"
                    )
                elif r.status != 200:
                    msg = await _extract_message(r)
                    raise RuntimeError(f"Failed to get resources: {r.status} - {msg}")

                self._available_resources = await r.json()
                self._resources_last_updated = current_time

                return self._available_resources

    async def _get_token(self):
        url = f"{self.url}/token/refresh"
        headers = {"Authorization": f"Bearer {self.refresh_token}"}
        async with RetryClient() as client:
            async with client.post(url, headers=headers, json=None) as r:
                if r.status == 200:
                    o = await r.json()
                    self.token = o["access_token"]
                else:
                    raise AuthorizationError(
                        f"ServiceX access token request rejected [{r.status} {r.reason}]"
                    )

    @staticmethod
    def _get_bearer_token_file():
        bearer_token_file = os.environ.get("BEARER_TOKEN_FILE")
        bearer_token = None
        if bearer_token_file:
            with open(bearer_token_file, "r") as f:
                bearer_token = f.read().strip()
        return bearer_token

    async def _get_authorization(self, force_reauth: bool = False) -> Dict[str, str]:
        now = time.time()
        if (
            self.token
            and jwt.decode(self.token, verify=False)["exp"] - now > 60
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
                or float(jwt.decode(self.token, verify=False)["exp"]) - now < 60
            ):
                await self._get_token()
            return {"Authorization": f"Bearer {self.token}"}

    async def get_transforms(self) -> List[TransformStatus]:
        headers = await self._get_authorization()
        retry_options = ExponentialRetry(attempts=3, start_timeout=10)
        async with RetryClient(retry_options=retry_options) as client:
            async with client.get(
                url=f"{self.url}/servicex/transformation", headers=headers
            ) as r:
                if r.status == 401:
                    raise AuthorizationError(
                        f"Not authorized to access serviceX at {self.url}"
                    )
                elif r.status > 400:
                    error_message = await _extract_message(r)
                    raise RuntimeError(
                        "ServiceX WebAPI Error during transformation "
                        f"submission: {r.status} - {error_message}"
                    )
                o = await r.json()
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

        async with ClientSession() as session:
            async with session.get(
                headers=headers, url=f"{self.url}/servicex/datasets", params=params
            ) as r:

                if r.status == 403:
                    raise AuthorizationError(
                        f"Not authorized to access serviceX at {self.url}"
                    )
                elif r.status != 200:
                    msg = await _extract_message(r)
                    raise RuntimeError(f"Failed to get datasets: {r.status} - {msg}")

                result = await r.json()

            datasets = [CachedDataset(**d) for d in result["datasets"]]
            return datasets

    async def get_dataset(self, dataset_id=None) -> CachedDataset:
        headers = await self._get_authorization()
        path_template = "/servicex/datasets/{dataset_id}"
        url = self.url + path_template.format(dataset_id=dataset_id)
        async with ClientSession() as session:
            async with session.get(headers=headers, url=url) as r:

                if r.status == 403:
                    raise AuthorizationError(
                        f"Not authorized to access serviceX at {self.url}"
                    )
                elif r.status == 404:
                    raise ValueError(f"Dataset {dataset_id} not found")
                elif r.status != 200:
                    msg = await _extract_message(r)
                    raise RuntimeError(f"Failed to get dataset {dataset_id} - {msg}")
                result = await r.json()

            dataset = CachedDataset(**result)
            return dataset

    async def delete_dataset(self, dataset_id=None) -> bool:
        headers = await self._get_authorization()
        path_template = "/servicex/datasets/{dataset_id}"
        url = self.url + path_template.format(dataset_id=dataset_id)

        async with ClientSession() as session:
            async with session.delete(headers=headers, url=url) as r:

                if r.status == 403:
                    raise AuthorizationError(
                        f"Not authorized to access serviceX at {self.url}"
                    )
                elif r.status == 404:
                    raise ValueError(f"Dataset {dataset_id} not found")
                elif r.status != 200:
                    msg = await _extract_message(r)
                    raise RuntimeError(f"Failed to delete dataset {dataset_id} - {msg}")
                result = await r.json()
                return result["stale"]

    async def delete_transform(self, transform_id=None):
        headers = await self._get_authorization()
        path_template = f"/servicex/transformation/{transform_id}"
        url = self.url + path_template.format(transform_id=transform_id)

        async with ClientSession() as session:
            async with session.delete(headers=headers, url=url) as r:

                if r.status == 403:
                    raise AuthorizationError(
                        f"Not authorized to access serviceX at {self.url}"
                    )
                elif r.status == 404:
                    raise ValueError(f"Transform {transform_id} not found")
                elif r.status != 200:
                    msg = await _extract_message(r)
                    raise RuntimeError(
                        f"Failed to delete transform {transform_id} - {msg}"
                    )

    @requires_resource("transformationresults")
    async def get_transformation_results(
        self, request_id: str, begin_at: datetime.datetime
    ):
        headers = await self._get_authorization()
        url = self.url + f"/servicex/transformation/{request_id}/results"

        params = {}
        if begin_at:
            params["begin_at"] = begin_at.isoformat()

        async with ClientSession() as session:
            async with session.get(headers=headers, url=url, params=params) as r:
                if r.status == 403:
                    raise AuthorizationError(
                        f"Not authorized to access serviceX at {self.url}"
                    )

                if r.status == 404:
                    raise ValueError(f"Request {request_id} not found")

                if r.status != 200:
                    msg = await _extract_message(r)
                    raise RuntimeError(f"Failed with message: {msg}")

                data = await r.json()
                return data.get("results")

    async def cancel_transform(self, transform_id=None):
        headers = await self._get_authorization()
        path_template = f"/servicex/transformation/{transform_id}/cancel"
        url = self.url + path_template.format(transform_id=transform_id)

        async with ClientSession() as session:
            async with session.get(headers=headers, url=url) as r:
                if r.status == 403:
                    raise AuthorizationError(
                        f"Not authorized to access serviceX at {self.url}"
                    )
                elif r.status == 404:
                    raise ValueError(f"Transform {transform_id} not found")
                elif r.status != 200:
                    msg = await _extract_message(r)
                    raise RuntimeError(
                        f"Failed to cancel transform {transform_id} - {msg}"
                    )

    async def submit_transform(self, transform_request: TransformRequest) -> str:
        headers = await self._get_authorization()
        retry_options = ExponentialRetry(attempts=3, start_timeout=30)
        async with RetryClient(retry_options=retry_options) as client:
            async with client.post(
                url=f"{self.url}/servicex/transformation",
                headers=headers,
                json=transform_request.model_dump(by_alias=True, exclude_none=True),
            ) as r:
                if r.status == 401:
                    raise AuthorizationError(
                        f"Not authorized to access serviceX at {self.url}"
                    )
                elif r.status == 400:
                    message = await _extract_message(r)
                    raise ValueError(f"Invalid transform request: {message}")
                elif r.status > 400:
                    error_message = await _extract_message(r)
                    raise RuntimeError(
                        "ServiceX WebAPI Error during transformation "
                        f"submission: {r.status} - {error_message}"
                    )
                else:
                    o = await r.json()
                    return o["request_id"]

    async def get_transform_status(self, request_id: str) -> TransformStatus:
        headers = await self._get_authorization()
        retry_options = ExponentialRetry(attempts=5, start_timeout=3)
        async with RetryClient(retry_options=retry_options) as client:
            try:
                async for attempt in AsyncRetrying(
                    retry=retry_if_not_exception_type(ValueError),
                    stop=stop_after_attempt(3),
                    wait=wait_fixed(3),
                    reraise=True,
                ):
                    with attempt:
                        async with client.get(
                            url=f"{self.url}/servicex/" f"transformation/{request_id}",
                            headers=headers,
                        ) as r:
                            if r.status == 401:
                                # perhaps we just ran out of auth validity the last time?
                                # refetch auth then raise an error for retry
                                headers = await self._get_authorization(True)
                                raise AuthorizationError(
                                    f"Not authorized to access serviceX at {self.url}"
                                )
                            if r.status == 404:
                                raise ValueError(f"Transform ID {request_id} not found")
                            elif r.status > 400:
                                error_message = await _extract_message(r)
                                raise RuntimeError(
                                    "ServiceX WebAPI Error during transformation: "
                                    f"{r.status} - {error_message}"
                                )
                            o = await r.json()
                            return TransformStatus(**o)
            except RuntimeError as e:
                raise RuntimeError(
                    "ServiceX WebAPI Error " f"while getting transform status: {e}"
                )
