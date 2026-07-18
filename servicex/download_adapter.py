# Copyright (c) 2022-2025, IRIS-HEP
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
import os.path
from hashlib import sha1
from pathlib import Path
from typing import List, Optional
from dataclasses import dataclass
import sys

from tenacity import retry, stop_after_attempt, wait_random_exponential

import aioboto3
from boto3.s3.transfer import TransferConfig
import asyncio
import httpx

from servicex.models import ResultFile, TransformStatus
from servicex.servicex_adapter import ServiceXAdapter

# Maximum five simultaneous streams per individual file download
_transferconfig = TransferConfig(max_concurrency=5)
# Maximum ten files simultaneously being downloaded (configurable with init_s3_config)
_file_transfer_sem = asyncio.Semaphore(10)
# Maximum five buckets being queried at once
_bucket_list_sem = asyncio.Semaphore(5)


def init_download_concurrency(concurrency: int = 10):
    "Update the number of concurrent connections"
    global _file_transfer_sem
    _file_transfer_sem = asyncio.Semaphore(concurrency)


def _sanitize_filename(fname: str):
    "No matter the string given, make it an acceptable filename on all platforms"
    return fname.replace("*", "_").replace(";", "_").replace(":", "_")


@dataclass
class URLAccessInfo:
    url: str
    headers: dict[str, str]
    expiration: int


class DownloadAdapter:
    @classmethod
    def hash_path(cls, file_name):
        """
        Make the path safe for object store or POSIX, by keeping the length
        less than MAX_PATH_LEN. Replace the leading (less interesting) characters with a
        forty character hash.
        :param file_name: Input filename
        :return: Safe path string
        """
        if len(file_name) > cls.MAX_PATH_LEN:
            hash = sha1(file_name.encode("utf-8")).hexdigest()
            return "".join(
                [
                    "_",
                    hash,
                    file_name[-1 * (cls.MAX_PATH_LEN - len(hash) - 1) :],  # noqa: E203
                ]
            )
        else:
            return file_name


class MinioAdapter(DownloadAdapter):
    # This must be at least 40, the length of the `hash` we are using, or
    # undefined things will happen.
    MAX_PATH_LEN = 60

    def __init__(
        self,
        endpoint_host: str,
        secure: bool,
        access_key: str,
        secret_key: str,
        bucket: str,
    ):
        self.minio = aioboto3.Session(
            aws_access_key_id=access_key, aws_secret_access_key=secret_key
        )

        self.endpoint_host = ("https://" if secure else "http://") + endpoint_host
        self.bucket = bucket

    @classmethod
    def for_transform(cls, transform: TransformStatus):
        return MinioAdapter(
            endpoint_host=transform.minio_endpoint,
            secure=transform.minio_secured,
            access_key=transform.minio_access_key,
            secret_key=transform.minio_secret_key,
            bucket=transform.request_id,
        )

    @retry(
        stop=stop_after_attempt(3), wait=wait_random_exponential(max=60), reraise=True
    )
    async def list_bucket(self) -> List[ResultFile]:
        async with _bucket_list_sem:
            async with self.minio.client("s3", endpoint_url=self.endpoint_host) as s3:
                paginator = s3.get_paginator("list_objects_v2")
                pagination = paginator.paginate(Bucket=self.bucket)
                listing = await pagination.build_full_result()
                rv = [
                    ResultFile(
                        filename=_["Key"],
                        size=_["Size"],
                        extension=_["Key"].split(".")[-1],
                    )
                    for _ in listing.get("Contents", [])
                    if not _["Key"].endswith("/")
                ]
                return rv

    @retry(
        stop=stop_after_attempt(3), wait=wait_random_exponential(max=60), reraise=True
    )
    async def download_file(
        self,
        object_name: str,
        local_dir: str,
        shorten_filename: bool = False,
        expected_size: Optional[int] = None,
    ) -> Path:
        os.makedirs(local_dir, exist_ok=True)
        path = Path(
            os.path.join(
                local_dir,
                _sanitize_filename(
                    self.hash_path(object_name) if shorten_filename else object_name,
                ),
            )
        )

        async with _file_transfer_sem:
            async with self.minio.client("s3", endpoint_url=self.endpoint_host) as s3:
                if expected_size is not None:
                    remotesize = expected_size
                else:
                    info = await s3.head_object(Bucket=self.bucket, Key=object_name)
                    remotesize = info["ContentLength"]
                if path.exists():
                    # if file size is the same, let's not download anything
                    # maybe move to a better verification mechanism with e-tags in the future
                    localsize = path.stat().st_size
                    if localsize == remotesize:
                        return path.resolve()
                await s3.download_file(
                    Bucket=self.bucket,
                    Key=object_name,
                    Filename=path.as_posix(),
                    Config=_transferconfig,
                )
                localsize = path.stat().st_size
                if localsize != remotesize:
                    raise RuntimeError(f"Download of {object_name} failed")
        return path.resolve()

    @retry(
        stop=stop_after_attempt(3), wait=wait_random_exponential(max=60), reraise=True
    )
    async def get_signed_url(self, object_name: str) -> URLAccessInfo:
        async with self.minio.client("s3", endpoint_url=self.endpoint_host) as s3:
            url = await s3.generate_presigned_url(
                "get_object",
                Params={"Bucket": self.bucket, "Key": object_name},
                ExpiresIn=365 * 24 * 60 * 60,
            )
            return URLAccessInfo(url=url, headers={}, expiration=sys.maxsize)

    async def update_cache(self, object_names: list[str]) -> None:
        # this does nothing
        pass


class HTTPDownloadAdapter(DownloadAdapter):
    # Ask the server what URL to download from
    # This must be at least 40, the length of the `hash` we are using, or
    # undefined things will happen.
    MAX_PATH_LEN = 60

    def __init__(
        self,
        servicex: ServiceXAdapter,
        bucket: str,
    ):
        self.session = httpx.AsyncClient()

        self.servicex = servicex
        self.bucket = bucket
        self.urlcache: dict[str, URLAccessInfo] = {}

    @classmethod
    def for_transform(cls, transform: TransformStatus, servicex: ServiceXAdapter):
        return HTTPDownloadAdapter(
            servicex=servicex,
            bucket=transform.request_id,
        )

    @retry(
        stop=stop_after_attempt(3), wait=wait_random_exponential(max=60), reraise=True
    )
    async def list_bucket(self) -> List[ResultFile]:
        return [
            ResultFile(
                filename=_.filename,
                size=_.total_bytes,
                extension=_.filename.split(".")[-1],
            )
            for _ in await self.servicex.get_transformation_results(self.bucket)
        ]

    @retry(
        stop=stop_after_attempt(3), wait=wait_random_exponential(max=60), reraise=True
    )
    async def download_file(
        self,
        object_name: str,
        local_dir: str,
        shorten_filename: bool = False,
        expected_size: Optional[int] = None,
    ) -> Path:
        os.makedirs(local_dir, exist_ok=True)
        path = Path(
            os.path.join(
                local_dir,
                _sanitize_filename(
                    self.hash_path(object_name) if shorten_filename else object_name,
                ),
            )
        )

        async with _file_transfer_sem:
            download_info = await self.get_signed_url(object_name)
            if expected_size is not None:
                remotesize = expected_size
            else:
                info = await self.session.head(
                    download_info.url, headers=download_info.headers
                )
                remotesize = info.headers.get("Content-Length")
                if remotesize is not None:
                    remotesize = int(remotesize)
            if path.exists() and remotesize is not None:
                # if file size is the same, let's not download anything
                # maybe move to a better verification mechanism with e-tags in the future
                localsize = path.stat().st_size
                if localsize == remotesize:
                    return path.resolve()

            async with self.session.stream(
                "GET", download_info.url, headers=download_info.headers
            ) as response:
                with open(path, "wb") as outfile:
                    async for chunk in response.aiter_bytes():
                        outfile.write(chunk)
            if remotesize is not None:
                localsize = path.stat().st_size
                if localsize != remotesize:
                    raise RuntimeError(f"Download of {object_name} failed")
        return path.resolve()

    async def get_signed_url(self, object_name: str) -> URLAccessInfo:
        urldata = self.urlcache.get(object_name)
        if urldata is None:
            await self.update_cache([object_name])
            urldata = self.urlcache.get(object_name)
            if urldata is None:
                raise RuntimeError(f"Obtaining URL for {object_name} failed")
        return urldata

    @retry(
        stop=stop_after_attempt(3), wait=wait_random_exponential(max=60), reraise=True
    )
    async def update_cache(self, object_names: list[str]) -> None:
        to_lookup: list[str] = []
        for key in object_names:
            if key not in self.urlcache:
                to_lookup.append(key)
        if to_lookup:
            download_info = await self.session.post(
                f"{self.servicex.url}/servicex/transformation/file-urls",
                json={"request_id": self.bucket, "file_list": to_lookup},
                headers={"Authorization": f"Bearer {self.servicex.token}"},
            )
            self.urlcache |= {
                key: URLAccessInfo(url=val[0], headers=val[1], expiration=val[2])
                for key, val in download_info.json()["uris"].items()
            }


async def get_download_adapter(
    current_status: TransformStatus, servicex: ServiceXAdapter
):
    if "generate_file_urls" in await servicex.get_servicex_capabilities():
        return HTTPDownloadAdapter.for_transform(current_status, servicex)
    else:
        return MinioAdapter.for_transform(current_status)
