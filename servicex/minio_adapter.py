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
import os.path
from hashlib import sha1
from pathlib import Path
from typing import List

from miniopy_async import Minio

from servicex.models import ResultFile, TransformStatus


def _sanitize_filename(fname: str):
    "No matter the string given, make it an acceptable filename on all platforms"
    return fname.replace("*", "_").replace(";", "_").replace(":", "_")


class MinioAdapter:
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
        self.minio = Minio(
            endpoint_host, access_key=access_key, secret_key=secret_key, secure=secure
        )

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

    async def list_bucket(self) -> List[ResultFile]:
        objects = await self.minio.list_objects(self.bucket)
        return [
            ResultFile(
                filename=obj.object_name,
                size=obj.size,
                extension=obj.object_name.split(".")[-1],
            )
            for obj in objects if not obj.is_dir
        ]

    async def download_file(
        self, object_name: str, local_dir: str, shorten_filename: bool = False
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

        _ = await self.minio.fget_object(
            bucket_name=self.bucket, object_name=object_name, file_path=path.as_posix()
        )
        return path.resolve()

    async def get_signed_url(self, object_name: str) -> str:
        return await self.minio.get_presigned_url(
            bucket_name=self.bucket, object_name=object_name, method="GET"
        )

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
