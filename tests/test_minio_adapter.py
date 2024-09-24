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
from pathlib import Path
from unittest.mock import AsyncMock

import pytest
from miniopy_async.datatypes import Object
from pytest_asyncio import fixture

from servicex.minio_adapter import MinioAdapter
from servicex.models import ResultFile


@fixture
def minio_adapter() -> MinioAdapter:
    return MinioAdapter("localhost", False, "access_key", "secret_key", "bucket")


@pytest.mark.asyncio
async def test_initialize_from_status(completed_status):
    minio = MinioAdapter.for_transform(completed_status)
    assert minio.minio._base_url.host == "minio.org:9000"
    assert minio.minio._provider._credentials.access_key == "miniouser"
    assert minio.minio._provider._credentials.secret_key == "secret"
    assert minio.bucket == "b8c508d0-ccf2-4deb-a1f7-65c839eebabf"


@pytest.mark.asyncio
async def test_list_bucket(minio_adapter):
    files = [ResultFile(filename="test.txt", size=10, extension="txt")]
    minio_adapter.minio.list_objects = AsyncMock(
        return_value=[
            Object(object_name=file.filename, size=file.size, bucket_name="bucket")
            for file in files
        ]
    )
    result = await minio_adapter.list_bucket()
    assert result == files
    minio_adapter.minio.list_objects.assert_called_with("bucket")


@pytest.mark.asyncio
async def test_download_file(minio_adapter):
    minio_adapter.minio.fget_object = AsyncMock(return_value="test.txt")
    result = await minio_adapter.download_file("test.txt", local_dir="/tmp/foo")
    assert str(result).endswith("test.txt")
    minio_adapter.minio.fget_object.assert_called_with(
        bucket_name="bucket", file_path="/tmp/foo/test.txt", object_name="test.txt"
    )


@pytest.mark.asyncio
async def test_download_bad_filename(minio_adapter):
    minio_adapter.minio.fget_object = AsyncMock(return_value="t::est.txt")
    result = await minio_adapter.download_file("t::est.txt", local_dir="/tmp/foo")
    assert str(result).endswith("t__est.txt")
    minio_adapter.minio.fget_object.assert_called_with(
        bucket_name="bucket", file_path="/tmp/foo/t__est.txt", object_name="t::est.txt"
    )


@pytest.mark.asyncio
async def test_download_short_filename_no_change(minio_adapter):
    minio_adapter.minio.fget_object = AsyncMock(return_value="test.txt")
    result = await minio_adapter.download_file(
        "test.txt", local_dir="/tmp/foo", shorten_filename=True
    )
    assert str(result).endswith("test.txt")
    minio_adapter.minio.fget_object.assert_called_with(
        bucket_name="bucket", file_path="/tmp/foo/test.txt", object_name="test.txt"
    )


@pytest.mark.asyncio
async def test_download_short_filename_change(minio_adapter):
    minio_adapter.minio.fget_object = AsyncMock(
        return_value="test12345678901234567890123456789012345678901234567898012345678901234567890.txt"  # noqa: E501
    )
    result = await minio_adapter.download_file(
        "test123456789012345678901234567890k12345678901234567898012345678901234567890.txt",
        local_dir="/tmp/foo",
        shorten_filename=True,
    )

    # Some of the filename should be left over still...
    assert str(result).endswith("01234567890.txt")

    # Make sure the length is right
    assert len(Path(result).name) == 60

    # Make sure we did the right call
    minio_adapter.minio.fget_object.assert_called_with(
        bucket_name="bucket",
        file_path="/tmp/foo/_7405534c58a3f71e5d01cdd2f59356bda6f50a06678901234567890.txt",
        object_name="test123456789012345678901234567890k12345678901234567898012345678901234567890.txt",  # noqa: E501
    )


@pytest.mark.asyncio
async def test_get_signed_url(minio_adapter):
    minio_adapter.minio.get_presigned_url = AsyncMock(
        return_value="https://pre-signed.me"
    )
    result = await minio_adapter.get_signed_url("test.txt")
    assert result == "https://pre-signed.me"
    minio_adapter.minio.get_presigned_url.assert_called_with(
        bucket_name="bucket", method="GET", object_name="test.txt"
    )
