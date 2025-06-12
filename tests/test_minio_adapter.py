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
import urllib.parse

import pytest
from pytest_asyncio import fixture

from servicex.minio_adapter import MinioAdapter
from servicex.models import ResultFile
from pathlib import Path

DOWNLOAD_PATCH_COUNTER = 0


def make_mock_downloader(target: Path):
    def mock_downloader(**args):
        global DOWNLOAD_PATCH_COUNTER
        DOWNLOAD_PATCH_COUNTER += 1
        if DOWNLOAD_PATCH_COUNTER == 1:
            raise Exception("lol")
        elif DOWNLOAD_PATCH_COUNTER == 2:
            open(target, "wb").write(b"\x01" * 5)
        elif DOWNLOAD_PATCH_COUNTER == 3:
            open(target, "wb").write(b"\x01" * 10)

    return mock_downloader


@fixture
def minio_adapter(moto_services, moto_patch_session) -> MinioAdapter:
    urlinfo = urllib.parse.urlparse(moto_services["s3"])
    return MinioAdapter(
        urlinfo.netloc, urlinfo.scheme == "https", "access_key", "secret_key", "bucket"
    )


@fixture(scope="function")
async def populate_bucket(request, minio_adapter):
    async with minio_adapter.minio.client(
        "s3", endpoint_url=minio_adapter.endpoint_host
    ) as s3:
        await s3.create_bucket(Bucket=minio_adapter.bucket)
        await s3.put_object(
            Bucket=minio_adapter.bucket, Key=request.param, Body=b"\x01" * 10
        )
        yield


@fixture
def session(mocker):
    mock_session = mocker.MagicMock()
    mock_ctor = mocker.patch("servicex.minio_adapter.aiohttp.ClientSession")
    mock_ctor.return_value.__enter__ = mock_session
    return mock_ctor


@pytest.mark.asyncio
async def test_initialize_from_status(completed_status):
    minio = MinioAdapter.for_transform(completed_status)
    assert minio.endpoint_host == "http://minio.org:9000"
    assert minio.minio._session._credentials.access_key == "miniouser"
    assert minio.minio._session._credentials.secret_key == "secret"
    assert minio.bucket == "b8c508d0-ccf2-4deb-a1f7-65c839eebabf"


@pytest.mark.parametrize("populate_bucket", ["test.txt"], indirect=True)
@pytest.mark.asyncio
async def test_list_bucket(minio_adapter, populate_bucket):
    files = [ResultFile(filename="test.txt", size=10, extension="txt")]
    result = await minio_adapter.list_bucket()
    assert result == files


@pytest.mark.parametrize("populate_bucket", ["test.txt"], indirect=True)
@pytest.mark.asyncio
async def test_download_file(minio_adapter, populate_bucket, tmp_path):
    result = await minio_adapter.download_file("test.txt", local_dir=tmp_path)
    assert str(result).endswith("test.txt")
    assert result.exists()
    assert result.read_bytes() == (b"\x01" * 10)
    result.unlink()  # it should exist, from above ...


@pytest.mark.parametrize("populate_bucket", ["test.txt"], indirect=True)
@pytest.mark.asyncio
async def test_download_file_with_expected_size(
    minio_adapter, populate_bucket, tmp_path
):
    info = await minio_adapter.list_bucket()
    result = await minio_adapter.download_file(
        "test.txt", local_dir=tmp_path, expected_size=info[0].size
    )
    assert str(result).endswith("test.txt")
    assert result.exists()
    assert result.read_bytes() == (b"\x01" * 10)
    result.unlink()  # it should exist, from above ...


@pytest.mark.parametrize("populate_bucket", ["t::est.txt"], indirect=True)
@pytest.mark.asyncio
async def test_download_bad_filename(minio_adapter, populate_bucket, tmp_path):
    result = await minio_adapter.download_file("t::est.txt", local_dir=tmp_path)
    assert str(result).endswith("t__est.txt")
    assert result.exists()
    assert result.read_bytes() == (b"\x01" * 10)
    result.unlink()  # it should exist, from above ...


@pytest.mark.parametrize("populate_bucket", ["test.txt"], indirect=True)
@pytest.mark.asyncio
async def test_download_short_filename_no_change(
    minio_adapter, populate_bucket, tmp_path
):
    result = await minio_adapter.download_file(
        "test.txt", local_dir=tmp_path, shorten_filename=True
    )
    assert str(result).endswith("test.txt")
    assert result.exists()
    assert result.read_bytes() == (b"\x01" * 10)
    result.unlink()  # it should exist, from above ...


@pytest.mark.parametrize(
    "populate_bucket",
    [
        "test12345678901234567890123456789012345678901234567898012345678901234567890.txt"  # noqa: E501
    ],
    indirect=True,
)
@pytest.mark.asyncio
async def test_download_short_filename_change(minio_adapter, populate_bucket, tmp_path):
    result = await minio_adapter.download_file(
        "test12345678901234567890123456789012345678901234567898012345678901234567890.txt",
        local_dir=tmp_path,
        shorten_filename=True,
    )

    # Some of the filename should be left over still...
    assert str(result).endswith("01234567890.txt")

    # Make sure the length is right
    assert len(result.name) == 60

    assert result.exists()
    assert result.read_bytes() == (b"\x01" * 10)
    result.unlink()  # it should exist, from above ...


@pytest.mark.parametrize("populate_bucket", ["test.txt"], indirect=True)
@pytest.mark.asyncio
async def test_download_repeat(minio_adapter, populate_bucket, tmp_path):
    import asyncio

    result = await minio_adapter.download_file("test.txt", local_dir=tmp_path)
    assert str(result).endswith("test.txt")
    assert result.exists()
    t0 = result.stat().st_mtime_ns
    await asyncio.sleep(4)  # hopefully long enough for Windows/FAT32 ... ?

    result2 = await minio_adapter.download_file("test.txt", local_dir=tmp_path)
    assert result2.exists()
    assert result2 == result
    assert t0 == result2.stat().st_mtime_ns
    result.unlink()  # it should exist, from above ...


@pytest.mark.parametrize("populate_bucket", ["test.txt"], indirect=True)
@pytest.mark.asyncio
async def test_get_signed_url(minio_adapter, moto_services, populate_bucket):
    result = await minio_adapter.get_signed_url("test.txt")
    assert result.startswith(moto_services["s3"])


@pytest.mark.parametrize("populate_bucket", ["test.txt"], indirect=True)
@pytest.mark.asyncio
async def test_download_file_retry(minio_adapter, populate_bucket, mocker, tmp_path):
    download_patch = mocker.patch(
        "aioboto3.s3.inject.download_file",
        side_effect=make_mock_downloader(tmp_path / "test.txt"),
    )
    result = await minio_adapter.download_file("test.txt", local_dir=tmp_path)
    assert str(result).endswith("test.txt")
    assert result.exists()
    assert download_patch.call_count == 3
    result.unlink()
