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
import pytest

from servicex.download_adapter import HTTPDownloadAdapter
from servicex.models import ResultFile
from servicex.servicex_adapter import ServiceXAdapter

DOWNLOAD_PATCH_COUNTER = 0


def adapter(httpserver) -> HTTPDownloadAdapter:
    mock_servicex = ServiceXAdapter(httpserver.url_for("/"))
    httpserver.expect_request("/servicex").respond_with_json(
        {
            "app-version": "1.9.0",
            "code-gen-image": {},
            "capabilities": ["poll_local_transformation_results", "generate_file_urls"],
        }
    )
    return HTTPDownloadAdapter(mock_servicex, "bucket")


def populate_bucket(request, httpserver):
    httpserver.expect_request(
        "/servicex/transformation/bucket/results"
    ).respond_with_json(
        {
            "results": [
                {
                    "s3-object-name": _,
                    "total-bytes": 10,
                    "transform_status": "success",
                    "created_at": "2026-07-14 12:00+0000",
                }
                for _ in request
            ]
        }
    )
    httpserver.expect_request("/servicex/transformation/file-urls").respond_with_json(
        {"uris": {_: (httpserver.url_for("/") + f"files/{_}", {}, 0) for _ in request}}
    )
    for _ in request:
        httpserver.expect_request(f"/files/{_}").respond_with_data(b"\x01" * 10)


@pytest.mark.parametrize("content", ["test.txt"])
@pytest.mark.asyncio
async def test_list_bucket(httpserver, content):
    populate_bucket([content], httpserver)
    http_adapter = adapter(httpserver)
    files = [ResultFile(filename="test.txt", size=10, extension="txt")]
    result = await http_adapter.list_bucket()
    assert result == files


@pytest.mark.parametrize("content", ["test.txt"])
@pytest.mark.asyncio
async def test_download_file(httpserver, content, tmp_path):
    populate_bucket([content], httpserver)
    http_adapter = adapter(httpserver)
    result = await http_adapter.download_file("test.txt", local_dir=tmp_path)
    assert str(result).endswith("test.txt")
    assert result.exists()
    assert result.read_bytes() == (b"\x01" * 10)
    result.unlink()  # it should exist, from above ...


@pytest.mark.parametrize("content", ["test.txt"])
@pytest.mark.asyncio
async def test_download_file_with_expected_size(httpserver, content, tmp_path):
    populate_bucket([content], httpserver)
    http_adapter = adapter(httpserver)
    info = await http_adapter.list_bucket()
    assert len(info) == 1
    result = await http_adapter.download_file(
        "test.txt", local_dir=tmp_path, expected_size=info[0].size
    )
    assert str(result).endswith("test.txt")
    assert result.exists()
    assert result.read_bytes() == (b"\x01" * 10)
    result.unlink()  # it should exist, from above ...


@pytest.mark.parametrize("content", ["t::est.txt"])
@pytest.mark.asyncio
async def test_download_bad_filename(httpserver, content, tmp_path):
    populate_bucket([content], httpserver)
    http_adapter = adapter(httpserver)
    result = await http_adapter.download_file("t::est.txt", local_dir=tmp_path)
    assert str(result).endswith("t__est.txt")
    assert result.exists()
    assert result.read_bytes() == (b"\x01" * 10)
    result.unlink()  # it should exist, from above ...


@pytest.mark.parametrize("content", ["test.txt"])
@pytest.mark.asyncio
async def test_download_short_filename_no_change(httpserver, content, tmp_path):
    populate_bucket([content], httpserver)
    http_adapter = adapter(httpserver)
    result = await http_adapter.download_file(
        "test.txt", local_dir=tmp_path, shorten_filename=True
    )
    assert str(result).endswith("test.txt")
    assert result.exists()
    assert result.read_bytes() == (b"\x01" * 10)
    result.unlink()  # it should exist, from above ...


@pytest.mark.parametrize(
    "content",
    [
        "test12345678901234567890123456789012345678901234567898012345678901234567890.txt"  # noqa: E501
    ],
)
@pytest.mark.asyncio
async def test_download_short_filename_change(httpserver, content, tmp_path):
    populate_bucket([content], httpserver)
    http_adapter = adapter(httpserver)
    result = await http_adapter.download_file(
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


@pytest.mark.parametrize("content", ["test.txt"])
@pytest.mark.asyncio
async def test_download_repeat(httpserver, content, tmp_path):
    import asyncio

    populate_bucket([content], httpserver)
    http_adapter = adapter(httpserver)

    result = await http_adapter.download_file("test.txt", local_dir=tmp_path)
    assert str(result).endswith("test.txt")
    assert result.exists()
    t0 = result.stat().st_mtime_ns
    await asyncio.sleep(4)  # hopefully long enough for Windows/FAT32 ... ?

    result2 = await http_adapter.download_file("test.txt", local_dir=tmp_path)
    assert result2.exists()
    assert result2 == result
    assert t0 == result2.stat().st_mtime_ns
    result.unlink()  # it should exist, from above ...


@pytest.mark.parametrize("content", ["test.txt"])
@pytest.mark.asyncio
async def test_get_signed_url(httpserver, content):
    populate_bucket([content], httpserver)
    http_adapter = adapter(httpserver)
    result = await http_adapter.get_signed_url("test.txt")
    assert result.url.startswith(httpserver.url_for("/"))


# @pytest.mark.parametrize("populate_bucket", ["test.txt"], indirect=True)
# @pytest.mark.asyncio
# async def test_download_file_retry(minio_adapter, populate_bucket, mocker, tmp_path):
#     download_patch = mocker.patch(
#         "aioboto3.s3.inject.download_file",
#         side_effect=make_mock_downloader(tmp_path / "test.txt"),
#     )
#     result = await minio_adapter.download_file("test.txt", local_dir=tmp_path)
#     assert str(result).endswith("test.txt")
#     assert result.exists()
#     assert download_patch.call_count == 3
#     result.unlink()
