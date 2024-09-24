# Copyright (c) 2024, IRIS-HEP
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
import tempfile
import os

from unittest.mock import AsyncMock, Mock, patch
from servicex.dataset_identifier import FileListDataset
from servicex.configuration import Configuration
from servicex.minio_adapter import MinioAdapter
from servicex.query_core import Query
from servicex.query_cache import QueryCache
from servicex.expandable_progress import ExpandableProgress
from servicex.query_core import ServiceXException

from pathlib import Path
from servicex.models import (
    Status,
)
from rich.progress import Progress


@pytest.mark.asyncio
async def test_as_signed_urls_happy(transformed_result):
    # Test when display_progress is True and provided_progress is None
    did = FileListDataset("/foo/bar/baz.root")
    dataset = Query(dataset_identifier=did, codegen="uproot",
                    title="", config=None,
                    sx_adapter=None, query_cache=None)
    dataset.submit_and_download = AsyncMock()
    dataset.submit_and_download.return_value = transformed_result

    result = dataset.as_signed_urls(display_progress=True, provided_progress=None)
    assert result == transformed_result


@pytest.mark.asyncio
async def test_as_signed_urls_happy_dataset_group(transformed_result):
    # Test when display_progress is True and provided_progress is None
    did = FileListDataset("/foo/bar/baz.root")
    dataset = Query(dataset_identifier=did, codegen="uproot",
                    title="", config=None,
                    sx_adapter=None, query_cache=None)
    dataset.submit_and_download = AsyncMock()
    dataset.submit_and_download.return_value = transformed_result

    result = dataset.as_signed_urls(display_progress=True, provided_progress=None,
                                    dataset_group=True)
    assert result == transformed_result


@pytest.mark.asyncio
async def test_as_files_happy(transformed_result):
    did = FileListDataset("/foo/bar/baz.root")
    dataset = Query(dataset_identifier=did, codegen="uproot",
                    title="", config=None,
                    sx_adapter=None, query_cache=None)
    dataset.submit_and_download = AsyncMock()
    dataset.submit_and_download.return_value = transformed_result

    result = dataset.as_files(display_progress=True, provided_progress=None)
    assert result == transformed_result


@pytest.mark.asyncio
async def test_as_files_cached(transformed_result, python_dataset):
    python_dataset.cache = AsyncMock()
    python_dataset.cache.get_transform_by_hash = Mock()
    python_dataset.cache.get_transform_by_hash.return_value = transformed_result
    result = python_dataset.as_files(display_progress=True, provided_progress=None)
    assert result.title == transformed_result.title
    assert result.request_id == transformed_result.request_id
    assert result.files == transformed_result.files
    assert result.file_list == transformed_result.file_list
    assert result.codegen == transformed_result.codegen
    assert result.data_dir == transformed_result.data_dir


@pytest.mark.asyncio
async def test_download_files(python_dataset):
    signed_urls_only = False
    download_progress = "download_task_id"
    minio_mock = AsyncMock()
    config = Configuration(cache_path="temp_dir", api_endpoints=[])
    python_dataset.configuration = config
    minio_mock.download_file.return_value = Path("/path/to/downloaded_file")
    minio_mock.get_signed_url.return_value = Path("http://example.com/signed_url")
    minio_mock.list_bucket.return_value = [Mock(filename="file1.txt"),
                                           Mock(filename="file2.txt")]

    progress_mock = Mock()
    python_dataset.minio_polling_interval = 0
    python_dataset.minio = minio_mock
    python_dataset.current_status = Mock(status="Complete")
    python_dataset.configuration.shortened_downloaded_filename = False

    result_uris = await python_dataset.download_files(
        signed_urls_only, progress_mock, download_progress, None
    )
    minio_mock.download_file.assert_awaited()
    minio_mock.get_signed_url.assert_not_awaited()
    assert result_uris == ["/path/to/downloaded_file", "/path/to/downloaded_file"]


@pytest.mark.asyncio
async def test_download_files_with_signed_urls(python_dataset):
    signed_urls_only = True
    download_progress = "download_task_id"
    minio_mock = AsyncMock()
    config = Configuration(cache_path="temp_dir", api_endpoints=[])
    python_dataset.configuration = config
    minio_mock.download_file.return_value = "/path/to/downloaded_file"
    minio_mock.get_signed_url.return_value = "http://example.com/signed_url"
    minio_mock.list_bucket.return_value = [Mock(filename="file1.txt"),
                                           Mock(filename="file2.txt")]
    progress_mock = Mock()

    python_dataset.minio_polling_interval = 0
    python_dataset.minio = minio_mock
    python_dataset.current_status = Mock(status="Complete")
    python_dataset.configuration.shortened_downloaded_filename = False

    result_uris = await python_dataset.download_files(
        signed_urls_only, progress_mock, download_progress, None
    )
    minio_mock.download_file.assert_not_called()
    minio_mock.get_signed_url.assert_called()
    assert result_uris == ["http://example.com/signed_url", "http://example.com/signed_url"]


@pytest.mark.asyncio
async def test_transform_status_listener_happy(python_dataset):
    progress = Mock(spec=Progress)
    progress_task = Mock()
    download_task = Mock()
    status = Mock(files=10, files_completed=5, files_failed=1, status=Status.complete)
    python_dataset.current_status = status
    python_dataset.retrieve_current_transform_status = AsyncMock(return_value=status)
    await python_dataset.transform_status_listener(progress, progress_task, "mock_title",
                                                   download_task, "mock_title")

    python_dataset.retrieve_current_transform_status.assert_awaited_once()
    # progress.update.assert_called_with(progress_task, total=10)
    # progress.start_task.assert_called_with(progress_task)
    # progress.update.assert_called_with(progress_task, completed=5)
    assert python_dataset.files_completed == 5
    assert python_dataset.files_failed == 1


@pytest.mark.asyncio
async def test_transform_status_listener_cancelled(python_dataset):
    progress = Mock(spec=Progress)
    progress_task = Mock()
    download_task = Mock()
    status = Mock(files=10, files_completed=5, files_failed=1, status=Status.canceled)
    python_dataset.current_status = status
    python_dataset.retrieve_current_transform_status = AsyncMock(return_value=status)
    with pytest.raises(ServiceXException, match=r"Request .*was canceled"):
        with patch("servicex.app.transforms.create_kibana_link_parameters") as mock_link:
            await python_dataset.transform_status_listener(progress, progress_task, "mock_title",
                                                           download_task, "mock_title")
            mock_link.assert_called_once()
            python_dataset.retrieve_current_transform_status.assert_awaited_once()
            assert python_dataset.files_completed == 5
            assert python_dataset.files_failed == 1


@pytest.mark.asyncio
async def test_retrieve_current_transform_status_status_none(python_dataset, completed_status):
    with tempfile.TemporaryDirectory() as temp_dir:
        python_dataset.current_status = None
        python_dataset.servicex = AsyncMock()
        config = Configuration(cache_path=temp_dir, api_endpoints=[])
        cache = QueryCache(config)
        python_dataset.cache = cache
        python_dataset.configuration = config
        python_dataset.servicex.get_transform_status.return_value = completed_status

        await python_dataset.retrieve_current_transform_status()
        assert python_dataset.current_status == completed_status
        result = Path(os.path.join(Path(python_dataset.configuration.cache_path),
                      completed_status.request_id))
        python_dataset.download_path = result
        assert python_dataset.minio is not None
        assert isinstance(python_dataset.minio, MinioAdapter)
        cache.close()


@pytest.mark.asyncio
async def test_retrieve_current_transform_status_status_not(python_dataset, completed_status):
    with tempfile.TemporaryDirectory() as temp_dir:
        python_dataset.servicex = AsyncMock()
        python_dataset.servicex.get_transform_status.return_value = completed_status
        config = Configuration(cache_path=temp_dir, api_endpoints=[])
        cache = QueryCache(config)
        python_dataset.cache = cache
        python_dataset.configuration = config

        await python_dataset.retrieve_current_transform_status()
        assert python_dataset.current_status == completed_status
        result = Path(os.path.join(Path(python_dataset.configuration.cache_path),
                      completed_status.request_id))
        python_dataset.download_path = result
        assert python_dataset.minio is not None
        assert isinstance(python_dataset.minio, MinioAdapter)
        cache.close()


@pytest.mark.asyncio
async def test_submit_and_download_cache_miss(python_dataset, completed_status):
    with tempfile.TemporaryDirectory() as temp_dir:
        python_dataset.current_status = None
        python_dataset.servicex = AsyncMock()
        config = Configuration(cache_path=temp_dir, api_endpoints=[])
        cache = QueryCache(config)
        python_dataset.cache = cache
        python_dataset.configuration = config

        python_dataset.servicex = AsyncMock()
        python_dataset.cache.get_transform_by_hash = Mock()
        python_dataset.cache.get_transform_by_hash.return_value = None
        python_dataset.servicex.get_transform_status = AsyncMock(id="12345")
        python_dataset.servicex.get_transform_status.return_value = completed_status
        python_dataset.servicex.submit_transform = AsyncMock()
        python_dataset.download_files = AsyncMock()
        python_dataset.download_files.return_value = []

        signed_urls_only = False
        expandable_progress = ExpandableProgress()

        result = await python_dataset.submit_and_download(signed_urls_only, expandable_progress)
        assert result is not None
        assert result.request_id == "b8c508d0-ccf2-4deb-a1f7-65c839eebabf"
        cache.close()


@pytest.mark.asyncio
async def test_submit_and_download_cache_miss_overall_progress(python_dataset, completed_status):
    with tempfile.TemporaryDirectory() as temp_dir:
        python_dataset.current_status = None
        python_dataset.servicex = AsyncMock()
        config = Configuration(cache_path=temp_dir, api_endpoints=[])
        cache = QueryCache(config)
        python_dataset.cache = cache
        python_dataset.configuration = config

        python_dataset.servicex = AsyncMock()
        python_dataset.cache.get_transform_by_hash = Mock()
        python_dataset.cache.get_transform_by_hash.return_value = None
        python_dataset.servicex.get_transform_status = AsyncMock(id="12345")
        python_dataset.servicex.get_transform_status.return_value = completed_status
        python_dataset.servicex.submit_transform = AsyncMock()
        python_dataset.download_files = AsyncMock()
        python_dataset.download_files.return_value = []

        signed_urls_only = False
        expandable_progress = ExpandableProgress(overall_progress=True)
        dataset_group = True

        result = await python_dataset.submit_and_download(signed_urls_only, expandable_progress,
                                                          dataset_group)
        assert result is not None
        assert result.request_id == "b8c508d0-ccf2-4deb-a1f7-65c839eebabf"
        cache.close()


@pytest.mark.asyncio
async def test_submit_and_download_no_result_format(python_dataset, completed_status):
    with tempfile.TemporaryDirectory() as temp_dir:
        python_dataset.current_status = None
        python_dataset.servicex = AsyncMock()
        config = Configuration(cache_path=temp_dir, api_endpoints=[])
        cache = QueryCache(config)
        python_dataset.cache = cache
        python_dataset.configuration = config
        with pytest.raises(ValueError,
                           match=r"Unable to determine the result file format. "
                                 r"Use set_result_format method"):
            python_dataset.result_format = None
            python_dataset.servicex = AsyncMock()
            python_dataset.cache.get_transform_by_hash = Mock()
            python_dataset.cache.get_transform_by_hash.return_value = None
            python_dataset.servicex.get_transform_status = AsyncMock(id="12345")
            python_dataset.servicex.get_transform_status.return_value = completed_status
            python_dataset.servicex.submit_transform = AsyncMock()
            python_dataset.download_files = AsyncMock()
            python_dataset.download_files.return_value = []
            signed_urls_only = False
            expandable_progress = ExpandableProgress()
            await python_dataset.submit_and_download(signed_urls_only, expandable_progress)
        cache.close()


def test_set_title(python_dataset):
    python_dataset.set_title("Title Title")
    assert python_dataset.title == "Title Title"


@pytest.mark.asyncio
async def test_submit_and_download_cache_miss_signed_urls_only(python_dataset, completed_status):
    with tempfile.TemporaryDirectory() as temp_dir:
        python_dataset.current_status = None
        python_dataset.servicex = AsyncMock()
        config = Configuration(cache_path=temp_dir, api_endpoints=[])
        cache = QueryCache(config)
        python_dataset.cache = cache
        python_dataset.configuration = config
        python_dataset.servicex = AsyncMock()
        python_dataset.cache.get_transform_by_hash = Mock()
        python_dataset.cache.get_transform_by_hash.return_value = None
        python_dataset.servicex.get_transform_status = AsyncMock(id="12345")
        python_dataset.servicex.get_transform_status.return_value = completed_status
        python_dataset.servicex.submit_transform = AsyncMock()
        python_dataset.download_files = AsyncMock()
        python_dataset.download_files.return_value = []

        signed_urls_only = True
        expandable_progress = ExpandableProgress()

        result = await python_dataset.submit_and_download(signed_urls_only, expandable_progress)
        assert result is not None
        assert result.request_id == "b8c508d0-ccf2-4deb-a1f7-65c839eebabf"
        cache.close()


@pytest.mark.asyncio
async def test_submit_and_download_cache_files_request_urls(python_dataset, transformed_result):
    with tempfile.TemporaryDirectory() as temp_dir:
        python_dataset.current_status = None
        python_dataset.servicex = AsyncMock()
        config = Configuration(cache_path=temp_dir, api_endpoints=[])
        cache = QueryCache(config)
        python_dataset.cache = cache
        python_dataset.configuration = config
        python_dataset.servicex = AsyncMock()
        python_dataset.cache.get_transform_by_hash = Mock()
        python_dataset.cache.get_transform_by_hash.return_value = transformed_result
        status = Mock(files=10, files_completed=5, files_failed=1, status=Status.complete)
        python_dataset.current_status = status
        python_dataset.retrieve_current_transform_status = AsyncMock(return_value=status)

        signed_urls_only = True
        expandable_progress = ExpandableProgress()
        result = await python_dataset.submit_and_download(signed_urls_only, expandable_progress)
        assert result is not None
        assert result.request_id == transformed_result.request_id
        cache.close()


@pytest.mark.asyncio
async def test_submit_and_download_cache_urls_request_files(python_dataset, transformed_result):
    with tempfile.TemporaryDirectory() as temp_dir:
        python_dataset.current_status = None
        python_dataset.servicex = AsyncMock()
        config = Configuration(cache_path=temp_dir, api_endpoints=[])
        cache = QueryCache(config)
        python_dataset.cache = cache
        python_dataset.configuration = config
        python_dataset.servicex = AsyncMock()
        python_dataset.cache.get_transform_by_hash = Mock()
        transformed_result.signed_url_list = ["a.b.c.com"]
        transformed_result.file_list = []
        python_dataset.cache.get_transform_by_hash.return_value = transformed_result
        status = Mock(files=10, files_completed=5, files_failed=1, status=Status.complete)
        python_dataset.current_status = status
        python_dataset.retrieve_current_transform_status = AsyncMock(return_value=status)

        signed_urls_only = False
        expandable_progress = ExpandableProgress()
        result = await python_dataset.submit_and_download(signed_urls_only, expandable_progress)
        assert result is not None
        assert result.request_id == transformed_result.request_id
        cache.close()


@pytest.mark.asyncio
async def test_network_loss(python_dataset, transformed_result):
    with tempfile.TemporaryDirectory() as temp_dir:
        python_dataset.current_status = None
        python_dataset.servicex = AsyncMock()
        config = Configuration(cache_path=temp_dir, api_endpoints=[])
        cache = QueryCache(config)
        python_dataset.cache = cache
        python_dataset.configuration = config
        python_dataset.download_path = Path("www.a.b.com")

        python_dataset.servicex = AsyncMock()
        status = Mock(files=10, files_completed=5, files_failed=1, status=Status.fatal)
        python_dataset.current_status = status

        python_dataset.cache.get_transform_by_hash = Mock()
        transformed_result.files = status.files
        python_dataset.cache.get_transform_by_hash.return_value = transformed_result

        python_dataset.servicex.get_transform_status = AsyncMock(id="12345")
        python_dataset.servicex.get_transform_status.return_value = status
        python_dataset.servicex.submit_transform = AsyncMock()
        python_dataset.download_files = AsyncMock()
        python_dataset.download_files.return_value = []

        signed_urls_only = False
        expandable_progress = ExpandableProgress()

        result = await python_dataset.submit_and_download(signed_urls_only, expandable_progress)
        assert result is not None
        assert result.request_id == "123-45-6789"
        cache.close()
