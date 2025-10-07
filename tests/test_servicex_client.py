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
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from pytest_asyncio import fixture

from servicex.models import TransformedResults, ResultFormat
from servicex.query_cache import QueryCache
from servicex.servicex_adapter import ServiceXAdapter
from servicex.servicex_client import ServiceXClient


@fixture
def servicex_adaptor(mocker):
    adapter_mock = mocker.patch("servicex.servicex_client.ServiceXAdapter")
    mock_adapter = MagicMock(spec=ServiceXAdapter)
    mock_adapter.get_code_generators.return_value = {
        "uproot": "http://uproot-codegen",
        "xaod": "http://xaod-codegen",
    }

    adapter_mock.return_value = mock_adapter
    return mock_adapter


@fixture
def mock_cache(mocker):
    cache_mock = mocker.patch("servicex.servicex_client.QueryCache")
    mock_cache = MagicMock(spec=QueryCache)
    cache_mock.return_value = mock_cache
    return cache_mock


def test_get_datasets(mock_cache, servicex_adaptor):
    sx = ServiceXClient(config_path="tests/example_config.yaml")
    sx.get_datasets()
    servicex_adaptor.get_datasets.assert_called_once()


def test_get_dataset(mock_cache, servicex_adaptor):
    sx = ServiceXClient(config_path="tests/example_config.yaml")
    sx.get_dataset("123")
    servicex_adaptor.get_dataset.assert_called_once_with("123")


def test_delete_dataset(mock_cache, servicex_adaptor):
    sx = ServiceXClient(config_path="tests/example_config.yaml")
    sx.delete_dataset("123")
    servicex_adaptor.delete_dataset.assert_called_once_with("123")


def test_delete_transform(mock_cache, servicex_adaptor):
    sx = ServiceXClient(config_path="tests/example_config.yaml")
    sx.delete_transform("123-45-6789")
    servicex_adaptor.delete_transform.assert_called_once_with("123-45-6789")


def test_cancel_transform(mock_cache, servicex_adaptor):
    sx = ServiceXClient(config_path="tests/example_config.yaml")
    sx.cancel_transform("123-45-6789")
    servicex_adaptor.cancel_transform.assert_called_once_with("123-45-6789")


@pytest.fixture
def transformed_results() -> TransformedResults:
    """
    Pytest fixture generating a realistic TransformedResults instance for testing.

    Returns:
        TransformedResults: A populated TransformedResults object with sample data
    """
    # Default data that can be overridden
    base_data = {
        "hash": "abc123def456",
        "title": "Test Transformation Request",
        "codegen": "python-selector",
        "request_id": "servicex-request-789",
        "submit_time": datetime.now(),
        "data_dir": "/tmp/servicex/results",
        "file_list": [
            "/tmp/servicex/results/output1.parquet",
            "/tmp/servicex/results/output2.parquet",
        ],
        "signed_url_list": [
            "https://example.com/signed-url-1",
            "https://example.com/signed-url-2",
        ],
        "files": 2,
        "result_format": ResultFormat.parquet,
        "log_url": "https://logs.servicex.com/request-789",
    }

    return TransformedResults(**base_data)


def test_delete_transform_from_cache(mock_cache, servicex_adaptor, transformed_results):
    with patch("servicex.servicex_client.QueryCache") as mock_cache:
        mock_cache.return_value.get_transform_by_request_id = MagicMock(
            return_value=transformed_results
        )
        with patch("servicex.servicex_client.shutil.rmtree") as mock_rmtree:
            sx = ServiceXClient(config_path="tests/example_config.yaml")
            sx.delete_transform_from_cache("servicex-request-789")

            mock_cache.return_value.get_transform_by_request_id.assert_called_once_with(
                "servicex-request-789"
            )
            mock_rmtree.assert_called_once_with(
                "/tmp/servicex/results", ignore_errors=True
            )
            mock_cache.return_value.delete_record_by_request_id.assert_called_once_with(
                "servicex-request-789"
            )


def test_invalid_backend_raises_error_with_filename():
    config_file = "tests/example_config.yaml"
    expected = Path(config_file).resolve()

    with pytest.raises(ValueError) as err:
        ServiceXClient(backend="badname", config_path=config_file)

    assert f"Backend badname not defined in {expected} file" in str(err.value)


def test_display_results_with_many_files():
    from servicex.servicex_client import _display_results, GuardList
    from unittest.mock import patch, MagicMock

    # Mock GuardList with more than 3 files to trigger lines 275-276
    mock_guard_list = MagicMock(spec=GuardList)
    mock_guard_list.valid.return_value = True
    mock_guard_list.__iter__.return_value = iter(
        [
            "file1.parquet",
            "file2.parquet",
            "file3.parquet",
            "file4.parquet",
            "file5.parquet",
        ]
    )

    out_dict = {"sample1": mock_guard_list}

    with patch("rich.get_console") as mock_get_console:
        mock_console = MagicMock()
        mock_get_console.return_value = mock_console

        with patch("servicex.servicex_client.Table") as mock_table:
            mock_table_instance = MagicMock()
            mock_table.return_value = mock_table_instance

            _display_results(out_dict)

            # Verify that add_row was called with the truncated file list
            mock_table_instance.add_row.assert_called_once()
            call_args = mock_table_instance.add_row.call_args[0]
            assert call_args[0] == "sample1"
            assert call_args[1] == "5"
            assert "... and 3 more files" in call_args[2]


@pytest.mark.asyncio
async def test_deliver_async_invalid_delivery_config():
    from servicex.servicex_client import deliver_async
    from unittest.mock import patch, MagicMock

    # Mock the config loading to return invalid delivery type
    with patch("servicex.servicex_client._load_ServiceXSpec") as mock_load_spec:
        with patch("servicex.servicex_client._build_datasets") as mock_build_datasets:
            with patch("servicex.minio_adapter.init_s3_config"):
                mock_config = MagicMock()
                mock_config.General.Delivery = (
                    "INVALID_DELIVERY"  # Invalid delivery type
                )
                mock_config.General.IgnoreLocalCache = False
                mock_config.Sample = []
                mock_load_spec.return_value = mock_config
                mock_build_datasets.return_value = []

                with pytest.raises(ValueError) as exc_info:
                    await deliver_async("test_spec.yaml")

                assert (
                    "unexpected value for config.general.Delivery: INVALID_DELIVERY"
                    in str(exc_info.value)
                )
