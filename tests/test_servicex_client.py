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
from unittest.mock import MagicMock

from pytest_asyncio import fixture

from servicex.query_cache import QueryCache
from servicex.servicex_adapter import ServiceXAdapter
from servicex.servicex_client import ServiceXClient


@fixture
def servicex_adaptor(mocker):
    adapter_mock = mocker.patch('servicex.servicex_client.ServiceXAdapter')
    mock_adapter = MagicMock(spec=ServiceXAdapter)

    adapter_mock.return_value = mock_adapter
    return mock_adapter


@fixture
def mock_cache(mocker):
    cache_mock = mocker.patch('servicex.servicex_client.QueryCache')
    mock_cache = MagicMock(spec=QueryCache)
    mock_cache.get_codegen_by_backend.return_value = {
        "codegens": {
            "ROOT": "my_root_generator",
            "UPROOT": "my_uproot_generator"
        }
    }
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
