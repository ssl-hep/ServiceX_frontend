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
import tempfile
from typing import Any, List
from unittest.mock import AsyncMock
from pathlib import PurePath

import pytest

from servicex.configuration import Configuration
from servicex.dataset_identifier import FileListDataset
from servicex.expandable_progress import ExpandableProgress
from servicex.func_adl.func_adl_dataset import FuncADLQuery_Uproot, FuncADLQuery
from servicex.models import (TransformStatus, Status, ResultFile, ResultFormat,
                             TransformRequest, TransformedResults)
from servicex.query_cache import QueryCache
from servicex.query_core import ServiceXException, GenericQuery
from servicex.servicex_client import ServiceXClient
from servicex.uproot_raw.uproot_raw import UprootRawQuery

transform_status = TransformStatus(
    **{
        "request_id": "b8c508d0-ccf2-4deb-a1f7-65c839eebabf",
        "did": "File List Provided in Request",
        "columns": None,
        "selection": "(Where (SelectMany (call EventDataset) (lambda (list e) (call (attr e 'Jets') 'AntiKt4EMTopoJets'))) (lambda (list j) (and (> (/ (call (attr j 'pt')) 1000) 20) (< (call abs (/ (call (attr j 'eta')) 1000)) 4.5))))",  # NOQA 501
        "tree-name": None,
        "image": "sslhep/servicex_func_adl_uproot_transformer:uproot4",
        "workers": None,
        "result-destination": "object-store",
        "result-format": "parquet",
        "workflow-name": "selection_codegen",
        "generated-code-cm": "b8c508d0-ccf2-4deb-a1f7-65c839eebabf-generated-source",
        "status": "Complete",
        "failure-info": None,
        "app-version": "develop",
        "code-gen-image": "sslhep/servicex_code_gen_func_adl_uproot:v1.2.0",
        "files": 0,
        "files-completed": 0,
        "files-failed": 0,
        "files-remaining": None,
        "submit-time": "2023-05-25T20:05:05.564137Z",
        "finish-time": None,
        "minio-endpoint": "minio.org:9000",
        "minio-secured": False,
        "minio-access-key": "miniouser",
        "minio-secret-key": "letmein",
        "log-url": "https://dummy/()",
    }
)

transform_status1 = transform_status.model_copy(
    update={
        "status": Status.running,
        "files_remaining": None,
        "files_completed": 0,
        "files": 0,
    }
)
transform_status2 = transform_status.model_copy(
    update={
        "status": Status.running,
        "files_remaining": 1,
        "files_completed": 1,
        "files": 2,
    }
)
transform_status3 = transform_status.model_copy(
    update={
        "status": Status.complete,
        "files_remaining": 0,
        "files_completed": 2,
        "files": 2,
    }
)
transform_status4 = transform_status.model_copy(
    update={
        "status": Status.canceled,
        "files_remaining": 1,
        "files_completed": 1,
        "files": 2,
    }
)
transform_status5 = transform_status.model_copy(
    update={
        "status": Status.fatal,
        "files_remaining": 0,
        "files_completed": 1,
        "files_failed": 1,
        "files": 2,
    }
)
transform_status6 = transform_status.model_copy(
    update={
        "status": Status.complete,
        "files_remaining": 0,
        "files_completed": 1,
        "files_failed": 1,
        "files": 2,
    }
)
transform_status4 = transform_status.model_copy(
    update={
        "status": Status.canceled,
        "files_remaining": 1,
        "files_completed": 1,
        "files": 2,
    }
)
transform_status5 = transform_status.model_copy(
    update={
        "status": Status.fatal,
        "files_remaining": 0,
        "files_completed": 1,
        "files_failed": 1,
        "files": 2,
    }
)
transform_status6 = transform_status.model_copy(
    update={
        "status": Status.complete,
        "files_remaining": 0,
        "files_completed": 1,
        "files_failed": 1,
        "files": 2,
    }
)

file1 = ResultFile(filename="file1", size=100, extension="parquet")
file2 = ResultFile(filename="file2", size=100, extension="parquet")


def cache_transform(transform: TransformRequest,
                    completed_status: TransformStatus, data_dir: str,
                    file_list: List[str],
                    signed_urls) -> TransformedResults:
    return TransformedResults(
        hash=transform.compute_hash(),
        title=transform.title,
        codegen=transform.codegen,
        request_id=completed_status.request_id,
        submit_time=completed_status.submit_time,
        data_dir=data_dir,
        file_list=file_list,
        signed_url_list=signed_urls,
        files=completed_status.files,
        result_format=transform.result_format,
        log_url=completed_status.log_url
    )


@pytest.mark.asyncio
async def test_submit(mocker):
    servicex = AsyncMock()
    servicex.submit_transform = AsyncMock()
    servicex.submit_transform.return_value = {"request_id": '123-456-789"'}
    servicex.get_transform_status = AsyncMock()
    servicex.get_transform_status.side_effect = [
        transform_status1,
        transform_status2,
        transform_status3,
    ]

    mock_minio = AsyncMock()
    mock_minio.list_bucket = AsyncMock(side_effect=[[file1], [file1, file2]])
    mock_minio.download_file = AsyncMock(side_effect=lambda a, _, shorten_filename: PurePath(a))

    mock_cache = mocker.MagicMock(QueryCache)
    mock_cache.get_transform_by_hash = mocker.MagicMock(return_value=None)
    mock_cache.cache_transform = mocker.MagicMock(side_effect=cache_transform)
    mock_cache.cache_path_for_transform = mocker.MagicMock(return_value=PurePath('.'))
    mocker.patch("servicex.minio_adapter.MinioAdapter", return_value=mock_minio)
    did = FileListDataset("/foo/bar/baz.root")
    datasource = GenericQuery(
        dataset_identifier=did,
        title="ServiceX Client",
        codegen="uproot",
        sx_adapter=servicex,
        query_cache=mock_cache,
        config=Configuration(api_endpoints=[]),
    )
    datasource.query_string_generator = FuncADLQuery_Uproot()
    with ExpandableProgress(display_progress=False) as progress:
        datasource.result_format = ResultFormat.parquet
        result = await datasource.submit_and_download(signed_urls_only=False,
                                                      expandable_progress=progress)
        print(mock_minio.download_file.call_args)
    assert result.file_list == ['file1', 'file2']


@pytest.mark.asyncio
async def test_submit_partial_success(mocker):
    servicex = AsyncMock()
    servicex.submit_transform = AsyncMock()
    servicex.submit_transform.return_value = {"request_id": '123-456-789"'}
    servicex.get_transform_status = AsyncMock()
    servicex.get_transform_status.side_effect = [
        transform_status1,
        transform_status2,
        transform_status6,
    ]

    mock_minio = AsyncMock()
    mock_minio.list_bucket = AsyncMock(side_effect=[[file1], [file1]])
    mock_minio.download_file = AsyncMock(side_effect=lambda a, _, shorten_filename: PurePath(a))

    mock_cache = mocker.MagicMock(QueryCache)
    mock_cache.get_transform_by_hash = mocker.MagicMock(return_value=None)
    mock_cache.cache_transform = mocker.MagicMock(side_effect=cache_transform)
    mock_cache.cache_path_for_transform = mocker.MagicMock(return_value=PurePath('.'))
    mocker.patch("servicex.minio_adapter.MinioAdapter", return_value=mock_minio)
    did = FileListDataset("/foo/bar/baz.root")
    datasource = GenericQuery(
        dataset_identifier=did,
        title="ServiceX Client",
        codegen="uproot",
        sx_adapter=servicex,
        query_cache=mock_cache,
        config=Configuration(api_endpoints=[]),
    )
    datasource.query_string_generator = FuncADLQuery_Uproot()
    with ExpandableProgress(display_progress=False) as progress:
        datasource.result_format = ResultFormat.parquet
        result = await datasource.submit_and_download(signed_urls_only=False,
                                                      expandable_progress=progress)
        print(mock_minio.download_file.call_args)
    assert result.file_list == ['file1']


@pytest.mark.asyncio
async def test_submit_cancel(mocker):
    servicex = AsyncMock()
    servicex.submit_transform = AsyncMock()
    servicex.submit_transform.return_value = {"request_id": '123-456-789"'}
    servicex.get_transform_status = AsyncMock()
    servicex.get_transform_status.side_effect = [
        transform_status1,
        transform_status4,
    ]

    mock_minio = AsyncMock()
    mock_minio.list_bucket = AsyncMock(side_effect=[[file1], [file1]])
    mock_minio.download_file = AsyncMock(side_effect=lambda a, _, shorten_filename: PurePath(a))

    mock_cache = mocker.MagicMock(QueryCache)
    mock_cache.get_transform_by_hash = mocker.MagicMock(return_value=None)
    mock_cache.cache_transform = mocker.MagicMock(side_effect=cache_transform)
    mock_cache.cache_path_for_transform = mocker.MagicMock(return_value=PurePath('.'))
    mocker.patch("servicex.minio_adapter.MinioAdapter", return_value=mock_minio)
    did = FileListDataset("/foo/bar/baz.root")
    datasource = GenericQuery(
        dataset_identifier=did,
        title="ServiceX Client",
        codegen="uproot",
        sx_adapter=servicex,
        query_cache=mock_cache,
        config=Configuration(api_endpoints=[]),
    )
    datasource.query_string_generator = FuncADLQuery_Uproot()
    with ExpandableProgress(display_progress=False) as progress:
        datasource.result_format = ResultFormat.parquet
        with pytest.raises(ServiceXException):
            _ = await datasource.submit_and_download(signed_urls_only=False,
                                                     expandable_progress=progress)


@pytest.mark.asyncio
async def test_submit_fatal(mocker):
    servicex = AsyncMock()
    servicex.submit_transform = AsyncMock()
    servicex.submit_transform.return_value = {"request_id": '123-456-789"'}
    servicex.get_transform_status = AsyncMock()
    servicex.get_transform_status.side_effect = [
        transform_status1,
        transform_status5,
    ]

    mock_minio = AsyncMock()
    mock_minio.list_bucket = AsyncMock(side_effect=[[file1], [file1]])
    mock_minio.download_file = AsyncMock(side_effect=lambda a, _, shorten_filename: PurePath(a))

    mock_cache = mocker.MagicMock(QueryCache)
    mock_cache.get_transform_by_hash = mocker.MagicMock(return_value=None)
    mock_cache.cache_transform = mocker.MagicMock(side_effect=cache_transform)
    mock_cache.cache_path_for_transform = mocker.MagicMock(return_value=PurePath('.'))
    mocker.patch("servicex.minio_adapter.MinioAdapter", return_value=mock_minio)
    did = FileListDataset("/foo/bar/baz.root")
    datasource = GenericQuery(
        dataset_identifier=did,
        title="ServiceX Client",
        codegen="uproot",
        sx_adapter=servicex,
        query_cache=mock_cache,
        config=Configuration(api_endpoints=[]),
    )
    datasource.query_string_generator = FuncADLQuery_Uproot()
    with ExpandableProgress(display_progress=False) as progress:
        datasource.result_format = ResultFormat.parquet
        with pytest.raises(ServiceXException):
            _ = await datasource.submit_and_download(signed_urls_only=False,
                                                     expandable_progress=progress)


@pytest.mark.asyncio
async def test_submit_generic(mocker):
    """ Uses Uproot-Raw classes which go through the generic query mechanism """
    import json
    sx = AsyncMock()
    sx.submit_transform = AsyncMock()
    sx.submit_transform.return_value = {"request_id": '123-456-789"'}
    sx.get_transform_status = AsyncMock()
    sx.get_transform_status.side_effect = [
        transform_status1,
        transform_status2,
        transform_status3,
    ]

    mock_minio = AsyncMock()
    mock_minio.list_bucket = AsyncMock(side_effect=[[file1], [file1, file2]])
    mock_minio.download_file = AsyncMock()

    mock_cache = mocker.MagicMock(QueryCache)
    mocker.patch("servicex.minio_adapter.MinioAdapter", return_value=mock_minio)
    did = FileListDataset("/foo/bar/baz.root")
    client = ServiceXClient(backend='servicex-uc-af', config_path='tests/example_config.yaml')
    client.servicex = sx
    client.query_cache = mock_cache

    datasource = client.generic_query(
        dataset_identifier=did,
        query=UprootRawQuery({'treename': 'CollectionTree'})
    )
    with ExpandableProgress(display_progress=False) as progress:
        datasource.result_format = ResultFormat.parquet
        _ = await datasource.submit_and_download(signed_urls_only=False,
                                                 expandable_progress=progress)

    # same thing but a list argument to UprootRawQuery (UprootRawQuery test...)
    datasource = client.generic_query(
        dataset_identifier=did,
        query=UprootRawQuery({'treename': 'CollectionTree'})
    )
    with ExpandableProgress(display_progress=False) as progress:
        datasource.result_format = ResultFormat.parquet
        _ = await datasource.submit_and_download(signed_urls_only=False,
                                                 expandable_progress=progress)
    assert isinstance(json.loads(datasource.generate_selection_string()), list)


def test_transform_request():
    servicex = AsyncMock()
    did = FileListDataset("/foo/bar/baz.root")
    with tempfile.TemporaryDirectory() as temp_dir:
        config = Configuration(cache_path=temp_dir, api_endpoints=[])
        cache = QueryCache(config)
        datasource = GenericQuery(
            dataset_identifier=did,
            title="ServiceX Client",
            codegen="uproot",
            sx_adapter=servicex,
            query_cache=None,
            config=Configuration(api_endpoints=[]),
        )
        datasource.query_string_generator = (FuncADLQuery_Uproot()
                                             .FromTree("nominal")
                                             .Select(lambda e: {"lep_pt": e["lep_pt"]}))

        q = (
            datasource.set_result_format(ResultFormat.parquet)
            .transform_request
        )
        assert q.selection == "(call Select (call EventDataset 'bogus.root' 'nominal') " \
                              "(lambda (list e) (dict (list 'lep_pt') " \
                              "(list (subscript e 'lep_pt')))))"
        cache.close()


def test_type():
    "Test that the item type for a dataset is correctly propagated"

    class my_type_info:
        "typespec for possible event type"

        def fork_it_over(self) -> int:
            ...

    datasource = FuncADLQuery[my_type_info](
        item_type=my_type_info
    )

    assert datasource.item_type == my_type_info


def test_type_any():
    "Test the type is any if no type is given"
    datasource = FuncADLQuery()
    assert datasource.item_type == Any
