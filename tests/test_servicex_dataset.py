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
from typing import Any
from unittest.mock import AsyncMock

import pytest

from servicex.configuration import Configuration
from servicex.dataset_identifier import FileListDataset
from servicex.expandable_progress import ExpandableProgress
from servicex.func_adl.func_adl_dataset import FuncADLQuery
from servicex.models import TransformStatus, Status, ResultFile, ResultFormat
from servicex.query_cache import QueryCache

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
    }
)

transform_status1 = transform_status.copy(
    update={
        "status": Status.running,
        "files-remaining": None,
        "files-completed": 0,
        "files": 0,
    }
)
transform_status2 = transform_status.copy(
    update={
        "status": Status.running,
        "files-remaining": 1,
        "files-completed": 1,
        "files": 2,
    }
)
transform_status3 = transform_status.copy(
    update={
        "status": Status.complete,
        "files-remaining": 0,
        "files-completed": 2,
        "files": 2,
    }
)

file1 = ResultFile(filename="file1", size=100, extension="parquet")
file2 = ResultFile(filename="file2", size=100, extension="parquet")


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
    mock_minio.download_file = AsyncMock()

    mock_cache = mocker.MagicMock(QueryCache)
    mocker.patch("servicex.minio_adapter.MinioAdapter", return_value=mock_minio)
    did = FileListDataset("/foo/bar/baz.root")
    datasource = FuncADLQuery(
        dataset_identifier=did,
        codegen="uproot",
        sx_adapter=servicex,
        query_cache=mock_cache,
    )
    with ExpandableProgress(display_progress=False) as progress:
        datasource.result_format = ResultFormat.parquet
        _ = await datasource.submit_and_download(signed_urls_only=False,
                                                 expandable_progress=progress)
        print(mock_minio.download_file.call_args)


def test_transform_request():
    servicex = AsyncMock()
    did = FileListDataset("/foo/bar/baz.root")
    with tempfile.TemporaryDirectory() as temp_dir:
        config = Configuration(cache_path=temp_dir, api_endpoints=[])
        cache = QueryCache(config)
        datasource = FuncADLQuery(
            dataset_identifier=did,
            codegen="uproot",
            sx_adapter=servicex,
            config=config,
            query_cache=cache,
        )

        q = (
            datasource.Select(lambda e: {"lep_pt": e["lep_pt"]})
            .set_result_format(ResultFormat.parquet)
            .transform_request
        )
        print("Qastle is ", q)
        cache.close()


def test_type():
    "Test that the item type for a dataset is correctly propagated"

    class my_type_info:
        "typespec for possible event type"

        def fork_it_over(self) -> int:
            ...

    did = FileListDataset("/foo/bar/baz.root")
    datasource = FuncADLQuery[my_type_info](
        dataset_identifier=did, codegen="uproot", item_type=my_type_info
    )

    assert datasource.item_type == my_type_info


def test_type_any():
    "Test the type is any if no type is given"
    did = FileListDataset("/foo/bar/baz.root")
    datasource = FuncADLQuery(dataset_identifier=did, codegen="uproot")
    assert datasource.item_type == Any
