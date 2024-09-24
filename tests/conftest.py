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

from pytest_asyncio import fixture
from servicex.python_dataset import PythonFunction
from servicex.query_core import Query
from servicex.models import (
    TransformRequest,
    ResultDestination,
    ResultFormat,
    TransformStatus,
    TransformedResults,
)

from servicex.dataset_identifier import FileListDataset
from servicex.minio_adapter import MinioAdapter

import pandas as pd
import os


@fixture
def transform_request() -> TransformRequest:
    return TransformRequest(
        title="Test submission",
        did="rucio://foo.bar",
        selection="(call EventDataset)",
        codegen="uproot",
        result_destination=ResultDestination.object_store,  # type: ignore
        result_format=ResultFormat.parquet,  # type: ignore
    )  # type: ignore


@fixture
def minio_adapter() -> MinioAdapter:
    return MinioAdapter("localhost", False, "access_key", "secret_key", "bucket")


@fixture
def python_dataset(dummy_parquet_file):
    did = FileListDataset(dummy_parquet_file)
    dataset = Query(
        title="Test submission",
        dataset_identifier=did,
        codegen="uproot",
        result_format=ResultFormat.parquet,
        sx_adapter=None,  # type: ignore
        config=None,  # type: ignore
        query_cache=None  # type: ignore
    )  # type: ignore

    def foo():
        return

    dataset.query_string_generator = PythonFunction(foo)
    return dataset


@fixture
def transformed_result_python_dataset(dummy_parquet_file) -> TransformedResults:
    return TransformedResults(
        hash="289e90f6fe3780253af35c428b784ac22d3ee9200a7581b8f0a9bdcc5ae93479",
        title="Test submission",
        codegen="uproot",
        request_id="b8c508d0-ccf2-4deb-a1f7-65c839eebabf",
        submit_time=datetime.now(),
        data_dir="/foo/bar",
        file_list=[dummy_parquet_file],
        signed_url_list=[],
        files=1,
        result_format=ResultFormat.parquet,
    )


@fixture
def transform_status_response() -> dict:
    return {
        "requests": [
            {
                "request_id": "b8c508d0-ccf2-4deb-a1f7-65c839eebabf",
                "did": "File List Provided in Request",
                "columns": None,
                "selection": "(Where (SelectMany (call EventDataset) (lambda (list e) (call (attr e 'Jets') 'AntiKt4EMTopoJets'))) (lambda (list j) (and (> (/ (call (attr j 'pt')) 1000) 20) (< (call abs (/ (call (attr j 'eta')) 1000)) 4.5))))",  # NOQA
                "tree-name": None,
                "image": "sslhep/servicex_func_adl_uproot_transformer:uproot4",
                "workers": None,
                "result-destination": "object-store",
                "result-format": "parquet",
                "workflow-name": "selection_codegen",
                "generated-code-cm": "b8c508d0-ccf2-4deb-a1f7-65c839eebabf-generated-source",  # NOQA
                "status": "Submitted",
                "failure-info": None,
                "app-version": "develop",
                "code-gen-image": "sslhep/servicex_code_gen_func_adl_uproot:v1.2.0",
                "files": 1,
                "files-completed": 0,
                "files-failed": 0,
                "files-remaining": 1,
                "submit-time": "2023-05-25T20:05:05.564137Z",
                "finish-time": "None",
            }
        ]
    }


@fixture
def completed_status() -> TransformStatus:
    return TransformStatus(
        **{
            "request_id": "b8c508d0-ccf2-4deb-a1f7-65c839eebabf",
            "did": "File List Provided in Request",
            "columns": None,
            "selection": "(Where (SelectMany (call EventDataset) (lambda (list e) (call (attr e 'Jets') 'AntiKt4EMTopoJets'))) (lambda (list j) (and (> (/ (call (attr j 'pt')) 1000) 20) (< (call abs (/ (call (attr j 'eta')) 1000)) 4.5))))",  # NOQA
            "tree-name": None,
            "image": "sslhep/servicex_func_adl_uproot_transformer:uproot4",
            "workers": None,
            "result-destination": "object-store",
            "result-format": "parquet",
            "workflow-name": "selection_codegen",
            "generated-code-cm": "b8c508d0-ccf2-4deb-a1f7-65c839eebabf-generated-source",
            "status": "Submitted",
            "failure-info": None,
            "app-version": "develop",
            "code-gen-image": "sslhep/servicex_code_gen_func_adl_uproot:v1.2.0",
            "files": 1,
            "files-completed": 0,
            "files-failed": 0,
            "files-remaining": 1,
            "submit-time": "2023-05-25T20:05:05.564137Z",
            "finish-time": None,
            "minio-endpoint": "minio.org:9000",
            "minio-secured": False,
            "minio-access-key": "miniouser",
            "minio-secret-key": "secret",
        }
    )


@fixture
def transformed_result(dummy_parquet_file) -> TransformedResults:
    return TransformedResults(
        hash="123-4455",
        title="Test",
        codegen="uproot",
        request_id="123-45-6789",
        submit_time=datetime.now(),
        data_dir="/foo/bar",
        file_list=[dummy_parquet_file],
        signed_url_list=[],
        files=1,
        result_format=ResultFormat.parquet,
    )


@fixture
def transformed_result_signed_url() -> TransformedResults:
    return TransformedResults(
        hash="123-4455",
        title="Test",
        codegen="uproot",
        request_id="123-45-6789",
        submit_time=datetime.now(),
        data_dir="/foo/bar",
        file_list=[],
        signed_url_list=['https://dummy.junk.io/1.parquet', 'https://dummy.junk.io/2.parquet'],
        files=2,
        result_format=ResultFormat.root_ttree,
    )


@fixture
def dummy_parquet_file():
    data = {'column1': [1, 2, 3, 4],
            'column2': ['A', 'B', 'C', 'D']}
    df = pd.DataFrame(data)
    parquet_file_path = '1.parquet'
    df.to_parquet(parquet_file_path, index=False)

    yield parquet_file_path

    if os.path.exists(parquet_file_path):
        os.remove(parquet_file_path)


@fixture
def codegen_list():
    return {'atlasr21': 'http://servicex-code-gen-atlasr21:8000',
            'atlasr22': 'http://servicex-code-gen-atlasr22:8000',
            'atlasxaod': 'http://servicex-code-gen-atlasxaod:8000',
            'cms': 'http://servicex-code-gen-cms:8000',
            'cmssw-5-3-32': 'http://servicex-code-gen-cmssw-5-3-32:8000',
            'python': 'http://servicex-code-gen-python:8000',
            'uproot': 'http://servicex-code-gen-uproot:8000',
            'uproot-raw': 'http://servicex-code-gen-uproot-raw:8000'}
