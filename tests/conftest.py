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
from datetime import datetime

from pytest_asyncio import fixture

from servicex_client.models import (
    TransformRequest,
    ResultDestination,
    ResultFormat,
    TransformStatus,
    TransformedResults,
)


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
            "finish-time": "None",
            "minio-endpoint": "minio.org:9000",
            "minio-secured": False,
            "minio-access-key": "miniouser",
            "minio-secret-key": "secret",
        }
    )


@fixture
def transformed_result() -> TransformedResults:
    return TransformedResults(
        hash="123-4455",
        title="Test",
        codegen="uproot",
        request_id="123-45-6789",
        submit_time=datetime.now(),
        data_dir="/foo/bar",
        file_list=["/tmp/1.root", "/tmp/2.root"],
        signed_url_list=[],
        files=2,
        result_format=ResultFormat.root_file,
    )
