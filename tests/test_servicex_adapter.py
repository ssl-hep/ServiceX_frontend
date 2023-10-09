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
import math
import os
import tempfile
from unittest.mock import patch

import httpx
import pytest
from pytest_asyncio import fixture

from servicex.models import TransformRequest, ResultDestination, ResultFormat
from servicex.servicex_adapter import ServiceXAdapter, AuthorizationError


@fixture
def servicex():
    return ServiceXAdapter("https://servicex.org")


def test_result_formats():
    """
    This test is just to make sure the enum string representations match the values
    expected by the service. Compare this to the json parser in
    servicex.resources.transformation.submit.SubmitTransformationRequest.make_api
    """
    assert ResultFormat.parquet == "parquet"
    assert ResultFormat.root == "root-file"


@pytest.mark.asyncio
@patch('servicex.servicex_adapter.httpx.AsyncClient.get')
async def test_get_transforms(get, servicex, transform_status_response):
    get.return_value = httpx.Response(200, json=transform_status_response)
    t = await servicex.get_transforms()
    assert len(t) == 1
    assert t[0].request_id == "b8c508d0-ccf2-4deb-a1f7-65c839eebabf"
    get.assert_called_with(url='https://servicex.org/servicex/transformation', headers={})


@pytest.mark.asyncio
@patch('servicex.servicex_adapter.httpx.AsyncClient.get')
async def test_get_transforms_auth_error(get, servicex):
    with pytest.raises(AuthorizationError):
        get.return_value = httpx.Response(401)
        await servicex.get_transforms()


@pytest.mark.asyncio
@patch('servicex.servicex_adapter.httpx.AsyncClient.get')
@patch('servicex.servicex_adapter.httpx.AsyncClient.post')
@patch('servicex.servicex_adapter.jwt.decode')
async def test_get_transforms_wlcg_bearer_token(decode, post, get, servicex,
                                                transform_status_response):
    token_file = tempfile.NamedTemporaryFile(mode="w+t", delete=False)
    token_file.write("luckycharms")
    token_file.close()

    os.environ['BEARER_TOKEN_FILE'] = token_file.name

    get.return_value = httpx.Response(200, json=transform_status_response)
    decode.return_value = {'exp': math.inf}
    await servicex.get_transforms()

    # Try with an expired token
    with pytest.raises(AuthorizationError):
        decode.return_value = {'exp': 0.0}
        await servicex.get_transforms()

    os.remove(token_file.name)
    del os.environ['BEARER_TOKEN_FILE']


@pytest.mark.asyncio
@patch('servicex.servicex_adapter.httpx.AsyncClient.post')
@patch('servicex.servicex_adapter.httpx.AsyncClient.get')
async def test_get_transforms_with_refresh(get, post, transform_status_response):
    servicex = ServiceXAdapter(url="https://servicex.org", refresh_token="refrescas")
    post.return_value = httpx.Response(200, json={"access_token": "luckycharms"})
    get.return_value = httpx.Response(200, json=transform_status_response)
    await servicex.get_transforms()

    post.assert_called_with('https://servicex.org/token/refresh',
                            headers={'Authorization': 'Bearer refrescas'}, json=None)

    get.assert_called_with(url='https://servicex.org/servicex/transformation',
                           headers={'Authorization': 'Bearer luckycharms'})


@patch('servicex.servicex_adapter.httpx.Client.get')
def test_get_codegens(get, servicex):
    get.return_value = httpx.Response(200, json={
        "uproot": "http://uproot-codegen",
        "xaod": "http://xaod-codegen"
    })
    c = servicex.get_code_generators()
    assert len(c) == 2
    assert c["uproot"] == "http://uproot-codegen"


@pytest.mark.asyncio
@patch('servicex.servicex_adapter.httpx.AsyncClient.post')
async def test_submit(post, servicex):
    post.return_value = httpx.Response(200, json={"request_id": "123-456-789"})
    request = TransformRequest(
        title="Test submission",
        did="rucio://foo.bar",
        selection="(call EventDataset)",
        codegen="uproot",
        result_destination=ResultDestination.object_store,
        result_format=ResultFormat.parquet
    )
    result = await servicex.submit_transform(request)
    assert result == "123-456-789"


@pytest.mark.asyncio
@patch('servicex.servicex_adapter.httpx.AsyncClient.get')
async def test_get_transform_status(get, servicex, transform_status_response):
    get.return_value = httpx.Response(200, json=transform_status_response['requests'][0])
    result = await servicex.get_transform_status("b8c508d0-ccf2-4deb-a1f7-65c839eebabf")
    assert result.request_id == "b8c508d0-ccf2-4deb-a1f7-65c839eebabf"


@pytest.mark.asyncio
@patch('servicex.servicex_adapter.httpx.AsyncClient.get')
async def test_get_transform_status_auth_error(get, servicex):
    with pytest.raises(AuthorizationError):
        get.return_value = httpx.Response(401)
        await servicex.get_transform_status("b8c508d0-ccf2-4deb-a1f7-65c839eebabf")
