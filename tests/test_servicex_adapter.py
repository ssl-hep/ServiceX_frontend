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
    assert ResultFormat.root_ttree == "root-file"


@pytest.mark.asyncio
@patch("servicex.servicex_adapter.RetryClient.get")
async def test_get_transforms(mock_get, servicex, transform_status_response):
    mock_get.return_value.__aenter__.return_value.json.return_value = transform_status_response
    mock_get.return_value.__aenter__.return_value.status = 200
    t = await servicex.get_transforms()
    assert len(t) == 1
    assert t[0].request_id == "b8c508d0-ccf2-4deb-a1f7-65c839eebabf"
    mock_get.assert_called_with(url='https://servicex.org/servicex/transformation', headers={})


@pytest.mark.asyncio
@patch("servicex.servicex_adapter.RetryClient.get")
async def test_get_transforms_auth_error(mock_get, servicex):
    with pytest.raises(AuthorizationError) as err:
        mock_get.return_value.__aenter__.return_value.status = 401
        await servicex.get_transforms()
        assert "Not authorized to access serviceX at" in str(err.value)


@pytest.mark.asyncio
@patch('servicex.servicex_adapter.jwt.decode')
async def test_get_transforms_wlcg_bearer_token(decode,
                                                servicex,
                                                transform_status_response):
    token_file = tempfile.NamedTemporaryFile(mode="w+t", delete=False)
    token_file.write("luckycharms")
    token_file.close()

    os.environ['BEARER_TOKEN_FILE'] = token_file.name

    # Try with an expired token
    with pytest.raises(AuthorizationError) as err:
        decode.return_value = {'exp': 0.0}
        await servicex.get_transforms()
        assert "ServiceX access token request rejected:" in str(err.value)

    os.remove(token_file.name)
    del os.environ['BEARER_TOKEN_FILE']


@pytest.mark.asyncio
@patch('servicex.servicex_adapter.RetryClient.post')
@patch('servicex.servicex_adapter.RetryClient.get')
async def test_get_transforms_with_refresh(get, post, transform_status_response):
    servicex = ServiceXAdapter(url="https://servicex.org", refresh_token="refrescas")
    post.return_value.__aenter__.return_value.json.return_value = {"access_token": "luckycharms"}
    post.return_value.__aenter__.return_value.status = 200
    get.return_value.__aenter__.return_value.json.return_value = transform_status_response
    get.return_value.__aenter__.return_value.status = 200
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


@patch('servicex.servicex_adapter.httpx.Client.get')
def test_get_codegens_error(get, servicex):
    get.return_value = httpx.Response(403)
    with pytest.raises(AuthorizationError) as err:
        servicex.get_code_generators()
        assert "Not authorized to access serviceX at" in str(err.value)


@pytest.mark.asyncio
@patch('servicex.servicex_adapter.RetryClient.post')
async def test_submit(post, servicex):
    post.return_value.__aenter__.return_value.json.return_value = {"request_id": "123-456-789"}
    post.return_value.__aenter__.return_value.status = 200
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
@patch('servicex.servicex_adapter.RetryClient.post')
async def test_submit_errors(post, servicex):
    post.return_value.__aenter__.return_value.status = 401
    request = TransformRequest(
        title="Test submission",
        did="rucio://foo.bar",
        selection="(call EventDataset)",
        codegen="uproot",
        result_destination=ResultDestination.object_store,
        result_format=ResultFormat.parquet
    )
    with pytest.raises(AuthorizationError) as err:
        await servicex.submit_transform(request)
        assert "Not authorized to access serviceX at" in str(err.value)

    post.return_value.__aenter__.return_value.json.return_value = {"message": "error_message"}
    post.return_value.__aenter__.return_value.status = 400
    with pytest.raises(ValueError) as err:
        await servicex.submit_transform(request)
        assert "Invalid transform request: error_message" == str(err.value)

    post.return_value.__aenter__.return_value.json.return_value = {"message": "error_message"}
    post.return_value.__aenter__.return_value.status = 410
    with pytest.raises(RuntimeError) as err:
        await servicex.submit_transform(request)
        assert "ServiceX WebAPI Error during transformation submission: 410 - error_message" \
               == str(err.value)


@pytest.mark.asyncio
@patch('servicex.servicex_adapter.RetryClient.get')
async def test_get_transform_status(get, servicex, transform_status_response):
    get.return_value.__aenter__.return_value.json.return_value = transform_status_response['requests'][0]  # NOQA: E501
    get.return_value.__aenter__.return_value.status = 200
    result = await servicex.get_transform_status("b8c508d0-ccf2-4deb-a1f7-65c839eebabf")
    assert result.request_id == "b8c508d0-ccf2-4deb-a1f7-65c839eebabf"


@pytest.mark.asyncio
@patch('servicex.servicex_adapter.RetryClient.get')
async def test_get_transform_status_auth_error(get, servicex):
    with pytest.raises(AuthorizationError) as err:
        get.return_value.__aenter__.return_value.status = 401
        await servicex.get_transform_status("b8c508d0-ccf2-4deb-a1f7-65c839eebabf")
        assert "Not authorized to access serviceX at " in str(err.value)

    with pytest.raises(ValueError) as err:
        get.return_value.__aenter__.return_value.status = 404
        await servicex.get_transform_status("b8c508d0-ccf2-4deb-a1f7-65c839eebabf")
        assert "Transform ID b8c508d0-ccf2-4deb-a1f7-65c839eebabf not found" == str(err.value)


@pytest.mark.asyncio
async def test_get_authorization(servicex):
    servicex.token = "token"
    r = await servicex._get_authorization()
    assert r.get("Authorization") == "Bearer token"
