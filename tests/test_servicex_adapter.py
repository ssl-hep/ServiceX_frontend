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
import time
from unittest.mock import patch, MagicMock, AsyncMock
import datetime

import httpx
import pytest
from json import JSONDecodeError
from pytest_asyncio import fixture

from servicex.models import (
    TransformRequest,
    ResultDestination,
    ResultFormat,
    ServiceXInfo,
)
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
@patch("servicex.servicex_adapter.AsyncClient.get")
async def test_get_transforms(mock_get, servicex, transform_status_response):
    mock_get.return_value = MagicMock()
    mock_get.return_value.json.return_value = transform_status_response
    mock_get.return_value.status_code = 200
    t = await servicex.get_transforms()
    assert len(t) == 1
    assert t[0].request_id == "b8c508d0-ccf2-4deb-a1f7-65c839eebabf"
    mock_get.assert_called_with(
        url="https://servicex.org/servicex/transformation", headers={}
    )


@pytest.mark.asyncio
@patch("servicex.servicex_adapter.AsyncClient.get")
async def test_get_transforms_error(mock_get, servicex, transform_status_response):
    mock_get.return_value = MagicMock()
    mock_get.return_value.json.return_value = {"message": "error_message"}
    mock_get.return_value.status_code = 500
    with pytest.raises(RuntimeError) as err:
        await servicex.get_transforms()
        assert (
            "ServiceX WebAPI Error during transformation submission: 500 - error_message"
            == str(err.value)
        )


@pytest.mark.asyncio
@patch("servicex.servicex_adapter.AsyncClient.get")
async def test_get_transforms_auth_error(mock_get, servicex):
    with pytest.raises(AuthorizationError) as err:
        mock_get.return_value.status_code = 401
        await servicex.get_transforms()
        assert "Not authorized to access serviceX at" in str(err.value)


@pytest.mark.asyncio
@patch("servicex.servicex_adapter.AsyncClient.get")
@patch("servicex.servicex_adapter.AsyncClient.post")
@patch("servicex.servicex_adapter.jwt.decode")
async def test_get_transforms_wlcg_bearer_token(
    decode, post, http_get, servicex, transform_status_response
):
    post.return_value.json.return_value = {"access_token": "luckycharms"}
    post.return_value.status_code = 401
    http_get.return_value.__aenter__.return_value.json.return_value = (
        transform_status_response
    )
    http_get.return_value.__aenter__.return_value.status = 200
    servicex.get_servicex_capabilities = AsyncMock(return_value=[])
    post.return_value.__aenter__.return_value.json.return_value = {
        "access_token": "luckycharms"
    }
    post.return_value.__aenter__.return_value.status = 401
    token_file = tempfile.NamedTemporaryFile(mode="w+t", delete=False)
    token_file.write(
        """"
    eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ.SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c
    """
    )
    token_file.close()

    with patch.dict(os.environ, {"BEARER_TOKEN_FILE": token_file.name}):
        # Try with no expiration at all
        with pytest.raises(RuntimeError):
            await servicex.get_transforms()

        # Try with an expired token
        with pytest.raises(AuthorizationError) as err:
            decode.return_value = {"exp": 0.0}
            await servicex.get_transforms()
            assert "ServiceX access token request rejected:" in str(err.value)

    os.remove(token_file.name)


@pytest.mark.asyncio
@patch("servicex.servicex_adapter.AsyncClient.post")
@patch("servicex.servicex_adapter.AsyncClient.get")
async def test_get_transforms_with_refresh(get, post, transform_status_response):
    servicex = ServiceXAdapter(url="https://servicex.org", refresh_token="refrescas")
    post.return_value = MagicMock()
    post.return_value.json.return_value = {"access_token": "luckycharms"}
    post.return_value.status_code = 200
    get.return_value = MagicMock()
    get.return_value.json.return_value = transform_status_response
    get.return_value.status_code = 200
    await servicex.get_transforms()

    post.assert_called_with(
        "https://servicex.org/token/refresh",
        headers={"Authorization": "Bearer refrescas"},
        json=None,
    )

    get.assert_called_with(
        url="https://servicex.org/servicex/transformation",
        headers={"Authorization": "Bearer luckycharms"},
    )


@patch("servicex.servicex_adapter.AsyncClient.get")
async def test_get_codegens(get, servicex):
    get.return_value = httpx.Response(
        200,
        json={
            "app-version": "0.0.0",
            "code-gen-image": {
                "uproot": "http://uproot-codegen",
                "xaod": "http://xaod-codegen",
            },
            "capabilities": [],
        },
    )
    c = await servicex.get_code_generators_async()
    assert len(c) == 2
    assert c["uproot"] == "http://uproot-codegen"


@patch("servicex.servicex_adapter.AsyncClient.get")
async def test_get_codegens_error(get, servicex):
    get.return_value = httpx.Response(403)
    with pytest.raises(AuthorizationError) as err:
        await servicex.get_code_generators()
        assert "Not authorized to access serviceX at" in str(err.value)


@fixture
def dataset():
    return {
        "id": 123,
        "name": "rucio://user.mtost:user.mtost.700349.Sh.DAOD_PHYS.e8351_s3681_r13144_r13146_p6026.Jul13_less_jet_and_new_GN?files=7",  # NOQA: E501
        "did_finder": "rucio",
        "n_files": 7,
        "size": 1359895862,
        "events": 0,
        "last_used": "2024-11-12T01:59:19.161655Z",
        "last_updated": "1969-12-31T18:00:00.000000Z",
        "lookup_status": "complete",
        "is_stale": False,
        "files": [
            {
                "id": 12,
                "adler32": "62c594d4",
                "file_size": 34831129,
                "file_events": 0,
                "paths": "https://xenia.nevis.columbia.edu:1094/atlas/dq2/rucio/user/mtost/06/a1/user.mtost.40294033._000002.less_jet_and_new_GN.root",  # NOQA: E501
            }
        ],
    }


@pytest.mark.asyncio
@patch("servicex.servicex_adapter.AsyncClient.get")
async def test_get_datasets(get, servicex, dataset):
    get.return_value = MagicMock()
    get.return_value.json.return_value = {"datasets": [dataset]}
    get.return_value.status_code = 200

    c = await servicex.get_datasets()
    assert len(c) == 1
    assert c[0].id == 123
    get.assert_called_with(
        url="https://servicex.org/servicex/datasets", params={}, headers={}
    )


@pytest.mark.asyncio
@patch("servicex.servicex_adapter.AsyncClient.get")
async def test_get_datasets_show_deleted(get, servicex, dataset):
    get.return_value = MagicMock()
    get.return_value.json.return_value = {"datasets": [dataset]}
    get.return_value.status_code = 200
    c = await servicex.get_datasets(show_deleted=True)
    assert len(c) == 1
    assert c[0].id == 123
    get.assert_called_with(
        url="https://servicex.org/servicex/datasets",
        params={"show-deleted": True},
        headers={},
    )


@pytest.mark.asyncio
@patch("servicex.servicex_adapter.AsyncClient.get")
async def test_get_datasets_auth_error(get, servicex):
    get.return_value.status_code = 403
    with pytest.raises(AuthorizationError) as err:
        await servicex.get_datasets()
    assert "Not authorized to access serviceX at" in str(err.value)


@pytest.mark.asyncio
@patch("servicex.servicex_adapter.AsyncClient.get")
async def test_get_datasets_miscellaneous_error(get, servicex):
    get.return_value = MagicMock()
    get.return_value.status_code = 500
    with pytest.raises(RuntimeError) as err:
        await servicex.get_datasets()
    assert "Failed to get datasets" in str(err.value)


@pytest.mark.asyncio
@patch("servicex.servicex_adapter.AsyncClient.get")
async def test_get_dataset(get, servicex, dataset):
    get.return_value = MagicMock()
    get.return_value.json.return_value = dataset
    get.return_value.status_code = 200
    c = await servicex.get_dataset(123)
    assert c
    assert c.id == 123


@pytest.mark.asyncio
@patch("servicex.servicex_adapter.AsyncClient.get")
async def test_get_dataset_errors(get, servicex, dataset):
    get.return_value = MagicMock()
    get.return_value.status_code = 403
    with pytest.raises(AuthorizationError) as err:
        await servicex.get_dataset(123)
    assert "Not authorized to access serviceX at" in str(err.value)

    get.return_value.status_code = 404
    with pytest.raises(ValueError) as err:
        await servicex.get_dataset(123)
    assert "Dataset 123 not found" in str(err.value)

    get.return_value.json.side_effect = JSONDecodeError("", "", 0)
    get.return_value.text = "error_message"
    get.return_value.status_code = 500
    with pytest.raises(RuntimeError) as err:
        await servicex.get_dataset(123)
    assert "Failed to get dataset 123 - error_message" in str(err.value)


@pytest.mark.asyncio
@patch("servicex.servicex_adapter.AsyncClient.delete")
async def test_delete_dataset(delete, servicex):
    delete.return_value = MagicMock()
    delete.return_value.json.return_value = {
        "dataset-id": 123,
        "stale": True,
    }
    delete.return_value.status_code = 200

    r = await servicex.delete_dataset(123)
    delete.assert_called_with(
        url="https://servicex.org/servicex/datasets/123", headers={}
    )
    assert r


@pytest.mark.asyncio
@patch("servicex.servicex_adapter.AsyncClient.delete")
async def test_delete_dataset_errors(delete, servicex):
    delete.return_value = MagicMock()
    delete.return_value.status_code = 403
    with pytest.raises(AuthorizationError) as err:
        await servicex.delete_dataset(123)
    assert "Not authorized to access serviceX at" in str(err.value)

    delete.return_value.status_code = 404
    with pytest.raises(ValueError) as err:
        await servicex.delete_dataset(123)
    assert "Dataset 123 not found" in str(err.value)

    delete.return_value.json.side_effect = JSONDecodeError("", "", 0)
    delete.return_value.text = "error_message"
    delete.return_value.status_code = 500
    with pytest.raises(RuntimeError) as err:
        await servicex.delete_dataset(123)
    assert "Failed to delete dataset 123 - error_message" in str(err.value)


@pytest.mark.asyncio
@patch("servicex.servicex_adapter.AsyncClient.delete")
async def test_delete_transform(delete, servicex):
    delete.return_value.status_code = 200
    await servicex.delete_transform("123-45-6789")
    delete.assert_called_with(
        url="https://servicex.org/servicex/transformation/123-45-6789", headers={}
    )


@pytest.mark.asyncio
@patch("servicex.servicex_adapter.AsyncClient.delete")
async def test_delete_transform_errors(delete, servicex):
    delete.return_value = MagicMock()
    delete.return_value.status_code = 403
    with pytest.raises(AuthorizationError) as err:
        await servicex.delete_transform("123-45-6789")
    assert "Not authorized to access serviceX at" in str(err.value)

    delete.return_value.status_code = 404
    with pytest.raises(ValueError) as err:
        await servicex.delete_transform("123-45-6789")
    assert "Transform 123-45-6789 not found" in str(err.value)

    delete.return_value.json.side_effect = JSONDecodeError("", "", 0)
    delete.return_value.text = "error_message"
    delete.return_value.status_code = 500
    with pytest.raises(RuntimeError) as err:
        await servicex.delete_transform("123-45-6789")
    assert "Failed to delete transform 123-45-6789 - error_message" in str(err.value)


@pytest.mark.asyncio
@patch("servicex.servicex_adapter.AsyncClient.get")
async def test_cancel_transform(get, servicex):
    get.return_value.json.return_value = {
        "message": "Canceled transformation request 123"
    }
    get.return_value.status_code = 200

    await servicex.cancel_transform(123)
    get.assert_called_with(
        url="https://servicex.org/servicex/transformation/123/cancel", headers={}
    )


@pytest.mark.asyncio
@patch("servicex.servicex_adapter.AsyncClient.get")
async def test_cancel_transform_errors(get, servicex):
    get.return_value = MagicMock()
    get.return_value.status_code = 403
    with pytest.raises(AuthorizationError) as err:
        await servicex.cancel_transform(123)
    assert "Not authorized to access serviceX at" in str(err.value)

    get.return_value.status_code = 404
    with pytest.raises(ValueError) as err:
        await servicex.cancel_transform(123)
    assert "Transform 123 not found" in str(err.value)

    get.return_value.json.side_effect = JSONDecodeError("", "", 0)
    get.return_value.text = "error_message"
    get.return_value.status_code = 500
    with pytest.raises(RuntimeError) as err:
        await servicex.cancel_transform(123)
    assert "Failed to cancel transform 123 - error_message" in str(err.value)


@pytest.mark.asyncio
@patch("servicex.servicex_adapter.AsyncClient.post")
async def test_submit(post, servicex):
    post.return_value = MagicMock()
    post.return_value.json.return_value = {"request_id": "123-456-789"}
    post.return_value.status_code = 200
    request = TransformRequest(
        title="Test submission",
        did="rucio://foo.bar",
        selection="(call EventDataset)",
        codegen="uproot",
        result_destination=ResultDestination.object_store,
        result_format=ResultFormat.parquet,
    )
    result = await servicex.submit_transform(request)
    assert result == "123-456-789"


@pytest.mark.asyncio
@patch("servicex.servicex_adapter.AsyncClient.post")
async def test_submit_errors(post, servicex):
    post.return_value = MagicMock()
    post.return_value.status_code = 401
    request = TransformRequest(
        title="Test submission",
        did="rucio://foo.bar",
        selection="(call EventDataset)",
        codegen="uproot",
        result_destination=ResultDestination.object_store,
        result_format=ResultFormat.parquet,
    )
    with pytest.raises(AuthorizationError) as err:
        await servicex.submit_transform(request)
    assert "Not authorized to access serviceX at" in str(err.value)

    post.return_value.json.side_effect = JSONDecodeError("", "", 0)
    post.return_value.text = "error_message"
    post.return_value.status_code = 500
    with pytest.raises(RuntimeError) as err:
        await servicex.submit_transform(request)
    assert (
        "ServiceX WebAPI Error during transformation submission: 500 - error_message"
        == str(err.value)
    )

    post.return_value.json.reset_mock()
    post.return_value.json.return_value = {"message": "error_message"}
    post.return_value.status_code = 400
    with pytest.raises(ValueError) as err:
        await servicex.submit_transform(request)
    assert "Invalid transform request: error_message" == str(err.value)

    post.return_value.json.return_value = {"message": "error_message"}
    post.return_value.status_code = 410
    with pytest.raises(RuntimeError) as err:
        await servicex.submit_transform(request)
    assert (
        "ServiceX WebAPI Error during transformation submission: 410 - error_message"
        == str(err.value)
    )


@pytest.mark.asyncio
@patch("servicex.servicex_adapter.AsyncClient.get")
async def test_get_transform_status(get, servicex, transform_status_response):
    get.return_value = MagicMock()
    get.return_value.json.return_value = transform_status_response["requests"][
        0
    ]  # NOQA: E501
    get.return_value.status_code = 200
    result = await servicex.get_transform_status("b8c508d0-ccf2-4deb-a1f7-65c839eebabf")
    assert result.request_id == "b8c508d0-ccf2-4deb-a1f7-65c839eebabf"


@pytest.mark.asyncio
@patch("servicex.servicex_adapter.AsyncClient.get")
async def test_get_transform_status_errors(get, servicex):
    get.return_value = MagicMock()
    with pytest.raises(AuthorizationError) as err:
        get.return_value.status_code = 401
        await servicex.get_transform_status("b8c508d0-ccf2-4deb-a1f7-65c839eebabf")
    assert "Not authorized to access serviceX at " in str(err.value)

    with pytest.raises(ValueError) as err:
        get.return_value.status_code = 404
        await servicex.get_transform_status("b8c508d0-ccf2-4deb-a1f7-65c839eebabf")
    assert "Transform ID b8c508d0-ccf2-4deb-a1f7-65c839eebabf not found" == str(
        err.value
    )

    with pytest.raises(RuntimeError) as err:
        get.return_value.status_code = 500
        get.return_value.json = lambda: {"message": "fifteen"}
        await servicex.get_transform_status("b8c508d0-ccf2-4deb-a1f7-65c839eebabf")
    assert "ServiceX WebAPI Error during transformation" in str(err.value)


@pytest.mark.asyncio
@patch("servicex.servicex_adapter.TransformStatus", side_effect=RuntimeError)
@patch("servicex.servicex_adapter.AsyncClient.get")
async def test_get_tranform_status_retry_error(
    get, mock_transform_status, servicex, transform_status_response
):
    with pytest.raises(RuntimeError) as err:
        get.return_value = MagicMock()
        get.return_value.json.return_value = transform_status_response["requests"][
            0
        ]  # NOQA: E501
        get.return_value.status_code = 200
        await servicex.get_transform_status("b8c508d0-ccf2-4deb-a1f7-65c839eebabf")
    assert "ServiceX WebAPI Error while getting transform status:" in str(err.value)


@pytest.mark.asyncio
async def test_get_authorization(servicex):
    servicex.token = "token"
    servicex.refresh_token = "refresh"
    with patch("google.auth.jwt.decode", return_value={"exp": time.time() + 90}):
        r = await servicex._get_authorization()
        assert r.get("Authorization") == "Bearer token"

    with patch(
        "servicex.servicex_adapter.ServiceXAdapter._get_token", return_value="token"
    ) as get_token:
        with patch("google.auth.jwt.decode", return_value={"exp": time.time() - 90}):
            r = await servicex._get_authorization()
            get_token.assert_called_once()


@pytest.mark.asyncio
@patch("servicex.servicex_adapter.AsyncClient.get")
async def test_get_transformation_results_success(get, servicex):
    servicex.get_servicex_capabilities = AsyncMock(
        return_value=["poll_local_transformation_results"]
    )
    get.return_value = MagicMock()
    get.return_value.json.return_value = {
        "results": [
            {
                "file-path": "file1.txt",
                "total-bytes": 100,
                "s3-object-name": "file1.txt",
                "created_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                "transform_status": "success",
            },
            {
                "file-path": "file2.txt",
                "total-bytes": 100,
                "s3-object-name": "file2.txt",
                "created_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                "transform_status": "success",
            },
        ]
    }
    get.return_value.status_code = 200

    request_id = "123-45-6789"
    now = datetime.datetime.now(datetime.timezone.utc)
    await servicex.get_transformation_results(request_id, now)

    get.assert_called_with(
        url=f"https://servicex.org/servicex/transformation/{request_id}/results",
        headers={},
        params={
            "later_than": now.isoformat(),
        },
    )


@pytest.mark.asyncio
@patch("servicex.servicex_adapter.AsyncClient.get")
async def test_get_transformation_results_no_feature_flag(get, servicex):
    servicex.get_servicex_capabilities = AsyncMock(return_value=[])
    request_id = "123-45-6789"
    now = datetime.datetime.now(datetime.timezone.utc)
    with pytest.raises(ValueError):
        await servicex.get_transformation_results(request_id, now)


@pytest.mark.asyncio
@patch("servicex.servicex_adapter.AsyncClient.get")
async def test_get_transformation_results_not_found(
    get_transformation_results, servicex
):
    servicex.get_servicex_capabilities = AsyncMock(
        return_value=["poll_local_transformation_results"]
    )
    get_transformation_results.return_value = MagicMock()
    get_transformation_results.return_value.status_code = 404

    request_id = "123-45-6789"
    now = datetime.datetime.now(datetime.timezone.utc)

    with pytest.raises(ValueError):
        await servicex.get_transformation_results(request_id, now)

    get_transformation_results.assert_called_with(
        url=f"https://servicex.org/servicex/transformation/{request_id}/results",
        headers={},
        params={
            "later_than": now.isoformat(),
        },
    )


@pytest.mark.asyncio
@patch("servicex.servicex_adapter.AsyncClient.get")
async def test_get_transformation_results_not_authorized(
    get_transformation_results, servicex
):
    servicex.get_servicex_capabilities = AsyncMock(
        return_value=["poll_local_transformation_results"]
    )
    get_transformation_results.return_value = MagicMock()
    get_transformation_results.return_value.status_code = 403
    request_id = "123-45-6789"
    now = datetime.datetime.now(datetime.timezone.utc)

    with pytest.raises(AuthorizationError):
        await servicex.get_transformation_results(request_id, now)

    get_transformation_results.assert_called_with(
        url=f"https://servicex.org/servicex/transformation/{request_id}/results",
        headers={},
        params={
            "later_than": now.isoformat(),
        },
    )


@pytest.mark.asyncio
@patch("servicex.servicex_adapter.AsyncClient.get")
async def test_get_transformation_results_server_error(
    get_transformation_results, servicex
):
    servicex.get_servicex_capabilities = AsyncMock(
        return_value=["poll_local_transformation_results"]
    )
    get_transformation_results.return_value = MagicMock()
    get_transformation_results.return_value.status = 500
    request_id = "123-45-6789"
    now = datetime.datetime.now(datetime.timezone.utc)

    with pytest.raises(RuntimeError):
        await servicex.get_transformation_results(request_id, now)

    get_transformation_results.assert_called_with(
        url=f"https://servicex.org/servicex/transformation/{request_id}/results",
        headers={},
        params={
            "later_than": now.isoformat(),
        },
    )


def test_get_bearer_token_file(tmp_path, monkeypatch):
    token_file = tmp_path / "btf"
    token_file.write_text("bearer123")
    monkeypatch.setenv("BEARER_TOKEN_FILE", str(token_file))
    assert ServiceXAdapter._get_bearer_token_file() == "bearer123"
    monkeypatch.delenv("BEARER_TOKEN_FILE", raising=False)
    assert ServiceXAdapter._get_bearer_token_file() is None


@patch("servicex.servicex_adapter.jwt.decode", return_value={"exp": 1600000000})
def test_get_token_expiration_success(decode):
    assert ServiceXAdapter._get_token_expiration("dummy") == 1600000000


@patch("servicex.servicex_adapter.jwt.decode", return_value={"sub": "noexp"})
def test_get_token_expiration_no_exp(decode):
    with pytest.raises(RuntimeError):
        ServiceXAdapter._get_token_expiration("dummy")


@pytest.mark.asyncio
async def test_get_authorization_no_token_no_refresh(servicex, monkeypatch):
    monkeypatch.delenv("BEARER_TOKEN_FILE", raising=False)
    headers = await servicex._get_authorization()
    assert headers == {}


@pytest.mark.asyncio
@patch("servicex.servicex_adapter.jwt.decode", return_value={"exp": time.time() + 600})
async def test_get_authorization_with_valid_token(decode, servicex):
    servicex.token = "tok123"
    headers = await servicex._get_authorization()
    assert headers == {"Authorization": "Bearer tok123"}


@pytest.mark.asyncio
async def test_get_authorization_with_refresh(monkeypatch):
    s = ServiceXAdapter("https://servicex.org", refresh_token="rftok")
    monkeypatch.delenv("BEARER_TOKEN_FILE", raising=False)

    async def fake_get_token(self):
        self.token = "newtoken"

    monkeypatch.setattr(ServiceXAdapter, "_get_token", fake_get_token)
    headers = await s._get_authorization()
    assert headers == {"Authorization": "Bearer newtoken"}


@pytest.mark.asyncio
@patch("servicex.servicex_adapter.AsyncClient.get")
async def test_get_servicex_info_success(mock_get, servicex):
    mock_get.return_value.status_code = 200
    mock_get.return_value.json = MagicMock(
        return_value={
            "capabilities": ["a", "b"],
            "app-version": "1.0",
            "code-gen-image": {"func_adl": "image1", "uproot": "image2"},
        }
    )
    info = await servicex.get_servicex_info()
    assert isinstance(info, ServiceXInfo)
    assert info.capabilities == ["a", "b"]


@pytest.mark.asyncio
@patch("servicex.servicex_adapter.AsyncClient.get")
async def test_get_servicex_info_auth_error(mock_get, servicex):
    mock_get.return_value.status_code = 401
    with pytest.raises(AuthorizationError):
        await servicex.get_servicex_info()


@pytest.mark.asyncio
@patch("servicex.servicex_adapter.AsyncClient.get")
async def test_get_servicex_info_server_error(mock_get, servicex):
    mock_get.return_value.status_code = 500
    mock_get.return_value.json = MagicMock(return_value={"message": "oops"})
    with pytest.raises(RuntimeError) as e:
        await servicex.get_servicex_info()
    assert "ServiceX WebAPI Error during transformation submission: 500 - oops" in str(
        e.value
    )


@pytest.mark.asyncio
async def test_get_servicex_info_caching(servicex):
    servicex_info_data = {
        "capabilities": ["a", "b"],
        "app-version": "1.0",
        "code-gen-image": {"func_adl": "image1", "uproot": "image2"},
    }

    with patch("servicex.servicex_adapter.AsyncClient.get") as mock_get:
        mock_get.return_value.status_code = 200
        mock_get.return_value.json = MagicMock(return_value=servicex_info_data)

        info1 = await servicex.get_servicex_info()
        assert isinstance(info1, ServiceXInfo)
        assert info1.capabilities == ["a", "b"]
        assert mock_get.call_count == 1

        # Second call should use cached ServiceXInfo without additional HTTP request
        info2 = await servicex.get_servicex_info()
        assert info2 is info1
        assert mock_get.call_count == 1


@pytest.mark.asyncio
async def test_get_servicex_capabilities(servicex):
    servicex_info_data = {
        "capabilities": ["feature1", "feature2", "feature3"],
        "app-version": "1.0",
        "code-gen-image": {"func_adl": "image1", "uproot": "image2"},
    }

    with patch("servicex.servicex_adapter.AsyncClient.get") as mock_get:
        mock_get.return_value.status_code = 200
        mock_get.return_value.json = MagicMock(return_value=servicex_info_data)

        capabilities1 = await servicex.get_servicex_capabilities()
        assert capabilities1 == ["feature1", "feature2", "feature3"]
        assert mock_get.call_count == 1

        # Second call should use cached ServiceXInfo without additional HTTP request
        capabilities2 = await servicex.get_servicex_capabilities()
        assert capabilities2 == ["feature1", "feature2", "feature3"]
        assert mock_get.call_count == 1


@pytest.mark.asyncio
@patch("servicex.servicex_adapter.AsyncClient.get")
async def test_get_transformation_results_parsing(mock_get, servicex):
    servicex.get_servicex_capabilities = AsyncMock(
        return_value=["poll_local_transformation_results"]
    )
    msg_time = datetime.datetime(2025, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc)
    mock_get.return_value = MagicMock()
    mock_get.return_value.status_code = 200
    mock_get.return_value.json = MagicMock(
        return_value={
            "results": [
                {
                    "file-path": "dir1/file.txt",
                    "s3-object-name": "dir1:file.txt",
                    "total-bytes": 100,
                    "created_at": msg_time.isoformat(),
                    "transform_status": "success",
                }
            ]
        }
    )
    res = await servicex.get_transformation_results("id123", None)
    assert len(res) == 1
    assert res[0].filename == "dir1:file.txt"
    assert res[0].created_at == msg_time


@pytest.mark.asyncio
@patch("servicex.servicex_adapter.AsyncClient.get")
async def test_get_transformation_results_empty(mock_get, servicex):
    servicex.get_servicex_capabilities = AsyncMock(
        return_value=["poll_local_transformation_results"]
    )
    mock_get.return_value.status_code = 200
    mock_get.return_value.json = MagicMock(return_value={"results": []})
    res = await servicex.get_transformation_results("id123", None)
    assert res == []


@pytest.mark.asyncio
@patch("servicex.servicex_adapter.AsyncClient.get")
async def test_get_transformation_results_failed_file(mock_get, servicex):
    servicex.get_servicex_capabilities = AsyncMock(
        return_value=["poll_local_transformation_results"]
    )
    msg_time = datetime.datetime(2025, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc)
    mock_get.return_value = MagicMock()
    mock_get.return_value.status_code = 200
    mock_get.return_value.json = MagicMock(
        return_value={
            "results": [
                {
                    "file-path": "dir1/file.txt",
                    "s3-object-name": "dir1:file.txt",
                    "total-bytes": 100,
                    "created_at": msg_time.isoformat(),
                    "transform_status": "failure",
                }
            ]
        }
    )
    res = await servicex.get_transformation_results("id123", None)
    assert len(res) == 0


@pytest.mark.asyncio
async def test_sample_title_limit(servicex):
    servicex.get_servicex_capabilities = AsyncMock(return_value=["irrelevant"])
    assert await servicex.get_servicex_sample_title_limit() is None
    servicex.get_servicex_capabilities = AsyncMock(
        return_value=["long_sample_titles_10240"]
    )
    assert await servicex.get_servicex_sample_title_limit() == 10240
    servicex.get_servicex_capabilities = AsyncMock(
        return_value=["long_sample_titles_invalid"]
    )
    with pytest.raises(RuntimeError):
        await servicex.get_servicex_sample_title_limit()
