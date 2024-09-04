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
from unittest.mock import AsyncMock

import pytest

from servicex import ResultFormat
from servicex.dataset_group import DatasetGroup
from servicex.query_core import ServiceXException


def test_set_result_format(mocker):
    ds1 = mocker.Mock()
    ds2 = mocker.Mock()
    group = DatasetGroup([ds1, ds2])
    group.set_result_format(ResultFormat.root_ttree)
    ds1.set_result_format.assert_called_once_with(ResultFormat.root_ttree)
    ds2.set_result_format.assert_called_once_with(ResultFormat.root_ttree)


@pytest.mark.asyncio
async def test_as_signed_urls(mocker, transformed_result):
    ds1 = mocker.Mock()
    ds1.as_signed_urls_async = AsyncMock(return_value=transformed_result)

    ds2 = mocker.Mock()
    ds2.as_signed_urls_async = AsyncMock(return_value=transformed_result.model_copy(
        update={"request_id": "98-765-432"}))

    group = DatasetGroup([ds1, ds2])
    results = await group.as_signed_urls_async()

    assert len(results) == 2
    assert results[0].request_id == "123-45-6789"
    assert results[1].request_id == "98-765-432"


@pytest.mark.asyncio
async def test_as_files(mocker, transformed_result):
    ds1 = mocker.Mock()
    ds1.as_files_async = AsyncMock(return_value=transformed_result)

    ds2 = mocker.Mock()
    ds2.as_files_async = AsyncMock(return_value=transformed_result.model_copy(
        update={"request_id": "98-765-432"}))

    group = DatasetGroup([ds1, ds2])
    results = await group.as_files_async()

    assert len(results) == 2
    assert results[0].request_id == "123-45-6789"
    assert results[1].request_id == "98-765-432"


@pytest.mark.asyncio
async def test_failure(mocker, transformed_result):
    ds1 = mocker.Mock()
    ds1.as_signed_urls_async = AsyncMock(return_value=transformed_result)

    ds2 = mocker.Mock()
    ds2.as_signed_urls_async = AsyncMock(side_effect=ServiceXException("dummy"))

    group = DatasetGroup([ds1, ds2])
    with pytest.raises(ServiceXException):
        await group.as_signed_urls_async()

    results = await group.as_signed_urls_async(return_exceptions=True)

    assert len(results) == 2
    assert results[0].request_id == "123-45-6789"
    assert isinstance(results[1], ServiceXException)
