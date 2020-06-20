from minio.api import Minio
import pytest
import asyncio
from json import dumps
from pathlib import Path
from servicex.servicex_adaptor import transform_status_stream, trap_servicex_failures
import servicex
from typing import Optional, List, Any
import minio

import aiohttp
from minio.error import ResponseError
import pytest

from servicex import ServiceXException, ServiceXUnknownRequestID, ServiceXAdaptor, MinioAdaptor

from .utils_for_testing import ClientSessionMocker, short_status_poll_time, as_async_seq


def make_minio_file(fname):
    from unittest import mock
    r = mock.MagicMock()
    r.object_name = fname
    return r


@pytest.fixture
def good_minio_client(mocker):

    minio_client = mocker.MagicMock(spec=minio.Minio)
    minio_client.list_objects.return_value = \
        [make_minio_file('root:::dcache-atlas-xrootd-wan.desy.de:1094::pnfs:desy.de:atlas:dq2:atlaslocalgroupdisk:rucio:mc15_13TeV:8a:f1:DAOD_STDM3.05630052._000001.pool.root.198fbd841d0a28cb0d9dfa6340c890273-1.part.minio')]

    mocker.patch('servicex.minio_adaptor.Minio', return_value=minio_client)

    p_rename = mocker.patch('servicex.minio_adaptor.Path.rename', mocker.MagicMock())

    mocker.patch('servicex.minio_adaptor.Path.mkdir', mocker.MagicMock())

    return p_rename, minio_client


@pytest.fixture
def bad_minio_client(mocker):
    minio_client = mocker.MagicMock(spec=minio.Minio)
    minio_client.list_objects.return_value = \
        [make_minio_file('root:::dcache-atlas-xrootd-wan.desy.de:1094::pnfs:desy.de:atlas:dq2:atlaslocalgroupdisk:rucio:mc15_13TeV:8a:f1:DAOD_STDM3.05630052._000001.pool.root.198fbd841d0a28cb0d9dfa6340c890273-1.part.minio')]
    minio_client.fget_object.side_effect = Exception('this copy really failed')

    mocker.patch('servicex.minio_adaptor.Minio', return_value=minio_client)

    p_rename = mocker.patch('servicex.minio_adaptor.Path.rename', mocker.MagicMock())
    mocker.patch('servicex.minio_adaptor.Path.mkdir', mocker.MagicMock())

    return p_rename, minio_client


@pytest.fixture
def bad_then_good_minio_listing(mocker):
    response1 = mocker.MagicMock()
    response1.data = '<xml></xml>'
    response2 = [make_minio_file('root:::dcache-atlas-xrootd-wan.desy.de:1094::pnfs:desy.de:atlas:dq2:atlaslocalgroupdisk:rucio:mc15_13TeV:8a:f1:DAOD_STDM3.05630052._000001.pool.root.198fbd841d0a28cb0d9dfa6340c890273-1.part.minio')]

    minio_client = mocker.MagicMock(spec=minio.Minio)
    minio_client.list_objects.side_effect = [ResponseError(response1, 'POST', 'Due'), response2]

    mocker.patch('servicex.minio_adaptor.Minio', return_value=minio_client)

    p_rename = mocker.patch('servicex.minio_adaptor.Path.rename', mocker.MagicMock())
    mocker.patch('servicex.minio_adaptor.Path.mkdir', mocker.MagicMock())

    return p_rename, minio_client


@pytest.fixture
def indexed_minio_client(mocker):

    count = 1

    def do_list(request_id):
        return [make_minio_file(f'root:::dcache-atlas-xrootd-wan.desy.de:1094::pnfs:desy.de:atlas:dq2:atlaslocalgroupdisk:rucio:mc15_13TeV:8a:f1:DAOD_STDM3.05630052._000001.pool.root.198fbd841d0a28cb0d9dfa6340c890273-{i}.part.minio') for i in range(count)]

    minio_client = mocker.MagicMock()
    minio_client.fget_object = copy_minio_file
    minio_client.list_objects = do_list

    def update_count(c: int):
        nonlocal count
        count = c

    return minio_client, update_count


@pytest.mark.asyncio
async def test_download_good(good_minio_client):
    ma = MinioAdaptor('localhost:9000')

    final_path = Path('/tmp/output-file.dude')  # type: Path
    await ma.download_file('111-22-333-444', 'dude-where-is-my-lunch', final_path)

    p_rename, p_minio = good_minio_client

    # Make sure copy was called.
    p_minio.fget_object.assert_called_once()
    expected_path = Path('/tmp/output-file.dude.temp')
    p_minio.fget_object.assert_called_with('111-22-333-444', 'dude-where-is-my-lunch', str(expected_path))

    # Make sure the rename was done.
    p_rename.assert_called_with(final_path)


@pytest.mark.asyncio
async def test_download_bad(bad_minio_client):
    ma = MinioAdaptor('localhost:9000')

    final_path = Path('/tmp/output-file.dude')
    with pytest.raises(ServiceXException) as e:
        await ma.download_file('111-22-333-444', 'dude-where-is-my-lunch', final_path)

    assert "Failed to copy" in str(e.value)

    p_rename, p_minio = bad_minio_client
    p_minio.fget_object.assert_called_once()

    # Make sure the rename was done.
    p_rename.assert_not_called()


@pytest.mark.asyncio
async def test_download_already_there(mocker, good_minio_client):
    p_exists = mocker.patch('servicex.minio_adaptor.Path.exists', return_value=True)

    ma = MinioAdaptor('localhost:9000')

    final_path = Path('/tmp/output-file.dude')  # type: Path
    await ma.download_file('111-22-333-444', 'dude-where-is-my-lunch', final_path)

    p_rename, p_minio = good_minio_client

    # Make sure copy was called.
    p_minio.fget_object.assert_not_called()

    # Make sure the rename was done.
    p_rename.assert_not_called()

    # Make sure the exists worked.
    p_exists.assert_called_once()


def test_list_objects(good_minio_client):
    ma = MinioAdaptor('localhost:9000')

    f = ma.get_files('111-222-333-444')
    assert len(f) == 1


def test_list_objects_with_null(bad_then_good_minio_listing):
    'Sometimes for reasons we do not understand we get back a response error from list_objects minio method'
    ma = MinioAdaptor('localhost:9000')
    f = ma.get_files('111-222-333-444')
    assert len(f) == 1

# TODO: test out find_new_bucket_files
