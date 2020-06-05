import asyncio
from json import dumps
from pathlib import Path
from typing import Optional

import aiohttp
from minio.error import ResponseError
import pytest

from servicex.utils import ServiceX_Exception

from .utils_for_testing import ClientSessionMocker


@pytest.fixture
def servicex_status_request(mocker):
    files_remaining = None
    files_failed = None
    files_processed = 0

    def get_status(a):
        r = {}

        def store(name: str, values: Optional[int]):
            nonlocal r
            if values is not None:
                r[name] = values
        store('files-remaining', files_remaining)
        store('files-skipped', files_failed)
        store('files-processed', files_processed)

        return ClientSessionMocker(dumps(r), 200)

    mocker.patch('aiohttp.ClientSession.get', side_effect=get_status)

    def set_it_up(remain: Optional[int], failed: Optional[int], processed: Optional[int]):
        nonlocal files_remaining, files_failed, files_processed
        files_remaining = remain
        files_failed = failed
        files_processed = processed

    return set_it_up


@pytest.fixture
def good_submit(mocker):
    client = mocker.MagicMock()
    r = ClientSessionMocker(dumps({'request_id': "111-222-333-444"}), 200)
    client.post = lambda d, json: r
    return client


@pytest.fixture
def servicex_status_unknown(mocker):
    r = ClientSessionMocker(dumps({'message': "unknown status"}), 500)
    mocker.patch('aiohttp.ClientSession.get', return_value=r)


def make_minio_file(fname):
    from unittest import mock
    r = mock.MagicMock()
    r.object_name = fname
    return r


def copy_minio_file(req: str, bucket: str, output_file: str):
    assert isinstance(output_file, str)
    with open(output_file, 'w') as o:
        o.write('hi there')

@pytest.fixture
def good_minio_client(mocker):

    def do_list(request_id):
        return [make_minio_file('root:::dcache-atlas-xrootd-wan.desy.de:1094::pnfs:desy.de:atlas:dq2:atlaslocalgroupdisk:rucio:mc15_13TeV:8a:f1:DAOD_STDM3.05630052._000001.pool.root.198fbd841d0a28cb0d9dfa6340c890273-1.part.minio')]

    minio_client = mocker.MagicMock()
    minio_client.fget_object = copy_minio_file
    minio_client.list_objects = do_list

    return minio_client


@pytest.fixture
def bad_then_good_minio_listing(mocker):
    response1 = mocker.MagicMock()
    response1.data = '<xml></xml>'
    response2 = [make_minio_file('root:::dcache-atlas-xrootd-wan.desy.de:1094::pnfs:desy.de:atlas:dq2:atlaslocalgroupdisk:rucio:mc15_13TeV:8a:f1:DAOD_STDM3.05630052._000001.pool.root.198fbd841d0a28cb0d9dfa6340c890273-1.part.minio')]

    minio_client = mocker.MagicMock()
    minio_client.list_objects = mocker.MagicMock(side_effect=[ResponseError(response1, 'POST', 'Due'), response2])

    return minio_client


@pytest.fixture
def bad_minio_client(mocker):
    def copy_it(req: str, bucket: str, output_file: str):
        raise Exception('this copy really failed')

    minio_client = mocker.MagicMock()
    minio_client.fget_object = copy_it

    return minio_client



@pytest.fixture
def clean_temp_dir():
    import tempfile
    import shutil
    p = Path(tempfile.gettempdir()) / 'servicex_testing_dir'
    if p.exists():
        shutil.rmtree(p)
    return p


@pytest.mark.asyncio
async def test_status_all_values(servicex_status_request):
    from servicex.servicex_remote import _get_transform_status

    servicex_status_request(1, 0, 10)
    async with aiohttp.ClientSession() as client:
        r = await _get_transform_status(client, 'http://localhost:5000/sx', '123-123-123-444')
        assert len(r) == 3
        assert r[0] == 1
        assert r[1] == 10
        assert r[2] == 0


@pytest.mark.asyncio
async def test_status_remain_unknown(servicex_status_request):
    from servicex.servicex_remote import _get_transform_status

    servicex_status_request(None, 0, 10)
    async with aiohttp.ClientSession() as client:
        r = await _get_transform_status(client, 'http://localhost:5000/sx', '123-123-123-444')
        assert len(r) == 3
        assert r[0] is None
        assert r[1] == 10
        assert r[2] == 0


@pytest.mark.asyncio
async def test_unknown_request(servicex_status_unknown):
    from servicex.servicex_remote import _get_transform_status

    with pytest.raises(ServiceX_Exception) as e:
        async with aiohttp.ClientSession() as client:
            await _get_transform_status(client, 'http://localhost:5000/sx', '123-123-123-444')

    assert 'transformation status' in str(e.value)


@pytest.mark.asyncio
async def test_download_good(good_minio_client, clean_temp_dir):
    from servicex.servicex_remote import _download_file

    final_path = clean_temp_dir / 'output-file.dude'
    await _download_file(good_minio_client, '111-22-333-444', 'dude-where-is-my-lunch', final_path)
    assert final_path.exists()


@pytest.mark.asyncio
async def test_download_bad(bad_minio_client, clean_temp_dir):
    from servicex.servicex_remote import _download_file

    final_path = clean_temp_dir / 'output-file.dude'
    with pytest.raises(ServiceX_Exception) as e:
        await _download_file(bad_minio_client, '111-22-333-444', 'dude-where-is-my-lunch', final_path)
    assert not final_path.exists()
    assert "Failed to copy" in str(e.value)


def test_list_objects(good_minio_client):
    from servicex.servicex_remote import _protected_list_objects
    f = _protected_list_objects(good_minio_client, '111-222-333-444')
    assert len(f) == 1


def test_list_objects_with_null(bad_then_good_minio_listing):
    'Sometimes for reasons we do not understand we get back a response error from list_objects minio method'
    from servicex.servicex_remote import _protected_list_objects
    f = _protected_list_objects(bad_then_good_minio_listing, '111-222-333-444')
    assert len(f) == 1


@pytest.mark.asyncio
async def test_files_one_shot(good_minio_client):
    from servicex.servicex_remote import _result_object_list

    ro = _result_object_list(good_minio_client, '111-222-444')
    items = []
    done = False

    async def get_files():
        async for f in ro.files():
            items.append(f)
        nonlocal done
        done = True

    t = asyncio.create_task(get_files())
    ro.trigger_scan()
    await asyncio.sleep(0.1)
    ro.shutdown()
    await t

    assert len(items) == 1


@pytest.mark.asyncio
async def test_files_no_repeat(good_minio_client):
    from servicex.servicex_remote import _result_object_list

    ro = _result_object_list(good_minio_client, '111-222-444')
    items = []
    done = False

    async def get_files():
        async for f in ro.files():
            items.append(f)
        nonlocal done
        done = True

    async def trigger():
        ro.trigger_scan()
        await asyncio.sleep(0.1)
        ro.trigger_scan()
        await asyncio.sleep(0.1)
        ro.shutdown()

    await asyncio.gather(get_files(), trigger())

    assert len(items) == 1


@pytest.mark.asyncio
async def test_submit_good(good_submit):
    from servicex.servicex_remote import _submit_query

    rid = await _submit_query(good_submit, 'http://bogus', {'hi': 'there'})
    assert rid is not None
    assert isinstance(rid, str)
    assert rid == '111-222-333-444'
