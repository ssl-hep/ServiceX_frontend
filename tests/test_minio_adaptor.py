import pytest
import asyncio
from json import dumps
from pathlib import Path
from servicex.servicex_adaptor import transform_status_stream, trap_servicex_failures
import servicex
from typing import Optional, List, Any

import aiohttp
from minio.error import ResponseError
import pytest

from servicex import ServiceXException, ServiceXUnknownRequestID, ServiceXAdaptor

from .utils_for_testing import ClientSessionMocker, short_status_poll_time, as_async_seq


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
    minio_client.fget_object = mocker.MagicMock(side_effect=copy_minio_file)
    minio_client.list_objects = do_list

    return minio_client


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


@pytest.mark.skip
@pytest.mark.asyncio
async def test_download_good(good_minio_client, clean_temp_dir):
    from servicex.servicex_remote import _download_file

    final_path = clean_temp_dir / 'output-file.dude'
    await _download_file(good_minio_client, '111-22-333-444', 'dude-where-is-my-lunch', final_path)
    assert final_path.exists()


@pytest.mark.skip
@pytest.mark.asyncio
async def test_download_bad(bad_minio_client, clean_temp_dir):
    from servicex.servicex_remote import _download_file

    final_path = clean_temp_dir / 'output-file.dude'
    with pytest.raises(ServiceXException) as e:
        await _download_file(bad_minio_client, '111-22-333-444', 'dude-where-is-my-lunch', final_path)
    assert not final_path.exists()
    assert "Failed to copy" in str(e.value)


@pytest.mark.skip
@pytest.mark.asyncio
async def test_download_already_there(good_minio_client, clean_temp_dir):
    from servicex.servicex_remote import _download_file

    final_path = clean_temp_dir / 'download_tests' / 'output-file.dude'
    if final_path.parent.exists():
        import shutil
        shutil.rmtree(final_path.parent)

    final_path.parent.mkdir(parents=True, exist_ok=True)
    if final_path.exists():
        final_path.rm()

    with final_path.open('w') as o:
        o.write('this is a line')

    await _download_file(good_minio_client, '111-22-333-444', 'dude-where-is-my-lunch', final_path)
    assert final_path.exists()
    good_minio_client.fget_object.assert_not_called()


@pytest.mark.skip
@pytest.mark.asyncio
async def test_download_with_temp_file_there(good_minio_client, clean_temp_dir):
    'This simulates a bad download - so an old temp file is left on disk'
    from servicex.servicex_remote import _download_file

    final_path = clean_temp_dir / 'download_tests' / 'output-file.dude'
    if final_path.parent.exists():
        import shutil
        shutil.rmtree(final_path.parent)

    final_path.parent.mkdir(parents=True, exist_ok=True)
    if final_path.exists():
        final_path.rm()

    temp_file = final_path.parent / (final_path.name + ".temp")
    with temp_file.open('w') as o:
        o.write('this is a line')

    await _download_file(good_minio_client, '111-22-333-444', 'dude-where-is-my-lunch', final_path)
    assert final_path.exists()
    good_minio_client.fget_object.assert_called_once()


@pytest.mark.skip
def test_list_objects(good_minio_client):
    from servicex.servicex_remote import _protected_list_objects
    f = _protected_list_objects(good_minio_client, '111-222-333-444')
    assert len(f) == 1


@pytest.mark.skip
def test_list_objects_with_null(bad_then_good_minio_listing):
    'Sometimes for reasons we do not understand we get back a response error from list_objects minio method'
    from servicex.servicex_remote import _protected_list_objects
    f = _protected_list_objects(bad_then_good_minio_listing, '111-222-333-444')
    assert len(f) == 1


@pytest.mark.skip
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

    t = asyncio.ensure_future(get_files())
    ro.trigger_scan()
    await asyncio.sleep(0.1)
    ro.shutdown()
    await t

    assert len(items) == 1


@pytest.mark.skip
@pytest.mark.asyncio
async def test_files_2_shot(indexed_minio_client):
    from servicex.servicex_remote import _result_object_list

    minio, update_count = indexed_minio_client

    ro = _result_object_list(minio, '111-222-444')
    items = []
    done = False

    async def get_files():
        async for f in ro.files():
            items.append(f)
            update_count(2)
        nonlocal done
        done = True

    t = asyncio.ensure_future(get_files())
    ro.trigger_scan()
    await asyncio.sleep(0.1)
    ro.trigger_scan()
    await asyncio.sleep(0.1)
    ro.shutdown()
    await t

    assert len(items) == 2


@pytest.mark.skip
@pytest.mark.asyncio
async def test_files_shutdown_not_files_lost(indexed_minio_client):
    from servicex.servicex_remote import _result_object_list

    minio, update_count = indexed_minio_client

    ro = _result_object_list(minio, '111-222-444')
    items = []
    done = False

    async def get_files():
        async for f in ro.files():
            items.append(f)
            update_count(2)
        nonlocal done
        done = True

    t = asyncio.ensure_future(get_files())
    ro.trigger_scan()
    ro.trigger_scan()
    ro.shutdown()
    await t

    assert len(items) == 2


@pytest.mark.skip
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


