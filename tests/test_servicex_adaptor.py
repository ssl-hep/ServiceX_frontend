import asyncio
from json import dumps
from pathlib import Path
from servicex.servicex_adaptor import transform_status_stream, watch_transform_status
import servicex
from typing import Optional, List, Any

import aiohttp
from minio.error import ResponseError
import pytest

from servicex import ServiceXException, ServiceXUnknownRequestID, ServiceXAdaptor

from .utils_for_testing import ClientSessionMocker, short_status_poll_time, as_async_seq


@pytest.fixture
def servicex_status_request(mocker):
    '''
    Fixture that emulates the async python library get call when used with a status.

      - Does not check the incoming http address
      - Does not check the Returns a standard triple status from servicex
      - Does not check the headers
      - Call this to set:
            servicex_status_request(1, 2, 3)
            Sets remaining to 1, failed to 2, and processed to 3.
    '''
    files_remaining = None
    files_failed = None
    files_processed = 0

    def get_status(a, headers=None):
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
    client.post = mocker.MagicMock(return_value=r)
    return client


@pytest.fixture
def bad_submit(mocker):
    client = mocker.MagicMock()
    r = ClientSessionMocker(dumps({'message': "bad text"}), 400)
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


@pytest.mark.asyncio
async def test_status_no_login(servicex_status_request):

    servicex_status_request(None, 0, 10)
    sa = ServiceXAdaptor('http://localhost:500/sx')
    async with aiohttp.ClientSession() as client:
        r = await sa.get_transform_status(client, '123-123-123-444')
        assert len(r) == 3
        assert r[0] is None
        assert r[1] == 10
        assert r[2] == 0


@pytest.mark.asyncio
async def test_status_unknown_request(servicex_status_unknown):

    sa = ServiceXAdaptor('http://localhost:500/sx')
    with pytest.raises(ServiceXUnknownRequestID) as e:
        async with aiohttp.ClientSession() as client:
            await sa.get_transform_status(client, '123-123-123-444')

    assert 'transformation status' in str(e.value)


def version_mock(mocker, spec):
    import sys
    if sys.version_info[1] < 8:
        from asyncmock import AsyncMock  # type: ignore
        return AsyncMock(spec=spec)
    else:
        return mocker.MagicMock(spec=spec)


@pytest.mark.asyncio
async def test_status_stream_simple_sequence(mocker):
    adaptor = version_mock(mocker, spec=ServiceXAdaptor)
    adaptor.get_transform_status.configure_mock(return_value=(0, 1, 1))

    async with aiohttp.ClientSession() as client:
        v = [a async for a in transform_status_stream(adaptor, client, '123-455')]

    assert len(v) == 1
    assert v[0] == (0, 1, 1)


@pytest.mark.asyncio
async def test_status_stream_simple_2sequence(short_status_poll_time, mocker):
    adaptor = version_mock(mocker, spec=ServiceXAdaptor)
    adaptor.get_transform_status.configure_mock(side_effect=[(1, 1, 1), (0, 1, 1)])

    async with aiohttp.ClientSession() as client:
        v = [a async for a in transform_status_stream(adaptor, client, '123-455')]

    assert len(v) == 2
    assert v[0] == (1, 1, 1)
    assert v[1] == (0, 1, 1)


@pytest.mark.asyncio
async def test_watch_no_fail(short_status_poll_time, mocker):
    v = [a async for a in watch_transform_status(as_async_seq([(1, 0, 0), (0, 1, 0)]))]

    assert len(v) == 2
    assert v[0] == (1, 0, 0)
    assert v[1] == (0, 1, 0)


@pytest.mark.asyncio
async def test_watch_fail_end(short_status_poll_time, mocker):
    v = []
    with pytest.raises(ServiceXException) as e:
        async for a in watch_transform_status(as_async_seq([(1, 0, 0), (0, 0, 1)])):
            v.append(a)

    assert len(v) == 2
    assert 'failed to transform' in str(e.value)


@pytest.mark.asyncio
async def test_watch_fail_start(short_status_poll_time, mocker):
    v = []
    with pytest.raises(ServiceXException) as e:
        async for a in watch_transform_status(as_async_seq([(2, 0, 0), (1, 0, 1), (0, 1, 1)])):
            v.append(a)

    assert len(v) == 3
    assert 'failed to transform' in str(e.value)


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
    with pytest.raises(ServiceXException) as e:
        await _download_file(bad_minio_client, '111-22-333-444', 'dude-where-is-my-lunch', final_path)
    assert not final_path.exists()
    assert "Failed to copy" in str(e.value)


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

    t = asyncio.ensure_future(get_files())
    ro.trigger_scan()
    await asyncio.sleep(0.1)
    ro.shutdown()
    await t

    assert len(items) == 1


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
async def test_submit_good_no_login(good_submit):
    sa = ServiceXAdaptor(endpoint='http://localhost:5000/sx')

    rid = await sa.submit_query(good_submit, {'hi': 'there'})

    good_submit.post.assert_called_once()
    args, kwargs = good_submit.post.call_args

    assert len(args) == 1
    assert args[0] == 'http://localhost:5000/sx/servicex/transformation'

    assert len(kwargs) == 2
    assert 'headers' in kwargs
    assert len(kwargs['headers']) == 0

    assert 'json' in kwargs
    assert kwargs['json'] == {'hi': 'there'}

    assert rid is not None
    assert isinstance(rid, str)
    assert rid == '111-222-333-444'


# @pytest.mark.asyncio
# async def test_submit_bad(bad_submit):
#     from servicex.servicex_remote import _submit_query

#     with pytest.raises(ServiceXException) as e:
#         await _submit_query(bad_submit, 'http://bogus', {'hi': 'there'})

#     assert "bad text" in str(e.value)
