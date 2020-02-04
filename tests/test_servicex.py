import asyncio
import json
import shutil
from unittest import mock

import pandas as pd
import pytest

import ServiceX_fe as fe


class ClientSessionMocker:
    def __init__(self, text, status):
        self._text = text
        self.status = status

    async def text(self):
        return self._text

    async def json(self):
        return json.loads(self._text)

    async def __aexit__(self, exc_type, exc, tb):
        pass

    async def __aenter__(self):
        return self


class fget_object_good_copy_callable(mock.MagicMock):
    def __call__(self, *args, **kwargs):
        assert len(args) == 3

        shutil.copy('tests/sample_servicex_output.root',
                    args[2])


def make_minio_file(fname):
    r = mock.MagicMock()
    r.object_name = fname
    return r


@pytest.fixture()
def files_back_1(mocker):
    mocker.patch('minio.api.Minio.list_objects', return_value=[make_minio_file('root:::dcache-atlas-xrootd-wan.desy.de:1094::pnfs:desy.de:atlas:dq2:atlaslocalgroupdisk:rucio:mc15_13TeV:8a:f1:DAOD_STDM3.05630052._000001.pool.root.198fbd841d0a28cb0d9dfa6340c890273-1.part.minio')])
    mocker.patch('minio.api.Minio.fget_object', new_callable=fget_object_good_copy_callable)
    return None


@pytest.fixture()
def files_back_2(mocker):
    mocker.patch('minio.api.Minio.list_objects', return_value=[make_minio_file('root:::dcache-atlas-xrootd-wan.desy.de:1094::pnfs:desy.de:atlas:dq2:atlaslocalgroupdisk:rucio:mc15_13TeV:8a:f1:DAOD_STDM3.05630052._000001.pool.root.198fbd841d0a28cb0d9dfa6340c890273-1.part.minio'),
                                                               make_minio_file('root:::dcache-atlas-xrootd-wan.desy.de:1094::pnfs:desy.de:atlas:dq2:atlaslocalgroupdisk:rucio:mc15_13TeV:8a:f1:DAOD_STDM3.05630052._000002.pool.root.198fbd841d0a28cb0d9dfa6340c890273-1.part.minio')])
    mocker.patch('minio.api.Minio.fget_object', new_callable=fget_object_good_copy_callable)
    return None


class list_objects_callable(mock.MagicMock):
    def __call__(self, *args, **kwargs):
        assert len(args) == 1
        req_id = args[0]
        assert isinstance(req_id, str)
        number = int(req_id)
        return [make_minio_file(f'root:::dcache-atlas-{i}') for i in range(0, number)]


@pytest.fixture()
def four_sets_of_files_back(mocker):
    mocker.patch('minio.api.Minio.list_objects', new_callable=list_objects_callable)
    mocker.patch('minio.api.Minio.fget_object', new_callable=fget_object_good_copy_callable)


@pytest.fixture()
def good_transform_request(mocker):
    '''
    Setup to run a good transform request that returns a single file.
    '''
    r1 = ClientSessionMocker(json.dumps({"request_id": "1234-4433-111-34-22-444"}), 200)
    mocker.patch('aiohttp.ClientSession.post', return_value=r1)
    r2 = ClientSessionMocker(json.dumps({"files-remaining": "0", "files-processed": "1"}), 200)
    mocker.patch('aiohttp.ClientSession.get', return_value=r2)

    return None


@pytest.fixture()
def four_good_transform_requests(mocker):
    '''
    We will setup four transform requests.
    '''
    # Get the original queries
    r1 = ClientSessionMocker(json.dumps({"request_id": "1"}), 200)
    r2 = ClientSessionMocker(json.dumps({"request_id": "2"}), 200)
    r3 = ClientSessionMocker(json.dumps({"request_id": "3"}), 200)
    r4 = ClientSessionMocker(json.dumps({"request_id": "4"}), 200)
    mocker.patch('aiohttp.ClientSession.post', side_effect=[r1, r2, r3, r4])

    # Now get back the returns, which are just going to always be the same.
    g1 = ClientSessionMocker(json.dumps({"files-remaining": "0", "files-processed": "4"}), 200)
    mocker.patch('aiohttp.ClientSession.get', return_value=g1)


@pytest.fixture()
def time_is_short(mocker):
    # TODO: Make sure this allows other co-routines to run!
    class AsyncMock(mock.MagicMock):
        async def __call__(self, *args, **kwargs):
            return super(AsyncMock, self).__call__(*args, **kwargs)

    mocker.patch('asyncio.sleep', new_callable=AsyncMock)


@pytest.mark.asyncio
async def test_good_run_single_ds_1file(good_transform_request, time_is_short, files_back_1):
    'Simple run with expected results'
    r = await fe.get_data_async('(valid qastle string)', 'one_ds')
    assert isinstance(r, pd.DataFrame)
    assert len(r) == 283458


@pytest.mark.asyncio
async def test_good_run_single_ds_2file(good_transform_request, time_is_short, files_back_2):
    'Simple run with expected results'
    r = await fe.get_data_async('(valid qastle string)', 'one_ds')
    assert isinstance(r, pd.DataFrame)
    assert len(r) == 283458*2


def test_good_run_single_ds_1file_noasync(good_transform_request, time_is_short, files_back_1):
    'Simple run with expected results, but with the non-async version'
    r = fe.get_data('(valid qastle string)', 'one_ds')
    assert isinstance(r, pd.DataFrame)
    assert len(r) == 283458


def test_good_run_single_ds_1file_noasync_with_loop(good_transform_request, time_is_short, files_back_1):
    'Async loop has been created for other reasons, and the non-async version still needs to work.'
    _ = asyncio.get_event_loop()
    r = fe.get_data('(valid qastle string)', 'one_ds')
    assert isinstance(r, pd.DataFrame)
    assert len(r) == 283458


def test_run_with_running_event_loop(good_transform_request, time_is_short, files_back_1):
    async def doit():
        r = fe.get_data('(valid qastle string)', 'one_ds')
        assert isinstance(r, pd.DataFrame)
        assert len(r) == 283458
    loop = asyncio.get_event_loop()
    loop.run_until_complete(doit())


@pytest.mark.asyncio
async def test_run_with_four_queries(four_good_transform_requests, time_is_short, four_sets_of_files_back):
    'Try to run four transform requests at same time. Make sure each retursn what we expect.'
    r1 = fe.get_data_async('(valid qastle string)', 'one_ds')
    r2 = fe.get_data_async('(valid qastle string)', 'two_ds')
    r3 = fe.get_data_async('(valid qastle string)', 'three_ds')
    r4 = fe.get_data_async('(valid qastle string)', 'four_ds')
    all_wait = await asyncio.gather(*[r1, r2, r3, r4])
    assert len(all_wait) == 4
    for cnt, r in enumerate(all_wait):
        assert isinstance(r, pd.DataFrame)
        assert len(r) == 283458*(cnt+1)


# TODO:
# Other tests
#  Loose connection for a while after we submit the request
#  Don't have request to submit the request
#  Fail during download due to bad (temporary) connection.
