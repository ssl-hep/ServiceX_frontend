import pytest
from requests.api import patch  # NOQA
import ServiceX_fe as fe
import pandas as pd
import json
from unittest import mock


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
        import shutil
        shutil.copy('G:\\func_adl_cache\\2b63659eadc83973437e8661e7bbffa0\\ANALYSIS_001.root',
                    args[2])


def make_minio_file(mocker, fname):
    r = mocker.MagicMock()
    r.object_name = fname
    return r

@pytest.fixture()
def files_back_1(mocker):
    mocker.patch('minio.api.Minio.list_objects', return_value=[make_minio_file(mocker, 'root:::dcache-atlas-xrootd-wan.desy.de:1094::pnfs:desy.de:atlas:dq2:atlaslocalgroupdisk:rucio:mc15_13TeV:8a:f1:DAOD_STDM3.05630052._000001.pool.root.198fbd841d0a28cb0d9dfa6340c890273-1.part.minio')])
    mocker.patch('minio.api.Minio.fget_object', new_callable=fget_object_good_copy_callable)
    return None


@pytest.fixture()
def files_back_2(mocker):
    mocker.patch('minio.api.Minio.list_objects', return_value=[make_minio_file(mocker, 'root:::dcache-atlas-xrootd-wan.desy.de:1094::pnfs:desy.de:atlas:dq2:atlaslocalgroupdisk:rucio:mc15_13TeV:8a:f1:DAOD_STDM3.05630052._000001.pool.root.198fbd841d0a28cb0d9dfa6340c890273-1.part.minio'),
                                                               make_minio_file(mocker, 'root:::dcache-atlas-xrootd-wan.desy.de:1094::pnfs:desy.de:atlas:dq2:atlaslocalgroupdisk:rucio:mc15_13TeV:8a:f1:DAOD_STDM3.05630052._000002.pool.root.198fbd841d0a28cb0d9dfa6340c890273-1.part.minio')])
    mocker.patch('minio.api.Minio.fget_object', new_callable=fget_object_good_copy_callable)
    return None


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
def time_is_short(mocker):
    # TODO: Make sure this allows other co-routines to run!
    class AsyncMock(mock.MagicMock):
        async def __call__(self, *args, **kwargs):
            return super(AsyncMock, self).__call__(*args, **kwargs)

    mocker.patch('asyncio.sleep', new_callable=AsyncMock)


@pytest.mark.asyncio
async def test_good_run_single_ds_1file(good_transform_request, time_is_short, files_back_1):
    'Simple run with expected results'
    r = await fe.get_data('(valid qastle string)', 'one_ds')
    assert isinstance(r, pd.DataFrame)
    assert len(r) == 22576


@pytest.mark.asyncio
async def test_good_run_single_ds_2file(good_transform_request, time_is_short, files_back_2):
    'Simple run with expected results'
    r = await fe.get_data('(valid qastle string)', 'one_ds')
    assert isinstance(r, pd.DataFrame)
    assert len(r) == 22576*2

# TODO:
# Other tests
#  Loose connection for a while after we submit the request
#  Don't have request to submit the request
#  Fail during download due to bad (temporary) connection.
