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


@pytest.fixture()
def good_transform_request(mocker):
    r = ClientSessionMocker(json.dumps({"request_id": "1234-4433-111-34-22-444"}), 200)
    mocker.patch('aiohttp.ClientSession.post', return_value=r)
    return r


@pytest.fixture()
def time_is_short(mocker):
    # TODO: Make sure this allows other co-routines to run!
    class AsyncMock(mock.MagicMock):
        async def __call__(self, *args, **kwargs):
            return super(AsyncMock, self).__call__(*args, **kwargs)

    mocker.patch('asyncio.sleep', new_callable=AsyncMock)

@pytest.mark.asyncio
async def test_good_run_single_ds(good_transform_request, time_is_short):
    'Simple run with expected results'
    r = await fe.get_data('(valid qastle string)', 'one_ds')
    assert isinstance(r, pd.DataFrame)
    assert len(r) == 4
