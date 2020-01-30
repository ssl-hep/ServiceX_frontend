import pytest
from requests.api import patch  # NOQA
import ServiceX_fe as fe
import pandas as pd


@pytest.fixture()
def good_transform_request(requests_mock):
    requests_mock.post(f'http://localhost:5000/servicex/transformation',
                       text='{"request_id": "1234"}')


@pytest.mark.asyncio
async def test_good_run_single_ds(good_transform_request):
    'Simple run with expected results'
    r = await fe.get_data('(valid qastle string)', 'one_ds')
    assert isinstance(r, pd.DataFrame)
