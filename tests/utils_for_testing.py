import pytest
from json import loads, dumps
import os
import tempfile
import shutil


class ClientSessionMocker:
    def __init__(self, text, status):
        self._text = text
        self.status = status

    async def text(self):
        return self._text

    async def json(self):
        return loads(self._text)

    async def __aexit__(self, exc_type, exc, tb):
        pass

    async def __aenter__(self):
        return self


@pytest.fixture()
def good_transform_request(mocker):
    '''
    Setup to run a good transform request that returns a single file.
    '''
    called_json_data = {}
    counter = 0

    def call_post(data_dict_to_save: dict, json=None):
        data_dict_to_save.update(json)
        nonlocal counter
        counter = counter + 1
        return ClientSessionMocker(dumps({"request_id": f"1234-4433-111-34-22-444-{counter}"}), 200)

    mocker.patch('aiohttp.ClientSession.post', side_effect=lambda _, json: call_post(called_json_data, json=json))

    r2 = ClientSessionMocker(dumps({"files-remaining": "0", "files-processed": "1"}), 200)
    mocker.patch('aiohttp.ClientSession.get', return_value=r2)

    return called_json_data


@pytest.fixture(autouse=True)
def delete_default_downloaded_files():
    download_location = os.path.join(tempfile.gettempdir(), 'servicex-testing')
    import servicex.utils as sx
    sx.default_file_cache_name = download_location
    if os.path.exists(download_location):
        shutil.rmtree(download_location)
    yield
    if os.path.exists(download_location):
        shutil.rmtree(download_location)
