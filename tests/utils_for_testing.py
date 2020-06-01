import pytest
from json import loads
from pathlib import Path
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
    Setup a good transform request
    '''
    counter = -1

    async def call_submit(client, endpoint, json):
        nonlocal counter
        counter += 1
        return f"1234-4433-111-34-22-444-{counter}"

    return mocker.patch('servicex.servicex._submit_query',
                        side_effect=call_submit)


@pytest.fixture()
def files_in_minio(mocker):
    '''
    How many files are we returning?
    '''
    count = 1

    def default_lambda():
        return range(count)

    def reverse_lambda():
        return range(count, 0, -1)

    file_range = default_lambda

    async def return_files():
        for i in file_range():
            yield f'file-name-{i}'

    mocker.patch('servicex.servicex._result_object_list.files', side_effect=return_files)

    async def get_status(c, ep, req_id):
        return 0, count, 0

    mocker.patch('servicex.servicex._get_transform_status', side_effect=get_status)

    async def download(client, request_id, fname, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open('w') as o:
            o.write('hi')
    mocker.patch('servicex.servicex._download_file', side_effect=download)

    def reset_files(n_files: int, reverse: bool = False):
        nonlocal count, file_range
        count = n_files
        if not reverse:
            file_range = default_lambda
        else:
            file_range = reverse_lambda

    return reset_files


@pytest.fixture(autouse=True)
def delete_default_downloaded_files():
    download_location = Path(tempfile.gettempdir()) / 'servicex-testing'
    import servicex.utils as sx
    sx.default_file_cache_name = download_location
    if os.path.exists(download_location):
        shutil.rmtree(download_location)
    import servicex.servicex as ssx
    # import weakref
    # ssx._data_cache = weakref.WeakValueDictionary()
    ssx._data_cache = {}
    ssx._query_locks = {}
    yield
    if os.path.exists(download_location):
        shutil.rmtree(download_location)
    ssx._data_cache = {}
    ssx._query_locks = {}
    # ssx._data_cache = weakref.WeakValueDictionary()
