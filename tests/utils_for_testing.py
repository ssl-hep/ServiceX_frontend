import asyncio
from json import loads
import os
from pathlib import Path
import shutil
import tempfile
from typing import Any, Dict, List, Optional, Tuple
from unittest.mock import Mock

import aiohttp
import pytest
from pytest_mock import MockFixture

from servicex import ServiceXException, ServiceXUnknownRequestID
from servicex.cache import cache
from servicex.servicex_adaptor import ServiceXAdaptor


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

    return mocker.patch('servicex.servicex_adaptor.ServiceXAdaptor.submit_query',
                        side_effect=call_submit)


@pytest.fixture()
def bad_transform_request(mocker):
    '''
    Setup a bad transform request
    '''

    return mocker.patch('servicex.servicex_adaptor.ServiceXAdaptor.submit_query',
                        side_effect=ServiceXException('Error transform 400'))


@pytest.fixture()
def files_in_minio(mocker):
    '''
    How many files are we returning?
    '''
    count = 1
    mark_failed = 0

    def default_lambda():
        return range(count - mark_failed)

    def reverse_lambda():
        return range(count, 0, -1)

    file_range = default_lambda
    status_call_count = 0

    async def return_files():
        while status_call_count > -1:
            await asyncio.sleep(0.02)
        for i in file_range():
            yield f'file-name-{i}'

    p_list_files = mocker.patch('servicex.minio_adaptor.ResultObjectList.files', side_effect=return_files)

    async def get_status(c, ep, req_id):
        nonlocal status_call_count
        if status_call_count > 0:
            status_call_count -= 1
            return count, 0, 0
        else:
            status_call_count = -1
            return 0, count - mark_failed, mark_failed

    p_get_transform_status = mocker.patch('servicex.servicex_adaptor.ServiceXAdaptor.get_transform_status', side_effect=get_status)

    async def download(client, request_id, fname, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open('w') as o:
            o.write('hi')

    p_download_file = mocker.patch('servicex.minio_adaptor.MinioAdaptor.download_file', side_effect=download)

    def reset_files(n_files: int, reverse: bool = False,
                    status_calls_before_complete: int = 0,
                    as_failed: int = 0) -> Dict[str, int]:

        nonlocal count, file_range, status_call_count, mark_failed

        count = n_files

        if not reverse:
            file_range = default_lambda
        else:
            file_range = reverse_lambda

        status_call_count = status_calls_before_complete

        if as_failed > count:
            raise Exception(f'Not allowed to have more failed ({as_failed}) than total files ({count})')
        mark_failed = as_failed

        return {
            'list_files': p_list_files,
            'get_transform_status': p_get_transform_status,
            'download_file': p_download_file
        }

    return reset_files


class MockServiceXAdaptor:
    def __init__(self, mocker: MockFixture, request_id: str, mock_transform_status: Mock = None, mock_query: Mock = None):
        self.request_id = request_id
        self._endpoint = "http://localhost:5000"
        self.requests_made = 0

        def do_unique_id():
            id = self.request_id.format(self.requests_made)
            self.requests_made += 1
            return id

        self.query = mock_query \
            if mock_query \
            else mocker.Mock(side_effect=do_unique_id)

        self.transform_status = mock_transform_status \
            if mock_transform_status \
            else mocker.Mock(return_value=(0, 1, 0))

    async def submit_query(self, client: aiohttp.ClientSession,
                           json_query: Dict[str, str]) -> str:
        self.query_json = json_query
        return self.query()

    async def get_transform_status(self, client: str, request_id: str) -> Tuple[Optional[int], int, Optional[int]]:
        # remaining, processed, skipped
        return self.transform_status()


class MockMinioAdaptor:
    def __init__(self, mocker: MockFixture, files: List[str] = []):
        self._files = files
        self.mock_download_file = mocker.Mock()
        pass

    async def download_file(self, request_id: str, minio_object_name: str, final_path: Path):
        self.mock_download_file(request_id, minio_object_name, final_path)

    def get_files(self, request_id) -> List[str]:
        'Return files in the bucket'
        return self._files


def build_cache_mock(mocker, query_cache_return=None) -> cache:
    c = mocker.MagicMock(spec=cache)

    c.lookup_inmem.return_value = None
    c.lookup_files.return_value = None

    if query_cache_return is None:
        c.lookup_query.return_value = None
    else:
        c.lookup_query.return_value = query_cache_return

    return c


@pytest.fixture
def servicex_adaptor(mocker):
    return mocker.AsyncMock(spec=ServiceXAdaptor)


@pytest.fixture
def servicex_state_machine(mocker):
    '''
    Implement a state machine to run more complex tests.
    '''
    valid_request_id = None
    file_count_stop = False

    async def call_submit(client, endpoint, json):
        import uuid
        nonlocal valid_request_id, file_count_stop
        valid_request_id = str(uuid.uuid4())
        file_count_stop = False
        return valid_request_id

    p_submit_query = mocker.patch('servicex.servicex_adaptor.ServiceXAdaptor.submit_query',
                                  side_effect=call_submit)

    steps = []
    step_index = 0

    def add_status_step(processed: int, remaining: int, failed: int):
        steps.append({'processed': processed, 'remaining': remaining, 'failed': failed})

    def add_status_fail(ex):
        steps.append({'raise': ex})

    file_count_in_minio = 0
    file_count_trigger = False

    def reset(keep_request_id: bool = False):
        nonlocal steps, step_index, file_count_in_minio, file_count_stop, file_count_trigger, valid_request_id
        steps = []
        step_index = 0
        file_count_in_minio = 0
        file_count_stop = False
        file_count_trigger = False
        if not keep_request_id:
            valid_request_id = None

    async def get_status(c, ep, req_id):
        nonlocal step_index

        if req_id != valid_request_id:
            raise ServiceXUnknownRequestID(f"Unknown request id {req_id} - so this test is going to bail. Might be test error (known: {valid_request_id}")

        i = step_index if step_index < len(steps) else len(steps) - 1
        step_index += 1
        if 'raise' in steps[i]:
            raise steps[i]['raise']
        nonlocal file_count_in_minio, file_count_trigger
        file_count_in_minio = steps[i]['processed']
        file_count_trigger = True
        return steps[i]['remaining'], steps[i]['processed'], steps[i]['failed']

    mocker.patch('servicex.servicex_adaptor.ServiceXAdaptor.get_transform_status', side_effect=get_status)

    async def return_files():
        nonlocal file_count_trigger
        sent = set()
        while (not file_count_stop) or (len(sent) < file_count_in_minio):
            for i in range(file_count_in_minio):
                if i not in sent:
                    yield f'file-name-{i}'
                    sent.add(i)
            while not file_count_stop and not file_count_trigger:
                await asyncio.sleep(0.05)
            file_count_trigger = False

    def files_shutdown():
        nonlocal file_count_stop
        file_count_stop = True

    mocker.patch('servicex.minio_adaptor.ResultObjectList.files', side_effect=return_files)
    mocker.patch('servicex.minio_adaptor.ResultObjectList.shutdown', side_effect=files_shutdown)

    async def download(client, request_id, fname, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open('w') as o:
            o.write('hi')

    mocker.patch('servicex.minio_adaptor.MinioAdaptor.download_file', side_effect=download)

    return {
        'reset': reset,
        'add_status_step': add_status_step,
        'add_status_file': add_status_step,
        'add_status_fail': add_status_fail,
        'patch_submit_query': p_submit_query
    }


@pytest.fixture
def no_files_in_minio(mocker):
    async def return_files():
        await asyncio.sleep(0.02)
        for i in range(0):
            yield f'file-name-{i}'

    mocker.patch('servicex.minio_adaptor.ResultObjectList.files', side_effect=return_files)


@pytest.fixture(autouse=True)
def delete_default_downloaded_files():
    download_location = Path(tempfile.gettempdir()) / 'servicex-testing'
    import servicex.utils as sx
    sx.default_file_cache_name = download_location
    if os.path.exists(download_location):
        shutil.rmtree(download_location)
    cache.reset_cache()
    yield
    if os.path.exists(download_location):
        shutil.rmtree(download_location)
    cache.reset_cache()


@pytest.fixture
def good_root_file_path():
    return Path("tests/sample_servicex_output.root")


@pytest.fixture
def good_pandas_file_data(mocker):
    import pandas as pd

    async def get_pandas_dummy_data(fname: str):
        df = pd.DataFrame({'JetPt': [0, 1, 2, 3, 4, 5]})
        return df

    mocker.patch('servicex.servicex._convert_root_to_pandas', side_effect=get_pandas_dummy_data)


@pytest.fixture
def good_awkward_file_data(mocker):
    import awkward as awk

    async def good_awkward_data(fname: str):
        df = {b'JetPt': awk.fromiter([0, 1, 2, 3, 4, 5])}
        return df

    mocker.patch('servicex.servicex._convert_root_to_awkward', side_effect=good_awkward_data)


@pytest.fixture
def short_status_poll_time():
    import servicex.servicex_adaptor as sxs
    old_value, sxs.servicex_status_poll_time = sxs.servicex_status_poll_time, 0.1
    yield
    sxs.servicex_status_poll_time = old_value


async def as_async_seq(seq: List[Any]):
    for i in seq:
        yield i
