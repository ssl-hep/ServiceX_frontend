from json import loads
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from unittest.mock import Mock

import aiohttp
import pytest
from pytest_mock import MockFixture

from servicex.cache import Cache


class ClientSessionMocker:
    def __init__(self, text, status):
        if type(text) == list:
            self._text_iter = iter(text)
        else:
            self._text_iter = iter([text])

        if type(status) == list:
            self._status_iter = iter(status)
        else:
            self._status_iter = iter([status])

    async def text(self):
        return next(self._text_iter)

    async def json(self):
        return loads(next(self._text_iter))

    @property
    def status(self):
        return next(self._status_iter)

    async def __aexit__(self, exc_type, exc, tb):
        pass

    async def __aenter__(self):
        return self


class MockServiceXAdaptor:
    def __init__(self, mocker: MockFixture, request_id: str, mock_transform_status: Mock = None,
                 mock_query: Mock = None):
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

    async def get_transform_status(self, client: str, request_id: str) \
            -> Tuple[Optional[int], int, Optional[int]]:
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


__g_inmem_value = None


def build_cache_mock(mocker, query_cache_return: str = None,
                     files: Optional[List[Tuple[str, str]]] = None,
                     in_memory: Any = None,
                     make_in_memory_work: bool = False) -> Cache:
    c = mocker.MagicMock(spec=Cache)

    if in_memory is None:
        c.lookup_inmem.return_value = None
    else:
        c.lookup_inmem.return_value = in_memory

    if make_in_memory_work:
        def save_it(h, v):
            global __g_inmem_value
            __g_inmem_value = v

        def find_it(h):
            global __g_inmem_value
            return __g_inmem_value

        c.set_inmem.side_effect = save_it

        c.lookup_inmem.side_effect = find_it

    if query_cache_return is None:
        c.lookup_query.return_value = None
    else:
        c.lookup_query.return_value = query_cache_return

    if files is None:
        c.lookup_files.return_value = None
    else:
        c.lookup_files.return_value = files

    return c


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