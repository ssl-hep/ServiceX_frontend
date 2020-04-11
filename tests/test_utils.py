from typing import Optional
import aiohttp

import pytest

from servicex.utils import _status_update_wrapper, _submit_or_lookup_transform

from .utils_for_testing import good_transform_request, delete_default_downloaded_files  # NOQA


def test_callback_no_updates():
    called = False

    def call_me(total: Optional[int], processed: int, downloaded: int):
        nonlocal called
        called = True

    u = _status_update_wrapper(call_me)
    u.broadcast()


def test_callback_processed_set():
    'Sometimes Servicex gets called when there is no total yet.'

    called = False
    p_total = None
    p_processed = None
    p_downloaded = None

    def call_me(total: Optional[int], processed: int, downloaded: int):
        nonlocal called, p_total, p_processed, p_downloaded
        called = True
        p_total = total
        p_processed = processed
        p_downloaded = downloaded

    u = _status_update_wrapper(call_me)
    u.update(processed=10)
    u.broadcast()
    assert called
    assert p_total is None
    assert p_downloaded == 0
    assert p_processed == 10


def test_callback_processed_and_downloaded():
    'Sometimes Servicex gets called when there is no total yet.'

    called = False
    p_total = None
    p_processed = None
    p_downloaded = None

    def call_me(total: Optional[int], processed: int, downloaded: int):
        nonlocal called, p_total, p_processed, p_downloaded
        called = True
        p_total = total
        p_processed = processed
        p_downloaded = downloaded

    u = _status_update_wrapper(call_me)
    u.update(processed=10)
    u.update(downloaded=3)
    u.broadcast()
    assert called
    assert p_total is None
    assert p_downloaded == 3
    assert p_processed == 10


def test_callback_everything():
    'Sometimes Servicex gets called when there is no total yet.'

    called = False
    p_total = None
    p_processed = None
    p_downloaded = None

    def call_me(total: Optional[int], processed: int, downloaded: int):
        nonlocal called, p_total, p_processed, p_downloaded
        called = True
        p_total = total
        p_processed = processed
        p_downloaded = downloaded

    u = _status_update_wrapper(call_me)
    u.update(processed=10)
    u.update(downloaded=3)
    u.update(total=12)
    u.broadcast()
    assert called
    assert p_total == 12
    assert p_downloaded == 3
    assert p_processed == 10


def test_callback_inc_with_nothing():
    called = False
    p_total = None
    p_processed = None
    p_downloaded = None

    def call_me(total: Optional[int], processed: int, downloaded: int):
        nonlocal called, p_total, p_processed, p_downloaded
        called = True
        p_total = total
        p_processed = processed
        p_downloaded = downloaded

    u = _status_update_wrapper(call_me)
    u.inc(downloaded=3)
    u.broadcast()
    assert p_downloaded == 3


def test_callback_inc_with_already_set():
    called = False
    p_total = None
    p_processed = None
    p_downloaded = None

    def call_me(total: Optional[int], processed: int, downloaded: int):
        nonlocal called, p_total, p_processed, p_downloaded
        called = True
        p_total = total
        p_processed = processed
        p_downloaded = downloaded

    u = _status_update_wrapper(call_me)
    u.update(downloaded=1)
    u.inc(downloaded=3)
    u.broadcast()
    assert p_downloaded == 4


def test_callback_none():
    'What if there is no callback?'

    u = _status_update_wrapper(None)
    u.update(processed=10)
    u.update(downloaded=3)
    u.update(total=12)
    u.broadcast()


@pytest.mark.asyncio
async def test_request_trans_once(good_transform_request):
    json_query = {
        'did': "dude_001",
        'selection': "(valid qastle)",
        'chunk-size': 1000,
        'workers': 10,
    }
    async with aiohttp.ClientSession() as client:
        rid = await _submit_or_lookup_transform(client, "http://localhost:5000/servicex", json_query)
        assert rid is not None
        req_json = good_transform_request
        assert req_json is not None


@pytest.mark.asyncio
async def test_request_trans_twice(good_transform_request):
    json_query = {
        'did': "dude_001",
        'selection': "(valid qastle)",
        'chunk-size': 1000,
        'workers': 10,
    }
    async with aiohttp.ClientSession() as client:
        rid1 = await _submit_or_lookup_transform(client, "http://localhost:5000/servicex", json_query)
        rid2 = await _submit_or_lookup_transform(client, "http://localhost:5000/servicex", json_query)

        assert rid1 == rid2


@pytest.mark.asyncio
async def test_request_trans_cache_workers(good_transform_request):
    json_query = {
        'did': "dude_001",
        'selection': "(valid qastle)",
        'chunk-size': 1000,
        'workers': 10,
    }
    async with aiohttp.ClientSession() as client:
        rid1 = await _submit_or_lookup_transform(client, "http://localhost:5000/servicex", json_query)
        json_query['workers'] = 100
        rid2 = await _submit_or_lookup_transform(client, "http://localhost:5000/servicex", json_query)

        assert rid1 == rid2


@pytest.mark.asyncio
async def test_request_trans_cache_selection(good_transform_request):
    json_query = {
        'did': "dude_001",
        'selection': "(valid qastle)",
        'chunk-size': 1000,
        'workers': 10,
    }
    async with aiohttp.ClientSession() as client:
        rid1 = await _submit_or_lookup_transform(client, "http://localhost:5000/servicex", json_query)
        json_query['selection'] = '(call valid qastle)'
        rid2 = await _submit_or_lookup_transform(client, "http://localhost:5000/servicex", json_query)

        assert rid1 != rid2


@pytest.mark.asyncio
async def test_request_trans_cache_did(good_transform_request):
    json_query = {
        'did': "dude_001",
        'selection': "(valid qastle)",
        'chunk-size': 1000,
        'workers': 10,
    }
    async with aiohttp.ClientSession() as client:
        rid1 = await _submit_or_lookup_transform(client, "http://localhost:5000/servicex", json_query)
        json_query['did'] = 'did_002'
        rid2 = await _submit_or_lookup_transform(client, "http://localhost:5000/servicex", json_query)

        assert rid1 != rid2


@pytest.mark.asyncio
async def test_request_trans_cache_unknown(good_transform_request):
    json_query = {
        'did': "dude_001",
        'selection': "(valid qastle)",
        'chunk-size': 1000,
        'workers': 10,
    }
    async with aiohttp.ClientSession() as client:
        rid1 = await _submit_or_lookup_transform(client, "http://localhost:5000/servicex", json_query)
        json_query['fork'] = 'me'
        rid2 = await _submit_or_lookup_transform(client, "http://localhost:5000/servicex", json_query)

        assert rid1 != rid2
