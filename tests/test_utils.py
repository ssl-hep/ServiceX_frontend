from typing import Optional

import pytest

from servicex.utils import (
    _query_cache_hash,
    _status_update_wrapper,
    clean_linq,
    stream_transform_updates,
)

from .utils_for_testing import as_async_seq


def test_callback_no_updates():
    called = False

    def call_me(total: Optional[int], processed: int, downloaded: int, failed: int):
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
    p_failed = None

    def call_me(total: Optional[int], processed: int, downloaded: int, failed: int):
        nonlocal called, p_total, p_processed, p_downloaded, p_failed
        called = True
        p_total = total
        p_processed = processed
        p_downloaded = downloaded
        p_failed = failed

    u = _status_update_wrapper(call_me)
    u.update(processed=10)
    u.broadcast()
    assert called
    assert p_total is None
    assert p_downloaded == 0
    assert p_processed == 10
    assert p_failed == 0


def test_callback_processed_and_downloaded():
    'Sometimes Servicex gets called when there is no total yet.'

    called = False
    p_total = None
    p_processed = None
    p_downloaded = None
    p_failed = None

    def call_me(total: Optional[int], processed: int, downloaded: int, failed: int):
        nonlocal called, p_total, p_processed, p_downloaded, p_failed
        called = True
        p_total = total
        p_processed = processed
        p_downloaded = downloaded
        p_failed = failed

    u = _status_update_wrapper(call_me)
    u.update(processed=10)
    u.update(downloaded=3)
    u.broadcast()
    assert called
    assert p_total is None
    assert p_downloaded == 3
    assert p_processed == 10
    assert p_failed == 0


def test_callback_everything():
    'Sometimes Servicex gets called when there is no total yet.'

    called = False
    p_total = None
    p_processed = None
    p_downloaded = None
    p_failed = None

    def call_me(total: Optional[int], processed: int, downloaded: int, failed: int):
        nonlocal called, p_total, p_processed, p_downloaded, p_failed
        called = True
        p_total = total
        p_processed = processed
        p_downloaded = downloaded
        p_failed = failed

    u = _status_update_wrapper(call_me)
    u.update(processed=10)
    u.update(downloaded=2)
    u.update(failed=1)
    u.update(remaining=1)
    u.broadcast()
    assert called
    assert p_total == 12
    assert p_downloaded == 2
    assert p_processed == 10
    assert p_failed == 1


def test_callback_reset():
    'We want to restart.'

    called = False
    p_total = None
    p_processed = None
    p_downloaded = None
    p_failed = None

    def call_me(total: Optional[int], processed: int, downloaded: int, failed: int):
        nonlocal called, p_total, p_processed, p_downloaded, p_failed
        called = True
        p_total = total
        p_processed = processed
        p_downloaded = downloaded
        p_failed = failed

    u = _status_update_wrapper(call_me)
    u.update(processed=10)
    u.update(downloaded=2)
    u.update(failed=1)
    u.update(remaining=2)
    u.reset()
    u.update(processed=10)
    u.update(downloaded=2)
    u.update(remaining=2)
    u.broadcast()
    assert called
    assert p_total == 12
    assert p_downloaded == 2
    assert p_processed == 10
    assert p_failed == 0


def test_callback_inc_with_nothing():
    called = False
    p_total = None
    p_processed = None
    p_downloaded = None
    p_failed = None

    def call_me(total: Optional[int], processed: int, downloaded: int, failed: int):
        nonlocal called, p_total, p_processed, p_downloaded, p_failed
        called = True
        p_total = total
        p_processed = processed
        p_downloaded = downloaded
        p_failed = failed

    u = _status_update_wrapper(call_me)
    u.inc(downloaded=3)
    u.broadcast()
    assert p_downloaded == 3


def test_callback_inc_with_already_set():
    called = False
    p_total = None
    p_processed = None
    p_downloaded = None
    p_failed = None

    def call_me(total: Optional[int], processed: int, downloaded: int, failed: int):
        nonlocal called, p_total, p_processed, p_downloaded, p_failed
        called = True
        p_total = total
        p_processed = processed
        p_downloaded = downloaded
        p_failed = failed

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
    u.update(remaining=12)
    u.update(failed=1)
    u.broadcast()


def test_callback_with_total_fluctuation():
    'Sometimes we get the total wrong..'

    p_total = None

    def call_me(total: Optional[int], processed: int, downloaded: int, failed: int):
        nonlocal p_total
        p_total = total

    u = _status_update_wrapper(call_me)
    u.update(processed=6, remaining=6)
    u.broadcast()
    assert p_total == 12
    u.update(processed=6, remaining=5)
    u.broadcast()
    assert p_total == 12
    u.update(processed=7, remaining=6)
    u.broadcast()
    assert p_total == 13


def test_callback_with_total_sequence():
    'Make sure we can del with multiple things at once'

    p_total = None

    def call_me(total: Optional[int], processed: int, downloaded: int, failed: int):
        nonlocal p_total
        p_total = total

    u = _status_update_wrapper(call_me)
    u.update(processed=0, remaining=6)
    u.update(processed=2, remaining=4)
    u.broadcast()
    assert p_total == 6


@pytest.mark.asyncio
async def test_transform_sequence():
    u = _status_update_wrapper()

    v = [i async for i in stream_transform_updates(as_async_seq([(1, 0, 0), (0, 1, 0)]), u)]

    assert len(v) == 2
    assert u.failed == 0
    assert u.total == 1


def test_cache_stable():
    json_query = {
        'did': "dude_001",
        'selection': "(valid qastle)",
        'chunk-size': 1000,
        'workers': 10,
    }
    h1 = _query_cache_hash(json_query)
    h2 = _query_cache_hash(json_query)

    assert h1 == h2


def test_cache_query_agnostic():
    json_query = {
        'did': "dude_001",
        'selection': "(lambda (list a0 a1) (+ a0 a1))",
        'chunk-size': 1000,
        'workers': 10,
    }
    h1 = _query_cache_hash(json_query)
    json_query['selection'] = '(lambda (list b0 b1) (+ b0 b1))'
    h2 = _query_cache_hash(json_query)

    assert h1 == h2


def test_request_trans_cache_workers():
    json_query = {
        'did': "dude_001",
        'selection': "(valid qastle)",
        'chunk-size': 1000,
        'workers': 10,
    }
    h1 = _query_cache_hash(json_query)
    json_query['workers'] = 100
    h2 = _query_cache_hash(json_query)
    assert h1 == h2


def test_request_trans_cache_selection():
    json_query = {
        'did': "dude_001",
        'selection': "(valid qastle)",
        'chunk-size': 1000,
        'workers': 10,
    }
    h1 = _query_cache_hash(json_query)
    json_query['selection'] = '(call valid qastle)'
    h2 = _query_cache_hash(json_query)
    assert h1 != h2


def test_request_trans_cache_did():
    json_query = {
        'did': "dude_001",
        'selection': "(valid qastle)",
        'chunk-size': 1000,
        'workers': 10,
    }
    h1 = _query_cache_hash(json_query)
    json_query['did'] = 'did_002'
    h2 = _query_cache_hash(json_query)
    assert h1 != h2


def test_request_trans_cache_unknown():
    json_query = {
        'did': "dude_001",
        'selection': "(valid qastle)",
        'chunk-size': 1000,
        'workers': 10,
    }
    h1 = _query_cache_hash(json_query)
    json_query['fork'] = 'me'
    h2 = _query_cache_hash(json_query)
    assert h1 != h2


def test_clean_linq_no_lambda():
    q = '(valid qastle)'
    assert clean_linq(q) == q


def test_clean_linq_bad_lambda():
    q = '(lambda a (+ a 1) (1 2 3))'
    assert clean_linq(q) == q


def test_clean_linq_invalid_qastle():
    q = '(lambda a (+ a 1))'
    assert clean_linq(q) == q


def test_clean_linq_single_lambda():
    q = '(lambda (list e0 e1) (+ e0 e1))'
    assert clean_linq(q) == '(lambda (list a0 a1) (+ a0 a1))'


def test_clean_linq_many_args():
    q = '(lambda (list e0 e1 e2 e3 e4 e5 e6 e7 e8 e9 e10 e11) (+ e0 e1 e2 e3 e4 e5 e6 e7 e8 e9 e10 e11))'
    assert clean_linq(q) == '(lambda (list a0 a1 a2 a3 a4 a5 a6 a7 a8 a9 a10 a11) (+ a0 a1 a2 a3 a4 a5 a6 a7 a8 a9 a10 a11))'


def test_clean_linq_funny_var_names():
    q = '(lambda (list e0 e000) (+ e000 e0))'
    assert clean_linq(q) == '(lambda (list a0 a1) (+ a1 a0))'


def test_clean_linq_nested_lambda():
    q = '(lambda (list e0 e1) (+ (call (lambda (list e0) (+ e0 1)) e0) e1))'
    assert clean_linq(q) == '(lambda (list a1 a2) (+ (call (lambda (list a0) (+ a0 1)) a1) a2))'


def test_clean_linq_arb_var_names():
    q = '(lambda (list a0 f1) (+ a0 f1))'
    assert clean_linq(q) == '(lambda (list a0 a1) (+ a0 a1))'


def test_clean_linq_empty():
    q = ''
    assert clean_linq(q) == ""
