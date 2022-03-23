import os
import tempfile
from datetime import timedelta
from pathlib import Path
from typing import Optional

import aiohttp
import backoff
import pytest
from servicex.utils import (
    _query_cache_hash,
    _status_update_wrapper,
    clean_linq,
    dataset_as_name,
    default_client_session,
    get_configured_cache_path,
    log_adaptor,
    on_exception_itr,
    stream_status_updates,
    stream_unique_updates_only,
    write_query_log,
)

from .conftest import as_async_seq


@pytest.fixture
def log_location_file(tmp_path):
    l_file = tmp_path / "log.csv"
    if l_file.exists():
        l_file.unlink()
    yield l_file
    if l_file.exists():
        l_file.unlink()


@pytest.mark.asyncio
async def test_client_session():
    c = await default_client_session()
    assert isinstance(c, aiohttp.ClientSession)


@pytest.mark.asyncio
async def test_client_session_same():
    # On same thread they should be the same.
    c1 = await default_client_session()
    c2 = await default_client_session()

    assert c1 is c2


@pytest.mark.asyncio
async def test_client_session_different_threads():
    async def get_a_client_async():
        return await default_client_session()

    from make_it_sync import make_sync

    get_a_client = make_sync(get_a_client_async)

    c1 = get_a_client()
    c2 = await get_a_client_async()

    assert c1 is not c2


def test_log_file_created(log_location_file):
    write_query_log(
        "123",
        10,
        0,
        timedelta(seconds=20),
        True,
        path_to_log_dir=log_location_file.parent,
    )
    assert log_location_file.exists()
    t = log_location_file.read_text().split("\n")[1]
    assert t == "123,10,0,20.0,1"


def test_log_file_none_files(log_location_file):
    write_query_log(
        "123",
        None,
        0,
        timedelta(seconds=20),
        True,
        path_to_log_dir=log_location_file.parent,
    )
    assert log_location_file.exists()
    t = log_location_file.read_text().split("\n")[1]
    assert t == "123,-1,0,20.0,1"


def test_log_file_write_minutes(log_location_file):
    write_query_log(
        "123",
        10,
        0,
        timedelta(minutes=2),
        True,
        path_to_log_dir=log_location_file.parent,
    )
    assert log_location_file.exists()
    t = log_location_file.read_text().split("\n")[1]
    assert t == "123,10,0,120.0,1"


def test_log_adaptor(log_location_file):
    log_adaptor.write_query_log(
        "123",
        10,
        0,
        timedelta(seconds=20),
        True,
        path_to_log_dir=log_location_file.parent,
    )
    assert log_location_file.exists()
    t = log_location_file.read_text().split("\n")[1]
    assert t == "123,10,0,20.0,1"


def test_log_file_write_2lines(log_location_file):
    write_query_log(
        "123",
        10,
        0,
        timedelta(minutes=2),
        True,
        path_to_log_dir=log_location_file.parent,
    )
    write_query_log(
        "124",
        10,
        0,
        timedelta(minutes=2),
        True,
        path_to_log_dir=log_location_file.parent,
    )
    assert log_location_file.exists()
    t = log_location_file.read_text()
    assert len(t.split("\n")) == 4


def test_callback_no_updates():
    called = False

    def call_me(total: Optional[int], processed: int, downloaded: int, failed: int):
        nonlocal called
        called = True

    u = _status_update_wrapper(call_me)
    u.broadcast()


def test_callback_processed_set():
    "Sometimes Servicex gets called when there is no total yet."

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
    "Sometimes Servicex gets called when there is no total yet."

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
    "Sometimes Servicex gets called when there is no total yet."

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
    "We want to restart."

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
    "What if there is no callback?"

    u = _status_update_wrapper(None)
    u.update(processed=10)
    u.update(downloaded=3)
    u.update(remaining=12)
    u.update(failed=1)
    u.broadcast()


def test_callback_with_total_fluctuation():
    "Sometimes we get the total wrong.."

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
    "Make sure we can del with multiple things at once"

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

    v = [
        i async for i in stream_status_updates(as_async_seq([(1, 0, 0), (0, 1, 0)]), u)
    ]

    assert len(v) == 2
    assert u.failed == 0
    assert u.total == 1


@pytest.mark.asyncio
async def test_transform_updates_unique():

    v = [
        i
        async for i in stream_unique_updates_only(as_async_seq([(1, 0, 0), (0, 1, 0)]))
    ]

    assert len(v) == 2


@pytest.mark.asyncio
async def test_transform_updates_unique_2():

    v = [
        i
        async for i in stream_unique_updates_only(as_async_seq([(1, 0, 0), (1, 0, 0)]))
    ]

    assert len(v) == 1


def test_cache_stable():
    json_query = {
        "did": "dude_001",
        "selection": "(valid qastle)",
        "chunk-size": 1000,
        "workers": 10,
    }
    h1 = _query_cache_hash(json_query)
    h2 = _query_cache_hash(json_query)

    assert h1 == h2


def test_cache_query_agnostic():
    json_query = {
        "did": "dude_001",
        "selection": "(lambda (list a0 a1) (+ a0 a1))",
        "chunk-size": 1000,
        "workers": 10,
    }
    h1 = _query_cache_hash(json_query)
    json_query["selection"] = "(lambda (list b0 b1) (+ b0 b1))"
    h2 = _query_cache_hash(json_query)

    assert h1 == h2


def test_request_trans_cache_workers():
    json_query = {
        "did": "dude_001",
        "selection": "(valid qastle)",
        "chunk-size": 1000,
        "workers": 10,
    }
    h1 = _query_cache_hash(json_query)
    json_query["workers"] = 100
    h2 = _query_cache_hash(json_query)
    assert h1 == h2


def test_request_trans_cache_selection():
    json_query = {
        "did": "dude_001",
        "selection": "(valid qastle)",
        "chunk-size": 1000,
        "workers": 10,
    }
    h1 = _query_cache_hash(json_query)
    json_query["selection"] = "(call valid qastle)"
    h2 = _query_cache_hash(json_query)
    assert h1 != h2


def test_request_trans_cache_did():
    json_query = {
        "did": "dude_001",
        "selection": "(valid qastle)",
        "chunk-size": 1000,
        "workers": 10,
    }
    h1 = _query_cache_hash(json_query)
    json_query["did"] = "did_002"
    h2 = _query_cache_hash(json_query)
    assert h1 != h2


def test_request_trans_cache_unknown():
    json_query = {
        "did": "dude_001",
        "selection": "(valid qastle)",
        "chunk-size": 1000,
        "workers": 10,
    }
    h1 = _query_cache_hash(json_query)
    json_query["fork"] = "me"
    h2 = _query_cache_hash(json_query)
    assert h1 != h2


def test_clean_linq_no_lambda():
    q = "(valid qastle)"
    assert clean_linq(q) == q


def test_clean_linq_bad_lambda():
    q = "(lambda a (+ a 1) (1 2 3))"
    assert clean_linq(q) == q


def test_clean_linq_invalid_qastle():
    q = "(lambda a (+ a 1))"
    assert clean_linq(q) == q


def test_clean_linq_single_lambda():
    q = "(lambda (list e0 e1) (+ e0 e1))"
    assert clean_linq(q) == "(lambda (list a0 a1) (+ a0 a1))"


def test_clean_linq_many_args():
    q = (
        "(lambda (list e0 e1 e2 e3 e4 e5 e6 e7 e8 e9 e10 e11) "
        "(+ e0 e1 e2 e3 e4 e5 e6 e7 e8 e9 e10 e11))"
    )
    assert (
        clean_linq(q) == "(lambda (list a0 a1 a2 a3 a4 a5 a6 a7 a8 a9 a10 a11) "
        "(+ a0 a1 a2 a3 a4 a5 a6 a7 a8 a9 a10 a11))"
    )


def test_clean_linq_funny_var_names():
    q = "(lambda (list e0 e000) (+ e000 e0))"
    assert clean_linq(q) == "(lambda (list a0 a1) (+ a1 a0))"


def test_clean_linq_nested_lambda():
    q = "(lambda (list e0 e1) (+ (call (lambda (list e0) (+ e0 1)) e0) e1))"
    assert (
        clean_linq(q)
        == "(lambda (list a1 a2) (+ (call (lambda (list a0) (+ a0 1)) a1) a2))"
    )


def test_clean_linq_arb_var_names():
    q = "(lambda (list a0 f1) (+ a0 f1))"
    assert clean_linq(q) == "(lambda (list a0 a1) (+ a0 a1))"


def test_clean_linq_empty():
    q = ""
    assert clean_linq(q) == ""


def test_configured_cache_default():
    from confuse import Configuration

    c = Configuration("servicex", "servicex")
    assert c["cache_path"].exists()


def test_configured_cache_temp_location():
    from confuse import Configuration

    c = Configuration("bogus", "bogus")
    c.clear()
    c["cache_path"] = "/tmp/servicex-dude"

    p = get_configured_cache_path(c)

    # Should default to temp directory - should work on all platforms!
    assert p.exists()
    assert str(p).startswith(tempfile.gettempdir())
    assert "servicex-dude" in str(p)


def test_configured_cache_location():
    from confuse import Configuration

    c = Configuration("bogus", "bogus")
    c.clear()
    here = Path("./servicex-dude").absolute()
    c["cache_path"] = str(here)

    p = get_configured_cache_path(c)

    # Should default to temp directory - should work on all platforms!
    assert p.exists()
    assert str(p) == str(here)


def test_cache_expansion_user():
    """On windows this will expand one way, and on linux another. So need to be a little careful here!"""
    # If we are running in a place where USER/Username does not exist!
    if "USER" not in os.environ and "UserName" not in os.environ:
        os.environ["USER"] = "bogus"

    from confuse import Configuration

    c = Configuration("bogus", "bogus")
    c.clear()
    c["cache_path"] = "/tmp/servicex_${USER}"

    # Get the right answer, depending on the definition of USER
    u_name = os.environ["USER"] if "USER" in os.environ else os.environ["UserName"]
    path_name = f"servicex_{u_name}"

    p = get_configured_cache_path(c)

    # Should default to temp directory - should work on all platforms!
    assert p.name == path_name


def test_cache_expansion_username():
    """On windows this will expand one way, and on linux another. So need to be a little careful here!"""
    # If we are running in a place where USER/Username does not exist!
    if "USER" not in os.environ and "UserName" not in os.environ:
        os.environ["USER"] = "bogus"

    from confuse import Configuration

    c = Configuration("bogus", "bogus")
    c.clear()
    c["cache_path"] = "/tmp/servicex_${UserName}"

    # Get the right answer, depending on the definition of USER
    u_name = os.environ["USER"] if "USER" in os.environ else os.environ["UserName"]
    path_name = f"servicex_{u_name}"

    p = get_configured_cache_path(c)

    # Should default to temp directory - should work on all platforms!
    assert p.name == path_name


def test_bar_name_title():
    "Test various uses of the progress bar title generator"
    assert dataset_as_name(None, None) == "<none>"

    assert dataset_as_name("sample1", None) == "sample1"
    assert (
        dataset_as_name("sample1sample1sample1sample1", None)
        == "sample1sample1sample..."
    )

    assert dataset_as_name(["s1", "s2", "s3"], None) == "[s1, s2, s3]"
    assert (
        dataset_as_name(
            ["sample1sample1sample1sample1", "sample1sample1sample1sample1"], None
        )
        == "[sample1sample1sampl..."
    )

    assert dataset_as_name(sample_name="sample1", title="hi") == "hi"
    assert (
        dataset_as_name("hi", "sample1sample1sample1sample1")
        == "sample1sample1sample..."
    )


def test_bar_name_long():
    "Try a longer version"
    assert (
        dataset_as_name("sample1sample1sample1sample1", max_len=None)
        == "sample1sample1sample1sample1"
    )


@pytest.mark.asyncio()
async def test_async_itr_good():
    @on_exception_itr(backoff.constant, ValueError, interval=0.01, max_tries=2)
    async def return_two():
        yield 1
        yield 2

    lst = [item async for item in return_two()]
    assert lst == [1, 2]


@pytest.mark.asyncio()
async def test_async_itr_exception_first():
    first = True

    @on_exception_itr(backoff.constant, ValueError, interval=0.01, max_tries=2)
    async def return_two():
        nonlocal first
        if first:
            first = False
            raise ValueError("hi")
        yield 1
        yield 2

    lst = [item async for item in return_two()]
    assert lst == [1, 2]


@pytest.mark.asyncio()
async def test_async_itr_exception_not_first():
    count = 0

    @on_exception_itr(backoff.constant, ValueError, interval=0.01, max_tries=2)
    async def return_two():
        nonlocal count
        count = count + 1
        yield 1
        raise ValueError("hi")

    with pytest.raises(ValueError) as e:
        [item async for item in return_two()]

    assert count == 1
    assert "hi" in str(e)


@pytest.mark.asyncio()
async def test_async_itr_exception_all():
    count = 0

    @on_exception_itr(backoff.constant, ValueError, interval=0.01, max_tries=2)
    async def return_two():
        nonlocal count
        count = count + 1
        raise ValueError("hi")
        yield 1  # NOQA

    with pytest.raises(ValueError) as e:
        [item async for item in return_two()]

    assert count == 2
    assert "hi" in str(e)


@pytest.mark.asyncio()
async def test_async_itr_exception_all_wait_iter():
    count = 0

    @on_exception_itr(backoff.constant, ValueError, interval=0.01, max_tries=2)
    async def return_two():
        nonlocal count
        count = count + 1
        raise ValueError("hi")
        yield 1

    with pytest.raises(ValueError) as e:
        [item async for item in return_two()]

    assert count == 2
    assert "hi" in str(e)
