from pathlib import Path
import tempfile

import pytest

from servicex.cache import Cache


@pytest.fixture
def cache_dir(autouse=True):
    p = Path(tempfile.gettempdir()) / 'servicex-cache-testing'
    if p.exists():
        import shutil
        shutil.rmtree(p)
    p.mkdir(parents=True)
    yield p
    if p.exists():
        import shutil
        shutil.rmtree(p)


@pytest.fixture()
def reset_in_memory_cache():
    Cache.reset_cache()
    yield
    Cache.reset_cache()


def test_create_cache(cache_dir):
    _ = Cache(cache_dir)


def test_query_miss(cache_dir):
    c = Cache(cache_dir)

    assert c.lookup_query({'hi': 'there'}) is None


def test_query_hit_1(cache_dir):
    c = Cache(cache_dir)
    c.set_query({'hi': 'there'}, 'dude')
    assert c.lookup_query({'hi': 'there'}) == 'dude'


def test_query_hit_2(cache_dir):
    c = Cache(cache_dir)
    c.set_query({'hi': 'there'}, 'dude1')
    c.set_query({'hi': 'there_1'}, 'dude2')
    assert c.lookup_query({'hi': 'there'}) == 'dude1'
    assert c.lookup_query({'hi': 'there_1'}) == 'dude2'


def test_query_lookup_from_file(cache_dir):
    c1 = Cache(cache_dir)
    c1.set_query({'hi': 'there'}, 'dude')

    c2 = Cache(cache_dir)
    assert c2.lookup_query({'hi': 'there'}) == 'dude'


def test_query_remove(cache_dir):
    c1 = Cache(cache_dir)
    c1.set_query({'hi': 'there'}, 'dude')
    c1.remove_query({'hi': 'there'})
    assert c1.lookup_query({'hi': 'there'}) is None


def test_files_miss(cache_dir):
    c = Cache(cache_dir)
    assert c.lookup_files('1234') is None


def test_files_hit(cache_dir):
    c = Cache(cache_dir)
    c.set_files('1234', [('hi', '1'), ('there', '1')])
    assert c.lookup_files('1234') == [['hi', '1'], ['there', '1']]


def test_files_hit_reloaded(cache_dir):
    c1 = Cache(cache_dir)
    c1.set_files('1234', [('hi', '1'), ('there', '1')])
    c2 = Cache(cache_dir)
    assert c2.lookup_files('1234') == [['hi', '1'], ['there', '1']]


def test_memory_miss(cache_dir):
    c = Cache(cache_dir)
    assert c.lookup_inmem('dude') is None


def test_memory_hit(cache_dir):
    c = Cache(cache_dir)
    r = 10
    c.set_inmem('dude', r)
    assert c.lookup_inmem('dude') is r


def test_memory_hit_accross(cache_dir):
    c1 = Cache(cache_dir)
    r = 10
    c1.set_inmem('dude', r)
    c2 = Cache(cache_dir)
    assert c2.lookup_inmem('dude') is r
