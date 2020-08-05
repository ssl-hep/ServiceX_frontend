import pytest

from servicex.cache import Cache


@pytest.fixture()
def reset_in_memory_cache():
    Cache.reset_cache()
    yield
    Cache.reset_cache()


def test_create_cache(tmp_path):
    _ = Cache(tmp_path)


def test_query_miss(tmp_path):
    c = Cache(tmp_path)

    assert c.lookup_query({'hi': 'there'}) is None


def test_query_hit_1(tmp_path):
    c = Cache(tmp_path)
    c.set_query({'hi': 'there'}, 'dude')
    assert c.lookup_query({'hi': 'there'}) == 'dude'


def test_query_hit_2(tmp_path):
    c = Cache(tmp_path)
    c.set_query({'hi': 'there'}, 'dude1')
    c.set_query({'hi': 'there_1'}, 'dude2')
    assert c.lookup_query({'hi': 'there'}) == 'dude1'
    assert c.lookup_query({'hi': 'there_1'}) == 'dude2'


def test_query_lookup_from_file(tmp_path):
    c1 = Cache(tmp_path)
    c1.set_query({'hi': 'there'}, 'dude')

    c2 = Cache(tmp_path)
    assert c2.lookup_query({'hi': 'there'}) == 'dude'


def test_query_remove(tmp_path):
    c1 = Cache(tmp_path)
    c1.set_query({'hi': 'there'}, 'dude')
    c1.remove_query({'hi': 'there'})
    assert c1.lookup_query({'hi': 'there'}) is None


def test_files_miss(tmp_path):
    c = Cache(tmp_path)
    assert c.lookup_files('1234') is None


def test_files_hit(tmp_path):
    c = Cache(tmp_path)
    c.set_files('1234', [('hi', '1'), ('there', '1')])
    assert c.lookup_files('1234') == [['hi', '1'], ['there', '1']]


def test_files_hit_reloaded(tmp_path):
    c1 = Cache(tmp_path)
    c1.set_files('1234', [('hi', '1'), ('there', '1')])
    c2 = Cache(tmp_path)
    assert c2.lookup_files('1234') == [['hi', '1'], ['there', '1']]


def test_memory_miss(tmp_path):
    c = Cache(tmp_path)
    assert c.lookup_inmem('dude') is None


def test_memory_hit(tmp_path):
    c = Cache(tmp_path)
    r = 10
    c.set_inmem('dude', r)
    assert c.lookup_inmem('dude') is r


def test_memory_hit_accross(tmp_path):
    c1 = Cache(tmp_path)
    r = 10
    c1.set_inmem('dude', r)
    c2 = Cache(tmp_path)
    assert c2.lookup_inmem('dude') is r


def test_data_file_location(tmp_path):
    c = Cache(tmp_path)
    p = c.data_file_location('123-456', 'junk.root')
    assert not p.exists()
    p.touch()
    assert p.exists()
    assert str(p).startswith(str(tmp_path))


def test_data_file_location_twice(tmp_path):
    c = Cache(tmp_path)
    _ = c.data_file_location('123-456', 'junk1.root')
    p2 = c.data_file_location('123-456', 'junk2.root')
    assert not p2.exists()
    p2.touch()
    assert p2.exists()


def test_data_file_bad_file(tmp_path):
    c = Cache(tmp_path)
    p = c.data_file_location(
        '123-456', 'root:::dcache-atlas-xrootd-wan.desy.de:1094::pnfs:desy.de:atlas'
        ':dq2:atlaslocalgroupdisk:rucio:mc15_13TeV:8a:f1:DAOD_STDM3.05630052'
        '._000001.pool.root.198fbd841d0a28cb0d9dfa6340c890273-1.part.minio')
    assert not p.exists()
    p.touch()
    assert p.exists()


def test_query_cache_status(tmp_path):
    c = Cache(tmp_path)

    info = {'request_id': '111-222-333', 'key': 'bogus'}
    c.set_query_status(info)
    info1 = c.lookup_query_status('111-222-333')
    assert info1['key'] == 'bogus'


def test_query_cache_status_bad(tmp_path):
    c = Cache(tmp_path)

    with pytest.raises(Exception):
        c.lookup_query_status('111-222-333')
