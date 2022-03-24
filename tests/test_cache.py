import random
from servicex.cache import reset_local_query_cache
import string
from pathlib import Path

import pytest
from servicex import Cache, ServiceXException, ignore_cache, update_local_query_cache


@pytest.fixture()
def reset_in_memory_cache():
    Cache.reset_cache()
    yield
    Cache.reset_cache()


@pytest.fixture(autouse=True)
def analysis_query_cache_reset():
    "Want to make sure the local query cache is reset for everything!"
    reset_local_query_cache()
    cf = Path("./servicex_query_cache.json")
    if cf.exists():
        cf.unlink()
    yield
    reset_local_query_cache()
    if cf.exists():
        cf.unlink()


def test_create_cache(tmp_path):
    c = Cache(tmp_path)
    assert c.path == tmp_path


def test_query_miss(tmp_path):
    c = Cache(tmp_path)

    assert c.lookup_query({"hi": "there"}) is None


def test_query_hit_1(tmp_path):
    c = Cache(tmp_path)
    c.set_query({"hi": "there"}, "dude")
    assert c.lookup_query({"hi": "there"}) == "dude"


def test_analysis_cache_set_twice_different(tmp_path: Path):
    "Make sure we cannot set with two different paths"
    update_local_query_cache(tmp_path / "analysis_cache1.json")
    with pytest.raises(ServiceXException):
        update_local_query_cache(tmp_path / "analysis_cache2.json")


def test_analysis_cache_set_twice_different_paths(tmp_path: Path):
    "Make sure we cannot set with two different paths"
    update_local_query_cache(tmp_path / "bogus1" / "analysis_cache.json")
    with pytest.raises(ServiceXException):
        update_local_query_cache(tmp_path / "bogus2" / "analysis_cache.json")


def test_analysis_cache_set_twice_same(tmp_path: Path):
    "Make sure we can set with two same paths"
    update_local_query_cache(tmp_path / "analysis_cache1.json")
    update_local_query_cache(tmp_path / "analysis_cache1.json")


def test_query_hit_analysis_cache(tmp_path: Path):
    "Make sure the analysis cache is updated"
    cache_loc_1 = tmp_path / "cache1"
    cache_loc_2 = tmp_path / "cache2"

    update_local_query_cache(tmp_path / "analysis_cache.json")

    c1 = Cache(cache_loc_1)
    c1.set_query({"hi": "there"}, "dude")

    c2 = Cache(cache_loc_2)
    assert c2.lookup_query({"hi": "there"}) == "dude"


def test_query_hit_analysis_no_cache(tmp_path: Path):
    "Make sure no caching occurs if the user doesn't want it to"
    cache_loc_1 = tmp_path / "cache1"
    cache_loc_2 = tmp_path / "cache2"

    c1 = Cache(cache_loc_1)
    c1.set_query({"hi": "there"}, "dude")

    c2 = Cache(cache_loc_2)
    assert c2.lookup_query({"hi": "there"}) is None


def test_query_miss_analysis_cache_keys(tmp_path: Path):
    "Make sure the analysis cache is updated"
    cache_loc_1 = tmp_path / "cache1"
    cache_loc_2 = tmp_path / "cache2"

    update_local_query_cache(tmp_path / "analysis_cache.json")

    c1 = Cache(cache_loc_1)
    c1.set_query({"hi": "there"}, "dude")

    c2 = Cache(cache_loc_2, analysis_query_key="bogus")
    assert c2.lookup_query({"hi": "there"}) is None


def test_query_hit_analysis_cache_update(tmp_path: Path):
    "Make sure the analysis cache is updated"
    cache_loc_1 = tmp_path / "cache1"
    cache_loc_2 = tmp_path / "cache2"

    update_local_query_cache(tmp_path / "analysis_cache.json")

    c1 = Cache(cache_loc_1)
    c1.set_query({"hi": "there"}, "dude")
    c1.set_query({"hi1": "there1"}, "dude1")

    c2 = Cache(cache_loc_2)
    assert c2.lookup_query({"hi": "there"}) == "dude"
    assert c2.lookup_query({"hi1": "there1"}) == "dude1"


def test_query_hit_analysis_cache_silent_read(tmp_path: Path):
    "Make sure we read the analysis cache if it happens to be present"
    cache_loc_1 = tmp_path / "cache1"
    cache_loc_2 = tmp_path / "cache2"

    update_local_query_cache()

    c1 = Cache(cache_loc_1)
    c1.set_query({"hi": "there"}, "dude")

    reset_local_query_cache()

    c2 = Cache(cache_loc_2)
    assert c2.lookup_query({"hi": "there"}) == "dude"


def test_query_hit_analysis_cache_empty_file(tmp_path: Path):
    "Make sure we read the analysis cache if it happens to be present"
    cache_loc_1 = tmp_path / "cache1"
    cache_loc_2 = tmp_path / "cache2"

    qc = Path("servicex_query_cache.json")
    qc.touch()

    c1 = Cache(cache_loc_1)
    c1.set_query({"hi": "there"}, "dude")

    reset_local_query_cache()

    c2 = Cache(cache_loc_2)
    assert c2.lookup_query({"hi": "there"}) == "dude"


def test_query_hit_analysis_cache_not_triggered(tmp_path: Path):
    "Make sure analysis cache is not written by default"
    cache_loc_1 = tmp_path / "cache1"
    cache_loc_2 = tmp_path / "cache2"

    c1 = Cache(cache_loc_1)
    c1.set_query({"hi": "there"}, "dude")

    c2 = Cache(cache_loc_2)
    assert c2.lookup_query({"hi": "there"}) is None


def test_query_hit_analysis_lookup_writes(tmp_path: Path):
    "make sure analysis query cache is written to"
    cache_loc_1 = tmp_path / "cache1"
    cache_loc_2 = tmp_path / "cache2"

    c1 = Cache(cache_loc_1)
    c1.set_query({"hi": "there"}, "dude")
    update_local_query_cache(tmp_path / "analysis_cache.json")
    assert c1.lookup_query({"hi": "there"}) == "dude"

    c2 = Cache(cache_loc_2)
    assert c2.lookup_query({"hi": "there"}) == "dude"


def test_query_hit_analysis_cache_removed_query_no_update(tmp_path: Path):
    "Make sure to forget a query when we are not updating the analysis cache"
    cache_loc_1 = tmp_path / "cache1"
    cache_loc_2 = tmp_path / "cache2"
    cache_loc_3 = tmp_path / "cache3"

    update_local_query_cache()

    c1 = Cache(cache_loc_1)
    c1.set_query({"hi": "there"}, "dude")

    reset_local_query_cache()

    c2 = Cache(cache_loc_2)
    c2.remove_query({"hi": "there"})
    assert c2.lookup_query({"hi": "there"}) is None

    reset_local_query_cache()

    # Make sure that the json file wasn't modified in this case!
    c3 = Cache(cache_loc_3)
    assert c3.lookup_query({"hi": "there"}) == "dude"


def test_query_hit_analysis_cache_removed_query_update(tmp_path: Path):
    "If we are updating the query cache, make sure to remove an item"
    cache_loc_1 = tmp_path / "cache1"
    cache_loc_2 = tmp_path / "cache2"

    update_local_query_cache()

    c1 = Cache(cache_loc_1)
    c1.set_query({"hi": "there"}, "dude")
    c1.remove_query({"hi": "there"})

    c2 = Cache(cache_loc_2)
    assert c2.lookup_query({"hi": "there"}) is None


def test_ic_query(tmp_path):
    c = Cache(tmp_path)
    c.set_query({"hi": "there"}, "dude")
    with ignore_cache():
        assert c.lookup_query({"hi": "there"}) is None


def test_ic_query_query_context(tmp_path):
    c = Cache(tmp_path)
    c.set_query({"hi": "there"}, "dude")
    with c.ignore_cache():
        assert c.lookup_query({"hi": "there"}) is None


def test_ic_query_ds_level(tmp_path):
    c = Cache(tmp_path, ignore_cache=True)
    c.set_query({"hi": "there"}, "dude")
    assert c.lookup_query({"hi": "there"}) is None


def test_query_hit_2(tmp_path):
    c = Cache(tmp_path)
    c.set_query({"hi": "there"}, "dude1")
    c.set_query({"hi": "there_1"}, "dude2")
    assert c.lookup_query({"hi": "there"}) == "dude1"
    assert c.lookup_query({"hi": "there_1"}) == "dude2"


def test_query_lookup_from_file(tmp_path):
    c1 = Cache(tmp_path)
    c1.set_query({"hi": "there"}, "dude")

    c2 = Cache(tmp_path)
    assert c2.lookup_query({"hi": "there"}) == "dude"


def test_query_remove(tmp_path):
    c1 = Cache(tmp_path)
    c1.set_query({"hi": "there"}, "dude")
    c1.remove_query({"hi": "there"})
    assert c1.lookup_query({"hi": "there"}) is None


def test_files_miss(tmp_path):
    c = Cache(tmp_path)
    assert c.lookup_files("1234") is None


def test_files_hit(tmp_path: Path):
    c = Cache(tmp_path)
    f1 = tmp_path / "f1.root"
    f2 = tmp_path / "f2.root"
    f1.touch()
    f2.touch()
    c.set_files("1234", [("hi", f1), ("there", f2)])
    assert c.lookup_files("1234") == [("hi", f1), ("there", f2)]


def test_files_deleted_data_file(tmp_path: Path):
    c = Cache(tmp_path)
    f1 = tmp_path / "f1.root"
    f2 = tmp_path / "f2.root"
    f1.touch()
    f2.touch()
    c.set_files("1234", [("hi", f1), ("there", f2)])
    f2.unlink()
    assert c.lookup_files("1234") is None


def test_ic_files_hit(tmp_path: Path):
    "The file list should not be affected by cache ignores"
    c = Cache(tmp_path)
    f1 = tmp_path / "f1.root"
    f2 = tmp_path / "f2.root"
    f1.touch()
    f2.touch()
    c.set_files("1234", [("hi", f1), ("there", f2)])
    with ignore_cache():
        assert c.lookup_files("1234") == [("hi", f1), ("there", f2)]


def test_files_hit_reloaded(tmp_path):
    c1 = Cache(tmp_path)
    f1 = tmp_path / "f1.root"
    f2 = tmp_path / "f2.root"
    f1.touch()
    f2.touch()
    c1.set_files("1234", [("hi", f1), ("there", f2)])
    c2 = Cache(tmp_path)
    assert c2.lookup_files("1234") == [("hi", f1), ("there", f2)]


def test_memory_miss(tmp_path):
    c = Cache(tmp_path)
    assert c.lookup_inmem("dude") is None


def test_memory_hit(tmp_path):
    c = Cache(tmp_path)
    r = 10
    c.set_inmem("dude", r)
    assert c.lookup_inmem("dude") is r


def test_ic_memory_hit(tmp_path):
    c = Cache(tmp_path)
    r = 10
    c.set_inmem("dude", r)
    with ignore_cache():
        assert c.lookup_inmem("dude") is None


def test_ic_memory_hit_ds_context(tmp_path):
    c = Cache(tmp_path)
    r = 10
    c.set_inmem("dude", r)
    with c.ignore_cache():
        assert c.lookup_inmem("dude") is None


def test_ic_memory_hit_ds_level(tmp_path):
    c = Cache(tmp_path, ignore_cache=True)
    r = 10
    c.set_inmem("dude", r)
    assert c.lookup_inmem("dude") is None


def test_memory_hit_accross(tmp_path):
    c1 = Cache(tmp_path)
    r = 10
    c1.set_inmem("dude", r)
    c2 = Cache(tmp_path)
    assert c2.lookup_inmem("dude") is r


def test_data_file_location(tmp_path):
    c = Cache(tmp_path)
    p = c.data_file_location("123-456", "junk.root")
    assert not p.exists()
    p.touch()
    assert p.exists()
    assert str(p).startswith(str(tmp_path))


def test_data_file_location_long_path(tmp_path):
    c = Cache(tmp_path)
    letters = string.ascii_lowercase
    file_significant_name = "junk.root"
    long_file_name = "".join(random.choice(letters) for i in range(230))

    p = c.data_file_location("123-456", long_file_name + file_significant_name)
    assert len(p.name) == c.max_path_len - len(p.parent.name)
    assert p.name.endswith(file_significant_name)


def test_data_file_location_twice(tmp_path):
    c = Cache(tmp_path)
    _ = c.data_file_location("123-456", "junk1.root")
    p2 = c.data_file_location("123-456", "junk2.root")
    assert not p2.exists()
    p2.touch()
    assert p2.exists()


def test_data_file_bad_file(tmp_path):
    "Check a very long bad filename to make sure it is sanitized"
    c = Cache(tmp_path)
    p = c.data_file_location(
        "123-456",
        "root:::dcache-atlas-xrootd-wan.desy.de:1094::pnfs:desy.de:atlas"
        ":dq2:atlaslocalgroupdisk:rucio:mc15_13TeV:8a:f1:DAOD_STDM3.05630052"
        "._000001.pool.root.198fbd841d0a28cb0d9dfa6340c890273-1.part.minio",
    )
    assert not p.exists()
    # If the follow fails, on windows, it could be because very-long-pathnames are not
    # enabled.
    p.touch()
    assert p.exists()


def test_query_cache_status(tmp_path):
    c = Cache(tmp_path)

    info = {"request_id": "111-222-333", "key": "bogus"}
    c.set_query_status(info)
    assert c.query_status_exists("111-222-333")
    info1 = c.lookup_query_status("111-222-333")
    assert info1["key"] == "bogus"


def test_query_cache_status_bad(tmp_path):
    c = Cache(tmp_path)

    assert not c.query_status_exists("111-222-333")
    with pytest.raises(ServiceXException):
        c.lookup_query_status("111-222-333")


def test_ic_query_cache_status(tmp_path):
    "Query status should be cached and accessed *during* a query"
    c = Cache(tmp_path)
    info = {"request_id": "111-222-333", "key": "bogus"}
    c.set_query_status(info)
    with ignore_cache():
        info1 = c.lookup_query_status("111-222-333")
        assert info1["key"] == "bogus"


def test_ic_restore(tmp_path):
    c = Cache(tmp_path)
    c.set_query({"hi": "there"}, "dude")
    with ignore_cache():
        pass
    assert c.lookup_query({"hi": "there"}) == "dude"


def test_ic_nesting(tmp_path):
    c = Cache(tmp_path)
    c.set_query({"hi": "there"}, "dude")
    with ignore_cache():
        with ignore_cache():
            pass
        assert c.lookup_query({"hi": "there"}) is None


def test_ic_nesting_ds_context(tmp_path):
    c = Cache(tmp_path)
    c.set_query({"hi": "there"}, "dude")
    with c.ignore_cache():
        with c.ignore_cache():
            pass
        assert c.lookup_query({"hi": "there"}) is None


def test_ic_enter_exit(tmp_path):
    c = Cache(tmp_path)
    c.set_query({"hi": "there"}, "dude")
    i = ignore_cache()
    i.__enter__()
    assert c.lookup_query({"hi": "there"}) is None
    i.__exit__(None, None, None)
    assert c.lookup_query({"hi": "there"}) == "dude"
