# Copyright (c) 2022, IRIS-HEP
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# * Redistributions of source code must retain the above copyright notice, this
#   list of conditions and the following disclaimer.
#
# * Redistributions in binary form must reproduce the above copyright notice,
#   this list of conditions and the following disclaimer in the documentation
#   and/or other materials provided with the distribution.
#
# * Neither the name of the copyright holder nor the names of its
#   contributors may be used to endorse or promote products derived from
#   this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
import multiprocessing
import os
import sys
import tempfile
import json
import pytest
from pathlib import Path
from unittest.mock import patch

from filelock import FileLock
from tinydb.storages import JSONStorage

from servicex.configuration import Configuration
from servicex.models import ResultFormat
from servicex.query_cache import QueryCache, CacheException

file_uris = ["/tmp/foo1.root", "/tmp/foo2.root"]


def test_hash(transform_request):
    request1 = transform_request.model_copy()
    request2 = transform_request.model_copy()

    assert request1.compute_hash() == request2.compute_hash()

    # Changing the title doesn't impact the hash
    request2.title = "This has changed"
    assert request1.compute_hash() == request2.compute_hash()

    # Changing the DID does
    request2.did = "rucio://baz.bar"
    assert request1.compute_hash() != request2.compute_hash()

    # Changing the did backend does
    request2 = request1.model_copy()
    request2.codegen = "foo"
    assert request1.compute_hash() != request2.compute_hash()

    # Changing result_format does
    request2 = request1.model_copy()
    request2.result_format = ResultFormat.root_ttree
    assert request1.compute_hash() != request2.compute_hash()

    # Add file names and shuffle the order
    request2 = request1.model_copy()
    request1.file_list = ["file1.txt", "file2.txt"]
    request2.file_list = ["file2.txt", "file1.txt"]
    assert request1.compute_hash() == request2.compute_hash()

    # Add different file names
    request2 = request1.model_copy()
    request1.file_list = ["file1.txt", "file2.txt"]
    request2.file_list = ["file3.txt", "file4.txt"]
    assert request1.compute_hash() != request2.compute_hash()


def test_cache_transform(transform_request, completed_status):
    with tempfile.TemporaryDirectory() as temp_dir:
        config = Configuration(cache_path=temp_dir, api_endpoints=[])  # type: ignore
        cache = QueryCache(config)
        cache.update_transform_status(transform_request.compute_hash(), "COMPLETE")
        cache.cache_transform(
            cache.transformed_results(
                transform=transform_request,
                completed_status=completed_status,
                data_dir="/foo/bar",
                file_list=file_uris,
                signed_urls=[],
                headers=[{}] * len(file_uris),
                expiries=[sys.maxsize] * len(file_uris),
            )
        )

        test = cache.get_transform_by_hash(transform_request.compute_hash())
        assert test
        assert test.title == "Test submission"
        assert test.request_id == "b8c508d0-ccf2-4deb-a1f7-65c839eebabf"

        test2 = cache.get_transform_by_request_id(
            "b8c508d0-ccf2-4deb-a1f7-65c839eebabf"
        )
        assert test2
        assert test2.title == "Test submission"
        assert test2.request_id == "b8c508d0-ccf2-4deb-a1f7-65c839eebabf"

        assert not cache.get_transform_by_hash("thishashdoesnotexist")
        assert not cache.get_transform_by_request_id("this-uuid-does-not-exist")

        # try to create a duplicate record
        cache.cache_transform(
            cache.transformed_results(
                transform=transform_request,
                completed_status=completed_status,
                data_dir="/foo/baz",
                file_list=file_uris,
                signed_urls=[],
                headers=[{}] * len(file_uris),
                expiries=[sys.maxsize] * len(file_uris),
            )
        )

        assert len(cache.cached_queries()) == 1

        # forcefully create a duplicate record
        record = json.loads(
            cache.transformed_results(
                transform=transform_request,
                completed_status=completed_status,
                data_dir="/foo/baz",
                file_list=file_uris,
                signed_urls=[],
                headers=[{}] * len(file_uris),
                expiries=[sys.maxsize] * len(file_uris),
            ).model_dump_json()
        )
        record["hash"] = transform_request.compute_hash()
        record["status"] = "COMPLETE"
        cache.db.insert(record)

        with pytest.raises(CacheException):
            cache.get_transform_by_hash(transform_request.compute_hash())

        with pytest.raises(CacheException):
            cache.get_transform_by_request_id("b8c508d0-ccf2-4deb-a1f7-65c839eebabf")

        cache.delete_record_by_request_id("b8c508d0-ccf2-4deb-a1f7-65c839eebabf")
        assert len(cache.cached_queries()) == 0

        cache.close()


def test_cache_path(completed_status):
    with tempfile.TemporaryDirectory() as temp_dir:
        config = Configuration(cache_path=temp_dir, api_endpoints=[])  # type: ignore
        cache = QueryCache(config)
        path_bits = os.path.split(cache.cache_path_for_transform(completed_status))
        assert path_bits[0] == temp_dir
        assert path_bits[1] == completed_status.request_id
        cache.close()


def test_record_delete(transform_request, completed_status):
    with tempfile.TemporaryDirectory() as temp_dir:
        config = Configuration(cache_path=temp_dir, api_endpoints=[])  # type: ignore
        cache = QueryCache(config)
        cache.cache_transform(
            cache.transformed_results(
                transform=transform_request,
                completed_status=completed_status,
                data_dir="/foo/bar",
                file_list=file_uris,
                signed_urls=[],
                headers=[{}] * len(file_uris),
                expiries=[sys.maxsize] * len(file_uris),
            )
        )
        transform_request.did = "rucio://foo.baz"
        cache.cache_transform(
            cache.transformed_results(
                transform=transform_request,
                completed_status=completed_status.model_copy(
                    update={"request_id": "02c64494-4529-49a7-a4a6-95661ea3936e"}
                ),
                data_dir="/foo/baz",
                file_list=file_uris,
                signed_urls=[],
                headers=[{}] * len(file_uris),
                expiries=[sys.maxsize] * len(file_uris),
            )
        )
        assert len(cache.cached_queries()) == 2
        cache.delete_record_by_request_id("02c64494-4529-49a7-a4a6-95661ea3936e")
        assert len(cache.cached_queries()) == 1
        cache.close()


def test_delete_transform_by_hash(transform_request, completed_status):
    with tempfile.TemporaryDirectory() as temp_dir:
        config = Configuration(cache_path=temp_dir, api_endpoints=[])  # type: ignore
        cache = QueryCache(config)
        cache.update_transform_status(transform_request.compute_hash(), "COMPLETE")
        cache.cache_transform(
            cache.transformed_results(
                transform=transform_request,
                completed_status=completed_status,
                data_dir="/foo/bar",
                file_list=file_uris,
                signed_urls=[],
                headers=[{}] * len(file_uris),
                expiries=[sys.maxsize] * len(file_uris),
            )
        )

        test = cache.get_transform_by_hash(transform_request.compute_hash())
        assert test
        assert test.title == "Test submission"
        assert test.request_id == "b8c508d0-ccf2-4deb-a1f7-65c839eebabf"

        cache.delete_record_by_hash(transform_request.compute_hash())
        empty = cache.get_transform_by_hash(transform_request.compute_hash())
        assert empty is None
        cache.close()


def test_contains_hash(transform_request, completed_status):
    with tempfile.TemporaryDirectory() as temp_dir:
        config = Configuration(cache_path=temp_dir, api_endpoints=[])  # type: ignore
        cache = QueryCache(config)
        cache.update_transform_status(transform_request.compute_hash(), "COMPLETE")
        cache.cache_transform(
            cache.transformed_results(
                transform=transform_request,
                completed_status=completed_status,
                data_dir="/foo/bar",
                file_list=file_uris,
                signed_urls=[],
                headers=[{}] * len(file_uris),
                expiries=[sys.maxsize] * len(file_uris),
            )
        )

        assert cache.contains_hash(transform_request.compute_hash()) is True

        assert cache.contains_hash("1234") is False
        cache.close()


def test_get_transform_request_id(transform_request, completed_status):
    with tempfile.TemporaryDirectory() as temp_dir:
        config = Configuration(cache_path=temp_dir, api_endpoints=[])  # type: ignore
        cache = QueryCache(config)
        hash_value = transform_request.compute_hash()

        # if the transform request id is not cached
        with pytest.raises(CacheException):
            request_id = cache.get_transform_request_id(hash_value)
            print(request_id)

        # cache the submitted transform and then check for the request id
        cache.cache_submitted_transform(transform_request, "123456")
        request_id = cache.get_transform_request_id(hash_value)
        assert request_id == "123456"

        # assert that in this state that cached_queries does NOT crash and returns nothing
        assert len(cache.cached_queries()) == 0

        cache.close()


def test_get_transform_request_status(transform_request, completed_status):
    with tempfile.TemporaryDirectory() as temp_dir:
        config = Configuration(cache_path=temp_dir, api_endpoints=[])  # type: ignore
        cache = QueryCache(config)
        hash_value = transform_request.compute_hash()

        assert cache.is_transform_request_submitted(hash_value) is False

        # cache transform
        cache.update_transform_status(hash_value, "COMPLETE")
        cache.cache_transform(
            cache.transformed_results(
                transform=transform_request,
                completed_status=completed_status,
                data_dir="/foo/bar",
                file_list=file_uris,
                signed_urls=[],
                headers=[{}] * len(file_uris),
                expiries=[sys.maxsize] * len(file_uris),
            )
        )

        assert cache.is_transform_request_submitted(hash_value) is False

        # cache submitted transform
        cache.cache_submitted_transform(
            transform_request, "b8c508d0-ccf2-4deb-a1f7-65c839eebabf"
        )

        assert cache.is_transform_request_submitted(hash_value) is True

        cache.close()


def test_cache_queries_in_state(transform_request):
    with tempfile.TemporaryDirectory() as temp_dir:
        config = Configuration(cache_path=temp_dir, api_endpoints=[])  # type: ignore
        cache = QueryCache(config)

        cache.cache_submitted_transform(transform_request, "123456")

        pending = cache.queries_in_state("SUBMITTED")
        assert len(pending) == 1
        assert pending[0]["status"] == "SUBMITTED"
        assert pending[0]["request_id"] == "123456"
        assert (
            cache.is_transform_request_submitted(transform_request.compute_hash())
            is True
        )

        cache.close()


def test_patch_old_db():
    with tempfile.TemporaryDirectory() as temp_dir:
        config = Configuration(cache_path=temp_dir, api_endpoints=[])  # type: ignore
        cache = QueryCache(config)
        cache.close()
        with open(Path(temp_dir) / ".servicex" / "db.json", "w") as f:
            f.write(
                """{"_default": {"207": {"hash": "hash", "title": "123456", "codegen": "uproot","""
                """ "result_format": "root-file", "request_id": "123456", "status": "COMPLETE","""
                """ "submit_time": "2026-02-04T17:53:56.642131Z", "data_dir": "123456","""
                """ "file_list": ["a"], "signed_url_list": [], "files": 1,"""
                """ "log_url": "https://testing/"}}} """
            )
        cache = QueryCache(config)
        transform = cache.get_transform_by_hash("hash")
        assert transform is not None
        assert transform.headers == [{}]
        assert transform.expiries == [sys.maxsize]


def _two_caches(temp_dir):
    """Two instances sharing one database, standing in for two processes."""
    config = Configuration(cache_path=temp_dir, api_endpoints=[])  # type: ignore
    return QueryCache(config), QueryCache(config)


def test_new_query_sees_write_from_other_process(transform_request):
    with tempfile.TemporaryDirectory() as temp_dir:
        writer, reader = _two_caches(temp_dir)
        other = transform_request.model_copy(update={"did": "rucio://other"})
        hash_value = transform_request.compute_hash()

        # Reading a different hash first fills the memo without caching this query
        assert reader.is_transform_request_submitted(other.compute_hash()) is False
        writer.cache_submitted_transform(transform_request, "request-id")

        assert reader.is_transform_request_submitted(hash_value) is True
        writer.close()
        reader.close()


def test_repeated_query_sees_write_from_other_process(transform_request):
    with tempfile.TemporaryDirectory() as temp_dir:
        writer, reader = _two_caches(temp_dir)
        hash_value = transform_request.compute_hash()

        assert reader.is_transform_request_submitted(hash_value) is False
        writer.cache_submitted_transform(transform_request, "request-id")

        assert reader.is_transform_request_submitted(hash_value) is True
        writer.close()
        reader.close()


def test_repeated_query_sees_delete_from_other_process(transform_request):
    with tempfile.TemporaryDirectory() as temp_dir:
        writer, reader = _two_caches(temp_dir)
        hash_value = transform_request.compute_hash()

        writer.cache_submitted_transform(transform_request, "request-id")
        assert reader.is_transform_request_submitted(hash_value) is True
        writer.delete_record_by_hash(hash_value)

        assert reader.is_transform_request_submitted(hash_value) is False
        writer.close()
        reader.close()


def test_repeated_reads_parse_file_once(transform_request):
    with tempfile.TemporaryDirectory() as temp_dir:
        writer, reader = _two_caches(temp_dir)
        hash_value = transform_request.compute_hash()
        writer.cache_submitted_transform(transform_request, "request-id")

        with patch("tinydb.storages.json.load", wraps=json.load) as parse:
            for _ in range(5):
                assert reader.is_transform_request_submitted(hash_value) is True
                assert reader.contains_hash(hash_value) is False
            assert parse.call_count == 1

            # This instance's own writes and later reads don't need a parse either
            reader.update_transform_status(hash_value, "COMPLETE")
            assert reader.is_transform_request_submitted(hash_value) is False
            assert parse.call_count == 1
        writer.close()
        reader.close()


def test_memo_matches_file_after_write(transform_request):
    with tempfile.TemporaryDirectory() as temp_dir:
        config = Configuration(cache_path=temp_dir, api_endpoints=[])  # type: ignore
        cache = QueryCache(config)

        cache.cache_submitted_transform(transform_request, "request-id")

        with open(Path(temp_dir) / ".servicex" / "db.json") as f:
            on_disk = json.load(f)
        assert cache.db.storage._memo == on_disk
        record = next(iter(cache.db.storage._memo["_default"].values()))
        assert type(record["result_format"]) is str
        cache.close()


def test_failed_write_does_not_change_later_reads(transform_request):
    with tempfile.TemporaryDirectory() as temp_dir:
        config = Configuration(cache_path=temp_dir, api_endpoints=[])  # type: ignore
        cache = QueryCache(config)
        hash_value = transform_request.compute_hash()
        cache.cache_submitted_transform(transform_request, "request-id")

        with patch.object(JSONStorage, "write", side_effect=OSError("disk full")):
            with pytest.raises(OSError):
                cache.update_transform_status(hash_value, "COMPLETE")

        assert cache.db.storage._memo is None
        assert cache.is_transform_request_submitted(hash_value) is True
        cache.close()


def test_insert_after_other_process_inserted(transform_request):
    with tempfile.TemporaryDirectory() as temp_dir:
        first, second = _two_caches(temp_dir)
        requests = [
            transform_request.model_copy(update={"did": f"rucio://request{i}"})
            for i in range(3)
        ]

        # The first insert makes TinyDB work out and remember the next ID
        first.cache_submitted_transform(requests[0], "request-0")
        # Another process takes that ID
        second.cache_submitted_transform(requests[1], "request-1")
        # Without recomputing, this reuses the taken ID and raises
        first.cache_submitted_transform(requests[2], "request-2")

        for i, request in enumerate(requests):
            assert second.get_transform_request_id(request.compute_hash()) == (
                f"request-{i}"
            )
        first.close()
        second.close()


def test_close_clears_memo(transform_request):
    with tempfile.TemporaryDirectory() as temp_dir:
        config = Configuration(cache_path=temp_dir, api_endpoints=[])  # type: ignore
        cache = QueryCache(config)
        cache.cache_submitted_transform(transform_request, "request-id")
        storage = cache.db.storage
        assert storage._memo is not None

        cache.close()

        assert storage._memo is None


def _cache_worker(cache_path, conn):
    """Run in a separate process: open a QueryCache and call its methods as the
    test asks, replying with each result, until sent None."""
    cache = QueryCache(
        Configuration(cache_path=cache_path, api_endpoints=[])  # type: ignore
    )
    conn.send(("ready", None))
    while (command := conn.recv()) is not None:
        name, args = command
        try:
            conn.send(("ok", getattr(cache, name)(*args)))
        except Exception as e:
            conn.send(("error", repr(e)))
    cache.close()


class _CacheProcess:
    """A QueryCache in its own process, driven one call at a time so the test
    decides exactly how operations from different processes interleave."""

    _TIMEOUT = 30

    def __init__(self, context, cache_path):
        self._conn, child_conn = context.Pipe()
        self._process = context.Process(
            target=_cache_worker, args=(cache_path, child_conn)
        )
        self._process.start()
        assert self.result() is None  # the cache is open

    def send(self, name, *args):
        self._conn.send((name, args))

    def replied_within(self, seconds):
        return self._conn.poll(seconds)

    def result(self):
        if not self._conn.poll(self._TIMEOUT):
            pytest.fail(f"cache process did not reply within {self._TIMEOUT} s")
        status, value = self._conn.recv()
        if status == "error":
            pytest.fail(f"cache process raised {value}")
        return value

    def call(self, name, *args):
        self.send(name, *args)
        return self.result()

    def stop(self):
        if self._process.is_alive():
            try:
                self._conn.send(None)
            except (BrokenPipeError, OSError):
                pass
            self._process.join(timeout=10)
        if self._process.is_alive():
            self._process.kill()


@pytest.fixture
def cache_processes(tmp_path):
    """Start QueryCache processes that share one database in tmp_path."""
    context = multiprocessing.get_context("spawn")
    started = []

    def start():
        process = _CacheProcess(context, str(tmp_path))
        started.append(process)
        return process

    yield start
    for process in started:
        process.stop()


def _request_ids(cache_path, requests):
    """Read each request's ID back from the database with a fresh cache."""
    cache = QueryCache(
        Configuration(cache_path=str(cache_path), api_endpoints=[])  # type: ignore
    )
    try:
        return [
            (
                cache.get_transform_request_id(r.compute_hash())
                if cache.is_transform_request_submitted(r.compute_hash())
                else None
            )
            for r in requests
        ]
    finally:
        cache.close()


def _requests(transform_request, count):
    return [
        transform_request.model_copy(update={"did": f"rucio://request{i}"})
        for i in range(count)
    ]


def test_processes_get_distinct_document_ids(
    transform_request, cache_processes, tmp_path
):
    a, b = cache_processes(), cache_processes()
    requests = _requests(transform_request, 3)

    a.call("cache_submitted_transform", requests[0], "request-0")  # A picks next ID
    b.call("cache_submitted_transform", requests[1], "request-1")  # B takes it
    # Fails if A reuses the ID it worked out before B's insert
    a.call("cache_submitted_transform", requests[2], "request-2")

    assert _request_ids(tmp_path, requests) == ["request-0", "request-1", "request-2"]


def test_process_sees_other_process_write_before_writing(
    transform_request, cache_processes, tmp_path
):
    a, b = cache_processes(), cache_processes()
    requests = _requests(transform_request, 3)

    b.call("cache_submitted_transform", requests[0], "request-0")
    # A reads, so it now holds a parsed copy of the database
    assert a.call("is_transform_request_submitted", requests[0].compute_hash())
    b.call("cache_submitted_transform", requests[1], "request-1")
    # If A wrote from its old copy, B's second record would be lost
    a.call("cache_submitted_transform", requests[2], "request-2")

    assert _request_ids(tmp_path, requests) == ["request-0", "request-1", "request-2"]


def test_process_waits_for_cache_lock(transform_request, cache_processes, tmp_path):
    b = cache_processes()
    request = transform_request

    lock = FileLock(os.path.join(tmp_path, ".servicex", "db.lock"))
    with lock:
        b.send("cache_submitted_transform", request, "request-0")
        # B must not finish while another process holds the lock. If locking were
        # missing, B would reply almost at once.
        assert not b.replied_within(0.5), "wrote while another process held the lock"
    b.result()

    assert _request_ids(tmp_path, [request]) == ["request-0"]
