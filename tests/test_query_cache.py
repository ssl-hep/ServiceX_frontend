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
import os
import tempfile
import json
import pytest

from servicex.configuration import Configuration
from servicex.models import ResultFormat
from servicex.query_cache import QueryCache, CacheException
from tinydb import Query
import asyncio

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


def test_cache_transform(transform_request, completed_status):
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
            )
        )

        assert len(cache.cached_queries()) == 1

        # forcefully create a duplicate record
        cache.db.insert(
            json.loads(
                cache.transformed_results(
                    transform=transform_request,
                    completed_status=completed_status,
                    data_dir="/foo/baz",
                    file_list=file_uris,
                    signed_urls=[],
            ).model_dump_json()))  # noqa

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
            )
        )
        assert len(cache.cached_queries()) == 2
        cache.delete_record_by_request_id("02c64494-4529-49a7-a4a6-95661ea3936e")
        assert len(cache.cached_queries()) == 1
        cache.close()


def test_get_codegen_by_backend_empty():
    with tempfile.TemporaryDirectory() as temp_dir:
        config = Configuration(cache_path=temp_dir, api_endpoints=[])  # type: ignore
        cache = QueryCache(config)
        result = cache.get_codegen_by_backend("non-existent")
        assert result is None
        cache.close()


def test_update_codegen_by_backend_single():
    with tempfile.TemporaryDirectory() as temp_dir:
        config = Configuration(cache_path=temp_dir, api_endpoints=[])  # type: ignore
        cache = QueryCache(config)
        codegens = Query()
        cache.update_codegen_by_backend('backend_1', ['codegen_1'])
        result = cache.db.search(codegens.backend == 'backend_1')
        assert len(result) == 1
        assert result[0] == {'backend': 'backend_1', 'codegens': ['codegen_1']}
        cache.close()


def test_get_codegen_by_backend_single():
    with tempfile.TemporaryDirectory() as temp_dir:
        config = Configuration(cache_path=temp_dir, api_endpoints=[])  # type: ignore
        cache = QueryCache(config)
        cache.update_codegen_by_backend('backend_1', ['codegen_1'])
        result = cache.get_codegen_by_backend("backend_1")
        assert result == {'backend': 'backend_1', 'codegens': ['codegen_1']}
        cache.close()


def test_delete_codegen_by_backend():
    with tempfile.TemporaryDirectory() as temp_dir:
        config = Configuration(cache_path=temp_dir, api_endpoints=[])  # type: ignore
        cache = QueryCache(config)
        cache.update_codegen_by_backend('backend_1', ['codegen_1'])
        result = cache.get_codegen_by_backend("backend_1")
        assert result == {'backend': 'backend_1', 'codegens': ['codegen_1']}

        cache.delete_codegen_by_backend('backend_1')
        result = cache.get_codegen_by_backend("backend_1")
        assert result is None
        cache.close()


def test_delete_codegen_by_backend_nonexistent():
    with tempfile.TemporaryDirectory() as temp_dir:
        config = Configuration(cache_path=temp_dir, api_endpoints=[])  # type: ignore
        cache = QueryCache(config)
        cache.delete_codegen_by_backend('backend_1')
        with pytest.raises(Exception):
            assert False
        cache.close()


def test_add_both_codegen_and_transform_to_cache(transform_request, completed_status):
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
            )
        )

        cache.update_codegen_by_backend('backend_1', ['codegen_1'])
        result = cache.get_codegen_by_backend("backend_1")
        assert result == {'backend': 'backend_1', 'codegens': ['codegen_1']}
        assert len(cache.cached_queries()) == 1
        cache.close()


def test_delete_codegen_by_hash(transform_request, completed_status):
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
        cache.cache_transform(
            cache.transformed_results(
                transform=transform_request,
                completed_status=completed_status,
                data_dir="/foo/bar",
                file_list=file_uris,
                signed_urls=[],
            )
        )

        assert cache.contains_hash(transform_request.compute_hash()) is True

        assert cache.contains_hash("1234") is False
        cache.close()


def test_queue_contains_hash(transform_request):
    with tempfile.TemporaryDirectory() as temp_dir:
        config = Configuration(cache_path=temp_dir, api_endpoints=[])  # type: ignore
        cache = QueryCache(config)
        cache.queue_transform(transform_request)

        assert cache.queue_contains_hash(transform_request.compute_hash()) is True

        assert cache.contains_hash("1234") is False
        cache.close()


def test_queue_transform(transform_request):
    with tempfile.TemporaryDirectory() as temp_dir:
        config = Configuration(cache_path=temp_dir, api_endpoints=[])  # type: ignore
        cache = QueryCache(config)
        cache.queue_transform(transform_request)

        # assert cache.queue_contains_hash(transform_request.compute_hash()) is True
        transform = Query()
        assert len(cache.queue.all()) == 1

        assert len(cache.queue.search(transform.hash == transform_request.compute_hash())) == 1
        # assert cache.contains_hash("1234") is False
        cache.close()


def test_queue_transform_update(transform_request):
    with tempfile.TemporaryDirectory() as temp_dir:
        config = Configuration(cache_path=temp_dir, api_endpoints=[])  # type: ignore
        cache = QueryCache(config)
        cache.queue_transform(transform_request)

        # assert cache.queue_contains_hash(transform_request.compute_hash()) is True
        transform = Query()
        assert len(cache.queue.all()) == 1

        assert "request_id" not in \
            cache.queue.search(transform.hash == transform_request.compute_hash())[0]

        cache.queue_transform_update(transform_request, "123456")

        assert "request_id" in \
            cache.queue.search(transform.hash == transform_request.compute_hash())[0]

        assert cache.queue.get(transform.hash == transform_request.compute_hash())['request_id'] \
            == "123456"
        cache.close()


@pytest.mark.asyncio
async def test_queue_get_transform_request_id(transform_request):
    with tempfile.TemporaryDirectory() as temp_dir:
        config = Configuration(cache_path=temp_dir, api_endpoints=[])  # type: ignore
        cache = QueryCache(config)

        # if the transform request is not in queue
        request_id = await cache.queue_get_transform_request_id(transform_request)
        assert request_id is None
        cache.queue_transform(transform_request)

        # assert cache.queue_contains_hash(transform_request.compute_hash()) is True
        transform = Query()
        assert len(cache.queue.all()) == 1

        assert "request_id" not in \
            cache.queue.search(transform.hash == transform_request.compute_hash())[0]

        # update the transform request with a request id and then check for the request id
        # Create 2 tasks to mimic asynchronous requests
        loop = asyncio.get_event_loop()
        task1 = loop.create_task(cache.queue_get_transform_request_id(transform_request))
        await asyncio.sleep(3)
        cache.queue_transform_update(transform_request, "123456")
        task2 = loop.create_task(cache.queue_get_transform_request_id(transform_request))
        request_id = await task1
        request_id2 = await task2
        assert request_id == "123456"
        assert request_id == request_id2

        # force duplicate records in queue
        cache.queue.insert({"hash": transform_request.compute_hash(),
                            "key": "value"})
        with pytest.raises(CacheException):
            await cache.queue_get_transform_request_id(transform_request)

        cache.close()


def test_queue_get_transform_request_hash(transform_request):
    with tempfile.TemporaryDirectory() as temp_dir:
        config = Configuration(cache_path=temp_dir, api_endpoints=[])  # type: ignore
        cache = QueryCache(config)
        cache.queue_transform(transform_request)

        hash_value = transform_request.compute_hash()

        assert cache.queue_get_transform_request_hash(hash_value).title == "Test submission"

        assert not cache.queue_get_transform_request_hash(hash_value).did == "rucio://foo.baz"

        assert cache.queue_get_transform_request_hash(hash_value).codegen == "uproot"

        # force duplicate entries in queue
        record = json.loads(transform_request.model_dump_json())
        record["hash"] = hash_value
        cache.queue.insert(record)
        with pytest.raises(CacheException):
            cache.queue_get_transform_request_hash(hash_value)

        cache.close()


def test_queue_delete_record(transform_request):
    with tempfile.TemporaryDirectory() as temp_dir:
        config = Configuration(cache_path=temp_dir, api_endpoints=[])  # type: ignore
        cache = QueryCache(config)
        cache.queue_transform(transform_request)
        hash_value = transform_request.compute_hash()
        assert cache.queue_get_transform_request_hash(hash_value).title == "Test submission"

        cache.queue_delete_record(transform_request)
        assert cache.queue_get_transform_request_hash(hash_value) is None
        cache.close()
