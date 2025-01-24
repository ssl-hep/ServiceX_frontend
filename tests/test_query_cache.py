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
        record = json.loads(
            cache.transformed_results(
                transform=transform_request,
                completed_status=completed_status,
                data_dir="/foo/baz",
                file_list=file_uris,
                signed_urls=[],
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
        cache.update_codegen_by_backend("backend_1", ["codegen_1"])
        result = cache.db.search(codegens.backend == "backend_1")
        assert len(result) == 1
        assert result[0] == {"backend": "backend_1", "codegens": ["codegen_1"]}
        cache.close()


def test_get_codegen_by_backend_single():
    with tempfile.TemporaryDirectory() as temp_dir:
        config = Configuration(cache_path=temp_dir, api_endpoints=[])  # type: ignore
        cache = QueryCache(config)
        cache.update_codegen_by_backend("backend_1", ["codegen_1"])
        result = cache.get_codegen_by_backend("backend_1")
        assert result == {"backend": "backend_1", "codegens": ["codegen_1"]}
        cache.close()


def test_delete_codegen_by_backend():
    with tempfile.TemporaryDirectory() as temp_dir:
        config = Configuration(cache_path=temp_dir, api_endpoints=[])  # type: ignore
        cache = QueryCache(config)
        cache.update_codegen_by_backend("backend_1", ["codegen_1"])
        result = cache.get_codegen_by_backend("backend_1")
        assert result == {"backend": "backend_1", "codegens": ["codegen_1"]}

        cache.delete_codegen_by_backend("backend_1")
        result = cache.get_codegen_by_backend("backend_1")
        assert result is None
        cache.close()


def test_delete_codegen_by_backend_nonexistent():
    with tempfile.TemporaryDirectory() as temp_dir:
        config = Configuration(cache_path=temp_dir, api_endpoints=[])  # type: ignore
        cache = QueryCache(config)
        cache.delete_codegen_by_backend("backend_1")
        with pytest.raises(AssertionError):
            raise AssertionError()
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

        cache.update_codegen_by_backend("backend_1", ["codegen_1"])
        result = cache.get_codegen_by_backend("backend_1")
        assert result == {"backend": "backend_1", "codegens": ["codegen_1"]}
        assert len(cache.cached_queries()) == 1
        cache.close()


def test_delete_codegen_by_hash(transform_request, completed_status):
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

        # update the transform request with a request id and then check for the request id
        cache.update_transform_status(hash_value, "SUBMITTED")
        cache.update_transform_request_id(hash_value, "123456")
        request_id = cache.get_transform_request_id(hash_value)
        assert request_id == "123456"

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
            )
        )

        assert cache.is_transform_request_submitted(hash_value) is False

        # cache transform
        cache.update_transform_status(hash_value, "SUBMITTED")
        cache.cache_transform(
            cache.transformed_results(
                transform=transform_request,
                completed_status=completed_status,
                data_dir="/foo/bar",
                file_list=file_uris,
                signed_urls=[],
            )
        )

        assert cache.is_transform_request_submitted(hash_value) is True

        cache.close()
