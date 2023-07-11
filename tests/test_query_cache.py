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

import pytest

from servicex_client.configuration import Configuration
from servicex_client.models import ResultFormat
from servicex_client.query_cache import QueryCache, CacheException

file_uris = ["/tmp/foo1.root", "/tmp/foo2.root"]


def test_hash(transform_request):
    request1 = transform_request.copy()
    request2 = transform_request.copy()

    assert request1.compute_hash() == request2.compute_hash()

    # Changing the title doesn't impact the hash
    request2.title = "This has changed"
    assert request1.compute_hash() == request2.compute_hash()

    # Changing the DID does
    request2.did = "rucio://baz.bar"
    assert request1.compute_hash() != request2.compute_hash()

    # Changing the did backend does
    request2 = request1.copy()
    request2.codegen = "foo"
    assert request1.compute_hash() != request2.compute_hash()

    # Changing result_format does
    request2 = request1.copy()
    request2.result_format = ResultFormat.root_file
    assert request1.compute_hash() != request2.compute_hash()


def test_cache_transform(transform_request, completed_status):
    with tempfile.TemporaryDirectory() as temp_dir:
        config = Configuration(cache_path=temp_dir, api_endpoints=[])
        cache = QueryCache(config)
        cache.cache_transform(
            transform=transform_request,
            completed_status=completed_status,
            data_dir="/foo/bar",
            file_list=file_uris,
            signed_urls=[],
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

        # make a duplicate record
        cache.cache_transform(
            transform=transform_request,
            completed_status=completed_status,
            data_dir="/foo/baz",
            file_list=file_uris,
            signed_urls=[],
        )

        with pytest.raises(CacheException):
            _ = cache.get_transform_by_hash(transform_request.compute_hash())

        with pytest.raises(CacheException):
            _ = cache.get_transform_by_request_id(
                "b8c508d0-ccf2-4deb-a1f7-65c839eebabf"
            )

        assert len(cache.cached_queries()) == 2

        cache.delete_record_by_request_id("b8c508d0-ccf2-4deb-a1f7-65c839eebabf")
        assert len(cache.cached_queries()) == 0

        cache.close()


def test_cache_path(completed_status):
    with tempfile.TemporaryDirectory() as temp_dir:
        config = Configuration(cache_path=temp_dir, api_endpoints=[])
        cache = QueryCache(config)
        path_bits = os.path.split(cache.cache_path_for_transform(completed_status))
        assert path_bits[0] == temp_dir
        assert path_bits[1] == completed_status.request_id
        cache.close()


def test_record_delete(transform_request, completed_status):
    with tempfile.TemporaryDirectory() as temp_dir:
        config = Configuration(cache_path=temp_dir, api_endpoints=[])
        cache = QueryCache(config)
        cache.cache_transform(
            transform=transform_request,
            completed_status=completed_status,
            data_dir="/foo/bar",
            file_list=file_uris,
            signed_urls=[],
        )

        cache.cache_transform(
            transform=transform_request,
            completed_status=completed_status.copy(
                update={"request_id": "02c64494-4529-49a7-a4a6-95661ea3936e"}
            ),
            data_dir="/foo/baz",
            file_list=file_uris,
            signed_urls=[],
        )

        assert len(cache.cached_queries()) == 2
        cache.delete_record_by_request_id("02c64494-4529-49a7-a4a6-95661ea3936e")
        assert len(cache.cached_queries()) == 1
        cache.close()
