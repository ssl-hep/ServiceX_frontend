# Copyright (c) 2026, IRIS-HEP
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
import tempfile
import datetime
import pytest
from pathlib import Path

from servicex.configuration import Configuration
from servicex.query_cache import QueryCache
from servicex.servicex_client import GuardList
from servicex import read_dir

file_uris = ["/tmp/foo1.root", "/tmp/foo2.root"]
file_uris_2 = ["/tmp/bar1.root", "/tmp/bar2.root"]
remote_urls = ["http://remote/foo1.root", "http://remote/foo2.root"]


def test_read_cache(transform_request, completed_status):
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
                signed_urls=remote_urls,
            )
        )

        data = read_dir(temp_dir)
        assert isinstance(data["Test submission"], GuardList)
        assert str(data) == str({"Test submission": file_uris})

        data = read_dir(temp_dir, local_preferred=False)
        assert isinstance(data["Test submission"], GuardList)
        assert str(data) == str({"Test submission": remote_urls})

        cache.close()


def test_read_cache_with_config(transform_request, completed_status, mocker):
    mocker.patch("servicex.configuration.getuser", return_value="cache_user")
    with tempfile.TemporaryDirectory() as temp_dir:
        mocker.patch(
            "servicex.configuration.tempfile.gettempdir", return_value=str(temp_dir)
        )
        cfg = Path(temp_dir) / "servicex.yaml"
        cfg.write_text(f"""
api_endpoints:
  - endpoint: http://localhost:5000
    name: localhost
cache_path: {temp_dir}
""")
        config = Configuration(cache_path=temp_dir, api_endpoints=[])  # type: ignore
        cache = QueryCache(config)
        cache.update_transform_status(transform_request.compute_hash(), "COMPLETE")
        cache.cache_transform(
            cache.transformed_results(
                transform=transform_request,
                completed_status=completed_status,
                data_dir="/foo/bar",
                file_list=file_uris,
                signed_urls=remote_urls,
            )
        )

        data = read_dir(config_path=cfg)
        assert str(data) == str({"Test submission": file_uris})
        cache.close()


def test_no_cache():
    with pytest.raises(ValueError):
        read_dir("let-us-assume-this-nonexists")

    with tempfile.TemporaryDirectory() as temp_dir:
        with pytest.raises(RuntimeError):
            read_dir(temp_dir)


def test_most_recent_cache(transform_request, completed_status):
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
        cache.cache_transform(
            cache.transformed_results(
                transform=transform_request,
                completed_status=completed_status.model_copy(
                    update={
                        "request_id": "02c64494-4529-49a7-a4a6-95661ea3936e",
                        "submit_time": datetime.datetime(2025, 12, 1),
                    }
                ),
                data_dir="/foo/bar",
                file_list=file_uris_2,
                signed_urls=[],
            )
        )

        data = read_dir(temp_dir)
        assert str(data) == str({"Test submission": file_uris_2})
