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
from datetime import datetime, timezone
import os
from pathlib import Path
from unittest.mock import Mock, patch

from servicex.models import ResultFormat, TransformedResults


def test_cache_list(script_runner, tmp_path) -> None:
    dummy_file: Path = tmp_path / "data.parquet"
    dummy_file.write_bytes(b"0" * (5 * 1024 * 1024))

    record_r = TransformedResults(
        hash="hash",
        title="Test",
        codegen="code",
        request_id="id",
        submit_time=datetime.now(timezone.utc),
        data_dir=str(tmp_path),
        file_list=[str(dummy_file)],
        signed_url_list=[],
        files=1,
        result_format=ResultFormat.parquet,
    )
    record_s = {
        "hash": "hash2",
        "title": "Test2",
        "codegen": "code2",
        "request_id": "id2",
        "submit_time": datetime.now(timezone.utc).isoformat(),
        "data_dir": str(tmp_path),
        "file_list": [str(dummy_file)],
        "signed_url_list": [],
        "files": 1,
        "result_format": ResultFormat.parquet.value,
    }

    with patch("servicex.app.cache.ServiceXClient") as mock_servicex:
        cache_mock = Mock()
        cache_mock.cached_queries.return_value = [record_r]
        cache_mock.queries_in_state.return_value = [record_s]
        mock_servicex.return_value.query_cache = cache_mock
        result = script_runner.run(["servicex", "cache", "list", "--size"])

    assert result.returncode == 0
    lines = [ln for ln in result.stdout.split("\n") if ln.strip() != ""]
    assert len(lines) == 2
    assert "Submitted" in lines[1]
    assert "Submitted" not in lines[0]


def test_cache_list_size(script_runner, tmp_path) -> None:
    dummy_file: Path = tmp_path / "data.parquet"
    dummy_file.write_bytes(b"0" * (5 * 1024 * 1024))

    record = TransformedResults(
        hash="hash",
        title="Test",
        codegen="code",
        request_id="id",
        submit_time=datetime.now(timezone.utc),
        data_dir=str(tmp_path),
        file_list=[str(dummy_file)],
        signed_url_list=[],
        files=1,
        result_format=ResultFormat.parquet,
    )

    with patch("servicex.app.cache.ServiceXClient") as mock_servicex:
        cache_mock = Mock()
        cache_mock.cached_queries.return_value = [record]
        cache_mock.queries_in_state.return_value = []
        mock_servicex.return_value.query_cache = cache_mock
        result = script_runner.run(["servicex", "cache", "list", "--size"])

    assert result.returncode == 0
    expected_size: float = os.path.getsize(dummy_file) / (1024 * 1024)
    result_row = result.stdout.split("  ")
    assert len(result_row) == 7
    assert result_row[-1].strip() == f"{expected_size:,.2f} MB"


def test_cache_list_without_size(script_runner, tmp_path) -> None:
    dummy_file: Path = tmp_path / "data.parquet"
    dummy_file.write_bytes(b"0" * (5 * 1024 * 1024))

    record = TransformedResults(
        hash="hash",
        title="Test",
        codegen="code",
        request_id="id",
        submit_time=datetime.now(timezone.utc),
        data_dir=str(tmp_path),
        file_list=[str(dummy_file)],
        signed_url_list=[],
        files=1,
        result_format=ResultFormat.parquet,
    )

    with patch("servicex.app.cache.ServiceXClient") as mock_servicex:
        cache_mock = Mock()
        cache_mock.cached_queries.return_value = [record]
        cache_mock.queries_in_state.return_value = []
        mock_servicex.return_value.query_cache = cache_mock
        result = script_runner.run(["servicex", "cache", "list"])

    assert result.returncode == 0
    result_row = result.stdout.split("  ")
    # Without the --size option, the output should have only six columns
    assert len(result_row) == 6


def test_cache_list_size_gb(script_runner, tmp_path) -> None:
    dummy_file: Path = tmp_path / "data.parquet"
    dummy_file.write_bytes(b"0")

    record = TransformedResults(
        hash="hash",
        title="Test",
        codegen="code",
        request_id="id",
        submit_time=datetime.now(timezone.utc),
        data_dir=str(tmp_path),
        file_list=[str(dummy_file)],
        signed_url_list=[],
        files=1,
        result_format=ResultFormat.parquet,
    )

    size_bytes: int = 2 * 1024**3
    with (
        patch("servicex.app.cache.ServiceXClient") as mock_servicex,
        patch(
            "servicex.app.cache.Path.stat",
            return_value=Mock(st_size=size_bytes),
        ),
    ):
        cache_mock = Mock()
        cache_mock.cached_queries.return_value = [record]
        cache_mock.queries_in_state.return_value = []
        mock_servicex.return_value.query_cache = cache_mock
        result = script_runner.run(["servicex", "cache", "list", "--size"])

    assert result.returncode == 0
    result_row = result.stdout.split("  ")
    assert result_row[-1].strip() == "2.00 GB"


def test_cache_list_size_tb(script_runner, tmp_path) -> None:
    dummy_file: Path = tmp_path / "data.parquet"
    dummy_file.write_bytes(b"0")

    record = TransformedResults(
        hash="hash",
        title="Test",
        codegen="code",
        request_id="id",
        submit_time=datetime.now(timezone.utc),
        data_dir=str(tmp_path),
        file_list=[str(dummy_file)],
        signed_url_list=[],
        files=1,
        result_format=ResultFormat.parquet,
    )

    size_bytes: int = 3 * 1024**4
    with (
        patch("servicex.app.cache.ServiceXClient") as mock_servicex,
        patch(
            "servicex.app.cache.Path.stat",
            return_value=Mock(st_size=size_bytes),
        ),
    ):
        cache_mock = Mock()
        cache_mock.cached_queries.return_value = [record]
        cache_mock.queries_in_state.return_value = []
        mock_servicex.return_value.query_cache = cache_mock
        result = script_runner.run(["servicex", "cache", "list", "--size"])

    assert result.returncode == 0
    result_row = result.stdout.split("  ")
    assert result_row[-1].strip() == "3.00 TB"
