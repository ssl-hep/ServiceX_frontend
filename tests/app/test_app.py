# Copyright (c) 2024, IRIS-HEP
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
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import Mock, patch

from servicex.models import ResultFormat, TransformedResults


def test_app_version(script_runner):
    import servicex._version

    result = script_runner.run(["servicex", "--version"])
    assert result.returncode == 0
    assert result.stdout == f"ServiceX {servicex._version.__version__}\n"


def test_deliver(script_runner):
    with patch("servicex.app.main.servicex_client") as mock_servicex_client:
        mock_servicex_client.deliver = Mock(
            return_value={"UprootRaw_YAML": ["/tmp/foo.root", "/tmp/bar.root"]}
        )
        result = script_runner.run(["servicex", "deliver", "foo.yaml"])
        assert result.returncode == 0
        result_rows = result.stdout.split("\n")
        assert result_rows[0] == "Delivering foo.yaml to ServiceX cache"
        mock_servicex_client.deliver.assert_called_once_with(
            "foo.yaml",
            servicex_name=None,
            config_path=None,
            ignore_local_cache=None,
            display_results=True,
        )


def test_deliver_hide_results(script_runner):
    with patch("servicex.app.main.servicex_client") as mock_servicex_client:
        mock_servicex_client.deliver = Mock(
            return_value={"UprootRaw_YAML": ["/tmp/foo.root", "/tmp/bar.root"]}
        )
        result = script_runner.run(
            ["servicex", "deliver", "foo.yaml", "--hide-results"]
        )
        assert result.returncode == 0
        result_rows = result.stdout.split("\n")
        assert result_rows[0] == "Delivering foo.yaml to ServiceX cache"
        # Verify that servicex_client.deliver was called with display_results=False
        mock_servicex_client.deliver.assert_called_once_with(
            "foo.yaml",
            servicex_name=None,
            config_path=None,
            ignore_local_cache=None,
            display_results=False,
        )


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


def test_cache_clear_force(script_runner, tmp_path):
    """Ensure the cache clear command with force (-y) calls close and rmtree."""
    # Prepare a fake cache path
    fake_cache_dir = tmp_path / "cache_dir"
    fake_cache_dir.mkdir()

    with (
        patch("servicex.app.cache.ServiceXClient") as mock_servicex,
        patch("shutil.rmtree") as mock_rmtree,
    ):
        # Configure the ServiceXClient instance mock
        instance = Mock()
        instance.query_cache = Mock()
        instance.config = Mock()
        instance.config.cache_path = str(fake_cache_dir)
        mock_servicex.return_value = instance

        result = script_runner.run(["servicex", "cache", "clear", "-y"])

    assert result.returncode == 0
    # Ensure the cache close was called
    instance.query_cache.close.assert_called_once()
    # Ensure rmtree was called with the configured cache path
    mock_rmtree.assert_called_once_with(str(fake_cache_dir))
    assert "Cache cleared" in result.stdout
