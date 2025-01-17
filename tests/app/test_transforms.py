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
from datetime import datetime
from pathlib import Path
from unittest.mock import patch, AsyncMock, Mock

import pytest

from servicex.models import (
    TransformStatus,
    ResultDestination,
    ResultFormat,
    Status,
    ResultFile,
)


@pytest.fixture
def transform_status_record() -> TransformStatus:
    """
    Pytest fixture generating a realistic TransformStatus instance for testing.

    Returns:
        TransformStatus: A populated TransformStatus object with sample data
    """

    # Default data that can be overridden
    base_data = {
        "request_id": "test-request-123",
        "did": "test-did-456",
        "title": "Test Transform Job",
        "selection": "(muon_pt > 20)",
        "tree-name": "mytree",
        "image": "servicex/transformer:latest",
        "result-destination": ResultDestination.object_store,
        "result-format": ResultFormat.parquet,
        "generated-code-cm": "generated-test-code",
        "status": Status.complete,
        "app-version": "1.2.3",
        "files": 10,
        "files-completed": 8,
        "files-failed": 1,
        "files-remaining": 1,
        "submit_time": datetime.now(),
        "finish_time": datetime.now(),
        "minio_endpoint": "minio.example.com",
        "minio_secured": True,
        "minio_access_key": "test-access-key",
        "minio_secret_key": "test-secret-key",
        "log_url": "https://logs.example.com/test-job",
    }

    return TransformStatus(**base_data)


@pytest.fixture
def result_files():
    return [
        ResultFile(filename="test_file", size=1024, extension="parquet"),
        ResultFile(filename="test_file2", size=2048, extension="parquet"),
    ]


def test_transforms_list(script_runner, transform_status_record):
    with patch("servicex.app.transforms.ServiceXClient") as mock_servicex:
        transform_status_record.status = Status.running
        mock_list_transforms = Mock(return_value=[transform_status_record])
        mock_servicex.return_value.get_transforms = mock_list_transforms
        result = script_runner.run(
            ["servicex", "transforms", "list", "-c", "tests/example_config.yaml"]
        )

        assert result.returncode == 0
        result_row = result.stdout.split("  ")
        assert len(result_row) == 4
        assert result_row[0].strip() == "test-request-123"
        assert result_row[1] == "Test Transform Job"
        assert result_row[2] == "Running"
        assert result_row[3].strip() == "8"

        mock_list_transforms.assert_called_once()
        mock_list_transforms.reset_mock()
        result = script_runner.run(
            [
                "servicex",
                "transforms",
                "list",
                "-c",
                "tests/example_config.yaml",
                "--complete",
            ]
        )
        assert result.returncode == 0
        assert len(result.stdout.strip()) == 0


@pytest.mark.parametrize(
    "transform_state, report_complete, report_running, expected",
    [
        # Scenario 1: No flags set (report all)
        (Status.complete, False, False, True),
        (Status.running, False, False, True),
        # Scenario 2: Complete records only
        (Status.complete, True, False, True),
        (Status.running, True, False, False),
        # Scenario 3: Running records only
        (Status.complete, False, True, False),
        (Status.running, False, True, True),
        # Scenario 4: Both flags set
        (Status.complete, True, True, True),
        (Status.running, True, True, True),
    ],
)
def test_transforms_list_filters(
    script_runner,
    transform_status_record,
    transform_state,
    report_complete,
    report_running,
    expected,
):
    with patch("servicex.app.transforms.ServiceXClient") as mock_servicex:
        transform_status_record.status = transform_state
        mock_list_transforms = Mock(return_value=[transform_status_record])
        mock_servicex.return_value.get_transforms = mock_list_transforms
        command_line = [
            "servicex",
            "transforms",
            "list",
            "-c",
            "tests/example_config.yaml",
        ]

        if report_complete:
            command_line.append("--complete")
        if report_running:
            command_line.append("--running")

        result = script_runner.run(command_line)

        assert result.returncode == 0
        assert (
            len(result.stdout.strip()) if expected else len(result.stdout.strip()) == 0
        )


def test_list_files(script_runner, transform_status_record, result_files):
    with patch("servicex.app.transforms.ServiceXClient") as mock_servicex:
        with patch("servicex.app.transforms.MinioAdapter") as mock_minio:
            mock_transform_status = AsyncMock(return_value=transform_status_record)
            mock_servicex.return_value.get_transform_status_async = (
                mock_transform_status
            )

            mock_minio_adapter = Mock()
            mock_minio_adapter.list_bucket = AsyncMock(return_value=result_files)
            mock_minio.for_transform = Mock(return_value=mock_minio_adapter)
            result = script_runner.run(
                [
                    "servicex",
                    "transforms",
                    "files",
                    "-c",
                    "tests/example_config.yaml",
                    "test-request-123",
                ]
            )
            assert result.returncode == 0
            result_rows = result.stdout.strip().split("\n")
            assert len(result_rows) == 2
            result_row = result_rows[1].split("  ")
            assert result_row[0].strip() == "test_file2"
            assert result_row[1] == "0.00"
            assert result_row[2] == "parquet"

            mock_transform_status.assert_called_once_with("test-request-123")
            mock_minio.for_transform.assert_called_once_with(transform_status_record)
            mock_minio_adapter.list_bucket.assert_called_once()


def test_download_files(script_runner, transform_status_record, result_files):
    with patch("servicex.app.transforms.ServiceXClient") as mock_servicex:
        with patch("servicex.app.transforms.MinioAdapter") as mock_minio:
            mock_transform_status = AsyncMock(return_value=transform_status_record)
            mock_servicex.return_value.get_transform_status_async = (
                mock_transform_status
            )

            mock_minio_adapter = Mock()
            mock_minio_adapter.list_bucket = AsyncMock(return_value=result_files)
            mock_minio_adapter.download_file = AsyncMock(
                return_value=Path("/tmp/test_file.parquet")
            )
            mock_minio.for_transform = Mock(return_value=mock_minio_adapter)
            result = script_runner.run(
                [
                    "servicex",
                    "transforms",
                    "download",
                    "-c",
                    "tests/example_config.yaml",
                    "test-request-123",
                ]
            )
            assert result.returncode == 0
            result_rows = result.stdout.strip().split("\n")
            assert len(result_rows) == 3
            assert result_rows[1] == "/tmp/test_file.parquet"

            mock_transform_status.assert_called_once_with("test-request-123")
            mock_minio.for_transform.assert_called_once_with(transform_status_record)
            mock_minio_adapter.list_bucket.assert_called_once()
            assert mock_minio_adapter.download_file.call_count == 2
            assert mock_minio_adapter.download_file.mock_calls[0].args[0] == "test_file"


def test_delete_transform(script_runner, transform_status_record):
    with patch("servicex.app.transforms.ServiceXClient") as mock_servicex:
        mock_delete_transform = AsyncMock(return_value=True)
        mock_servicex.return_value.delete_transform = mock_delete_transform

        mock_delete_local = Mock(return_value=True)
        mock_servicex.return_value.delete_local_transform = mock_delete_local

        result = script_runner.run(
            [
                "servicex",
                "transforms",
                "delete",
                "-c",
                "tests/example_config.yaml",
                "test-request-123",
            ]
        )
        assert result.returncode == 0
        assert result.stdout == "Transform test-request-123 deleted\n"
        mock_delete_transform.assert_called_once_with("test-request-123")


def test_cancel_transform(script_runner, transform_status_record):
    with patch("servicex.app.transforms.ServiceXClient") as mock_servicex:
        mock_cancel_transform = AsyncMock(return_value=True)
        mock_servicex.return_value.cancel_transform = mock_cancel_transform

        result = script_runner.run(
            [
                "servicex",
                "transforms",
                "cancel",
                "-c",
                "tests/example_config.yaml",
                "test-request-123",
            ]
        )
        assert result.returncode == 0
        assert result.stdout == "Transform test-request-123 cancelled\n"
        mock_cancel_transform.assert_called_once_with("test-request-123")
