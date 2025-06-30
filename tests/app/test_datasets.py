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
import json
from datetime import datetime
from unittest.mock import patch, MagicMock

import pytest

from servicex.models import CachedDataset, DatasetFile


@pytest.fixture
def dataset():
    dataset_files = [
        DatasetFile(
            id=1,
            adler32="some_adler32_hash",
            file_size=1024,
            file_events=100,
            paths="/path/to/file1",
        ),
        DatasetFile(
            id=2,
            adler32="another_adler32_hash",
            file_size=2048,
            file_events=200,
            paths="/path/to/file2",
        ),
    ]

    cached_dataset = CachedDataset(
        id=42,
        name="test_dataset",
        did_finder="some_finder",
        n_files=2,
        size=3072,
        events=300,
        last_used=datetime.now(),
        last_updated=datetime.now(),
        lookup_status="completed",
        is_stale=False,
        files=dataset_files,
    )
    return cached_dataset


def test_datasets_list(script_runner, dataset):
    with patch("servicex.app.datasets.ServiceXClient") as mock_servicex:
        mock_get_datasets = MagicMock(return_value=[dataset])
        mock_servicex.return_value.get_datasets = mock_get_datasets

        result = script_runner.run(
            ["servicex", "datasets", "list", "-c", "tests/example_config.yaml"]
        )
        assert result.returncode == 0
        result_row = result.stdout.split("  ")
        assert len(result_row) == 7, f"Expected 7 elements, got {len(result_row)}"

        # Assert specific index values
        assert result_row[0].strip() == "42"
        assert result_row[1] == "test_dataset"
        assert result_row[2] == "2"
        assert result_row[3] == "0MB"
        assert result_row[4] == "completed"

        mock_get_datasets.assert_called_once_with(did_finder=None, show_deleted=False)

        mock_get_datasets.reset_mock()
        result = script_runner.run(
            [
                "servicex",
                "datasets",
                "list",
                "-c",
                "tests/example_config.yaml",
                "--did-finder",
                "some_finder",
                "--show-deleted",
            ]
        )
        assert result.returncode == 0
        mock_get_datasets.assert_called_once_with(
            did_finder="some_finder", show_deleted=True
        )


def test_dataset_get(script_runner, dataset):
    with patch("servicex.app.datasets.ServiceXClient") as mock_servicex:
        mock_get_dataset = MagicMock(return_value=dataset)
        mock_servicex.return_value.get_dataset = mock_get_dataset

        result = script_runner.run(
            ["servicex", "datasets", "get", "42", "-c", "tests/example_config.yaml"]
        )
        assert result.returncode == 0
        mock_get_dataset.assert_called_once_with(42)

        # The output is a json document
        result_doc = json.loads(result.stdout)
        assert result_doc["dataset"]["id"] == 42
        assert len(result_doc["dataset"]["files"]) == 2


def test_dataset_delete(script_runner):
    with patch("servicex.app.datasets.ServiceXClient") as mock_servicex:
        mock_delete_dataset = MagicMock(return_value=True)
        mock_servicex.return_value.delete_dataset = mock_delete_dataset

        result = script_runner.run(
            ["servicex", "datasets", "delete", "-c", "tests/example_config.yaml", "42"]
        )
        assert result.returncode == 0
        assert result.stdout == "Dataset 42 deleted\n"
        mock_delete_dataset.assert_called_once_with(42)

        mock_delete_dataset_not_found = MagicMock(return_value=False)
        mock_servicex.return_value.delete_dataset = mock_delete_dataset_not_found
        result = script_runner.run(
            ["servicex", "datasets", "delete", "-c", "tests/example_config.yaml", "42"]
        )
        assert result.returncode == 1
        mock_delete_dataset.assert_called_once_with(42)
        assert result.stdout == "Dataset 42 not found\n"
