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
import json
from unittest.mock import patch

import pytest
from typer.testing import CliRunner

from servicex.app.main import app

runner = CliRunner()


@pytest.fixture
def mock_get_structure():
    with patch(
        "servicex.app.structure.get_structure", return_value="STRUCTURE OUTPUT"
    ) as mock:
        yield mock


def test_get_structure_single_did(mock_get_structure):
    result = runner.invoke(app, ["get-structure", "scope:did1"])

    assert result.exit_code == 0, result.output
    mock_get_structure.assert_called_once_with(["scope:did1"], filter="")
    assert "STRUCTURE OUTPUT" in result.output


def test_get_structure_multiple_dids(mock_get_structure):
    result = runner.invoke(app, ["get-structure", "scope:did1", "scope:did2"])

    assert result.exit_code == 0, result.output
    mock_get_structure.assert_called_once_with(["scope:did1", "scope:did2"], filter="")


def test_get_structure_filter_branch(mock_get_structure):
    result = runner.invoke(
        app, ["get-structure", "scope:did1", "--filter-branch", "jet"]
    )

    assert result.exit_code == 0, result.output
    mock_get_structure.assert_called_once_with(["scope:did1"], filter="jet")


def test_get_structure_json_file(mock_get_structure, tmp_path):
    datasets = {"sample_A": "scope:did1", "sample_B": "scope:did2"}
    json_file = tmp_path / "datasets.json"
    json_file.write_text(json.dumps(datasets))

    result = runner.invoke(app, ["get-structure", str(json_file)])

    assert result.exit_code == 0, result.output
    mock_get_structure.assert_called_once_with(datasets, filter="")


def test_get_structure_json_missing(mock_get_structure, tmp_path):
    result = runner.invoke(app, ["get-structure", str(tmp_path / "missing.json")])

    assert result.exit_code == 1
    mock_get_structure.assert_not_called()


def test_get_structure_json_not_dict(mock_get_structure, tmp_path):
    json_file = tmp_path / "datasets.json"
    json_file.write_text(json.dumps(["scope:did1", "scope:did2"]))

    result = runner.invoke(app, ["get-structure", str(json_file)])

    assert result.exit_code == 1
    mock_get_structure.assert_not_called()


def test_get_structure_json_invalid(mock_get_structure, tmp_path):
    json_file = tmp_path / "datasets.json"
    json_file.write_text("{not valid json")

    result = runner.invoke(app, ["get-structure", str(json_file)])

    assert result.exit_code == 1
    mock_get_structure.assert_not_called()


def test_get_structure_no_dataset(mock_get_structure):
    result = runner.invoke(app, ["get-structure"])

    assert result.exit_code != 0
    mock_get_structure.assert_not_called()
