# Copyright (c) 2025, IRIS-HEP
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
import pytest
import uproot
import json
import os
from servicex_analysis_utils import file_peeking
from pathlib import Path


@pytest.fixture
def build_test_samples(tmp_path):

    test_path = str(tmp_path / "test_metadata.root")
    # example data for two branches
    tree_data = {
        "FileMetaDataAuxDyn.test_100": [100],
        "FileMetaDataAuxDyn.test_abc": ["abc"],
    }

    # Create tmp .root files
    with uproot.create(test_path) as file:
        file["MetaData"] = tree_data

    return test_path


# Test run_query and print_structure_from_str
def test_metadata_retrieval(build_test_samples, tmp_path, capsys):

    path = build_test_samples
    query_output = file_peeking.run_query(path)
    # Check result
    expected_result = {
        "FileMetaData": {"test_100": "100", "test_abc": "abc"},
        "MetaData": {
            "FileMetaDataAuxDyn.test_100": "AsDtype('>i8')",
            "FileMetaDataAuxDyn.test_abc": "AsStrings()",
        },
    }
    encoded_result = json.loads(query_output[0])

    assert encoded_result == expected_result

    # Produce servicex.deliver() like dict
    # i.e {"Sample Name":"Path"}
    tree_data = {"branch": query_output}
    with uproot.create(tmp_path / "encoded.root") as file:
        file["servicex"] = tree_data
    assert os.path.exists(
        tmp_path / "encoded.root"
    ), f"servicex-like test file not found."
    deliver_dict = {"test_file": [str(tmp_path / "encoded.root")]}

    ## Test str formating
    output_str = file_peeking.print_structure_from_str(deliver_dict)

    expected_path = Path("tests/data/expected_metadata.txt")
    expected = expected_path.read_text(encoding="utf-8")

    assert (
        expected == output_str
    ), f"Output does not match expected.\n Output: {output_str}"
