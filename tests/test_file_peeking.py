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
import awkward as ak
import os
import sys
import numpy as np
from servicex_analysis_utils import file_peeking
import types
import servicex 
import filecmp


@pytest.fixture
def build_test_samples(tmp_path):

    test_path = str(tmp_path / "test_file1.root")
    # example data for two branches
    tree_data1 = {
    "branch1": np.ones(100),
    "branch2": np.zeros(100)
    }
    tree_data2 = {
    "branch1": np.ones(10),
    }

    # Create tmp .root files
    with uproot.create(test_path) as file:
        file["background"] = tree_data1
        file["signal"] = tree_data2

    return test_path

# Test helper functions: run_query, print_structure_from_str
def test_encoding(build_test_samples,tmp_path, capsys):

    path=build_test_samples
    query_output=file_peeking.run_query(path)
    
    #Check return types
    assert isinstance(query_output, ak.Array), "run_query() does not produce an awkward.Array"
    encoded_result=query_output[0]
    assert isinstance(encoded_result, str), "run_query array content is not str"
    
    #Check result
    expected_result= (
        "Tree: background; TBranch: branch1 ; dtype: AsDtype('>f8'), TBranch: branch2 ; dtype: AsDtype('>f8')"
        "\nTree: signal; TBranch: branch1 ; dtype: AsDtype('>f8')"
    )    
    assert encoded_result == expected_result

    # Produce servicex.deliver() like dict
    tree_data= {"branch" : query_output}
    with uproot.create(tmp_path / "encoded.root") as file:
        file["servicex"]= tree_data
    assert os.path.exists(tmp_path / "encoded.root"), f"servicex-like test file not found."
    deliver_dict={"test_file":[str(tmp_path / "encoded.root")]}

    
    ## Test decoding structure formating
    # save_to_txt
    file_peeking.print_structure_from_str(deliver_dict,save_to_txt=True)
    out_txt="samples_structure.txt"
    assert os.path.exists(out_txt), "save_to_txt arg not producing files"

    with open(out_txt, "r", encoding="utf-8") as f:
        written_str = f.read()

    # direct return
    output_str=file_peeking.print_structure_from_str(deliver_dict)

    # do_print
    file_peeking.print_structure_from_str(deliver_dict,do_print=True)
    captured = capsys.readouterr()
    
    # Check if all returns match 
    assert captured.out[0:-1] == written_str == output_str , "saved, printed and direct return formats are different"
    
    # Compare with expected return
    test_txt="tests/data/expected_structure.txt"
    assert filecmp.cmp(out_txt, test_txt), "Formatted str does not match expected return"

    # Test filter_branch
    filtered_str=file_peeking.print_structure_from_str(deliver_dict,filter_branch="branch2")
    assert "branch1" not in filtered_str, "filter_branch argument is not removing branch1"


# Test user-facing function errors:

