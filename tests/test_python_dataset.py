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
from base64 import b64decode

import pytest

from servicex.dataset_identifier import FileListDataset
from servicex.python_dataset import PythonDataset


def test_no_provided_function():
    did = FileListDataset("/foo/bar/baz.root")
    datasource = PythonDataset(dataset_identifier=did,
                               codegen="uproot",
                               sx_adapter=None,
                               query_cache=None)

    with pytest.raises(ValueError):
        print(datasource.generate_selection_string())


def test_generate_transform():
    did = FileListDataset("/foo/bar/baz.root")
    datasource = PythonDataset(dataset_identifier=did,
                               codegen="uproot",
                               sx_adapter=None,
                               query_cache=None)

    def run_query(input_filenames=None):
        print("Greetings from your query")
        return []

    selection = datasource.with_uproot_function(run_query).generate_selection_string()
    print(selection)
    print("==============")
    print(b64decode(selection))


def test_function_as_string():
    did = FileListDataset("/foo/bar/baz.root")
    datasource = PythonDataset(dataset_identifier=did,
                               codegen="uproot",
                               sx_adapter=None,
                               query_cache=None)

    string_function = """
        def run_query(input_filenames=None):
            print("Greetings from your query")
            return []
    """
    selection = datasource.with_uproot_function(string_function).generate_selection_string()
    print(selection)
    print("==============")
    print(b64decode(selection))