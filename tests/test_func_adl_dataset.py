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
from servicex.func_adl.func_adl_dataset import FuncADLQuery_Uproot, FuncADLQuery
from typing import Any


def test_set_from_tree():
    query = FuncADLQuery_Uproot()
    query = query.FromTree("TREE_NAME")

    assert "TREE_NAME" in query.generate_selection_string()


def test_a_query():
    query = FuncADLQuery_Uproot()
    query = query.FromTree("nominal") \
                 .Select(lambda e: {"lep_pt": e["lep_pt"]})

    assert (query.generate_selection_string()
            == "(call Select (call EventDataset 'bogus.root' 'nominal') "
               "(lambda (list e) (dict (list 'lep_pt') "
               "(list (subscript e 'lep_pt')))))"
            )


def test_set_query():
    qastle = "(call Select (call EventDataset 'bogus.root' 'nominal') " \
             "(lambda (list e) (dict (list 'lep_pt') " \
             "(list (subscript e 'lep_pt')))))"
    query = FuncADLQuery_Uproot()
    query.set_provided_qastle(qastle)

    assert (query.generate_selection_string() == qastle)


def test_type():
    "Test that the item type for a dataset is correctly propagated"

    class my_type_info:
        "typespec for possible event type"

        def fork_it_over(self) -> int:
            ...

    datasource = FuncADLQuery[my_type_info](
        item_type=my_type_info
    )

    assert datasource.item_type == my_type_info


def test_type_any():
    "Test the type is any if no type is given"
    datasource = FuncADLQuery()
    assert datasource.item_type == Any
