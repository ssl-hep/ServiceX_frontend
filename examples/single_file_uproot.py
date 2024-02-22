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

import ast

import qastle

from servicex import ServiceXSpec, General, Sample
from servicex.func_adl.func_adl_dataset import FuncADLDataset
from servicex.servicex_client import deliver

query = FuncADLDataset().Select(lambda e: {'lep_pt': e['lep_pt']}). \
    Where(lambda e: e['lep_pt'] > 1000)

qstr = """
FuncADLDataset().Select(lambda e: {'lep_pt': e['lep_pt']}). \
         Where(lambda e: e['lep_pt'] > 1000)
"""
query_ast = ast.parse(qstr)
qastle_query = qastle.python_ast_to_text_ast(qastle.insert_linq_nodes(query_ast))
print("From str", qastle_query)
q2 = FuncADLDataset()
q2.set_provided_qastle(qastle_query)
print(q2.generate_selection_string())
print("From python", query.generate_selection_string())
spec = ServiceXSpec(
    General=General(
        ServiceX="testing1",
        Codegen="uproot",
        OutputFormat="parquet",
        Delivery="LocalCache"
    ),
    Sample=[
        Sample(
            Name="mc_345060.ggH125_ZZ4lep.4lep",
            RootFile="root://eospublic.cern.ch//eos/opendata/atlas/OutreachDatasets/2020-01-22/4lep/MC/mc_345060.ggH125_ZZ4lep.4lep.root", # NOQA E501
            Query=query
        )
    ]
)

print(deliver(spec))
