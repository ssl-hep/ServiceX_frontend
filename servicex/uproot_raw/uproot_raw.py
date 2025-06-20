# Copyright (c) 2024-2025, IRIS-HEP
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

# pydantic 2 API

import pydantic
from ..models import DocStringBaseModel
from typing import List, Union, Mapping, Optional, get_args
from ..query_core import QueryStringGenerator


class TreeSubQuery(DocStringBaseModel):
    """Express an uproot-raw query on an ntuple. Arguments have the same meaning as the
    ones of the same name for uproot.arrays():
    https://uproot.readthedocs.io/en/stable/uproot.behaviors.TBranch.HasBranches.html#uproot-behaviors-tbranch-hasbranches-arrays
    """

    model_config = {"use_attribute_docstrings": True}

    treename: Union[Mapping[str, str], List[str], str]
    """Name of input ntuple in file"""
    expressions: Optional[Union[List[str], str]] = None
    """Awkward Array expressions to evaluate for output"""
    cut: Optional[str] = None
    """Awkward Array expressions to select passing rows"""
    filter_name: Optional[Union[List[str], str]] = None
    """Expression to filter the list of considered input variables by name"""
    filter_typename: Optional[Union[List[str], str]] = None
    """Expression to filter the list of considered input variables by type name"""
    aliases: Optional[Mapping[str, str]] = None
    """Define aliases to use in computation and expressions"""
    fail_on_missing_trees: Optional[bool] = None
    """Make queries fail if input trees are missing (default False)"""
    use_standard_awkward_axis: Optional[bool] = None
    """Do not override standard axis default arguments in awkward expressions (default False)"""


class CopyHistogramSubQuery(DocStringBaseModel):
    """Request the copying of a ROOT object from the input file to the output."""

    model_config = {"use_attribute_docstrings": True}

    copy_histograms: Union[List[str], str]
    """Objects to copy"""


SubQuery = Union[TreeSubQuery, CopyHistogramSubQuery]


@pydantic.dataclasses.dataclass
class UprootRawQuery(QueryStringGenerator):
    yaml_tag = "!UprootRaw"

    query: Union[List[SubQuery], SubQuery]
    default_codegen: str = "uproot-raw"

    def generate_selection_string(self):
        import json

        final_query: List[SubQuery]
        if isinstance(
            self.query, get_args(SubQuery)
        ):  # from Python 3.10 we don't need "get_args"
            final_query = [self.query]
        else:
            final_query = self.query
        return json.dumps([json.loads(_.model_dump_json()) for _ in final_query])

    @classmethod
    def from_yaml(cls, _, node):
        code = node.value
        import json

        queries = json.loads(code)
        q = cls(queries)
        return q
