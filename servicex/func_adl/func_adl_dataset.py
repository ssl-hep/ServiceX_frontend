# Copyright (c) 2023, IRIS-HEP
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
from __future__ import annotations

import ast
import copy
from typing import (
    Optional,
    Any,
    Type,
    TypeVar,
)
from qastle import python_ast_to_text_ast

from func_adl import EventDataset, find_EventDataset
from servicex.query_core import QueryStringGenerator
from abc import ABC

T = TypeVar("T")


class FuncADLQuery(QueryStringGenerator, EventDataset[T], ABC):
    r"""
    ServiceX Dataset class that uses func_adl query syntax.
    """

    # These are methods that are translated locally
    _execute_locally = ["ResultPandasDF", "ResultAwkwardArray"]
    default_codegen = None

    async def execute_result_async(
        self, a: ast.AST, title: Optional[str] = None
    ) -> Any:
        "Required override of EventDataset"

    def check_data_format_request(self, f_name: str):
        "Required override of EventDataset"

    def __init__(
        self,
        item_type: Type = Any,
    ):
        EventDataset.__init__(self, item_type=item_type)
        self.provided_qastle = None

    def set_provided_qastle(self, qastle: str):
        self.provided_qastle = qastle

    def generate_selection_string(self) -> str:
        if self.provided_qastle:
            return self.provided_qastle
        else:
            return self.generate_qastle(self.query_ast)

    def set_tree(self, tree_name: str) -> FuncADLQuery[T]:
        r"""Set the tree name for the query.
        Args:
            tree_name (str): Name of the tree to use for the query

        Returns:
            The Dataset with the tree appended to the first call object
        """

        clone = self.clone_with_new_ast(copy.deepcopy(self.query_ast), self._item_type)
        c = find_EventDataset(clone.query_ast)
        c.args.append(ast.Str(s="bogus.root"))
        c.args.append(ast.Str(s=tree_name))
        return clone

    def generate_qastle(self, a: ast.AST) -> str:
        r"""Generate the qastle from the ast of the query.

        1. The top level function is already marked as being "ok"
        1. If the top level function is something we have to process locally,
           then we strip it off.

        Args:
            a (ast.AST): The complete AST of the request.

        Returns:
            str: Qastle that should be sent to servicex
        """
        return python_ast_to_text_ast(a)

    def as_qastle(self):
        r"""
        Generate Qastle from this AST

        :returns:
            Qastle representation of the target's AST
        """
        return self.value()


class FuncADLQuery_Uproot(FuncADLQuery):
    yaml_tag = "!FuncADL_Uproot"
    default_codegen = "uproot"

    def __init__(
        self,
        item_type: Type = Any,
    ):
        super().__init__(item_type)
        self.tree_is_set = False

    def FromTree(self, tree_name):
        self.tree_is_set = True
        return self.set_tree(tree_name=tree_name)

    def generate_selection_string(self):
        if not self.tree_is_set:
            raise ValueError(
                "Uproot FuncADL query requires "
                "that you set a tree name with FromTree()"
            )
        return super().generate_selection_string()

    def set_provided_qastle(self, qastle_query: str):
        # we do not validate provided qastle, so we don't know if a tree name is specified.
        # assume user knows what they're doing
        self.tree_is_set = True
        super().set_provided_qastle(qastle_query)

    @classmethod
    def from_yaml(cls, _, node):
        import qastle
        import re

        # We have modified the funcADL spec a bit here. For uproot queries
        # we need to pick the optional tree specification up from the query
        # string and add it to the EventDataset constructor.
        from_tree_re = r'(?:FromTree\(["\']([^"\']+)["\']\)\.)?(.+)'
        tree_match = re.match(from_tree_re, node.value)

        if tree_match:
            query_string = (
                f"EventDataset('bogus.root', '{tree_match.group(1)}')."
                + tree_match.group(2)
            )
        else:
            query_string = "EventDataset('bogus.root', 'events')." + node.value

        qastle_query = qastle.python_ast_to_text_ast(
            qastle.insert_linq_nodes(ast.parse(query_string))
        )
        query = cls()
        query.set_provided_qastle(qastle_query)
        return query


class FuncADLQuery_ATLASr21(FuncADLQuery):
    yaml_tag = "!FuncADL_ATLASr21"
    default_codegen = "atlasr21"


class FuncADLQuery_ATLASr22(FuncADLQuery):
    yaml_tag = "!FuncADL_ATLASr22"
    default_codegen = "atlasr22"


class FuncADLQuery_ATLASxAOD(FuncADLQuery):
    yaml_tag = "!FuncADL_ATLASxAOD"
    default_codegen = "atlasxaod"


class FuncADLQuery_CMS(FuncADLQuery):
    yaml_tag = "!FuncADL_CMS"
    default_codegen = "cms"
