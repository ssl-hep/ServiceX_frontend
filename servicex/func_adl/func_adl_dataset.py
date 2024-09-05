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
    cast,
    List,
)
from qastle import python_ast_to_text_ast

from func_adl import EventDataset, find_EventDataset
from servicex.query_core import QueryStringGenerator
from servicex.func_adl.util import has_tuple
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
        top_function = cast(ast.Name, a.func).id
        source = a
        if top_function in self._execute_locally:
            # Request the default type here
            default_format = self._ds.first_supported_datatype(["parquet", "root-ttree"])
            assert default_format is not None, "Unsupported ServiceX returned format"
            method_to_call = self._format_map[default_format]

            stream = a.args[0]
            col_names = a.args[1]
            if method_to_call == "get_data_rootfiles_async":
                # If we have no column names, then we must be using a dictionary to
                # set them - so just pass that
                # directly.
                assert isinstance(
                    col_names, (ast.List, ast.Constant, ast.Str)
                ), f"Programming error - type name not known {type(col_names).__name__}"
                if isinstance(col_names, ast.List) and len(col_names.elts) == 0:
                    source = stream
                else:
                    source = ast.Call(
                        func=ast.Name(id="ResultTTree", ctx=ast.Load()),
                        args=[
                            stream,
                            col_names,
                            ast.Str("treeme"),
                            ast.Str("junk.root"),
                        ],
                    )
            elif method_to_call == "get_data_parquet_async":
                source = stream
                # See #32 for why this is commented out
                # source = ast.Call(
                #     func=ast.Name(id='ResultParquet', ctx=ast.Load()),
                #     args=[stream, col_names, ast.Str('junk.parquet')])
            else:  # pragma: no cover
                # This indicates a programming error
                assert False, f"Do not know how to call {method_to_call}"

        elif top_function == "ResultParquet":
            # Strip off the Parquet function, do a select if there are arguments for column names
            source = a.args[0]
            col_names = cast(ast.List, a.args[1]).elts

            def encode_as_tuple_reference(c_names: List) -> List[ast.AST]:
                # Encode each column ref as a index into the tuple we are being passed
                return [
                    ast.Subscript(
                        value=ast.Name(id="x", ctx=ast.Load()),
                        slice=ast.Constant(idx),
                        ctx=ast.Load(),
                    )
                    for idx, _ in enumerate(c_names)
                ]

            def encode_as_single_reference():
                # Single reference for a bare (non-col) variable
                return [
                    ast.Name(id="x", ctx=ast.Load()),
                ]

            if len(col_names) > 0:
                # Add a select on top to set the column names
                if len(col_names) == 1:
                    # Determine if they built a tuple or not
                    values = (
                        encode_as_tuple_reference(col_names)
                        if has_tuple(source)
                        else encode_as_single_reference()
                    )
                elif len(col_names) > 1:
                    values = encode_as_tuple_reference(col_names)
                else:
                    assert False, "make sure that type checkers can figure this out"

                d = ast.Dict(keys=col_names, values=values)
                tup_func = ast.Lambda(
                    args=ast.arguments(args=[ast.arg(arg="x")]), body=d
                )
                c = ast.Call(
                    func=ast.Name(id="Select", ctx=ast.Load()),
                    args=[source, tup_func],
                    keywords=[],
                )
                source = c

        return python_ast_to_text_ast(source)

    def as_qastle(self):
        r"""
        Generate Qastle from this AST

        :returns:
            Qastle representation of the target's AST
        """
        return self.value()


class FuncADLQuery_Uproot(FuncADLQuery):
    yaml_tag = '!FuncADL_Uproot'
    default_codegen = 'uproot'

    def FromTree(self, tree_name):
        return self.set_tree(tree_name=tree_name)

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
            query_string = f"EventDataset('bogus.root', '{tree_match.group(1)}')." \
                           + tree_match.group(2)
        else:
            query_string = "EventDataset('bogus.root', 'events')." + node.value

        qastle_query = qastle.python_ast_to_text_ast(
            qastle.insert_linq_nodes(
                ast.parse(query_string)
            )
        )
        query = cls()
        query.set_provided_qastle(qastle_query)
        return query


class FuncADLQuery_ATLASr21(FuncADLQuery):
    yaml_tag = '!FuncADL_ATLASr21'
    default_codegen = 'atlasr21'


class FuncADLQuery_ATLASr22(FuncADLQuery):
    yaml_tag = '!FuncADL_ATLASr22'
    default_codegen = 'atlasr22'


class FuncADLQuery_ATLASxAOD(FuncADLQuery):
    yaml_tag = '!FuncADL_ATLASxAOD'
    default_codegen = 'atlasxaod'


class FuncADLQuery_CMS(FuncADLQuery):
    yaml_tag = '!FuncADL_CMS'
    default_codegen = 'cms'
