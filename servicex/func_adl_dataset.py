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
    Union,
    Callable,
    Iterable,
    Dict,
)
from pydantic import typing
from qastle import python_ast_to_text_ast

from func_adl import EventDataset
from func_adl.object_stream import S
from servicex.configuration import Configuration
from servicex.dataset import Dataset
from servicex.func_adl.util import has_tuple
from servicex.models import ResultFormat
from servicex.query_cache import QueryCache
from servicex.servicex_adapter import ServiceXAdapter
from servicex.types import DID

T = TypeVar("T")


class FuncADLDataset(Dataset, EventDataset[T]):
    r"""
    ServiceX Dataset class that uses func_adl query syntax.
    """
    # These are methods that are translated locally
    _execute_locally = ["ResultPandasDF", "ResultAwkwardArray"]

    async def execute_result_async(
        self, a: ast.AST, title: Optional[str] = None
    ) -> Any:
        pass

    def check_data_format_request(self, f_name: str):
        pass

    def __init__(
        self,
        dataset_identifier: DID,
        sx_adapter: ServiceXAdapter = None,
        title: str = "ServiceX Client",
        codegen: str = None,
        config: Configuration = None,
        query_cache: QueryCache = None,
        result_format: Optional[ResultFormat] = None,
        item_type: Type = Any,
    ):
        Dataset.__init__(
            self,
            dataset_identifier=dataset_identifier,
            title=title,
            codegen=codegen,
            sx_adapter=sx_adapter,
            config=config,
            query_cache=query_cache,
            result_format=result_format,
        )
        EventDataset.__init__(self, item_type=item_type)
        self.provided_qastle = None

    def set_provided_qastle(self, qastle: str):
        self.provided_qastle = qastle

    def clone_with_new_ast(self, new_ast: ast.AST, new_type: typing.Any):
        """
        Override the method from ObjectStream - We need to be careful because the query
        cache is a tinyDB database that holds an open file pointer. We are not allowed
        to clone an open file handle, so for this property we will copy by reference
        and share it between the clones. Turns out ast class is also picky about copies,
        so we set that explicitly.

        :param new_ast:
        :param new_type:
        :return:
        """
        clone = copy.copy(self)
        for attr, value in vars(self).items():
            if type(value) == QueryCache:
                setattr(clone, attr, value)
            elif attr == "_q_ast":
                setattr(clone, attr, new_ast)
            else:
                setattr(clone, attr, copy.deepcopy(value))

        clone._item_type = new_type
        return clone

    def SelectMany(
        self, func: Union[str, ast.Lambda, Callable[[T], Iterable[S]]]
    ) -> FuncADLDataset[S]:
        r"""
        Given the current stream's object type is an array or other iterable, return
        the items in this objects type, one-by-one. This has the effect of flattening a
        nested array.

        Arguments:

            func:   The function that should be applied to this stream's objects to return
                    an iterable. Each item of the iterable is now the stream of objects.

        Returns:
            The Dataset with the SelectMany operator applied

        Notes:
            - The function can be a `lambda`, the name of a one-line function, a string that
              contains a lambda definition, or a python `ast` of type `ast.Lambda`.
        """
        return super().SelectMany(func)

    def Select(self, f: Union[str, ast.Lambda, Callable[[T], S]]) -> FuncADLDataset[S]:
        r"""
        Apply a transformation function to each object in the stream, yielding a new type of
        object. There is a one-to-one correspondence between the input objects and output objects.

        Arguments:
            f:      selection function (lambda)

        Returns:
            The Dataset with the Select operator applied

        Notes:
            - The function can be a `lambda`, the name of a one-line function, a string that
              contains a lambda definition, or a python `ast` of type `ast.Lambda`.
        """

        return super().Select(f)

    def generate_selection_string(self) -> str:
        if self.provided_qastle:
            return self.provided_qastle
        else:
            return self.generate_qastle(self.query_ast)

    def Where(
        self, filter: Union[str, ast.Lambda, Callable[[T], bool]]
    ) -> FuncADLDataset[T]:
        r"""
        Filter the object stream, allowing only items for which `filter` evaluates as true through.

        Arguments:

            filter      A filter lambda that returns True/False.

        Returns:

            The Dataset with the Where operator applied

        Notes:
            - The function can be a `lambda`, the name of a one-line function, a string that
              contains a lambda definition, or a python `ast` of type `ast.Lambda`.
        """

        return super().Where(filter)

    def MetaData(self, metadata: Dict[str, Any]) -> FuncADLDataset[T]:
        r"""Add metadata to the current object stream. The metadata is an arbitrary set of string
        key-value pairs. The backend must be able to properly interpret the metadata.

        Returns:
            The Dataset with the MetaData operator applied
        """

        return super().MetaData(metadata)

    def QMetaData(self, metadata: Dict[str, Any]) -> FuncADLDataset[T]:
        r"""Add query metadata to the current object stream.

        - Metadata is never transmitted to any back end
        - Metadata is per-query, not per sample.

        Warnings are issued if metadata is overwriting metadata.

        Args:
            metadata (Dict[str, Any]): Metadata to be used later

        Returns:
            The Dataset with the QMetaData operator applied
        """
        return super().QMetaData(metadata)

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
            default_format = self._ds.first_supported_datatype(["parquet", "root-file"])
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
