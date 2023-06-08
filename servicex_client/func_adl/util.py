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
from typing import Optional, cast


class FuncADLServerException (Exception):
    'Thrown when an exception happens contacting the server'
    def __init__(self, msg):
        Exception.__init__(self, msg)


def has_tuple(a: ast.AST) -> bool:
    """Determine if this query used tuples in its final result

    NOTE: This can't do depth searches in a really complex
    query - then you need to follow best practices.

    Args:
        a (ast.AST): The query

    Returns:
        bool: True if the final Select statement has tuples or not.
    """

    def find_Select(a: ast.AST) -> Optional[ast.Call]:
        # Descent the call chain until we find a Select.
        while isinstance(a, ast.Call):
            if isinstance(a.func, ast.Name):
                if cast(ast.Name, a.func).id == "Select":
                    return a
            a = a.args[0]
        return None

    select_statement = find_Select(a)
    if select_statement is None:
        return False

    # Ok - we have a select statement. Now we need to see if it is returning a tuple.
    func_called = select_statement.args[1]
    assert isinstance(func_called, ast.Lambda)
    body = func_called.body
    return isinstance(body, ast.Tuple)


def has_col_names(a: ast.AST) -> bool:
    """Determine if any column names were specified
    in this request.

    Args:
        a (ast.AST): The complete AST of the request.

    Returns:
        bool: True if no column names were specified, False otherwise.
    """
    assert isinstance(a, ast.Call)
    func_ast = a
    top_function = cast(ast.Name, a.func).id

    if top_function == "ResultAwkwardArray":
        if len(a.args) >= 2:
            cols = a.args[1]
            if isinstance(cols, ast.List):
                if len(cols.elts) > 0:
                    return True
            elif isinstance(ast.literal_eval(cols), str):
                return True
        func_ast = a.args[0]
        assert isinstance(func_ast, ast.Call)

    top_function = cast(ast.Name, func_ast.func).id
    if top_function not in ["Select", "SelectMany"]:
        return False

    # Grab the lambda and see if it is returning a dict
    func_called = func_ast.args[1]
    assert isinstance(func_called, ast.Lambda)
    body = func_called.body
    if isinstance(body, ast.Dict):
        return True

    # Ok - we didn't find evidence of column names being
    # specified. It could still happen, but not as far
    # as we can tell.

    return False
