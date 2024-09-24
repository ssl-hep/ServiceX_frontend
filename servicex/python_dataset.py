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
import inspect
from typing import Optional, Union, Callable
from base64 import b64encode
from textwrap import dedent
from servicex.query_core import QueryStringGenerator
import sys
if sys.version_info < (3, 11):
    from typing_extensions import Self
else:
    from typing import Self


class PythonFunction(QueryStringGenerator):
    yaml_tag = '!PythonFunction'
    default_codegen = 'python'

    def __init__(self, python_function: Optional[Union[str, Callable]] = None):
        self.python_function: Optional[Union[str, Callable]] = python_function

    def with_uproot_function(self, f: Union[str, Callable]) -> Self:
        self.python_function = f
        return self

    def generate_selection_string(self) -> str:
        if not self.python_function:
            raise ValueError("You must provide a python function using with_uproot_function")

        if isinstance(self.python_function, str):
            return b64encode(dedent(self.python_function).encode("utf-8")).decode("utf-8")
        else:
            return b64encode(dedent(inspect.getsource(self.python_function))
                             .encode("utf-8"))\
                .decode("utf-8")

    @classmethod
    def from_yaml(cls, _, node):
        code = node.value
        try:
            exec(code)
        except SyntaxError as e:
            raise SyntaxError(f"Syntax error {e} interpreting\n{code}")
        q = PythonFunction(code)
        return q
