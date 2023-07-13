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
import typing
from base64 import b64encode

from servicex.configuration import Configuration
from servicex.dataset import Dataset
from servicex.models import ResultFormat
from servicex.query_cache import QueryCache
from servicex.servicex_adapter import ServiceXAdapter
from servicex.types import DID


class PythonDataset(Dataset):

    def __init__(self, dataset_identifier: DID,
                 sx_adapter: ServiceXAdapter = None,
                 title: str = "ServiceX Client",
                 codegen: str = None,
                 config: Configuration = None,
                 query_cache: QueryCache = None,
                 result_format: typing.Optional[ResultFormat] = None
                 ):
        super().__init__(dataset_identifier=dataset_identifier,
                         title=title,
                         codegen=codegen,
                         sx_adapter=sx_adapter,
                         config=config,
                         query_cache=query_cache,
                         result_format=result_format)

        self.python_function = None

    def with_uproot_function(self, f: typing.Callable) -> Dataset:
        self.python_function = f
        return self

    def generate_selection_string(self) -> str:
        if not self.python_function:
            raise ValueError("You must provide a python function using with_uproot_function")

        return b64encode(inspect.getsource(self.python_function)
                         .encode("utf-8"))\
            .decode("utf-8")
