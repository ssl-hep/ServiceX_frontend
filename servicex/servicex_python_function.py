#  Copyright (c) 2022 , IRIS-HEP
#   All rights reserved.
#
#   Redistribution and use in source and binary forms, with or without
#   modification, are permitted provided that the following conditions are met:
#
#   * Redistributions of source code must retain the above copyright notice, this
#     list of conditions and the following disclaimer.
#
#   * Redistributions in binary form must reproduce the above copyright notice,
#     this list of conditions and the following disclaimer in the documentation
#     and/or other materials provided with the distribution.
#
#   * Neither the name of the copyright holder nor the names of its
#     contributors may be used to endorse or promote products derived from
#     this software without specific prior written permission.
#
#   THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
#   AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
#   IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
#   DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
#   FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
#   DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
#   SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
#   CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
#   OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
#   OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#

#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
#
#
#
import inspect
from base64 import b64encode
from pathlib import Path
from typing import Optional, List, Callable

from servicex import ServiceXDataset


class ServiceXPythonFunction(ServiceXDataset):
    @staticmethod
    def _encode_function(selection_function: Callable):
        return b64encode(inspect.getsource(selection_function).encode("utf-8")).decode(
            "utf-8"
        )

    async def get_data_rootfiles_async(
        self, selection_function: Callable, title: Optional[str] = None
    ) -> List[Path]:
        return await self._file_return(
            self._encode_function(selection_function), "root-file", title
        )

    async def get_data_awkward_async(
        self, selection_function: Callable, title: Optional[str] = None
    ):
        return self._converter.combine_awkward(
            await self._data_return(
                self._encode_function(selection_function),
                lambda f: self._converter.convert_to_awkward(f),
                title,
            )
        )
