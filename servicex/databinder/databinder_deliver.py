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
from typing import Any, Dict
# from rich.progress import Progress, TextColumn, BarColumn, MofNCompleteColumn, \
#     TimeRemainingColumn
import asyncio
import nest_asyncio

from servicex.databinder.databinder_requests import DataBinderRequests
from servicex.expandable_progress import ExpandableProgress

nest_asyncio.apply()


class DataBinderDeliver:
    """
    a
    """

    def __init__(self, updated_config: Dict[str, Any]) -> None:
        self._config = updated_config
        self._requests = DataBinderRequests(self._config).get_requests()

    async def deliver_and_copy(self, req, progress):
        if req['delivery'] == "objectstore":
            results = await req['ds_query'].as_signed_urls_async(provided_progress=progress)
        else:
            results = await req['ds_query'].as_files_async(provided_progress=progress)
        return results

    async def get_data(self):
        tasks = []
        outputs = []
        with ExpandableProgress() as progress:
            for req in self._requests:
                tasks.append(self.deliver_and_copy(req, progress))

            for f in asyncio.as_completed(tasks):
                value = await f
                outputs.append(value)
                # print(value)

        return outputs