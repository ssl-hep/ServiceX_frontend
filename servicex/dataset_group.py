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
import asyncio
from typing import List, Optional, Union
from rich.progress import Progress

from servicex.query_core import Query
from servicex.expandable_progress import ExpandableProgress
from servicex.models import TransformedResults, ResultFormat
from make_it_sync import make_sync


DatasetGroupMember = Query


class DatasetGroup:
    def __init__(self, datasets: List[DatasetGroupMember]):
        r"""
        A group of datasets that are to be transformed together. This is a convenience
        class to allow you to submit multiple datasets to a ServiceX instance and
        then wait for all of them to complete.

        :param datasets: List of transform request as dataset instances
        """
        self.tasks = []
        self.datasets = datasets

    def set_result_format(self, result_format: ResultFormat):
        r"""
        Set the result format for all the datasets in the group.

        :param result_format: ResultFormat instance
        """
        for dataset in self.datasets:
            dataset.set_result_format(result_format)
        return self

    async def as_signed_urls_async(
        self,
        display_progress: bool = True,
        provided_progress: Optional[Progress] = None,
        return_exceptions: bool = False,
    ) -> List[Union[TransformedResults, BaseException]]:
        with ExpandableProgress(display_progress, provided_progress) as progress:
            self.tasks = [
                d.as_signed_urls_async(provided_progress=progress)
                for d in self.datasets
            ]
            return await asyncio.gather(*self.tasks, return_exceptions=return_exceptions)

    as_signed_urls = make_sync(as_signed_urls_async)

    async def as_files_async(self,
                             display_progress: bool = True,
                             provided_progress: Optional[Progress] = None,
                             return_exceptions: bool = False,
                             ) -> List[Union[TransformedResults, BaseException]]:
        with ExpandableProgress(display_progress, provided_progress) as progress:
            self.tasks = [
                d.as_files_async(provided_progress=progress)
                for d in self.datasets
            ]
            return await asyncio.gather(*self.tasks, return_exceptions=return_exceptions)

    as_files = make_sync(as_files_async)
