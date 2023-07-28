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
from __future__ import annotations

from typing import Optional

from rich.progress import Progress, TextColumn, BarColumn, MofNCompleteColumn, \
    TimeRemainingColumn


class ExpandableProgress:
    def __init__(self,
                 display_progress: bool = True,
                 provided_progress: Optional[Progress | ExpandableProgress] = None):
        """
        We want to be able to use rich progress bars in the async code, but there are
        some situtations where the user doesn't want them. Also we might be running
        several simultaneous progress bars, and we want to be able to control that.

        We still want to keep the context manager interface, so this class implements
        the context manager but if display_progress is False, then it does nothing.
        If provided_progress is set then we just use that. Otherwise we create a new
        progress bar

        :param display_progress:
        :param provided_progress:
        """
        self.display_progress = display_progress
        self.provided_progress = provided_progress
        if display_progress:
            if provided_progress:
                self.progress = provided_progress if isinstance(provided_progress, Progress) \
                    else provided_progress.progress
            else:
                self.progress = Progress(
                    TextColumn("[progress.description]{task.description}"),
                    BarColumn(),
                    MofNCompleteColumn(),
                    TimeRemainingColumn(compact=True, elapsed_when_finished=True)
                )
        else:
            self.progress = None

    def __enter__(self):
        """
        Start the progress bar if it is not already started and the user wants one.
        :return:
        """
        if self.display_progress and not self.provided_progress:
            self.progress.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Close the progress bar if it is not already closed and the user wanted one in
        the first place.
        :param exc_type:
        :param exc_val:
        :param exc_tb:
        :return:
        """
        if self.display_progress and not self.provided_progress:
            self.progress.stop()

    def add_task(self, param, start, total):
        if self.display_progress:
            return self.progress.add_task(param, start=start, total=total)
