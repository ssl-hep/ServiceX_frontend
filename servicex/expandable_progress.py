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

from rich.progress import (
    Progress,
    TextColumn,
    BarColumn,
    MofNCompleteColumn,
    TimeRemainingColumn,
    TaskID,
)

DEFAULT_STYLE = [
    TextColumn("[progress.description]{task.description}"),
    BarColumn(complete_style="rgb(114,156,31)", finished_style="rgb(0,255,0)"),
    MofNCompleteColumn(),
    TimeRemainingColumn(compact=True, elapsed_when_finished=True),
]

BROKEN_STYLE = [
    TextColumn("[progress.description]{task.description}"),
    BarColumn(complete_style="rgb(255,0,0)"),
    MofNCompleteColumn(),
    TimeRemainingColumn(compact=True, elapsed_when_finished=True),
]


class ProgressCounts:
    def __init__(
        self,
        description: str,
        task_id: TaskID,
        start: Optional[int] = None,
        total: Optional[int] = None,
        completed: Optional[int] = None,
    ):

        self.description = description
        self.taskId = task_id
        self.start = start
        self.total = total
        self.completed = completed


class ExpandableProgress:
    def __init__(
        self,
        display_progress: bool = True,
        provided_progress: Optional[Progress | ExpandableProgress] = None,
        overall_progress: bool = False,
    ):
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
        self.overall_progress = overall_progress
        self.overall_progress_transform_task = None
        self.overall_progress_download_task = None
        self.progress_counts = {}
        if display_progress:
            if self.overall_progress or not provided_progress:
                self.progress = TranformStatusProgress(*DEFAULT_STYLE)

            if provided_progress:
                self.progress = (
                    provided_progress
                    if isinstance(provided_progress, Progress)
                    else provided_progress.progress
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
        if self.display_progress and self.overall_progress:
            if (
                not self.overall_progress_download_task
                and not self.overall_progress_transform_task
            ):
                self.overall_progress_transform_task = self.progress.add_task(
                    "Transform", start=False, total=None
                )
                self.overall_progress_download_task = self.progress.add_task(
                    "Download/URLs", start=False, total=None
                )

            task_id = self.progress.add_task(
                param, start=start, total=total, visible=False
            )
            new_task = ProgressCounts(
                "Transform" if param.endswith("Transform") else param,
                task_id,
                start=start,
                total=total,
            )
            self.progress_counts[task_id] = new_task
            return task_id
        if self.display_progress and not self.overall_progress:
            return self.progress.add_task(param, start=start, total=total)

    def update(self, task_id, task_type, total=None, completed=None, **fields):

        if self.display_progress and self.overall_progress:
            if task_type.endswith("Transform"):
                task_type = "Transform"
            # Calculate and update
            overall_completed = 0
            overall_total = 0
            if completed:
                self.progress_counts[task_id].completed = completed

            if total:
                self.progress_counts[task_id].total = total

            for task in self.progress_counts:
                if (
                    self.progress_counts[task].description == task_type
                    and self.progress_counts[task].completed
                ):
                    overall_completed += self.progress_counts[task].completed

            for task in self.progress_counts:
                if (
                    self.progress_counts[task].description == task_type
                    and self.progress_counts[task].total
                ):
                    overall_total += self.progress_counts[task].total

            if task_type == "Transform":
                return self.progress.update(
                    self.overall_progress_transform_task,
                    completed=overall_completed,
                    total=overall_total,
                )
            else:
                return self.progress.update(
                    self.overall_progress_download_task,
                    completed=overall_completed,
                    total=overall_total,
                )

        if self.display_progress and not self.overall_progress:
            return self.progress.update(
                task_id, completed=completed, total=total, **fields
            )

    def start_task(self, task_id, task_type):
        if self.display_progress and self.overall_progress:
            if task_type.endswith("Transform"):
                self.progress.start_task(task_id=self.overall_progress_transform_task)
            else:
                self.progress.start_task(task_id=self.overall_progress_download_task)
        elif self.display_progress and not self.overall_progress:
            self.progress.start_task(task_id=task_id)

    def advance(self, task_id, task_type):
        if self.display_progress and self.overall_progress:
            if self.progress_counts[task_id].completed is not None:
                self.progress_counts[task_id].completed += 1
            else:
                self.progress_counts[task_id].completed = 1
            if task_type.endswith("Transform"):
                self.progress.advance(task_id=self.overall_progress_transform_task)
            else:
                self.progress.advance(task_id=self.overall_progress_download_task)
        elif self.display_progress and not self.overall_progress:
            self.progress.advance(task_id=task_id)

    def refresh(self):
        if self.progress is not None:
            self.progress.refresh()


class TranformStatusProgress(Progress):
    """
    A rich Progress display that stays usable when a request has many samples and
    can show individual transforms as failed.

    Rich's defaults assume a handful of bars, but ServiceX adds two per sample and
    every draw covers all of them. Left alone, a request with hundreds of samples
    can spend minutes drawing bars before any transform reaches the server, because
    drawing blocks the event loop that submits them.
    """

    def __init__(self, *args, refresh_per_second: float = 2, **kwargs):
        # Prevents the background refresh thread from competing with the event loop.
        # At rich's default of 10 Hz, a slow draw leaves the thread busy most of the
        # time, delaying network requests. Status only changes every few seconds, so
        # a slower rate costs little beyond choppier animation on pulsing bars.
        super().__init__(*args, refresh_per_second=refresh_per_second, **kwargs)

    def refresh(self) -> None:
        # Prevents a full redraw every time a bar is added. Rich calls this from
        # add_task, so adding N bars draws N times, and the total cost grows with the
        # square of the bar count. It is safe to skip when auto-refresh is on: the
        # background thread draws the latest state shortly after, and stopping the
        # display draws the final state. Without that thread, this is the only way
        # to draw, so the call must go through.
        if not self.live.auto_refresh:
            super().refresh()

    def get_renderables(self):
        # Rich uses one set of columns per table, so each task gets its own table to
        # let a failed transform show a red bar while the others stay green.
        for task in self.tasks:
            if task.fields.get("bar") == "failure":
                self.columns = BROKEN_STYLE
            else:
                self.columns = DEFAULT_STYLE
            yield self.make_tasks_table([task])
