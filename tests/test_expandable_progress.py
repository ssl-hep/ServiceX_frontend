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
import threading
from unittest.mock import patch, MagicMock

import pytest

from servicex.expandable_progress import ExpandableProgress, TranformStatusProgress
from rich.progress import TextColumn, BarColumn, MofNCompleteColumn, TimeRemainingColumn


@patch(
    "servicex.expandable_progress.TranformStatusProgress",
    return_value=MagicMock(TranformStatusProgress),
)
def test_progress(mock_progress):
    with ExpandableProgress() as progress:
        assert progress.progress == mock_progress.return_value
        mock_progress.return_value.start.assert_called_once()
        assert progress.display_progress
    assert mock_progress.return_value.stop.call_count == 1


@patch(
    "servicex.expandable_progress.TranformStatusProgress",
    return_value=MagicMock(TranformStatusProgress),
)
def test_overall_progress(mock_progress):
    with ExpandableProgress(overall_progress=True) as progress:
        assert progress.progress == mock_progress.return_value
        mock_progress.return_value.start.assert_called_once()
        assert progress.display_progress
    assert mock_progress.return_value.stop.call_count == 1


@patch(
    "servicex.expandable_progress.TranformStatusProgress",
    return_value=MagicMock(TranformStatusProgress),
)
def test_overall_progress_mock(mock_progress):
    with ExpandableProgress(overall_progress=True) as progress:
        assert progress.progress == mock_progress.return_value
        mock_progress.return_value.start.assert_called_once()
        assert progress.display_progress
    assert mock_progress.return_value.stop.call_count == 1


@patch(
    "servicex.expandable_progress.TranformStatusProgress",
    return_value=MagicMock(TranformStatusProgress),
)
def test_refresh_mock(mock_progress):
    with ExpandableProgress(overall_progress=True) as progress:
        progress.refresh()
        mock_progress.return_value.refresh.assert_called_once()


def test_provided_progress(mocker):
    class MockedProgress(TranformStatusProgress):
        def __init__(self):
            self.start_call_count = 0
            self.stop_call_count = 0

        def start(self) -> None:
            self.start_call_count += 1

        def stop(self) -> None:
            self.stop_call_count += 1

    provided_progress = MockedProgress()
    provided_progress.start = mocker.Mock()
    provided_progress.stop = mocker.Mock()

    with ExpandableProgress(provided_progress=provided_progress) as progress:
        assert progress.progress == provided_progress
        assert provided_progress.start.call_count == 0
        assert progress.display_progress
    assert provided_progress.stop.call_count == 0


@patch(
    "servicex.expandable_progress.TranformStatusProgress",
    return_value=MagicMock(TranformStatusProgress),
)
def test_no_progress(mock_progress):
    with ExpandableProgress(display_progress=False) as progress:
        assert not progress.progress
        mock_progress.return_value.assert_not_called()
        mock_progress.return_value.start.assert_not_called()
        assert not progress.display_progress
        assert not progress.progress
    assert mock_progress.return_value.stop.call_count == 0


def test_nested_expandable_progress():
    inner_progress = TranformStatusProgress()
    with ExpandableProgress(provided_progress=inner_progress) as progress:
        with ExpandableProgress(provided_progress=progress) as progress2:
            assert progress2.progress == progress.progress
            assert progress2.display_progress
            assert progress2.progress == progress.progress


@patch("servicex.expandable_progress.TranformStatusProgress.make_tasks_table")
def test_get_renderables_without_failure(mock_make_tasks_table):
    progress = TranformStatusProgress(
        TextColumn("[progress.description]{task.description}"),
        BarColumn(complete_style="rgb(114,156,31)", finished_style="rgb(0,255,0)"),
        MofNCompleteColumn(),
        TimeRemainingColumn(compact=True, elapsed_when_finished=True),
    )
    progress.add_task("test_without_failure")
    list(progress.get_renderables())
    mock_make_tasks_table.assert_called()
    mock_make_tasks_table.assert_called_with(progress.tasks)
    assert progress.columns[1].complete_style == "rgb(114,156,31)"


def test_get_renderables_with_failure():
    progress = TranformStatusProgress(
        TextColumn("[progress.description]{task.description}"),
        BarColumn(complete_style="rgb(114,156,31)", finished_style="rgb(0,255,0)"),
        MofNCompleteColumn(),
        TimeRemainingColumn(compact=True, elapsed_when_finished=True),
    )
    progress.add_task("test_with_failure", bar="failure")
    list(progress.get_renderables())
    assert len(progress.columns) == 4
    assert isinstance(progress.columns[1], BarColumn)
    assert progress.columns[1].complete_style == "rgb(255,0,0)"


def test_get_renderables_skips_invisible_tasks():
    """Regression test: get_renderables() should skip tasks with visible=False."""
    progress = TranformStatusProgress(
        TextColumn("[progress.description]{task.description}"),
        BarColumn(complete_style="rgb(114,156,31)", finished_style="rgb(0,255,0)"),
        MofNCompleteColumn(),
        TimeRemainingColumn(compact=True, elapsed_when_finished=True),
    )
    progress.add_task("visible_task", visible=True)
    for i in range(10):
        progress.add_task(f"hidden_task_{i}", visible=False)

    renderables = list(progress.get_renderables())
    assert len(renderables) == 1


def test_download_aggregation_ignores_title_padding():
    """Regression test: the Download/URLs aggregate should sum every dataset, not just one."""
    with ExpandableProgress(overall_progress=True) as progress:
        short_transform_title = "a: Transform"
        short_download_title = "Download".rjust(len(short_transform_title))

        long_transform_title = "a_much_longer_dataset_name: Transform"
        long_download_title = "Download".rjust(len(long_transform_title))

        # Sanity check: the two download titles really are padded differently.
        assert short_download_title != long_download_title

        progress.add_task(short_transform_title, start=False, total=None)
        d1 = progress.add_task(short_download_title, start=False, total=None)
        progress.add_task(long_transform_title, start=False, total=None)
        d2 = progress.add_task(long_download_title, start=False, total=None)

        progress.update(d1, short_download_title, total=10, completed=4)
        progress.update(d2, long_download_title, total=20, completed=6)

        download_task = progress.progress.tasks[progress.overall_progress_download_task]
        assert download_task.total == 30
        assert download_task.completed == 10


@pytest.mark.asyncio
async def test_no_background_refresh_thread():
    """Regression test: no background thread is created; refresh runs via asyncio."""
    before = threading.active_count()
    with ExpandableProgress(display_progress=True, overall_progress=True) as progress:
        assert progress.progress.live.auto_refresh is False
        assert progress._refresh_task is not None
        during = threading.active_count()

        t_id = progress.add_task("0001_x: Transform", start=True, total=10)
        for _ in range(3):
            progress.advance(t_id, "Transform")
        # Give the asyncio refresh task a couple of turns to prove it runs.
        await asyncio.sleep(0.25)
        assert progress.progress.tasks[0].completed == 3

    # Let the cancellation raised inside _auto_refresh actually propagate.
    await asyncio.sleep(0)
    after = threading.active_count()

    assert during == before
    assert after == before
    assert progress._refresh_task.cancelled() or progress._refresh_task.done()


def test_progress_advance():
    with ExpandableProgress() as progress:
        t_id = progress.add_task("Transform", True, 100)
        d_id = progress.add_task("Download", True, 100)
        completed = 12
        total = 100
        progress.update(t_id, "Transform", total, completed)
        progress.advance(t_id, "Transform")
        assert progress.progress.tasks[0].completed - 1 == completed

    with ExpandableProgress(overall_progress=True) as progress:
        t_id = progress.add_task("Transform", True, 100)
        completed = 12
        total = 100
        progress.update(t_id, "Transform", total, completed)
        progress.advance(t_id, "Transform")
        assert progress.progress.tasks[0].completed - 1 == completed

        d_id = progress.add_task("Download", True, 100)
        completed = 12
        total = 100
        progress.update(d_id, "Download", total, completed)
        progress.advance(d_id, "Download")
        assert progress.progress.tasks[1].completed - 1 == completed

    with ExpandableProgress(overall_progress=True) as progress:
        d_id = progress.add_task("Download", True, 100)
        progress.advance(d_id, "Download")
        assert progress.progress.tasks[1].completed == 1
