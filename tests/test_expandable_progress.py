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
from unittest.mock import patch, MagicMock

from rich.progress import Progress

from servicex.expandable_progress import ExpandableProgress


@patch("servicex.expandable_progress.Progress", return_value=MagicMock(Progress))
def test_progress(mock_progress):
    with ExpandableProgress() as progress:
        assert progress.progress == mock_progress.return_value
        mock_progress.return_value.start.assert_called_once()
        assert progress.display_progress
    assert mock_progress.return_value.stop.call_count == 1


@patch("servicex.expandable_progress.Progress", return_value=MagicMock(Progress))
def test_provided_progress(mock_progress):
    provided_progress = MagicMock(Progress)
    with ExpandableProgress(provided_progress=provided_progress) as progress:
        assert progress.progress == provided_progress
        mock_progress.return_value.start.assert_not_called()
        provided_progress.return_value.start.assert_not_called()
        assert progress.display_progress
    assert provided_progress.stop.call_count == 0
    assert mock_progress.return_value.stop.call_count == 0


@patch("servicex.expandable_progress.Progress", return_value=MagicMock(Progress))
def test_nod_progress(mock_progress):
    with ExpandableProgress(display_progress=False) as progress:
        assert not progress.progress
        mock_progress.return_value.assert_not_called()
        mock_progress.return_value.start.assert_not_called()
        assert not progress.display_progress
        assert not progress.progress
    assert mock_progress.return_value.stop.call_count == 0
