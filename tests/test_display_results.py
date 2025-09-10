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
from unittest.mock import Mock, patch
import pytest

from servicex.servicex_client import (
    GuardList,
    _display_results,
    _get_progress_options,
    ProgressBarFormat,
    ServiceXClient,
)


def test_display_results_basic():
    """Test _display_results basic functionality - covers most code paths."""
    # Test with valid files (covers main path)
    valid_files = GuardList(["/tmp/file1.root", "/tmp/file2.root"])
    # Test with error case (covers error path)
    error_files = GuardList(ValueError("Test error"))

    out_dict = {"ValidSample": valid_files, "ErrorSample": error_files}

    with patch("rich.get_console") as mock_get_console:
        mock_console = Mock()
        mock_get_console.return_value = mock_console

        with patch("servicex.servicex_client.Table"):
            _display_results(out_dict)

            # Just verify it was called - don't over-test internal details
            mock_get_console.assert_called_once()
            assert mock_console.print.call_count >= 2  # At least completion + total


def test_essential_valueerrors():
    """Test the most important ValueError cases in one simple test."""
    # Test progress options
    assert _get_progress_options(ProgressBarFormat.expanded) == {}
    with pytest.raises(ValueError, match="Invalid value"):
        _get_progress_options("invalid")

    # Test ServiceX client errors - simplest possible
    with pytest.raises(ValueError, match="Only specify backend or url"):
        with patch("servicex.servicex_client.Configuration") as mock_config_class:
            mock_config = Mock()
            mock_config.endpoint_dict.return_value = {}
            mock_config.default_endpoint = None
            mock_config_class.read.return_value = mock_config
            ServiceXClient(backend="test", url="http://test.com")


def test_guardlist_basics():
    """Test GuardList basic functionality."""
    # Valid case
    valid_list = GuardList([1, 2, 3])
    assert len(valid_list) == 3
    assert valid_list[0] == 1
    assert valid_list.valid()

    # Error case
    from servicex.servicex_client import ReturnValueException

    error_list = GuardList(ValueError("error"))
    assert not error_list.valid()
    with pytest.raises(ReturnValueException):
        _ = error_list[0]
