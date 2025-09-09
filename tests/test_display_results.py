# Copyright (c) 2024, IRIS-HEP
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

from servicex.servicex_client import GuardList, _display_results


def test_display_results_with_valid_files():
    """Test _display_results with valid GuardList containing files."""
    # Create actual GuardList with valid files
    files = ["/tmp/file1.root", "/tmp/file2.root"]
    guard_list = GuardList(files)

    out_dict = {"UprootRaw_YAML": guard_list}

    with patch("rich.get_console") as mock_get_console:
        mock_console = Mock()
        mock_get_console.return_value = mock_console

        with patch("servicex.servicex_client.Table") as mock_table_class:
            mock_table = Mock()
            mock_table_class.return_value = mock_table

            _display_results(out_dict)

            # Verify get_console was called
            mock_get_console.assert_called_once()

            # Verify console.print was called for completion message
            assert (
                mock_console.print.call_count >= 2
            )  # At least completion message and table

            # Verify Table was created with proper parameters
            mock_table_class.assert_called_once_with(
                title="Delivered Files", show_header=True, header_style="bold magenta"
            )

            # Verify table columns were added
            expected_calls = [
                (("Sample",), {"style": "cyan", "no_wrap": True}),
                (("File Count",), {"justify": "right", "style": "green"}),
                (("Files",), {"style": "dim"}),
            ]
            for expected_args, expected_kwargs in expected_calls:
                mock_table.add_column.assert_any_call(*expected_args, **expected_kwargs)

            # Verify table row was added
            mock_table.add_row.assert_called_once_with(
                "UprootRaw_YAML", "2", "/tmp/file1.root\n/tmp/file2.root"
            )


def test_display_results_with_many_files():
    """Test _display_results with more than 3 files (ellipsis case)."""
    files_list = [f"/tmp/file{i}.root" for i in range(1, 6)]  # 5 files
    guard_list = GuardList(files_list)

    out_dict = {"Sample1": guard_list}

    with patch("rich.get_console") as mock_get_console:
        mock_console = Mock()
        mock_get_console.return_value = mock_console

        with patch("servicex.servicex_client.Table") as mock_table_class:
            mock_table = Mock()
            mock_table_class.return_value = mock_table

            _display_results(out_dict)

            # Verify table row was added with ellipsis
            expected_files_display = (
                "/tmp/file1.root\n/tmp/file2.root\n... and 3 more files"
            )
            mock_table.add_row.assert_called_once_with(
                "Sample1", "5", expected_files_display
            )


def test_display_results_with_invalid_files():
    """Test _display_results with invalid GuardList (error case)."""
    # Create GuardList with an exception to make it invalid
    guard_list = GuardList(ValueError("Sample error"))

    out_dict = {"FailedSample": guard_list}

    with patch("rich.get_console") as mock_get_console:
        mock_console = Mock()
        mock_get_console.return_value = mock_console

        with patch("servicex.servicex_client.Table") as mock_table_class:
            mock_table = Mock()
            mock_table_class.return_value = mock_table

            _display_results(out_dict)

            # Verify error row was added
            mock_table.add_row.assert_called_once_with(
                "FailedSample",
                "[red]Error[/red]",
                "[red]Failed to retrieve files[/red]",
            )


def test_display_results_with_mixed_samples():
    """Test _display_results with both valid and invalid samples."""
    # Valid sample
    valid_guard_list = GuardList(["/tmp/valid.root"])

    # Invalid sample
    invalid_guard_list = GuardList(ValueError("Invalid sample"))

    out_dict = {"ValidSample": valid_guard_list, "InvalidSample": invalid_guard_list}

    with patch("rich.get_console") as mock_get_console:
        mock_console = Mock()
        mock_get_console.return_value = mock_console

        with patch("servicex.servicex_client.Table") as mock_table_class:
            mock_table = Mock()
            mock_table_class.return_value = mock_table

            _display_results(out_dict)

            # Verify both rows were added
            assert mock_table.add_row.call_count == 2

            # Check that both types of calls were made
            calls = mock_table.add_row.call_args_list
            call_args = [call[0] for call in calls]

            # Should have one valid and one error call
            assert ("ValidSample", "1", "/tmp/valid.root") in call_args
            assert (
                "InvalidSample",
                "[red]Error[/red]",
                "[red]Failed to retrieve files[/red]",
            ) in call_args


def test_display_results_total_files_calculation():
    """Test that total files count is calculated correctly."""
    # Sample 1: 2 files
    guard_list1 = GuardList(["/tmp/file1.root", "/tmp/file2.root"])

    # Sample 2: 3 files
    guard_list2 = GuardList(["/tmp/file3.root", "/tmp/file4.root", "/tmp/file5.root"])

    # Sample 3: Invalid (should not count)
    guard_list3 = GuardList(ValueError("Invalid sample"))

    out_dict = {"Sample1": guard_list1, "Sample2": guard_list2, "Sample3": guard_list3}

    with patch("rich.get_console") as mock_get_console:
        mock_console = Mock()
        mock_get_console.return_value = mock_console

        with patch("servicex.servicex_client.Table"):
            _display_results(out_dict)

            # Check that the total files message includes the correct count (2 + 3 = 5)
            print_calls = mock_console.print.call_args_list
            total_message_call = None
            for call in print_calls:
                if call[0] and "Total files delivered: 5" in str(call[0][0]):
                    total_message_call = call
                    break

            assert (
                total_message_call is not None
            ), f"Expected total files message not found in calls: {print_calls}"


def test_display_results_console_print_calls():
    """Test that all expected console.print calls are made."""
    guard_list = GuardList(["/tmp/file.root"])

    out_dict = {"Sample": guard_list}

    with patch("rich.get_console") as mock_get_console:
        mock_console = Mock()
        mock_get_console.return_value = mock_console

        with patch("servicex.servicex_client.Table") as mock_table_class:
            mock_table = Mock()
            mock_table_class.return_value = mock_table

            _display_results(out_dict)

            # Should have exactly 3 print calls:
            # 1. Completion message
            # 2. Table
            # 3. Total files message
            assert mock_console.print.call_count == 3

            # Verify console.print was called with table
            mock_console.print.assert_any_call(mock_table)
