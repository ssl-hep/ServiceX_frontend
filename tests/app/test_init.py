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

import pytest
from pathlib import Path
from unittest.mock import patch, AsyncMock, Mock
import tempfile

from servicex.app.init import verify_token


@pytest.fixture
def temp_dir():
    """Fixture providing a temporary directory."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.mark.asyncio
async def test_verify_token_success():
    """Test verify_token with successful authentication."""
    with patch("servicex.app.init.ServiceXAdapter") as mock_adapter_class:
        mock_adapter = Mock()
        mock_adapter.get_servicex_info = AsyncMock(return_value={})
        mock_adapter_class.return_value = mock_adapter

        result = await verify_token("https://servicex.af.uchicago.edu", "test-token")

        assert result is True
        mock_adapter_class.assert_called_once_with(
            url="https://servicex.af.uchicago.edu", refresh_token="test-token"
        )
        mock_adapter.get_servicex_info.assert_called_once()


@pytest.mark.asyncio
async def test_verify_token_failure():
    """Test verify_token with failed authentication."""
    with patch("servicex.app.init.ServiceXAdapter") as mock_adapter_class:
        mock_adapter = Mock()
        mock_adapter.get_servicex_info = AsyncMock(
            side_effect=Exception("Authentication failed")
        )
        mock_adapter_class.return_value = mock_adapter

        with patch("servicex.app.init.get_console") as mock_console:
            mock_console_obj = Mock()
            mock_console.return_value = mock_console_obj

            result = await verify_token("https://servicex.af.uchicago.edu", "bad-token")

            assert result is False
            call_args = mock_console_obj.print.call_args[0][0]
            assert "Failed to authenticate" in call_args


@patch("servicex.app.init.asyncio.run", return_value=True)
@patch("servicex.app.init.verify_token", return_value=True)
@patch("servicex.app.init.Prompt.ask", return_value="./downloads")
@patch("servicex.app.init.getpass.getpass", return_value="test-token-123")
@patch("servicex.app.init.Path.cwd")
def test_init_uchicago_command(
    mock_cwd,
    mock_getpass,
    mock_prompt,
    mock_verify,
    mock_async,
    script_runner,
    temp_dir,
):
    """Test the uchicago subcommand."""
    mock_cwd.return_value = temp_dir

    result = script_runner.run(["servicex", "init", "uchicago"])

    assert result.returncode == 0
    # Verify the config file was created
    config_file = temp_dir / "servicex.yaml"
    assert config_file.exists()
    content = config_file.read_text()
    assert "https://servicex.af.uchicago.edu" in content
    assert "servicex-uc-af" in content
    assert "test-token-123" in content


@patch("servicex.app.init.asyncio.run", return_value=True)
@patch("servicex.app.init.Prompt.ask", return_value="./downloads")
@patch("servicex.app.init.getpass.getpass", return_value="test-token")
@patch("servicex.app.init.Path.cwd")
def test_init_with_custom_url(
    mock_cwd, mock_getpass, mock_prompt, mock_async, script_runner, temp_dir
):
    """Test init with custom URL flag."""
    mock_cwd.return_value = temp_dir

    result = script_runner.run(
        [
            "servicex",
            "init",
            "--url",
            "https://custom.servicex.com",
        ]
    )

    assert result.returncode == 0
    config_file = temp_dir / "servicex.yaml"
    assert config_file.exists()
    content = config_file.read_text()
    assert "https://custom.servicex.com" in content
    assert "custom-servicex" in content


@patch("servicex.app.init.getpass.getpass")
@patch("servicex.app.init.Prompt.ask")
@patch("servicex.app.init.Path.cwd")
def test_init_with_auth_disabled(mock_cwd, mock_prompt, mock_getpass, script_runner):
    """Test init with authentication disabled."""
    with tempfile.TemporaryDirectory() as tmpdir:
        mock_cwd.return_value = Path(tmpdir)
        mock_prompt.return_value = "./downloads"

        result = script_runner.run(
            [
                "servicex",
                "init",
                "--url",
                "http://localhost:8000",
                "--auth-disabled",
            ]
        )

        assert result.returncode == 0
        # getpass should not have been called
        mock_getpass.assert_not_called()
        config_file = Path(tmpdir) / "servicex.yaml"
        assert config_file.exists()
        content = config_file.read_text()
        assert "http://localhost:8000" in content
        # Token should not be in the config
        assert "token:" not in content


@patch("servicex.app.init.asyncio.run", return_value=True)
@patch("servicex.app.init.Prompt.ask")
@patch("servicex.app.init.getpass.getpass", return_value="test-token")
@patch("servicex.app.init.Path.cwd")
def test_init_interactive_wizard_uchicago(
    mock_cwd, mock_getpass, mock_prompt, mock_async, script_runner, temp_dir
):
    """Test the interactive wizard selecting UChicago."""
    mock_cwd.return_value = temp_dir
    # First call for experiment selection, second for download dir
    mock_prompt.side_effect = ["A", "./downloads"]

    result = script_runner.run(["servicex", "init"])

    assert result.returncode == 0
    config_file = temp_dir / "servicex.yaml"
    assert config_file.exists()
    content = config_file.read_text()
    assert "https://servicex.af.uchicago.edu" in content


@patch("servicex.app.init.Prompt.ask", return_value="C")
@patch("servicex.app.init.Path.cwd")
def test_init_interactive_wizard_invalid_choice(
    mock_cwd, mock_prompt, script_runner, temp_dir
):
    """Test the interactive wizard with invalid choice."""
    mock_cwd.return_value = temp_dir

    result = script_runner.run(["servicex", "init"])

    assert result.returncode == 1


@patch("servicex.app.init.asyncio.run", return_value=False)
@patch("servicex.app.init.getpass.getpass", return_value="bad-token")
@patch("servicex.app.init.Path.cwd")
def test_init_token_verification_failure(
    mock_cwd, mock_getpass, mock_async, script_runner, temp_dir
):
    """Test init when token verification fails."""
    mock_cwd.return_value = temp_dir

    result = script_runner.run(["servicex", "init", "uchicago"])

    assert result.returncode == 1


@patch("servicex.app.init.asyncio.run", return_value=True)
@patch("servicex.app.init.Prompt.ask")
@patch("servicex.app.init.getpass.getpass", return_value="test-token")
@patch("servicex.app.init.Path.cwd")
def test_init_creates_download_directory(
    mock_cwd, mock_getpass, mock_prompt, mock_async, script_runner, temp_dir
):
    """Test that init creates the download directory if it doesn't exist."""
    mock_cwd.return_value = temp_dir
    download_path = temp_dir / "my" / "nested" / "downloads"
    mock_prompt.return_value = str(download_path)

    result = script_runner.run(["servicex", "init", "uchicago"])

    assert result.returncode == 0
    # Verify the nested directory was created
    assert download_path.exists()
    assert download_path.is_dir()


def test_run_without_source_or_url():
    """Test that run() raises RuntimeError when neither source nor custom_url is provided."""
    from servicex.app.init import run

    with pytest.raises(RuntimeError):
        run(source=None, custom_url=None)


@patch("servicex.app.init.asyncio.run", return_value=True)
@patch("servicex.app.init.Prompt.ask", return_value="./downloads")
@patch("servicex.app.init.getpass.getpass", return_value="token123")
@patch("servicex.app.init.Path.cwd")
def test_run_with_source_uchicago(
    mock_cwd, mock_getpass, mock_prompt, mock_async, temp_dir
):
    """Test run() with source='uchicago'."""
    from servicex.app.init import run

    mock_cwd.return_value = temp_dir

    run(source="uchicago")

    config_file = temp_dir / "servicex.yaml"
    assert config_file.exists()
    content = config_file.read_text()
    assert "https://servicex.af.uchicago.edu" in content
    assert "servicex-uc-af" in content
    assert "UChicago" in content or "uchicago" in content


@patch("servicex.app.init.getpass.getpass")
@patch("servicex.app.init.Prompt.ask", return_value="./downloads")
@patch("servicex.app.init.Path.cwd")
def test_run_with_custom_url_and_auth_disabled(
    mock_cwd, mock_prompt, mock_getpass, temp_dir
):
    """Test run() with custom_url and auth_disabled=True."""
    from servicex.app.init import run

    mock_cwd.return_value = temp_dir

    run(custom_url="http://localhost:8000", auth_disabled=True)

    # getpass should not be called when auth is disabled
    mock_getpass.assert_not_called()

    config_file = temp_dir / "servicex.yaml"
    assert config_file.exists()
    content = config_file.read_text()
    assert "http://localhost:8000" in content
    assert "custom-servicex" in content
    # Should not have token line
    assert "token:" not in content


@patch("servicex.app.init.config", {})
@patch("servicex.app.init.Prompt.ask", return_value="A")
@patch("servicex.app.init.Path.cwd")
def test_init_wizard_with_missing_config_key(
    mock_cwd, mock_prompt, script_runner, temp_dir
):
    """Test the interactive wizard when config key is missing (edge case)."""
    mock_cwd.return_value = temp_dir

    result = script_runner.run(["servicex", "init"])

    assert result.returncode == 1
