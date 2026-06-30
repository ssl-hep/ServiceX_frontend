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

from unittest.mock import AsyncMock, Mock, patch

from servicex.configuration import Configuration, Endpoint


def _make_config(endpoints):
    """Helper to create a Configuration with given endpoints."""
    return Configuration(api_endpoints=endpoints)


@patch("servicex.app.check.Configuration.read")
def test_check_no_endpoints(mock_read, script_runner):
    """Test check command when no endpoints are configured."""
    mock_read.return_value = _make_config([])

    result = script_runner.run(["servicex", "check"])

    assert result.returncode == 0
    assert "No ServiceX endpoints configured" in result.stdout


@patch("servicex.app.check.ServiceXAdapter")
@patch("servicex.app.check.Configuration.read")
def test_check_single_endpoint_success(mock_read, mock_adapter_class, script_runner):
    """Test check command with a single endpoint that authenticates successfully."""
    mock_read.return_value = _make_config(
        [Endpoint(endpoint="https://servicex.example.com", name="test", token="tok")]
    )

    mock_adapter = Mock()
    mock_adapter.verify_authentication = AsyncMock(return_value=True)
    mock_adapter_class.return_value = mock_adapter

    result = script_runner.run(["servicex", "check"])

    assert result.returncode == 0
    assert "https://servicex.example.com" in result.stdout
    mock_adapter_class.assert_called_once_with(
        url="https://servicex.example.com", refresh_token="tok"
    )
    assert "Checking ServiceX endpoints" in result.stdout
    mock_adapter.verify_authentication.assert_called_once()


@patch("servicex.app.check.ServiceXAdapter")
@patch("servicex.app.check.Configuration.read")
def test_check_single_endpoint_failure(mock_read, mock_adapter_class, script_runner):
    """Test check command with a single endpoint that fails authentication."""
    mock_read.return_value = _make_config(
        [Endpoint(endpoint="https://servicex.example.com", name="test", token="bad")]
    )

    mock_adapter = Mock()
    mock_adapter.verify_authentication = AsyncMock(return_value=False)
    mock_adapter_class.return_value = mock_adapter

    result = script_runner.run(["servicex", "check"])

    assert result.returncode == 0
    assert "Checking ServiceX endpoints" in result.stdout
    assert "https://servicex.example.com" in result.stdout


@patch("servicex.app.check.ServiceXAdapter")
@patch("servicex.app.check.Configuration.read")
def test_check_multiple_endpoints(mock_read, mock_adapter_class, script_runner):
    """Test check command with multiple endpoints, some succeeding and some failing."""
    mock_read.return_value = _make_config(
        [
            Endpoint(
                endpoint="https://servicex1.example.com", name="ep1", token="tok1"
            ),
            Endpoint(
                endpoint="https://servicex2.example.com", name="ep2", token="tok2"
            ),
        ]
    )

    adapter1 = Mock()
    adapter1.verify_authentication = AsyncMock(return_value=True)
    adapter2 = Mock()
    adapter2.verify_authentication = AsyncMock(return_value=False)
    mock_adapter_class.side_effect = [adapter1, adapter2]

    result = script_runner.run(["servicex", "check"])

    assert result.returncode == 0
    assert "https://servicex1.example.com" in result.stdout
    assert "https://servicex2.example.com" in result.stdout
    assert mock_adapter_class.call_count == 2


@patch("servicex.app.check.ServiceXAdapter")
@patch("servicex.app.check.Configuration.read")
def test_check_endpoint_without_token(mock_read, mock_adapter_class, script_runner):
    """Test check command with an endpoint that has no token (auth disabled)."""
    mock_read.return_value = _make_config(
        [Endpoint(endpoint="http://localhost:8000", name="local", token="")]
    )

    mock_adapter = Mock()
    mock_adapter.verify_authentication = AsyncMock(return_value=True)
    mock_adapter_class.return_value = mock_adapter

    result = script_runner.run(["servicex", "check"])

    assert result.returncode == 0
    assert "http://localhost:8000" in result.stdout
    mock_adapter_class.assert_called_once_with(
        url="http://localhost:8000", refresh_token=""
    )
