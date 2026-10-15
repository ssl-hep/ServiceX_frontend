# Copyright (c) 2026, IRIS-HEP
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
from unittest.mock import patch

SUMMARY = {
    "output_file": "salt.h5",
    "n_events": 1234,
    "events_per_sample": {"sig": 1000, "bkg": 234},
    "widths": {"jet": 11, "el": 2},
    "merge_seconds": 0.5,
    "servicex_seconds": 12.0,
    "failed_samples": [],
}


@patch("servicex.tools.salt_data.get_salt_data", return_value=SUMMARY)
def test_get_salt_data(mock_get, script_runner):
    result = script_runner.run(
        ["servicex", "get-salt-data", "config.yaml", "samples.yaml"]
    )

    assert result.returncode == 0
    assert "Written salt.h5" in result.stdout
    assert "1,234 events" in result.stdout
    assert "jet: 11" in result.stdout
    assert "sig" in result.stdout and "bkg" in result.stdout
    mock_get.assert_called_once_with(
        "config.yaml",
        "samples.yaml",
        output_file=None,
        max_events=None,
        delivery="LocalCache",
        servicex_name=None,
        config_path=None,
        ignore_local_cache=False,
    )


@patch("servicex.tools.salt_data.get_salt_data", return_value=SUMMARY)
def test_get_salt_data_options(mock_get, script_runner):
    result = script_runner.run(
        [
            "servicex",
            "get-salt-data",
            "config.yaml",
            "samples.yaml",
            "-o",
            "out.h5",
            "--max-events",
            "100",
            "--urls",
            "--ignore-cache",
            "-b",
            "testing",
            "-c",
            "servicex.yaml",
        ]
    )

    assert result.returncode == 0
    kwargs = mock_get.call_args.kwargs
    assert kwargs["output_file"] == "out.h5"
    assert kwargs["max_events"] == 100
    assert kwargs["delivery"] == "URLs"
    assert kwargs["ignore_local_cache"] is True
    assert kwargs["servicex_name"] == "testing"
    assert kwargs["config_path"] == "servicex.yaml"


@patch(
    "servicex.tools.salt_data.get_salt_data",
    return_value={**SUMMARY, "failed_samples": ["bkg"]},
)
def test_get_salt_data_failed_samples(mock_get, script_runner):
    result = script_runner.run(
        ["servicex", "get-salt-data", "config.yaml", "samples.yaml"]
    )

    assert result.returncode == 1
    assert "Written salt.h5" in result.stdout
    assert "Samples without results" in result.stdout
    assert "bkg" in result.stdout


@patch(
    "servicex.tools.salt_data.get_salt_data",
    side_effect=ValueError("Salt config: 'tree_name' is required"),
)
def test_get_salt_data_error(mock_get, script_runner):
    result = script_runner.run(
        ["servicex", "get-salt-data", "config.yaml", "samples.yaml"]
    )

    assert result.returncode == 1
    assert "tree_name" in result.stdout


def test_get_salt_data_missing_arguments(script_runner):
    result = script_runner.run(["servicex", "get-salt-data", "config.yaml"])
    assert result.returncode != 0
