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
import os
from pathlib import Path
from unittest.mock import patch
import pytest

from servicex.configuration import Configuration


@patch("servicex.configuration.tempfile.gettempdir", return_value="./mytemp")
def test_config_read(tempdir):
    # Windows style user name
    os.environ["UserName"] = "p_higgs"
    c = Configuration.read(config_path="tests/example_config.yaml")
    assert c.cache_path == "mytemp/servicex_p_higgs"

    # Reset environment
    del os.environ["UserName"]

    # Linux style user name
    os.environ["USER"] = "p_higgs2"
    c = Configuration.read(config_path="tests/example_config.yaml")
    assert c.cache_path == "mytemp/servicex_p_higgs2"

    # but what if there is no file at all?
    with pytest.raises(NameError):
        Configuration.read(config_path="invalid.yaml")


@patch("servicex.configuration.tempfile.gettempdir", return_value="./mytemp")
def test_default_cache_path(tempdir):

    # Windows style user name
    os.environ["UserName"] = "p_higgs"
    c = Configuration.read(config_path="tests/example_config_no_cache_path.yaml")
    assert c.cache_path == "mytemp/servicex_p_higgs"
    del os.environ["UserName"]

    # Linux style user name
    os.environ["USER"] = "p_higgs"
    c = Configuration.read(config_path="tests/example_config_no_cache_path.yaml")
    assert c.cache_path == "mytemp/servicex_p_higgs"
    del os.environ["USER"]


def test_read_from_home(monkeypatch, tmp_path):
    """Ensure configuration can be located in the user's home directory."""

    # Create a fake home directory with a servicex.yaml file
    home = tmp_path / "home"
    home.mkdir()
    cfg = home / "servicex.yaml"
    cfg.write_text(
        """
api_endpoints:
  - endpoint: http://localhost:5000
    name: localhost
"""
    )

    # Patch Path.home to point to our fake home and move cwd elsewhere
    monkeypatch.setattr(Path, "home", lambda: home)
    work = tmp_path / "work"
    work.mkdir()
    monkeypatch.chdir(work)

    c = Configuration.read()
    assert c.api_endpoints[0].endpoint == "http://localhost:5000"


@pytest.mark.parametrize("config_filename", ["servicex.yaml", ".servicex"])
def test_read_from_default_files(monkeypatch, tmp_path, config_filename):
    """
    Ensure config can be located in the user's home directory for servicex.yaml and .servicex.
    """

    # Create a fake home directory with the config file
    cfg = tmp_path / config_filename
    cfg.write_text(
        """
api_endpoints:
  - endpoint: http://localhost:5012
    name: localhost
"""
    )

    monkeypatch.chdir(tmp_path)

    c = Configuration.read()
    assert c.api_endpoints[0].endpoint == "http://localhost:5012"
