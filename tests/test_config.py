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
from unittest.mock import patch

from servicex.configuration import Configuration


@patch('servicex.configuration.tempfile.gettempdir', return_value="./mytemp")
def test_config_read(tempdir):
    # Windows style user name
    os.environ['UserName'] = "p_higgs"
    c = Configuration.read(config_path="tests/example_config.yaml")
    assert c.cache_path == "mytemp/servicex_p_higgs"

    # Reset environment
    del os.environ['UserName']

    # Linux style user name
    os.environ['USER'] = "p_higgs2"
    c = Configuration.read(config_path="tests/example_config.yaml")
    assert c.cache_path == "mytemp/servicex_p_higgs2"


@patch('servicex.configuration.tempfile.gettempdir', return_value="./mytemp")
def test_default_cache_path(tempdir):

    # Windows style user name
    os.environ['UserName'] = "p_higgs"
    c = Configuration.read(config_path="tests/example_config_no_cache_path.yaml")
    assert c.cache_path == "mytemp/servicex_p_higgs"
    del os.environ['UserName']

    # Linux style user name
    os.environ['USER'] = "p_higgs"
    c = Configuration.read(config_path="tests/example_config_no_cache_path.yaml")
    assert c.cache_path == "mytemp/servicex_p_higgs"
    del os.environ['USER']
