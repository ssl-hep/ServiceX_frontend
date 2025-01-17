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


def test_app_version(script_runner):
    import servicex._version

    result = script_runner.run(["servicex", "--version"])
    assert result.returncode == 0
    assert result.stdout == f"ServiceX {servicex._version.__version__}\n"


def test_deliver(script_runner):
    with patch("servicex.app.main.servicex_client") as mock_servicex_client:
        mock_servicex_client.deliver = Mock(
            return_value={"UprootRaw_YAML": ["/tmp/foo.root", "/tmp/bar.root"]}
        )
        result = script_runner.run(["servicex", "deliver", "foo.yaml"])
        assert result.returncode == 0
        result_rows = result.stdout.split("\n")
        assert result_rows[0] == "Delivering foo.yaml to ServiceX cache"
        assert (
            result_rows[1] == "{'UprootRaw_YAML': ['/tmp/foo.root', '/tmp/bar.root']}"
        )
