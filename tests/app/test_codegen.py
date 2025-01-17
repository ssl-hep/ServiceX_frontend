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


def test_codegen_list(script_runner):
    with patch(
        "servicex.servicex_adapter.ServiceXAdapter.get_code_generators",
        return_value={"uproot": "http://uproot-codegen", "xaod": "http://xaod-codegen"},
    ):
        result = script_runner.run(
            ["servicex", "codegen", "list", "-c", "tests/example_config.yaml"]
        )
        assert result.returncode == 0
        assert (
            result.stdout
            == """{
  "uproot": "http://uproot-codegen",
  "xaod": "http://xaod-codegen"
}
"""
        )


def test_codegen_flush(script_runner):
    with patch("servicex.query_cache.QueryCache.delete_codegen_by_backend") as p:
        result = script_runner.run(
            [
                "servicex",
                "codegen",
                "flush",
                "-c",
                "tests/example_config.yaml",
                "-b",
                "localhost",
            ]
        )
        assert result.returncode == 0
        p.assert_called_once_with("localhost")
