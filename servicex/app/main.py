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
from typing import Optional

import rich
import typer
from servicex._version import __version__
from servicex.app.cli_options import url_cli_option, backend_cli_option
from servicex.app.transforms import transforms_app
from servicex.app.cache import cache_app
from servicex.servicex_client import ServiceXClient

app = typer.Typer(no_args_is_help=True)

app.add_typer(transforms_app)
app.add_typer(cache_app)


def show_version(show: bool):
    """Display the installed version and quit."""
    if show:
        rich.print(f"garden-ai {__version__}")
        raise typer.Exit()


@app.callback()
def main_info(
    version: Optional[bool] = typer.Option(
        None, "--version", callback=show_version, is_eager=True
    )
):
    """
    🌱 Hello, Garden 🌱
    """
    pass


@app.command(no_args_is_help=False)
def codegens(
        url: Optional[str] = url_cli_option,
        backend: Optional[str] = backend_cli_option
):
    sx = ServiceXClient(url=url, backend=backend)
    rich.print_json(data=sx.get_code_generators())


if __name__ == "__main__":
    app()
