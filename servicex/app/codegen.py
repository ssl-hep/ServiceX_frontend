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

import rich
import typer

from servicex.app.cli_options import url_cli_option, backend_cli_option
from servicex.servicex_client import ServiceXClient
from typing import Optional

codegen_app = typer.Typer(name="codegen", no_args_is_help=True)


@codegen_app.command(no_args_is_help=False)
def flush(
        url: Optional[str] = url_cli_option,
        backend: Optional[str] = backend_cli_option):
    """
    Flush the available code generators from the cache
    """
    sx = ServiceXClient(url=url, backend=backend)
    cache = sx.query_cache
    cache.delete_codegen_by_backend(backend)
    rich.print("Deleted cached code generators.")


@codegen_app.command(no_args_is_help=False)
def list(
        url: Optional[str] = url_cli_option,
        backend: Optional[str] = backend_cli_option):
    """
    List the available code generators
    """
    sx = ServiceXClient(url=url, backend=backend)
    rich.print_json(data=sx.get_code_generators())
