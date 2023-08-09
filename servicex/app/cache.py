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
import shutil

import rich
import typer
from rich.prompt import Confirm
from rich.table import Table

from servicex.servicex_client import ServiceXClient

cache_app = typer.Typer(name="cache", no_args_is_help=True)


@cache_app.callback()
def cache():
    """
    Sub-commands for creating and manipulating the local query cache
    """
    pass


@cache_app.command()
def list():
    """
    List the cached queries
    """
    sx = ServiceXClient()
    cache = sx.query_cache
    table = Table(title="Cached Queries")
    table.add_column("Title")
    table.add_column("Codegen")
    table.add_column("Transform ID")
    table.add_column("Run Date")
    table.add_column("Files")
    table.add_column("Format")
    runs = cache.cached_queries()
    for r in runs:
        table.add_row(
            r.title,
            r.codegen,
            r.request_id,
            r.submit_time.astimezone().strftime("%a, %Y-%m-%d %H:%M"),
            str(r.files),
            r.result_format
        )
    rich.print(table)


@cache_app.command()
def clear(force: bool = typer.Option(False, "-y", help="Force, don't ask for permission")):
    """
    Clear the local query cache
    """
    if force or Confirm.ask("Really clear cache and delete downloaded files?"):
        sx = ServiceXClient()
        sx.query_cache.close()
        shutil.rmtree(sx.config.cache_path)
        rich.print("Cache cleared")


@cache_app.command(no_args_is_help=True)
def delete(transform_id: str = typer.Option(None, "-t", "--transform-id", help="Transform ID")):
    """
    Delete a cached query. Use -t to specify the transform ID
    """
    sx = ServiceXClient()
    cache = sx.query_cache
    rec = cache.get_transform_by_request_id(transform_id)
    shutil.rmtree(rec.data_dir)
    cache.delete_record_by_request_id(rec.request_id)
