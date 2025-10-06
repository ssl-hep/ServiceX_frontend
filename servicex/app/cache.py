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
from pathlib import Path
from rich.prompt import Confirm
from typing import List

from servicex.app import pipeable_table
from servicex.models import TransformedResults
from servicex.servicex_client import ServiceXClient


def _format_size(size_bytes: int) -> str:
    """Return human readable string for size in bytes."""
    if size_bytes >= 1024**4:
        size = size_bytes / (1024**4)
        unit = "TB"
    elif size_bytes >= 1024**3:
        size = size_bytes / (1024**3)
        unit = "GB"
    else:
        size = size_bytes / (1024**2)
        unit = "MB"
    return f"{size:,.2f} {unit}"


cache_app = typer.Typer(name="cache", no_args_is_help=True)
force_opt = typer.Option(False, "-y", help="Force, don't ask for permission")
transform_id_arg = typer.Argument(help="Transform ID")


@cache_app.callback()
def cache():
    """
    Sub-commands for creating and manipulating the local query cache
    """
    pass


@cache_app.command()
def list(
    show_size: bool = typer.Option(False, "--size", help="Include size of cached files")
) -> None:
    """
    List the cached queries
    """
    sx = ServiceXClient()
    cache = sx.query_cache
    table = pipeable_table(title="Cached Queries")
    table.add_column("Title")
    table.add_column("Codegen")
    table.add_column("Transform ID")
    table.add_column("Run Date")
    table.add_column("Files")
    table.add_column("Format")
    if show_size:
        table.add_column("Size")

    runs: List[TransformedResults] = cache.cached_queries()
    submitted = cache.queries_in_state("SUBMITTED")

    for r in runs:
        row = [
            r.title,
            r.codegen,
            r.request_id,
            r.submit_time.astimezone().strftime("%a, %Y-%m-%d %H:%M"),
            str(r.files),
            r.result_format,
        ]
        if show_size:
            total_size: int = sum(
                Path(f).stat().st_size for f in r.file_list if Path(f).exists()
            )
            # Convert to human readable string, keeping two decimal places
            row.append(_format_size(total_size))
        table.add_row(*row)
    for r in submitted:
        row = [
            r.get("title", ""),
            r.get("codegen", ""),
            r.get("request_id", ""),
            "Submitted",
            "Submitted",
            str(r.get("result_format", "")),
        ]
        if show_size:
            row.append("N/A")
        table.add_row(*row)
    rich.print(table)


@cache_app.command()
def clear(force: bool = force_opt):
    """
    Clear the local query cache
    """
    if force or Confirm.ask("Really clear cache and delete downloaded files?"):
        sx = ServiceXClient()
        sx.query_cache.close()
        if sx.config.cache_path is not None:
            shutil.rmtree(sx.config.cache_path)
        rich.print("Cache cleared")


@cache_app.command(no_args_is_help=True)
def delete(transform_id: str = transform_id_arg):
    """
    Delete a cached query. Use -t to specify the transform ID
    """
    sx = ServiceXClient()
    if not sx.delete_transform_from_cache(transform_id):
        rich.print(f"Transform {transform_id} not found in cache")
