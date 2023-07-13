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
import asyncio
from pathlib import Path
from typing import Optional, List

import rich
import typer
from rich.progress import Progress
from rich.table import Table

from servicex.app.cli_options import url_cli_option, backend_cli_option
from servicex.minio_adapter import MinioAdapter
from servicex.models import Status, ResultFile
from servicex.servicex_client import ServiceXClient

transforms_app = typer.Typer(name="transforms", no_args_is_help=True)


@transforms_app.callback()
def transforms():
    """
    sub-commands for creating and manipulating Gardens
    """
    pass


@transforms_app.command(no_args_is_help=True)
def list(
    url: Optional[str] = url_cli_option,
    backend: Optional[str] = backend_cli_option,
    complete: Optional[bool] = typer.Option(
        None, "--complete", help="Only show successfully completed transforms"
    ),
):
    sx = ServiceXClient(url=url, backend=backend)
    table = Table(title="ServiceX Transforms")
    table.add_column("Transform ID")
    table.add_column("Title")
    table.add_column("Status")
    table.add_column("Files")
    transforms = sx.get_transforms()
    for t in transforms:
        if not complete or complete and t.status == Status.complete:
            table.add_row(
                t.request_id, "Not implemented", t.status, str(t.files_completed)
            )

    rich.print(table)


@transforms_app.command(no_args_is_help=True)
def files(
    url: Optional[str] = url_cli_option,
    backend: Optional[str] = backend_cli_option,
    transform_id: str = typer.Option(None, "-t", "--transform-id", help="Transform ID"),
):
    async def list_files(sx: ServiceXClient, transform_id: str) -> List[ResultFile]:
        transform = await sx.get_transform_status_async(transform_id)
        minio = MinioAdapter.for_transform(transform)
        return await minio.list_bucket()

    sx = ServiceXClient(url=url, backend=backend)
    result_files = asyncio.run(list_files(sx, transform_id))
    table = rich.table.Table(title=f"Files from {transform_id}")
    table.add_column("filename")
    table.add_column("Size(Mb)")
    table.add_column("Filetype")
    for f in result_files:
        table.add_row(f.filename, "%.2f" % (f.size / 1e6), f.extension)
    rich.print(table)


@transforms_app.command(no_args_is_help=True)
def download(
    url: Optional[str] = url_cli_option,
    backend: Optional[str] = backend_cli_option,
    transform_id: str = typer.Option(None, "-t", "--transform-id", help="Transform ID"),
    local_dir: str = typer.Option(".", "-d", help="Local dir to download to"),
):
    async def download_files(sx: ServiceXClient, transform_id: str, local_dir):
        async def download_with_progress(filename) -> Path:
            p = await minio.download_file(
                filename,
                local_dir,
                shorten_filename=sx.config.shortened_downloaded_filename,
            )
            progress.advance(download_progress)
            return p

        transform = await sx.get_transform_status_async(transform_id)
        minio = MinioAdapter.for_transform(transform)
        file_list = await minio.list_bucket()
        progress.update(download_progress, total=len(file_list))
        progress.start_task(download_progress)

        tasks = [download_with_progress(f.filename) for f in file_list]
        return await asyncio.gather(*tasks)

    with Progress() as progress:
        download_progress = progress.add_task("Downloading", start=False, total=None)
        sx = ServiceXClient(url=url, backend=backend)
        result_files = asyncio.run(download_files(sx, transform_id, local_dir))

    for path in result_files:
        print(path.as_posix())
