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
import webbrowser
import re
from enum import Enum

import rich
import rich.box
import typer
from rich.progress import Progress

from servicex.app import pipeable_table
from servicex.app.cli_options import backend_cli_option, config_file_option
from servicex.minio_adapter import MinioAdapter
from servicex.models import Status, ResultFile
from servicex.servicex_client import ServiceXClient

transforms_app = typer.Typer(name="transforms", no_args_is_help=True)
complete_opt = typer.Option(
    None, "--complete", help="Only show successfully completed transforms"
)
running_opt = typer.Option(
    None, "--running", help="Only show transforms that are currently running"
)
transform_id_arg = typer.Argument(help="Transform ID")
local_dir_opt = typer.Option(".", "-d", help="Local dir to download to")
concurrency_opt = typer.Option(
    20, "--concurrency", help="Number of concurrent downloads"
)
log_level_opt = typer.Option(
    "ERROR", "-l", "--log-level", help="Level of Logs", case_sensitive=False
)
time_frame_opt = typer.Option(
    "month", "-f", "--time-frame", help="Time Frame", case_sensitive=False
)


@transforms_app.callback()
def transforms():
    """
    Sub-commands for interacting with transforms.
    """
    pass


@transforms_app.command(no_args_is_help=False)
def list(
    backend: Optional[str] = backend_cli_option,
    config_path: Optional[str] = config_file_option,
    complete: Optional[bool] = complete_opt,
    running: Optional[bool] = running_opt,
):
    """
    List the transforms that have been run.
    """

    def transform_filter(status: Status) -> bool:
        if complete and status == Status.complete:
            return True
        if running and status == Status.running:
            return True
        if not complete and not running:
            return True
        return False

    sx = ServiceXClient(backend=backend, config_path=config_path)

    table = pipeable_table(title="ServiceX Transforms")
    table.add_column("Transform ID")
    table.add_column("Title")
    table.add_column("Status")
    table.add_column("Files")
    transforms = sx.get_transforms()
    for t in transforms:
        if transform_filter(t.status):
            table.add_row(t.request_id, t.title, t.status, str(t.files_completed))

    rich.print(table)


@transforms_app.command(no_args_is_help=True)
def files(
    backend: Optional[str] = backend_cli_option,
    config_path: Optional[str] = config_file_option,
    transform_id: str = transform_id_arg,
):
    """
    List the files that were produced by a transform.
    """

    async def list_files(sx: ServiceXClient, transform_id: str) -> List[ResultFile]:
        transform = await sx.get_transform_status_async(transform_id)
        minio = MinioAdapter.for_transform(transform)
        return await minio.list_bucket()

    sx = ServiceXClient(backend=backend, config_path=config_path)
    result_files = asyncio.run(list_files(sx, transform_id))
    table = pipeable_table(title=f"Files from {transform_id}")
    table.add_column("filename")
    table.add_column("Size(Mb)")
    table.add_column("Filetype")
    for f in result_files:
        table.add_row(f.filename, "%.2f" % (f.size / 1e6), f.extension)
    rich.print(table)


@transforms_app.command(no_args_is_help=True)
def download(
    backend: Optional[str] = backend_cli_option,
    config_path: Optional[str] = config_file_option,
    transform_id: str = transform_id_arg,
    local_dir: str = local_dir_opt,
    concurrency: int = concurrency_opt,
):
    """
    Download the files that were produced by a transform.
    """

    async def download_files(sx: ServiceXClient, transform_id: str, local_dir):
        s3_semaphore = asyncio.Semaphore(concurrency)

        async def download_with_progress(filename) -> Path:
            async with s3_semaphore:
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
        sx = ServiceXClient(backend=backend, config_path=config_path)
        result_files = asyncio.run(download_files(sx, transform_id, local_dir))

    for path in result_files:
        print(path.as_posix())


@transforms_app.command(no_args_is_help=True)
def delete(
    backend: Optional[str] = backend_cli_option,
    config_path: Optional[str] = config_file_option,
    transform_id_list: List[str] = transform_id_arg,
):
    """
    Delete a completed transform along with the result files.
    """
    sx = ServiceXClient(backend=backend, config_path=config_path)
    for transform_id in transform_id_list:
        asyncio.run(sx.delete_transform(transform_id))
        sx.delete_transform_from_cache(transform_id)

        print(f"Transform {transform_id} deleted")


@transforms_app.command(no_args_is_help=True)
def cancel(
    backend: Optional[str] = backend_cli_option,
    config_path: Optional[str] = config_file_option,
    transform_id_list: List[str] = transform_id_arg,
):
    """
    Cancel a running transform request.
    """
    sx = ServiceXClient(backend=backend, config_path=config_path)
    for transform_id in transform_id_list:
        asyncio.run(sx.cancel_transform(transform_id))
        print(f"Transform {transform_id} cancelled")


class TimeFrame(str, Enum):
    r"""
    Time Frame levels: 'day', 'week' & 'month'
    """

    day = ("day",)
    week = ("week",)
    month = ("month",)


class LogLevel(str, Enum):
    r"""
    Level of the log messages: INFO & ERROR
    """

    info = ("INFO",)
    error = ("ERROR",)


def add_query(key, value):
    """
    Creates query string from the key and value pairs
    """
    query_string = "(query:(match_phrase:({0}:'{1}')))".format(key, value)
    return query_string


def select_time(time_frame=TimeFrame.day):
    """
    Takes input as 'day','week','month' and returns the time filter
    """
    time_string = time_frame
    if time_frame.lower() == TimeFrame.day:
        time_string = "time:(from:now%2Fd,to:now%2Fd)"
    elif time_frame.lower() == TimeFrame.week:
        time_string = "time:(from:now%2Fw,to:now%2Fw)"
    elif time_frame.lower() == TimeFrame.month:
        time_string = "time:(from:now-30d%2Fd,to:now)"
    else:
        rich.print("Got a time frame apart from 'day', 'week', 'month'")
    return time_string


def create_kibana_link_parameters(
    log_url, transform_id=None, log_level=None, time_frame=None
):
    """
    Create the _a and _g parameters for the kibana dashboard link
    """
    if log_level:
        a_parameter = (
            f"&_a=(filters:!({add_query('requestId', transform_id)},"
            f"{add_query('level', log_level.value.lower())}))"
        )
    else:
        a_parameter = f"&_a=(filters:!({add_query('requestId', transform_id)}))"
    g_parameter = f"&_g=({select_time(time_frame.value.lower())})"
    kibana_link = re.sub(r"\&\_g\=\(\)", g_parameter + a_parameter, log_url)
    return kibana_link


@transforms_app.command(no_args_is_help=True)
def logs(
    backend: Optional[str] = backend_cli_option,
    transform_id: str = transform_id_arg,
    log_level: Optional[LogLevel] = log_level_opt,
    time_frame: Optional[TimeFrame] = time_frame_opt,
):
    """
    Open the URL to the Kibana dashboard of the logs of a tranformer
    """
    sx = ServiceXClient(backend=backend)
    transforms = sx.get_transform_status(transform_id)
    if transforms and transforms.request_id == transform_id:
        kibana_link = create_kibana_link_parameters(
            transforms.log_url,
            transform_id=transform_id,
            log_level=log_level,
            time_frame=time_frame,
        )
        print(kibana_link)
        webbrowser.open(kibana_link)
    else:
        rich.print("Invalid Request ID")
