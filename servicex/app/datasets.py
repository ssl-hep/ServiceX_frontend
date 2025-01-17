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
import asyncio
from typing import Optional

import rich

from servicex.app import pipeable_table, is_terminal_output
from servicex.app.cli_options import backend_cli_option, config_file_option

import typer

from servicex.servicex_client import ServiceXClient
from rich.table import Table

datasets_app = typer.Typer(name="datasets", no_args_is_help=True)


@datasets_app.command(no_args_is_help=False)
def list(
    backend: Optional[str] = backend_cli_option,
    config_path: Optional[str] = config_file_option,
    did_finder: Optional[str] = typer.Option(
        None,
        help="Filter datasets by DID finder. Some useful values are 'rucio' or 'user'",
        show_default=False,
    ),
    show_deleted: Optional[bool] = typer.Option(
        False,
        help="Show deleted datasets",
        show_default=True,
    ),
):
    """
    List the datasets. Use fancy formatting if printing to a terminal.
    Output as plain text if redirected.
    """
    sx = ServiceXClient(backend=backend, config_path=config_path)
    table = pipeable_table(title="ServiceX Datasets")
    table.add_column("ID")
    table.add_column("Name")
    table.add_column("Files")
    table.add_column("Size")
    table.add_column("Status")
    table.add_column("Created")
    if show_deleted:
        table.add_column("Deleted")

    datasets = asyncio.run(
        sx.get_datasets(did_finder=did_finder, show_deleted=show_deleted)
    )
    for d in datasets:
        # Format the CachedDataset object into a table row
        # The last_updated field is what we should be displaying, but that is
        # currently set to 1970-00-00 in the server and is never updated.
        # Stick with the last_used field until
        # https://github.com/ssl-hep/ServiceX/issues/906 is resolved
        d_name = d.name if d.did_finder != "user" else "File list"
        is_stale = "Yes" if d.is_stale else ""
        last_used = d.last_used.strftime("%Y-%m-%dT%H:%M:%S")
        table.add_row(
            str(d.id),
            d_name,
            "%d" % d.n_files,
            "{:,}MB".format(round(d.size / 1e6)),
            d.lookup_status,
            last_used,
            is_stale,
        )
    rich.print(table)


@datasets_app.command(no_args_is_help=True)
def get(
    backend: Optional[str] = backend_cli_option,
    config_path: Optional[str] = config_file_option,
    dataset_id: int = typer.Argument(..., help="The ID of the dataset to get"),
):
    """
    Get the details of a dataset. Output as a pretty, nested table if printing to a terminal.
    Output as json if redirected.
    """
    sx = ServiceXClient(backend=backend, config_path=config_path)
    if is_terminal_output():
        table = Table(title=f"Dataset ID {dataset_id}")
        table.add_column("Paths")
    else:
        table = None

    dataset = asyncio.run(sx.get_dataset(dataset_id))

    if table:
        for file in dataset.files:
            sub_table = Table(title="")
            sub_table.add_column(f"File ID: {file.id}")
            for path in file.paths.split(","):
                sub_table.add_row(path)

            table.add_row(sub_table)
        # Set alternating row styles
        table.row_styles = ["", ""]
        rich.print(table)
    else:
        data = {
            "dataset": {
                "id": dataset.id,
                "name": dataset.name,
                "files": [
                    {"id": file.id, "paths": file.paths.split(",")}
                    for file in dataset.files
                ],
            }
        }
        rich.print_json(data=data)


@datasets_app.command(no_args_is_help=True)
def delete(
    backend: Optional[str] = backend_cli_option,
    config_path: Optional[str] = config_file_option,
    dataset_id: int = typer.Argument(..., help="The ID of the dataset to delete"),
):
    sx = ServiceXClient(backend=backend, config_path=config_path)
    result = asyncio.run(sx.delete_dataset(dataset_id))
    if result:
        typer.echo(f"Dataset {dataset_id} deleted")
    else:
        typer.echo(f"Dataset {dataset_id} not found")
        raise typer.Abort()
