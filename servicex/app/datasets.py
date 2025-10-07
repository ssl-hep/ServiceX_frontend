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
from fnmatch import fnmatch
from typing import List, Optional

import rich

from servicex.app import pipeable_table, is_terminal_output
from servicex.app.cli_options import backend_cli_option, config_file_option

import typer

from servicex.servicex_client import ServiceXClient
from servicex.models import CachedDataset
from rich.table import Table

datasets_app = typer.Typer(name="datasets", no_args_is_help=True)


@datasets_app.callback()
def datasets() -> None:
    """Sub-commands for interacting with the list of looked-up datasets on the server."""
    pass


did_finder_opt = typer.Option(
    None,
    help="Filter datasets by DID finder. Some useful values are 'rucio' or 'user'",
    show_default=False,
)
show_deleted_opt = typer.Option(
    False,
    help="Show deleted datasets",
    show_default=True,
)
dataset_id_get_arg = typer.Argument(..., help="The ID of the dataset to get")
dataset_ids_delete_arg = typer.Argument(..., help="IDs of the datasets to delete")


@datasets_app.command(no_args_is_help=False)
def list(
    name_pattern: Optional[str] = typer.Argument(
        None,
        help="Filter datasets by name. Use '*' as a wildcard for any number of characters.",
    ),
    backend: Optional[str] = backend_cli_option,
    config_path: Optional[str] = config_file_option,
    did_finder: Optional[str] = did_finder_opt,
    show_deleted: Optional[bool] = show_deleted_opt,
) -> None:
    """
    List the datasets on the server.

    Use fancy formatting if printing to a terminal.
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

    datasets: List[CachedDataset] = sx.get_datasets(
        did_finder=did_finder, show_deleted=show_deleted
    )

    if name_pattern:
        # Allow substring matching when no wildcard is provided by surrounding the pattern
        # with '*' characters. Users can still provide wildcards explicitly to narrow the match.
        effective_pattern = name_pattern if "*" in name_pattern else f"*{name_pattern}*"

        def matches_pattern(dataset: CachedDataset) -> bool:
            display_name = dataset.name if dataset.did_finder != "user" else "File list"
            return any(
                fnmatch(candidate, effective_pattern)
                for candidate in {dataset.name, display_name}
            )

        datasets = [dataset for dataset in datasets if matches_pattern(dataset)]
    assert show_deleted is not None

    for d in datasets:
        # Format the CachedDataset object into a table row
        # The last_updated field is what we should be displaying, but that is
        # currently set to 1970-00-00 in the server and is never updated.
        # Stick with the last_used field until
        # https://github.com/ssl-hep/ServiceX/issues/906 is resolved
        d_name = d.name if d.did_finder != "user" else "File list"
        is_stale = "Yes" if d.is_stale else ""
        last_used = d.last_used.strftime("%Y-%m-%dT%H:%M:%S")

        # Convert byte size into a human-readable string with appropriate units
        size_in_bytes = d.size
        if size_in_bytes >= 1e12:
            size_value = size_in_bytes / 1e12
            unit = "TB"
        elif size_in_bytes >= 1e9:
            size_value = size_in_bytes / 1e9
            unit = "GB"
        else:
            size_value = size_in_bytes / 1e6
            unit = "MB"
        size_str = f"{size_value:,.2f} {unit}"

        table.add_row(
            str(d.id),
            d_name,
            f"{d.n_files}",
            size_str,
            d.lookup_status,
            last_used,
            is_stale,
        )
    rich.print(table)


@datasets_app.command(no_args_is_help=True)
def get(
    backend: Optional[str] = backend_cli_option,
    config_path: Optional[str] = config_file_option,
    dataset_id: int = dataset_id_get_arg,
):
    """
    List the files in a dataset.

    Known replicas on the GRID are listed.

    Output as a pretty, nested table if printing to a terminal.
    Output as json if redirected.
    """
    sx = ServiceXClient(backend=backend, config_path=config_path)
    if is_terminal_output():
        table = Table(title=f"Dataset ID {dataset_id}")
        table.add_column("Paths")
    else:
        table = None

    dataset = sx.get_dataset(dataset_id)

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
    dataset_ids: List[int] = dataset_ids_delete_arg,
):
    """
    Remove a dataset from the ServiceX.

    The next time it is queried, it will have to be looked up again. This command should only be
    used when debugging.
    """
    sx = ServiceXClient(backend=backend, config_path=config_path)
    any_missing: bool = False  # Track if any dataset ID is not found
    for dataset_id in dataset_ids:
        result = sx.delete_dataset(dataset_id)
        if result:
            typer.echo(f"Dataset {dataset_id} deleted")
        else:
            typer.echo(f"Dataset {dataset_id} not found")
            any_missing = True
    if any_missing:
        raise typer.Abort()
