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

from servicex.app.cli_options import url_cli_option, backend_cli_option

import typer

from servicex.servicex_client import ServiceXClient
from rich.table import Table

datasets_app = typer.Typer(name="datasets", no_args_is_help=True)


@datasets_app.command(no_args_is_help=True)
def list(
        url: Optional[str] = url_cli_option,
        backend: Optional[str] = backend_cli_option,
        did_finder: Optional[str] = typer.Option(
            None,
            help="Filter datasets by DID finder. Some useful values are 'rucio' or 'user'",
            show_default=False,
        ),
):
    """
    List the datasets.
    """
    sx = ServiceXClient(url=url, backend=backend)
    table = Table(title="ServiceX Datasets")
    table.add_column("ID")
    table.add_column("Name")
    table.add_column("Files")
    table.add_column("Size")
    table.add_column("Status")
    table.add_column("Created")
    datasets = asyncio.run(sx.get_datasets(did_finder=did_finder))
    for d in datasets:
        # Format the CachedDataset object into a table row
        # The last_updated field is what we should be displaying, but that is
        # currently set to 1970-00-00 in the server and is never updated.
        # Stick with the last_used field until
        # https://github.com/ssl-hep/ServiceX/issues/906 is resolved
        table.add_row(
            str(d.id),
            d.name if d.did_finder != "user" else "File list",
            "%d" % d.n_files,
            "{:,}MB".format(round(d.size / 1e6)),
            d.lookup_status,
            d.last_used.strftime('%Y-%m-%dT%H:%M:%S'),
        )
    rich.print(table)


@datasets_app.command(no_args_is_help=True)
def get(
        url: Optional[str] = url_cli_option,
        backend: Optional[str] = backend_cli_option,
        dataset_id: int = typer.Argument(..., help="The ID of the dataset to get")
):
    sx = ServiceXClient(url=url, backend=backend)
    table = Table(title=f"Dataset ID {dataset_id}")
    table.add_column("Paths")
    dataset = asyncio.run(sx.get_dataset(dataset_id))
    for file in dataset.files:
        sub_table = Table(title="")
        sub_table.add_column(f"File ID: {file.id}")
        for path in file.paths.split(','):
            sub_table.add_row(path)

        table.add_row(
            sub_table
        )
    # Set alternating row styles
    table.row_styles = ["", ""]
    rich.print(table)
