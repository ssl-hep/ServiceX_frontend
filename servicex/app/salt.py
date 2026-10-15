# Copyright (c) 2026, IRIS-HEP
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

import typer
from rich import get_console
from rich.table import Table

from servicex.app.cli_options import backend_cli_option, config_file_option


def get_salt_data_command(
    salt_config: str = typer.Argument(
        ..., help="Salt config YAML: tree, cuts and objects (columns -> branches)."
    ),
    samples: str = typer.Argument(
        ...,
        help="Samples YAML: {name: {dataset: <Rucio DID> | files: [...], label: int}}.",
    ),
    output: Optional[str] = typer.Option(
        None, "-o", "--output", help="Merged Salt file (default: config output_file)."
    ),
    max_events: Optional[int] = typer.Option(
        None, "--max-events", help="Keep the first N events of each sample."
    ),
    urls: bool = typer.Option(
        False,
        "--urls",
        help="Get URLs from ServiceX and download only the files the merge needs.",
    ),
    ignore_cache: bool = typer.Option(
        False, "--ignore-cache", help="Ignore local cache and always submit to ServiceX"
    ),
    backend: Optional[str] = backend_cli_option,
    config_path: Optional[str] = config_file_option,
):
    """Build a Salt training file (.h5) from ntuples with ServiceX."""
    from servicex.tools.salt_data import get_salt_data

    console = get_console()
    try:
        summary = get_salt_data(
            salt_config,
            samples,
            output_file=output,
            max_events=max_events,
            delivery="URLs" if urls else "LocalCache",
            servicex_name=backend,
            config_path=config_path,
            ignore_local_cache=ignore_cache,
        )
    except (ValueError, RuntimeError, OSError) as err:
        console.print(f"[red]Error:[/red] {err}")
        raise typer.Exit(code=1)

    table = Table(
        title="Salt training file", show_header=True, header_style="bold magenta"
    )
    table.add_column("Sample", style="cyan")
    table.add_column("Events", justify="right", style="green")
    for name, n_events in summary["events_per_sample"].items():
        table.add_row(name, f"{n_events:,}")
    console.print(table)

    widths = ", ".join(f"{obj}: {w}" for obj, w in summary["widths"].items())
    console.print(
        f"[bold green]Written {summary['output_file']}[/bold green]: "
        f"{summary['n_events']:,} events ({widths})"
    )
    console.print(
        f"ServiceX {summary['servicex_seconds']:.1f} s, merge {summary['merge_seconds']:.1f} s"
    )
    if summary["failed_samples"]:
        console.print(
            "[red]Samples without results:[/red] "
            + ", ".join(summary["failed_samples"])
        )
        raise typer.Exit(code=1)
