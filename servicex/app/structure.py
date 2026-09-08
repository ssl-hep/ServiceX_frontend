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

import json
import logging
import os
from typing import List

import typer

from servicex.tools.file_peeking import get_structure

structure_app = typer.Typer(
    name="get-structure",
    invoke_without_command=True,
)


def make_dataset_list(dataset_arg: List[str]):
    if len(dataset_arg) == 1 and dataset_arg[0].endswith(".json"):
        dataset_file = dataset_arg[0]

        if not os.path.isfile(dataset_file):
            logging.error(f"Error: JSON file '{dataset_file}' not found.")
            raise typer.Exit(code=1)

        try:
            with open(dataset_file, "r") as f:
                dataset = json.load(f)

            if not isinstance(dataset, dict):
                logging.error("Error: The JSON file must contain a dictionary.")
                raise typer.Exit(code=1)

            return dataset

        except json.JSONDecodeError:
            logging.error(
                f"Error: '{dataset_file}' is not a valid JSON file.",
                exc_info=True,
            )
            raise typer.Exit(code=1)

    return dataset_arg


@structure_app.callback()
def get_structure_command(
    dataset: List[str] = typer.Argument(
        ...,
        help="Input datasets (Rucio DID) or path to JSON file containing datasets in a dict.",
    ),
    filter_branch: str = typer.Option(
        "",
        "--filter-branch",
        help="Only display branches containing this string.",
    ),
):
    """Display the structure of rucio datasets."""

    ds_format = make_dataset_list(dataset)

    result = get_structure(
        ds_format,
        filter=filter_branch,
        do_print=False,
    )

    print(result)
