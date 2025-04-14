import sys
import json
import os
import logging
from .file_peeking import get_structure
import typer
from typing import List

app = typer.Typer()


def make_dataset_list(dataset_arg):
    """
    Helper to handle the user input daset argument.
    Loads to dict if input is .json else returns default input
    Output is given to get_structure()

    Parameters:
    dataset_arg (str, [str]): Single DS identifier, list of multiple identifiers or path/to/.json containig identifiers and sample names.

    Returns:
    dataset (str, [str], dict): dictionary loaded from the json
    """
    if len(dataset_arg) == 1 and dataset_arg[0].endswith(".json"):
        dataset_file = dataset_arg[0]

        if not os.path.isfile(dataset_file):
            logging.error(f"Error: JSON file '{dataset_file}' not found.")
            sys.exit(1)

        try:
            with open(dataset_file, "r") as f:
                dataset = json.load(f)

                if not isinstance(dataset, dict):
                    logging.error(f"Error: The JSON file must contain a dictionary.")
                    sys.exit(1)

        except json.JSONDecodeError:
            logging.error(
                f"Error: '{dataset_file}' is not a valid JSON file.", exc_info=True
            )
            sys.exit(1)

    else:
        # If DS is provided in CLI instead of json, use it as a list (default)
        dataset = dataset_arg

    return dataset


@app.command()
def run_from_command(
    dataset: List[str] = typer.Argument(
        ...,
        help="Input datasets (Rucio DID) or path to JSON file containing datasets in a dict.",
    ),
    filter_branch: str = typer.Option(
        "", "--filter-branch", help="Only display branches containing this string."
    ),
):
    """
    Calls the get_structure function and sends results to stdout.
    To run on command line: servicex-get-structure -dataset --filter-branch
    """
    ds_format = make_dataset_list(dataset)
    result = get_structure(ds_format, filter_branch=filter_branch, do_print=False)

    print(result)


if __name__ == "__main__":
    app()
