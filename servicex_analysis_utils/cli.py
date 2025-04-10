import argparse
import sys
import json
import os
import logging
from .file_peeking import get_structure


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


def run_from_command():
    """
    Calls the get_structure function and sends results to stdout.
    To run on command line: servicex-get-structure -dataset --fileter-branch
    """
    parser = argparse.ArgumentParser(
        description="CLI tool for retrieving ROOT file structures."
    )

    parser.add_argument(
        "dataset",
        nargs="+",
        help="Input datasets (Rucio DID) or a JSON file containing datasets in a dict.",
    )
    parser.add_argument(
        "--filter-branch",
        default="",
        help="Only display branches containing this string.",
    )

    args = parser.parse_args()

    ds_format = make_dataset_list(args.dataset)

    result = get_structure(ds_format, filter_branch=args.filter_branch, do_print=False)

    print(result)
