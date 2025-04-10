import argparse
import sys
import json
import os
import logging
from .file_peeking import get_structure


def run_from_command():
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

    if len(args.dataset) == 1 and args.dataset[0].endswith(".json"):
        dataset_file = args.dataset[0]

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
        # If dataset is provided directly in CLI, use it as a list
        dataset = args.dataset

    result = get_structure(dataset, filter_branch=args.filter_branch, do_print=False)

    print(result)
