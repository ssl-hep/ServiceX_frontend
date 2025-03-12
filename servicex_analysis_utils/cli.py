import argparse
import sys
import json
import os
from .file_peeking import get_structure 

def run_from_command():
    parser = argparse.ArgumentParser(description="CLI tool for retrieving ROOT file structures.")

    parser.add_argument("dataset", nargs='+', help="Input datasets (Rucio DID) or a JSON file containing datasets in a dict.")  
    parser.add_argument("--filter-branch", default="", help="Only display branches containing this string.")
    parser.add_argument("--save-to-txt", action="store_true", help="Save output to a text file instead of printing.")

    args = parser.parse_args()

    if len(args.dataset) == 1 and args.dataset[0].endswith(".json"):
        dataset_file = args.dataset[0]
        
        if not os.path.isfile(dataset_file):
            print(f"\033[91mError: JSON file '{dataset_file}' not found.\033[0m", file=sys.stderr)
            sys.exit(1)

        try:
            with open(dataset_file, "r") as f:
                dataset = json.load(f) 
                
                if not isinstance(dataset, dict):
                    print(f"\033[91mError: The JSON file must contain a dictionary.\033[0m", file=sys.stderr)
                    sys.exit(1)
            
        except json.JSONDecodeError:
            print(f"\033[91mError: '{dataset_file}' is not a valid JSON file.\033[0m", file=sys.stderr)
            sys.exit(1)

    else:
        # If dataset is provided directly in CLI, use it as a list
        dataset = args.dataset

    result = get_structure(dataset, filter_branch=args.filter_branch, save_to_txt=args.save_to_txt, do_print=False)

    if not args.save_to_txt:
        print(result)
    else:
        print("Saved to samples_structure.txt")
