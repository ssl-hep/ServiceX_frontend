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

from servicex import deliver, query, dataset
import uproot
import numpy as np
import awkward as ak
import json
import logging
from servicex.dataset_identifier import DataSetIdentifier


def run_query(input_filename):
    import uproot
    import awkward as ak
    import logging

    logging.basicConfig(level=logging.INFO)

    trees = {}

    with uproot.open(input_filename) as file:
        for tree_name, tree in file.items():
            tree_name = tree_name.rstrip(";1")
            classname = getattr(tree, "classname", type(tree).__name__)
            print(classname, "\n\n\n")

            try:
                # Build one Awkward array per tree
                arrays = tree.arrays(library="ak", entry_stop=1)
                # add classname to array
                arrays["_classname"] = ak.Array([classname])
                trees[tree_name] = arrays
                logging.info(f"Successfully processed tree {tree_name}: {arrays.type}")
            except Exception as e:
                logging.info(f"No arrays in {tree_name} (type={classname}): {e}")
                trees[tree_name] = ak.Array([{"_classname": classname}])

    return trees


def _build_deliver_spec(datasets):
    """
    Helper to build the servicex.deliver configuration.
    Supports multiple inputs for multiple sample queries.

    Parameters:
    datasets (str, [str], dict, DataSetIdentifier): Rucio DIDs (str) or DataSetIdentifier object.
                                If dict, custom names can be inputed for each dataset

    Returns:
    spec_python (dict): The spec for the python function query (Name, Query, Dataset, NFiles)
    """
    # Servicex query using the PythonFunction backend
    query_PythonFunction = query.PythonFunction().with_uproot_function(run_query)

    # Create a dict with sample name for ServiceX query & datasetID
    dataset_dict = {}
    user_in = type(datasets)

    # Rucio DID as str
    if user_in == str:
        dataset_dict.update({datasets: datasets})  # Use dataset ID as sample name
    elif user_in == list and type(datasets[0]) is str:
        for ds in datasets:
            dataset_dict.update({ds: ds})
    elif user_in == dict:  # Custom sample names
        dataset_dict = datasets
    # Single DataSetIdentifier object
    elif isinstance(datasets, DataSetIdentifier):
        dataset_dict.update({"Dataset": datasets})
    else:
        raise ValueError(
            f"Unsupported dataset input type: {user_in}.\n"
            "Input must be str or list of str of Rucio DIDs, "
            "a DataSetIdentifier object or a dict ('sample_name':'dataset_id')"
        )

    sample_list = [
        {
            "NFiles": 1,
            "Name": name,
            "Dataset": dataset.Rucio(did) if isinstance(did, str) else did,
            "Query": query_PythonFunction,
        }
        for name, did in dataset_dict.items()
    ]
    spec_python = {"Sample": sample_list, "General": {"OutputFormat": "root-rntuple"}}

    return spec_python


def _show_structure(deliver_dict, filter="", save_to_txt=False, do_print=False):
    """
    Formats a string to present the file structure from ServiceX into a readable summary.

    Parameters:
      deliver_dict (dict): ServiceX deliver output (keys: sample names, values: paths or URLs).
      filterh (str): If provided, only columns containing this string are included.
      save_to_txt (bool): If True, saves output to a text file instead of returning it.
      do_print (bool): If True, prints the output to the terminal and returns None.

    Returns:
      result_str (str): The formatted file structure.
    """

    output_lines = []
    _long = "-------" * 10
    _short = "------" * 6

    for sample_name, path in deliver_dict.items():
        output_lines = []
        output_lines.append(
            f"\n{_long}\n" f"\U0001f4c1 Sample: {sample_name}\n" f"{_long}"
        )
        with uproot.open(path[0]) as f:
            output_lines.append("\nFile Metadata \u2139\ufe0f :\n")
            output_lines.append("Coming soon...")
            output_lines.append(f"\n{_short}")

            output_lines.append(f"\nFile structure with column filter '{filter}':")

            for key in f.keys():
                tree = f[key]
                arrays = tree.arrays(library="ak", entry_stop=1)
                output_lines.append(
                    f"\n\U0001f333  {str(key).strip(';1')}: {arrays['_classname'][0]}"
                )
                if arrays.fields == ["_classname"]:
                    continue
                else:
                    output_lines.append("   ├── Columns:")
                    for field in arrays.fields:
                        if field == "_classname" or filter.strip() not in field:
                            continue  # skip the _classname field

                        output_lines.append(
                            f"   │   ├── {field}:  {str(ak.type(arrays[field]))[4:]}"
                        )

    result_str = "\n".join(output_lines).encode("utf-8").decode("utf-8")

    if save_to_txt:
        with open("samples_structure.txt", "w") as f:
            f.write(result_str)
        print("File structure saved to 'samples_structure.txt'.")
    elif do_print:
        print(result_str)
        return
    else:
        return result_str


def _get_arrays(deliver_dict):
    """
    Outputs arrays from the ServiceX deliver output in a dictionary format.
    Only consideres TTrees and RNTuples, and ignores other objects in the ROOT file.

    Parameters:
      deliver_dict (dict): ServiceX deliver output (keys: sample names, values: paths or URLs).

    Returns:
      sample_arrays (dict): A dictionary containing the arrays from the ServiceX deliver output.
    """
    multi_sample = len(deliver_dict.keys()) > 1
    arrays_dict = {}
    sample_arrays = {}
    for sample_name, path in deliver_dict.items():
        with uproot.open(path[0]) as f:
            for key in f.keys():
                tree = f[key]
                arrays = tree.arrays(library="ak", entry_stop=1)
                if arrays.fields == ["_classname"]:
                    continue
                else:
                    arrays_dict[key.strip(";1")] = arrays
            if multi_sample:
                sample_arrays[sample_name] = arrays_dict
            else:
                sample_arrays = arrays_dict
    return sample_arrays


def get_structure(datasets, array_out=False, **kwargs):
    """
    Utility function.
    Creates and sends the ServiceX request from user inputed datasets to retrieve file stucture.
    Calls print_structure_from_str() to dump the structure in a user-friendly format

    Parameters:
      datasets (dict,str,[str]): The datasets from which to print the file structures.
                                A custom sample name per dataset can be given in a dict form:
                                {'sample_name':'dataset_id'}
      kwargs : Arguments to be propagated to _show_structure (filter, save_to_txt, do_print)
    """
    spec_python = _build_deliver_spec(datasets)

    output = deliver(spec_python)

    if array_out:
        return _get_arrays(output)
    else:
        return _show_structure(output, **kwargs)
