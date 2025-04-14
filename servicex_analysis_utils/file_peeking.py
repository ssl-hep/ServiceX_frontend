# Copyright (c) 2025, IRIS-HEP
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


def run_query(input_filenames):
    import uproot
    import awkward as ak
    import json

    def is_tree(obj):
        """Helper to check if a root file item is TTree."""
        if hasattr(obj, "classname"):
            cls_attr = obj.classname
            cls_value = cls_attr() if callable(cls_attr) else cls_attr
            return "TTree" in cls_value
        elif hasattr(obj, "classnames"):
            cls_attr = obj.classnames
            cls_values = cls_attr() if callable(cls_attr) else cls_attr
            return any("TTree" in cls for cls in cls_values)
        return False

    """
    Opens a ROOT file and returns a JSON-formatted string describing the structure,
    encoded inside an ak.Array for ServiceX.
    """
    tree_dict = {}

    with uproot.open(input_filenames) as file:
        for tree_name in file.keys():
            tree_name_clean = tree_name.rstrip(";1")
            tree = file[tree_name]

            if not is_tree(tree):
                continue

            branch_dict = {}
            for branch_name, branch in tree.items():
                branch_type = str(branch.interpretation)
                branch_dict[branch_name] = branch_type

            tree_dict[tree_name_clean] = branch_dict

    # Serialize tree_dict to JSON string
    json_str = json.dumps(tree_dict)

    # Return JSON string wrapped in an awkward array
    return ak.Array([json_str])


def build_deliver_spec(datasets):
    """
    Helper to build the servicex.deliver configuration.
    Supports multiple inputs for multiple sample queries.

    Parameters:
    datasets (str, [str], or dict): Rucio DIDs to be checked by the servicex workers.
                                   If dict, custom names can be inputed

    Returns:
    spec_python (dict): The specification for the python function query containing Name, Query, Dataset, NFiles
    """
    # Servicex query using the PythonFunction backend
    query_PythonFunction = query.PythonFunction().with_uproot_function(run_query)

    # Create a dict with sample name for ServiceX query & datasetID
    dataset_dict = {}
    user_in = type(datasets)

    if user_in == str:
        dataset_dict.update({datasets: datasets})
    elif user_in == list and type(datasets[0]) is str:
        for ds in datasets:
            dataset_dict.update({ds: ds})
    elif user_in == dict:
        dataset_dict = datasets
    else:
        raise ValueError(
            f"Unsupported dataset input type: {user_in}.\nInput must be dict ('sample_name':'dataset_id'), str or list of str"
        )

    sample_list = [
        {
            "NFiles": 1,
            "Name": name,
            "Dataset": dataset.Rucio(did),
            "Query": query_PythonFunction,
        }
        for name, did in dataset_dict.items()
    ]
    spec_python = {"Sample": sample_list}

    return spec_python


def print_structure_from_str(
    deliver_dict, filter_branch="", save_to_txt=False, do_print=False
):
    """
    Re-formats the JSON structure string from ServiceX into a readable summary.

    Parameters:
      deliver_dict (dict): ServiceX deliver output (keys: sample names, values: file paths or URLs).
      filter_branch (str): If provided, only branches containing this string are included.
      save_to_txt (bool): If True, saves output to a text file instead of returning it.
      do_print (bool): If True, prints the output to the terminal and returns None.

    Returns:
      result_str (str): The formatted file structure.
    """
    import uproot
    import json

    output_lines = []
    output_lines.append(
        f"\nFile structure of all samples with branch filter '{filter_branch}':"
    )

    for sample_name, path in deliver_dict.items():
        output_lines.append(
            f"\n---------------------------\n"
            f"\U0001f4c1 Sample: {sample_name}\n"
            f"---------------------------"
        )

        with uproot.open(path[0]) as f:
            json_str = f["servicex"]["branch"].array()[0]
            structure_dict = json.loads(json_str)

        for tree_name, branches in structure_dict.items():
            output_lines.append(f"\n\U0001f333 Tree: {tree_name}")
            output_lines.append("   ├── Branches:")
            for branch_name, dtype in branches.items():
                if filter_branch and filter_branch not in branch_name:
                    continue
                output_lines.append(f"   │   ├── {branch_name} ; dtype: {dtype}")

    result_str = "\n".join(output_lines)

    if save_to_txt:
        with open("samples_structure.txt", "w") as f:
            f.write(result_str)
        return "File structure saved to 'samples_structure.txt'."
    elif do_print:
        print(result_str)
        return
    else:
        return result_str


def parse_jagged_depth_and_dtype(dtype_str):
    """
    Helper to decode the dtype str for each branch.

    Parses uproot-style interpretation strings such as:
    - "AsJagged(AsJagged(AsDtype('>f4')))"

    Returns the number of nested layers and the inner dtype.
    Used in str_to_array to reconstruct the ak.array.

    Parameters:
        dtype_str (str): The dtype part of a branch info str; from the delivered file structure.

    Returns:
        int, str: jagged_depth, base_numpy_dtype_str or None if not recognized.
    """
    depth = 0
    current = dtype_str.strip()

    # Count how many nested AsJagged(...) wrappers exist
    while current.startswith("AsJagged("):
        depth += 1
        current = current[
            len("AsJagged(") : -1
        ].strip()  # Strip outermost wrapper, up to -1 to remove )

    # Extract the base dtype string from AsDtype('<np-format>')
    if current.startswith("AsDtype('") and current.endswith("')"):
        base_dtype = current[len("AsDtype('") : -2]
        return depth, base_dtype
    else:
        return depth, None


def str_to_array(encoded_json_str):
    """
    Helper to reconstruct ak.Arrays from a JSON-formatted file-structure string.
    Returns an array mimicking TTrees and TBranches with correct field names and dtypes.

    Parameters:
        encoded_json_str (str): JSON string from run_query.

    Returns:
        ak.Array: An array containing a dictionary of trees with branch structures and dummy typed values.
    """
    reconstructed_data = {}
    structure_dict = json.loads(encoded_json_str)

    for treename, branch_dict in structure_dict.items():
        branches = {}

        for branch_name, dtype_str in branch_dict.items():
            # Get jagged depth and numpy base dtype
            depth, base_dtype_str = parse_jagged_depth_and_dtype(dtype_str)
            if base_dtype_str is None:
                branches[branch_name] = None
                continue

            try:
                np_dtype = np.dtype(base_dtype_str)
            except TypeError:
                branches[branch_name] = None
                continue

            dummy = np_dtype.type(0)
            for _ in range(depth):
                dummy = [dummy]

            branches[branch_name] = ak.Array([dummy])

        if branches:
            reconstructed_data[treename] = ak.Array([branches])

    return ak.Array(reconstructed_data).type


def get_structure(datasets, array_out=False, **kwargs):
    """
    Utility function.
    Creates and sends the ServiceX request from user inputed datasets to retrieve file stucture.
    Calls print_structure_from_str() to dump the structure in a user-friendly format

    Parameters:
      datasets (dict,str,[str]): The datasets from which to print the file structures.
                                A custom sample name per dataset can be given in a dict form: {'sample_name':'dataset_id'}
      kwargs : Arguments to be propagated to print_structure_from_str
    """
    spec_python = build_deliver_spec(datasets)

    output = deliver(spec_python)

    if array_out == True:
        all_arrays = {}
        for sample, path in output.items():
            with uproot.open(path[0]) as f:
                structure_str = f["servicex"]["branch"].array()[0]
            sample_array = str_to_array(structure_str)
            all_arrays[sample] = sample_array
        return all_arrays

    else:
        return print_structure_from_str(output, **kwargs)
