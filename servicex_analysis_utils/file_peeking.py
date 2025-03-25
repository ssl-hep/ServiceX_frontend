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

import servicex
import uproot
import numpy as np
import awkward as ak

def run_query(input_filenames=None):
    import uproot
    import awkward as ak
    """
    Sent to ServiceX python transformers.
    Open a file and return one array containing a single string that describes the DataSet root file structure.
    
    The string will be formatted like:
    "Tree: TreeName1; TBranch: Branchname1 ; dtype: BranchType1, TBranch: Branchname2 ; dtype: BranchType2, ...
     Tree: TreeName2; TBranch: Branchname1 ; dtype: BranchType1, ..."
    """
    def is_tree(obj):
        """
        Helper to check if a root file item is TTree. Different object types use .classname or .classnames
        """
        # Check for 'classname'
        if hasattr(obj, "classname"):
            cls_attr = obj.classname
            # Call if it's callable
            cls_value = cls_attr() if callable(cls_attr) else cls_attr
            return "TTree" in cls_value
        # Check for 'classnames'
        elif hasattr(obj, "classnames"):
            cls_attr = obj.classnames
            cls_values = cls_attr() if callable(cls_attr) else cls_attr
            return any("TTree" in cls for cls in cls_values)
        return False
        
    trees_info = []  #  list of str info for each tree

    with uproot.open(input_filenames) as file:
        for tree_name in file.keys():
            # Remove uproot tree sufix
            tree_name_clean = tree_name.rstrip(";1")
            tree = file[tree_name]

            # Only TTrees
            if not is_tree(tree):
                continue

            # Gather branch info
            branch_info_list = []
            for branch_name, branch in tree.items():
                # Using uproot type interpretor
                branch_type = str(branch.interpretation)
                branch_info_list.append(f"TBranch: {branch_name} ; dtype: {branch_type}")

            # Join branch info & separate by ,
            tree_info = f"Tree: {tree_name_clean}; " + ", ".join(branch_info_list)
            trees_info.append(tree_info)

    # Join all trees & separate by \n
    final_str = "\n".join(trees_info)
    
    # Return str in an array
    return ak.Array([final_str])

def build_deliver_spec(dataset):
    """
    Helper to build the servicex.deliver configuration.
    Supports multiple inputs for multiple sample queries.

    Parameters:
    dataset (str, [str], or dict): Rucio DIDs to be checked by the servicex workers. 
                                   If dict, custom names can be inputed

    Returns:
    spec_python (dict): The specification for the python function query containing Name, Query, Dataset, NFiles
    """
    #Servicex query using the PythonFunction backend
    query_PythonFunction = servicex.query.PythonFunction().with_uproot_function(run_query)
    
    #Create a dict with sample name for ServiceX query & datasetID
    dataset_dict={}
    user_in=type(dataset)
    
    if user_in == str:
        dataset_dict.update({"Sample":dataset})
    elif user_in == list and type(dataset[0]) is str:
        for i in range(len(dataset)):
            name="Sample"+str(i+1) #write number for humans
            dataset_dict.update({name:dataset[i]})
    elif user_in == dict:
        dataset_dict=dataset
    else:
        raise ValueError(f"Unsupported dataset input type: {user_in}.\nInput must be dict ('sample_name':'dataset_id'), str or list of str")
    
    sample_list = [
        {
            "NFiles": 1,
            "Name": name,
            "Dataset": servicex.dataset.Rucio(did),
            "Query": query_PythonFunction,
        }
        for name, did in dataset_dict.items()
    ]
    spec_python = {"Sample": sample_list}

    return spec_python   

def print_structure_from_str(deliver_dict, filter_branch="", save_to_txt=False, do_print=False):
    """
    Re-formats the deliver-retrieve str structure for readability with a filter for branch selection. 
    The string can be printed, written out or returned

    Parameters:
      deliver_dict (dict): ServiceX deliver output (keys: sample names, values: file paths or URLs).
      filter_branch (str): If provided, only branches containing this string are included in the output.
      save_to_txt (bool): If True, saves output to a text file instead of returning it.
      do_print (bool): If True, dumps the ouput to the terminal and returns None. Not called if save_to_txt is True

    Returns:
      result_str (str): The formatted file structure.
    """
    output_lines = []
    output_lines.append(f"\nFile structure of all samples with branch filter '{filter_branch}':")

    for sample_name, path in deliver_dict.items():
        output_lines.append(
            f"\n---------------------------\n"
            f"\U0001F4C1 Sample: {sample_name}\n"
            f"---------------------------"
        )

        with uproot.open(path[0]) as f:
            structure_str = f["servicex"]["branch"].array()[0]

        # Trees separated by \n
        tree_lines = structure_str.split("\n")
        for line in tree_lines:
            if not line.strip():
                continue  # Skip empty lines
            
            #Separate Tree header from branches
            parts = line.split(";", 1)
            tree_header = parts[0]
            output_lines.append(f"\n\U0001F333 {tree_header}")

            if len(parts) > 1:
                branch_infos = parts[1].split(",") # Branches separated by ,
                output_lines.append("   ├── Branches:")
                for b in branch_infos:
                    branch_line = b.strip() 
                    # Removes lines w/o filter str
                    if filter_branch not in branch_line:
                        continue
                    if branch_line.startswith("TBranch:"):
                        output_lines.append(f"   │   ├── {branch_line[8:]}")
    
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
        current = current[len("AsJagged("):-1].strip()  # Strip outermost wrapper, up to -1 to remove )

    # Extract the base dtype string from AsDtype('<np-format>')
    if current.startswith("AsDtype('") and current.endswith("')"):
        base_dtype = current[len("AsDtype('"):-2]
        return depth, base_dtype
    else:
        return depth, None

def str_to_array(encoded_str):
    """
    Helper to reconstruct ak.Arrays from an encoded file-structure string.
    Retruned array mimicks TTrees, TBranches with correct field names and dtypes. 

    Parameters:
        encoded_str (str): The encoded string from run_query.

    Returns:
        reconstructed_data (ak.Array): Contains trees and branches with typed dumy values.
    """

    #Separate trees
    tree_sections = encoded_str.strip().split("\n")
    reconstructed_data = {}

    for tree_section in tree_sections:
        tree_section = tree_section.strip()
        if not tree_section:
            continue #skip empty lines

        parts = tree_section.split(";", 1) #Tree and branches separated by ; 
        tree_header = parts[0].strip()

        # Extract tree name
        treename = tree_header[len("Tree: "):]
        branches = {}

        if len(parts) > 1:
            branches_str = parts[1].strip()
            branch_infos = branches_str.split(",") #Branches separated by

            for branch in branch_infos:
                branch = branch.strip()

                if " ; dtype: " in branch:  # line with branch info
                    name_str, dtype_str = branch.split(" ; dtype: ", 1)
                    # Extract name
                    branch_name = name_str[len("TBranch: "):].strip()
                    dtype_str = dtype_str.strip()

                    # Get nesting depth and base dtype from interpretation string
                    depth, base_dtype_str = parse_jagged_depth_and_dtype(dtype_str)
                    if base_dtype_str is None:
                        branches[branch_name] = None
                        continue

                    try:
                        np_dtype = np.dtype(base_dtype_str)
                    except TypeError:
                        branches[branch_name] = None
                        continue

                    dummy = np_dtype.type(0) # Typed placeholder value

                    # Simulate jagged structure by nesting the value in lists
                    for _ in range(depth):
                        dummy = [dummy] 

                    # Wrap dummy in a length-1 ak.Array
                    branches[branch_name] = ak.Array([dummy])

        if branches:
            # Each tree becomes a record array with 1 entry (dict of branch arrays)
            reconstructed_data[treename] = ak.Array([branches])

    return ak.Array(reconstructed_data).type

def get_structure(dataset, array_out=False, **kwargs):
    """
    Utility function. 
    Creates and sends the ServiceX request from user inputed datasets to retrieve file stucture.
    Calls print_structure_from_str() to get the structure in a user-friendly format

    Parameters:
      dataset (dict,str,[str]): The datasets from which to print the file structures.
                                A custom sample name per dataset can be given in a dict form: {'sample_name':'dataset_id'}
      kwargs : Arguments to be propagated to print_structure_from_str 
    """
    spec_python=build_deliver_spec(dataset)

    output=servicex.deliver(spec_python)

    if array_out==True:
        all_arrays={}
        for sample, path in output.items():
            with uproot.open(path[0]) as f:
                structure_str = f["servicex"]["branch"].array()[0] 
            sample_array=str_to_array(structure_str)
            all_arrays[sample]=sample_array 
        return all_arrays
    
    else:
        return print_structure_from_str(output, **kwargs)