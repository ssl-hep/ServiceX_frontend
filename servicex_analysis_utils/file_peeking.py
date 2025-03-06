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

def run_query(input_filenames=None):
    import uproot
    import awkward as ak
    """
    Helper. Open a file and return one array containing a single string that describes the DataSet root file structure.
    Sent to ServiceX python transformers.

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


def print_structure_from_str(deliver_dict, filter_branch="", save_to_txt=False ):
    """
    Helper. Takes the structure strings for all samples from servicex.deliver output 
    and prints them in a friendly formatted view.
    
    The expected structure string format is:
    
      Tree: TreeName1; TBranch: Branchname1 ; dtype: BranchType1, TBranch: Branchname2 ; dtype: BranchType2, ...
      Tree: TreeName2; TBranch: Branchname1 ; dtype: BranchType1, ...
    
    Parameters:
      deliver_dict (dict): The return dictionary of servicex.deliver
                          (keys are sample names, values are file paths or URLs)
      filter_branch (str): Optional. Only Branch names containing it are printed.
      save_to_txt (bool): Optional. Select if file structure is printed or dumped to .txt
    """
    print(f"File structure of all samples with branch filter {filter_branch}:")

    for sample_name, path in deliver_dict.items():
        #Sample name with icon and bands
        print(
            f"\n---------------------------\n"
            f"\U0001F4C1 Sample: {sample_name}\n"
            f"---------------------------"
        )

        with uproot.open(path[0]) as f:
            #Expected position of structure_str from servicex.deliver
            structure_str=f["servicex"]["branch"].array()[0]
        
        # Split at each \n each new line represents one tree.
        tree_lines = structure_str.split("\n")
        
        for line in tree_lines:
            if not line.strip():
                continue  # Skip empty lines
            
            # First part before ';' is the tree header.
            parts = line.split(";", 1)
            tree_header = parts[0] 
            print(f"\n\U0001F333 {tree_header}")  # Print tree header with icon
            
            # Check for branches
            if len(parts) > 1:
                # branch info separated by ','
                branch_info_str = parts[1]
                branch_infos = branch_info_str.split(",")
                print("   ├── Branches:")
                for b in branch_infos:
                    branch_line = b.strip()
                    if filter_branch not in branch_line:
                        continue
                    # Only print lines that start with "TBranch:"
                    if branch_line.startswith("TBranch:"):
                        print(f"   │   ├── {branch_line[8:]}")


def get_structure(dataset_dict, **kwargs):
    """
    Utility function. 
    Creates and sends the ServiceX request from user input.
    Calls print_structure_from_str()

    Parameters:
      dataset_dict (dict): The datasets to print the structures from, with the associated sample name for readability 
                            note - should add default sample names and option to add just did or list of dids 
      kwargs : Arguments to be propagated to print_structure_from_str 
    """
    #Servicex query using the PythonFunction backend
    query_PythonFunction = servicex.query.PythonFunction().with_uproot_function(run_query)
    sample_list=[]
    
    for name, did in dataset_dict.items():
        tmp_dict={
            "NFiles":1,
            "Name": name,
            "Dataset": servicex.dataset.Rucio(did),
            "Query": query_PythonFunction,
        }
        sample_list.append(tmp_dict)
    
    spec_python = {
    "Sample": sample_list
    }

    output=servicex.deliver(spec_python)

    print_structure_from_str(output,**kwargs)
    
    