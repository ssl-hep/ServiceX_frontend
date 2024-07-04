from servicex.func_adl.func_adl_dataset import FuncADLQuery_Uproot
from servicex.dataset_identifier import FileListDataset


def test_set_from_tree():
    did = FileListDataset("/foo/bar/baz.root")
    query = FuncADLQuery_Uproot()
    query = query.FromTree("TREE_NAME")

    assert "TREE_NAME" in query.generate_selection_string()
