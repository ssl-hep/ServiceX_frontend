from servicex.func_adl.func_adl_dataset import FuncADLQuery_Uproot


def test_set_from_tree():
    query = FuncADLQuery_Uproot()
    query = query.FromTree("TREE_NAME")

    assert "TREE_NAME" in query.generate_selection_string()
