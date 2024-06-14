# flake8: noqa
from servicex.databinder import DataBinder
from servicex.python_dataset import PythonQuery


def run_query(input_filenames=None):
    import uproot  # type: ignore
    with uproot.open({input_filenames:"mini"}) as o: 
        br = o.arrays("lep_pt")
    return br


config = {
    "General":
    {
        "ServiceX": "servicex-uc-af",
        "Delivery": "LocalCache"
    },
    "Sample":
    [
        {
            "Name": "ttH",
            "XRootDFiles": "root://eospublic.cern.ch//eos/opendata/atlas/OutreachDatasets/2020-01-22/4lep/MC/mc_345060.ggH125_ZZ4lep.4lep.root",
            "Query": PythonQuery().with_uproot_function(run_query)
        }
    ]
}

sx = DataBinder(config=config)
o = sx.deliver()

print(o)
