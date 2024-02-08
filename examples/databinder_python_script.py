# flake8: noqa
from servicex.databinder import DataBinder


def run_query(input_filenames=None):
    import uproot
    with uproot.open({input_filenames:"mini"}) as o: 
        br = o.arrays("lep_pt")
    return br


config = {
    "General":
    {
        "ServiceX": "uc-af",
        "Codegen": "python",
        "Delivery": "LocalCache"
    },
    "Sample":
    [
        {
            "Name": "ttH",
            "XRootDFiles": "root://eospublic.cern.ch//eos/opendata/atlas/OutreachDatasets/2020-01-22/4lep/MC/mc_345060.ggH125_ZZ4lep.4lep.root",
            "Function": run_query
        }
    ]
}

sx = DataBinder(config=config)
o = sx.deliver()

print(o)
