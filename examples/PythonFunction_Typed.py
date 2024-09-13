from servicex import Sample, ServiceXSpec, query, dataset, deliver


def run_query(input_filenames=None):
    import uproot  # type: ignore
    with uproot.open({input_filenames: "CollectionTree"}) as o:
        br = o.arrays("AnalysisElectronsAuxDyn.pt")
    return br


spec = ServiceXSpec(
    Sample=[
        Sample(
            Name="PythonFunction_Typed",
            Dataset=dataset.FileList(
                [
                    "root://eospublic.cern.ch//eos/opendata/atlas/rucio/data16_13TeV/DAOD_PHYSLITE.37019878._000001.pool.root.1",  # noqa: E501
                    "root://eospublic.cern.ch//eos/opendata/atlas/rucio/data16_13TeV/DAOD_PHYSLITE.37019878._000002.pool.root.1",  # noqa: E501
                    "root://eospublic.cern.ch//eos/opendata/atlas/rucio/data16_13TeV/DAOD_PHYSLITE.37019878._000003.pool.root.1",  # noqa: E501
                ]
            ),
            Query=query.PythonFunction().with_uproot_function(run_query)
        )
    ]
)

print(f"Files: {deliver(spec)}")
