from servicex import query, dataset, deliver

spec = {
    "Sample": [
        {
            "Name": "FuncADL_Uproot_Dict",
            "Dataset": dataset.FileList(
                [
                    "root://eospublic.cern.ch//eos/opendata/atlas/rucio/data16_13TeV/DAOD_PHYSLITE.37019878._000001.pool.root.1",  # noqa: E501
                    "root://eospublic.cern.ch//eos/opendata/atlas/rucio/data16_13TeV/DAOD_PHYSLITE.37019878._000002.pool.root.1",  # noqa: E501
                    "root://eospublic.cern.ch//eos/opendata/atlas/rucio/data16_13TeV/DAOD_PHYSLITE.37019878._000003.pool.root.1",  # noqa: E501
                ]
            ),
            "Query": query.FuncADL_Uproot()
            .FromTree("CollectionTree")
            .Select(lambda e: {"el_pt": e["AnalysisElectronsAuxDyn.pt"]}),  # type: ignore
        }
    ]
}

print(f"Files: {deliver(spec)}")
