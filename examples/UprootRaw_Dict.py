from servicex import query, dataset, deliver

spec = {
    "Sample": [
        {
            "Name": "UprootRaw_Dict",
            "Dataset": dataset.FileList(
                [
                    "root://eospublic.cern.ch//eos/opendata/atlas/rucio/data16_13TeV/DAOD_PHYSLITE.37019878._000001.pool.root.1",  # noqa: E501
                    "root://eospublic.cern.ch//eos/opendata/atlas/rucio/data16_13TeV/DAOD_PHYSLITE.37019878._000002.pool.root.1",  # noqa: E501
                    "root://eospublic.cern.ch//eos/opendata/atlas/rucio/data16_13TeV/DAOD_PHYSLITE.37019878._000003.pool.root.1",  # noqa: E501
                ]
            ),
            "Query": query.UprootRaw(
                [
                    {
                        "treename": "CollectionTree",
                        "filter_name": "AnalysisElectronsAuxDyn.pt",
                    }
                ]
            ),
        }
    ]
}

files = deliver(spec)
if len(files["UprootRaw_Dict"]) != 3:
    raise RuntimeError(f"Expect three files from server; got {files}")

print(f"Files: {files}")
