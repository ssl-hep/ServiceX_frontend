from servicex import Sample, ServiceXSpec, query, dataset, deliver


spec = ServiceXSpec(
    Sample=[
        Sample(
            Name="TopCP_Typed",
            Dataset=dataset.FileList(
                [
                    "root://eospublic.cern.ch//eos/opendata/atlas/rucio/data16_13TeV/DAOD_PHYSLITE.37019878._000001.pool.root.1",  # noqa: E501
                    "root://eospublic.cern.ch//eos/opendata/atlas/rucio/data16_13TeV/DAOD_PHYSLITE.37019878._000002.pool.root.1",  # noqa: E501
                    "root://eospublic.cern.ch//eos/opendata/atlas/rucio/data16_13TeV/DAOD_PHYSLITE.37019878._000003.pool.root.1",  # noqa: E501
                ]
            ),
            Query=query.TopCP(
                reco="reco.yaml",
                max_events=1000
            ),
        )
    ]
)

print(f"Files: {deliver(spec)}")
