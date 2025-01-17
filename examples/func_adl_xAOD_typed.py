from servicex import deliver, dataset
from func_adl_servicex_xaodr22 import FuncADLQueryPHYSLITE, cpp_float


def func_adl_xaod_typed():
    query = FuncADLQueryPHYSLITE()  # type: ignore
    jets_per_event = query.Select(lambda e: e.Jets("AnalysisJets"))
    jet_info_per_event = jets_per_event.Select(
        lambda jets: {
            "pt": jets.Select(lambda j: j.pt()),
            "eta": jets.Select(lambda j: j.eta()),
            "emf": jets.Select(lambda j: j.getAttribute[cpp_float]("EMFrac")),  # type: ignore
        }
    )

    spec = {
        "Sample": [
            {
                "Name": "func_adl_xAOD_simple",
                "Dataset": dataset.FileList(
                    [
                        "root://eospublic.cern.ch//eos/opendata/atlas/rucio/mc20_13TeV/DAOD_PHYSLITE.37622528._000013.pool.root.1",  # noqa: E501
                    ]
                ),
                "Query": jet_info_per_event,
                "Codegen": "atlasr22",
            }
        ]
    }
    files = deliver(spec, servicex_name="servicex-uc-af")
    assert files is not None, "No files returned from deliver! Internal error"
    return files


if __name__ == "__main__":
    files = func_adl_xaod_typed()
    assert len(files["func_adl_xAOD_simple"]) == 1
