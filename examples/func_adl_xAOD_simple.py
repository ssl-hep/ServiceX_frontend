import logging
from servicex import query as q, deliver, dataset


def func_adl_xaod_simple():
    query = q.FuncADL_ATLASr22()  # type: ignore
    jets_per_event = query.Select(lambda e: e.Jets())
    jet_info_per_event = jets_per_event.Select(
        lambda e: e.Select(lambda j: {'pt': j.pt(), 'eta': j.eta()})
    )

    spec = {
        'Sample': [{
            'Name': "func_adl_xAOD_simple",
            'Dataset': dataset.FileList(
                [
                    "root://eospublic.cern.ch//eos/opendata/atlas/rucio/mc20_13TeV/DAOD_PHYSLITE.37622528._000013.pool.root.1",  # noqa: E501
                ]
            ),
            'Query': jet_info_per_event
        }]
    }
    files = deliver(spec, servicex_name="servicex-uc-af")
    assert files is not None, "No files returned from deliver! Internal error"
    return files


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info("Hi there")
    files = func_adl_xaod_simple()
    # assert len(files['func_adl_xAOD_simple']) == 1
