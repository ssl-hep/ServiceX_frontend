from servicex import query as q, deliver, dataset


def uproot_pythonfunction_dict():

    def run_query(input_filenames=None):
        import uproot  # type: ignore
        with uproot.open({input_filenames: "reco"}) as o:
            br = o.arrays("el_pt_NOSYS")
        return br

    query1 = q.PythonFunction().with_uproot_function(run_query)

    query2 = q.FuncADL_ATLASr22()  # type: ignore
    jets_per_event = query2.Select(lambda e: e.Jets('AnalysisJets'))
    jet_info_per_event = jets_per_event.Select(
        lambda jets: {
            'pt': jets.Select(lambda j: j.pt()),
            'eta': jets.Select(lambda j: j.eta())
        }
    )

    spec = {
        'Sample': [{
            'Name': "Uproot_PythonFunction_Dict",
            'RucioDID': "user.mtost:user.mtost.singletop.p6026.Jun13",
            'Query': query1
        },
        {
            'Name': "func_adl_xAOD_simple",
            'Dataset': dataset.FileList(
                [
                    "root://eospublic.cern.ch//eos/opendata/atlas/rucio/mc20_13TeV/DAOD_PHYSLITE.37622528._000013.pool.root.1",  # noqa: E501
                ]
            ),
            'Query': jet_info_per_event
        }]
    }
    return deliver(spec, servicex_name="servicex-uc-af")

if __name__ == "__main__":
    files = uproot_pythonfunction_dict()
    assert len(files["Uproot_PythonFunction_Dict"])==27
    assert len(files["func_adl_xAOD_simple"])==1
