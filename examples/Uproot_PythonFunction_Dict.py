from servicex import PythonFunction, deliver


def uproot_pythonfunction_dict():

    def run_query(input_filenames=None):
        import uproot  # type: ignore
        with uproot.open({input_filenames: "reco"}) as o:
            br = o.arrays("el_pt_NOSYS")
        return br

    query = PythonFunction().with_uproot_function(run_query)

    spec = {
        'General': {
            'ServiceX': "servicex-uc-af"
        },
        'Sample': [{
            'Name': "Uproot_PythonFunction_Dict",
            'RucioDID': "user.mtost:user.mtost.singletop.p6026.Jun13",
            'Query': query
        }]
    }
    return deliver(spec)


if __name__ == "__main__":
    files = uproot_pythonfunction_dict()
    assert len(files['Uproot_PythonFunction_Dict']) == 27
