from servicex import PythonQuery, deliver


def uproot_pythonfunction_dict():

    def run_query(input_filenames=None):
        import uproot  # type: ignore
        with uproot.open({input_filenames: "reco"}) as o:
            br = o.arrays("el_pt_NOSYS")
        return br

    query = PythonQuery().with_uproot_function(run_query)

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


if __name__ == "main":
    uproot_pythonfunction_dict()
