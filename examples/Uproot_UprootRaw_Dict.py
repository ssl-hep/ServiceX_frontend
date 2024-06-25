from servicex import UprootRaw, deliver


def uproot_uproot_raw_dict():

    query = UprootRaw([{"treename": "reco", "filter_name": "el_pt_NOSYS"}])

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
    uproot_uproot_raw_dict()
