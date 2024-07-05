from servicex import UprootRaw, deliver
import sys


def uproot_uproot_raw_dict():

    query = UprootRaw([{"treename": "reco", "filter_name": "el_pt_NOSYS"}])

    spec = {
        'Sample': [{
            'Name': "Uproot_PythonFunction_Dict",
            'RucioDID': "user.mtost:user.mtost.singletop.p6026.Jun13",
            'Query': query
        }]
    }
    return deliver(spec)


if __name__ == "__main__":
    files = uproot_uproot_raw_dict()
    sys.exit(0 if len(files['Uproot_PythonFunction_Dict']) == 27 else 1)
