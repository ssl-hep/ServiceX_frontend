from servicex import FuncADLQuery_Uproot, deliver


def uproot_funcadl_dict():
    query = FuncADLQuery_Uproot(). \
        Select(lambda e: {'el_pt_NOSYS': e['el_pt_NOSYS']})  # type: ignore

    spec = {
        'General': {
            'ServiceX': "servicex-uc-af"
        },
        'Sample': [{
            'Name': "Uproot_FuncADL_Dict",
            'RucioDID': "user.mtost:user.mtost.singletop.p6026.Jun13",
            'Tree': "reco",
            'Query': query
        }]
    }
    return deliver(spec)


if __name__ == "main":
    uproot_funcadl_dict()
