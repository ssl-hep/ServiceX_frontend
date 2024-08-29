from servicex import query as q, deliver


def uproot_funcadl_dict():
    query = q.FuncADL_Uproot().FromTree('reco'). \
        Select(lambda e: {'el_pt_NOSYS': e['el_pt_NOSYS']})  # type: ignore

    spec = {
        'Sample': [{
            'Name': "Uproot_FuncADL_Dict",
            'RucioDID': "user.mtost:user.mtost.singletop.p6026.Jun13",
            'Query': query
        }]
    }
    return deliver(spec, servicex_name="servicex-uc-af")


if __name__ == "__main__":
    files = uproot_funcadl_dict()
    assert len(files['Uproot_FuncADL_Dict']) == 27
