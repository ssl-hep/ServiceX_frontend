from servicex import query as q, deliver


def uproot_uproot_raw_dict():

    query = q.UprootRaw([{"treename": "reco", "filter_name": "el_pt_NOSYS"}])

    spec = {
        'Sample': [{
            'Name': "Uproot_UprootRaw_Dict",
            'RucioDID': "user.mtost:user.mtost.singletop.p6026.Jun13",
            'Query': query
        }]
    }
    return deliver(spec)


if __name__ == "__main__":
    files = uproot_uproot_raw_dict()
    assert len(files['Uproot_UprootRaw_Dict']) == 27
