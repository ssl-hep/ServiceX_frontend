from servicex import query as q, deliver


def func_adl_xaod_simple():
    query = q.FuncADL_ATLASr22()  # type: ignore
    jets_per_event = query.Select(lambda e: e.Jets())
    jet_info_per_event = jets_per_event.Select(
        lambda e: e.Select(lambda j: {'pt': j.pt(), 'eta': j.eta()})
    )

    spec = {
        'Sample': [{
            'Name': "func_adl_xAOD_simple",
            'RucioDID': "user.mtost:user.mtost.singletop.p6026.Jun13",
            'Query': jet_info_per_event
        }]
    }
    files = deliver(spec, servicex_name="servicex-uc-af")
    assert files is not None, "No files returned from deliver! Internal error"
    return files


if __name__ == "__main__":
    files = func_adl_xaod_simple()
    assert len(files['func_adl_xAOD_simple']) == 1
