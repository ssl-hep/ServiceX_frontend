from servicex import ServiceXClient, RucioDatasetIdentifier, ResultFormat, DatasetGroup

ds_name = r"mc16_13TeV:mc16_13TeV.361106.PowhegPythia8EvtGen_AZNLOCTEQ6L1_Zee.deriv.DAOD_PHYS.e3601_e5984_s3126_r10201_r10210_p5313"  # NOQA 501

sx = ServiceXClient(backend="testing4")
did = RucioDatasetIdentifier(ds_name, num_files=10)
ds_raw = sx.func_adl_dataset(
    did,
    codegen="atlasr21",
    title="Zee",
    result_format=ResultFormat.parquet,  # , item_type=Event
)

ds = ds_raw
# ds = calib_tools.apply_calibration(ds_raw, "PHYS") <this is what we should have>
# ds = calib_tools.query_update(ds_raw, calib_tools.default_config("PHYSLITE"))

good_ele = ds.Select(
    lambda e: {
        "run": e.EventInfo("EventInfo").runNumber(),
        "event": e.EventInfo("EventInfo").eventNumber(),
        "good_ele": e.Electrons("Electrons").Where(
            lambda e: (e.pt() / 1000 > 25.0) and (abs(e.eta()) < 2.5)
        ),
    }
)

electron_pt = good_ele.Select(
    lambda e: {
        "run": e.run,
        "event": e.event,
        "pt": e.good_ele.Select(lambda ele: ele.pt() / 1000.0),
    }
)

electron_etaphi = good_ele.Select(
    lambda e: {
        "run": e.run,
        "event": e.event,
        "eta": e.good_ele.Select(lambda ele: ele.eta()),
        "phi": e.good_ele.Select(lambda ele: ele.phi()),
    }
)

group = DatasetGroup([electron_pt, electron_etaphi])
group.set_result_format(ResultFormat.parquet)

print(group.as_signed_urls())
