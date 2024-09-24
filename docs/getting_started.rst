Getting Started
===============

- Introduce the client with a minimal example, working through line by line (pydantic version)
- Top-level explanation DataBinder configuration yaml


First request
--------------

Once you've been approved, you're ready to go!

You can interact with ServiceX by making a transformation request. A transformation request includes the following information:

- An input dataset
- Filters to be applied
- Computation of new columns (if any)
- Columns to be returned to the user

Below are some basic examples which you can run to confirm that ServiceX is working for you.

xAOD
~~~~~

.. code-block:: python

    from servicex import ServiceXClient, RucioDatasetIdentifier, ResultFormat
    from func_adl_servicex_xaodr22.event_collection import Event
    from func_adl_servicex_xaodr22 import calib_tools

    # A Z to ee sample - Release 21
    ds_name = (
        r"mc16_13TeV:mc16_13TeV.361106.PowhegPythia8EvtGen_AZNLOCTEQ6L1_Zee"
        r".deriv.DAOD_PHYS.e3601_e5984_s3126_r10201_r10210_p5313")

    sx = ServiceXClient(backend="uc-af")
    did = RucioDatasetIdentifier(ds_name, num_files=10)

    ds_raw = sx.func_adl_dataset(
        did, codegen="atlasr21", title="Zee", result_format=ResultFormat.parquet, item_type=Event)

    # ds = calib_tools.apply_calibration(ds_raw, "PHYS") <this is what we should have>
    ds = calib_tools.query_update(ds_raw, calib_tools.default_config("PHYSLITE"))

    good_ele = ds.Select(
        lambda e: {
            "run": e.EventInfo("EventInfo").runNumber(),
            "event": e.EventInfo("EventInfo").eventNumber(),
            "good_ele": e.Electrons("Electrons")
                        .Where(lambda e: (e.pt() / 1000 > 25.0) and (abs(e.eta()) < 2.5)),
        }
    )

    electron_pt = good_ele.Select(lambda e: {
        "run": e.run,
        "event": e.event,
        "pt": e.good_ele.Select(lambda ele: ele.pt() / 1000.0),
    })

    r = electron_pt.as_signed_urls()
    print(f"number of URLs: {len(r.signed_url_list)}")


Expected output:

.. code-block:: python

                JetPt
    entry            
    0       36.319766
    1       34.331914
    2       16.590844
    3       11.389335
    4        9.441805
    ...           ...
    857133   6.211655
    857134  47.653145
    857135  32.738951
    857136   6.260789
    857137   5.394783

    [11355980 rows x 1 columns]


uproot
~~~~~~~

Instead of a rucio dataset, here we will use a file directly available over `https`,
and a slightly more complex query, and we'll ask for the data to be locally downloaded
so we can access the files directly.

.. code-block:: python

    
    import ast

    import qastle

    from servicex import ServiceXSpec, General, Sample
    from servicex.func_adl.func_adl_dataset import FuncADLQuery
    from servicex.servicex_client import deliver

    query = FuncADLQuery().Select(lambda e: {'lep_pt': e['lep_pt']}). \
        Where(lambda e: e['lep_pt'] > 1000)

    qstr = """
    FuncADLDataset().Select(lambda e: {'lep_pt': e['lep_pt']}). \
            Where(lambda e: e['lep_pt'] > 1000)
    """
    query_ast = ast.parse(qstr)
    qastle_query = qastle.python_ast_to_text_ast(qastle.insert_linq_nodes(query_ast))
    print("From str", qastle_query)
    q2 = FuncADLQuery()
    q2.set_provided_qastle(qastle_query)
    print(q2.generate_selection_string())
    print("From python", query.generate_selection_string())
    spec = ServiceXSpec(
        General=General(
            ServiceX="testing1",
            Codegen="uproot",
            OutputFormat="parquet",
            Delivery="LocalCache"
        ),
        Sample=[
            Sample(
                Name="mc_345060.ggH125_ZZ4lep.4lep",
                XRootDFiles="root://eospublic.cern.ch//eos/opendata/atlas/OutreachDatasets/2020-01-22/4lep/MC/mc_345060.ggH125_ZZ4lep.4lep.root", # NOQA E501
                Query=query
            )
        ]
    )

    print(deliver(spec))



Expected output:

.. code-block:: python

    [{pt: [36.3, 24.7], eta: [2.87, 3.13], phi: [, ... -2.15], mass: [12.3, 6.51, 3.98]}]
    349


Next steps
-----------

Check out the [requests guide](requests.md) to learn more about specifying transformation requests using func-ADL.