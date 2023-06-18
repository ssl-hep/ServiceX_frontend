Examples
========

Simple Func_ADL Example
-----------------------
This simple example reads a single root file form the CERN Opendata repo

.. code:: python

    sx = ServiceXClient(backend="localhost")
    dataset_id = FileListDataset("root://eospublic.cern.ch//eos/opendata/atlas/OutreachDatasets/2020-01-22/4lep/MC/mc_345060.ggH125_ZZ4lep.4lep.root")  # NOQA 501

    ds = sx.func_adl_dataset(dataset_id, codegen="uproot",
                             title="Root",
                             result_format=ResultFormat.parquet)

    sx3 = ds.Select(lambda e: {'lep_pt': e['lep_pt']}). \
        Where(lambda e: e['lep_pt'] > 1000). \
        as_pandas()
    print(sx3)

Func_ADL Example With Rucio Dataset
-----------------------------------
This example uses the Rucio Dataset Identifier and returns a list of downloaded
parquet files

.. code:: python

    sx = ServiceXClient(backend="testing4")
    print(sx.get_code_generators())

    dataset_id = RucioDatasetIdentifier("user.kchoi:user.kchoi.fcnc_tHq_ML.ttH.v8")

    ds = sx.func_adl_dataset(dataset_id)

    sx2 = ds.Select(lambda e: {'el_pt': e['el_pt']}).as_parquet_files()

    print(sx2)

Python Code Generator
---------------------
This example is using the python code generator. For this we don't use func_adl,
but pass in a python function that assumes a filename comes in as an argument and
returns an awkward array

.. code:: python

    sx = ServiceXClient(backend="localhost")
    dataset_id = FileListDataset("root://eospublic.cern.ch//eos/opendata/atlas/OutreachDatasets/2020-01-22/4lep/MC/mc_345060.ggH125_ZZ4lep.4lep.root")  # NOQA 501

    ds = sx.python_dataset(dataset_id, codegen="python", title="Python")


    def run_query(input_filenames=None):
        import uproot
        o = uproot.lazy({input_filenames: "mini"})
        return o.lep_pt


    sx3 = ds.with_uproot_function(run_query).as_pandas())
    print(sx3)
