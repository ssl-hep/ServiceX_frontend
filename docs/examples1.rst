Examples1
========

Simple Func_ADL Example
-----------------------
This simple example reads a single root file form the CERN Opendata repo

.. code:: python

    from servicex_client.dataset_identifier import FileListDataset
    from servicex_client.models import ResultFormat
    from servicex_client.servicex_client import ServiceXClient

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

    from servicex_client.dataset_identifier import RucioDatasetIdentifier
    from servicex_client.models import ResultFormat
    from servicex_client.servicex_client import ServiceXClient

    sx = ServiceXClient(backend="testing4")

    dataset_id = RucioDatasetIdentifier("user.kchoi:user.kchoi.fcnc_tHq_ML.ttH.v8")

    ds = sx.func_adl_dataset(dataset_id)

    sx2 = ds.Select(lambda e: {'el_pt': e['el_pt']})\
        .set_result_format(ResultFormat.parquet)\
        .as_files()

    print(sx2)


func_adl Example reading xAOD from ATLAS PHYSLITE on OpenData
---------------------------------------------

This example loads the $p_T$ and $\eta$ for jets from an xAOD PHYSLITE file found in the ATLAS OpenData release.

.. .. code-include :: :func:`examples.func_adl_xAOD_simple.func_adl_xaod_simple`



.. ..     automethod:: func_adl_xaod_simple
.. .. literalinclude:: ../examples/func_adl_xAOD_simple.py


func_adl Example reading xAOD using a Typed Dataset.
---------------------------------------------

This example loads the $p_T$ and $\eta$ for jets from an xAOD PHYSLITE file found in the ATLAS OpenData release.
It uses the full type information, allowing one to access everything that could be translated in the xAOD.

.. .. code-include :: :func:`examples.func_adl_xAOD_typed.func_adl_xaod_typed`



.. ..     automethod:: func_adl_xaod_typed
.. .. literalinclude:: ../examples/func_adl_xAOD_typed.py



Python Code Generator
---------------------
This example is using the python code generator. For this we don't use func_adl,
but pass in a python function that assumes a filename comes in as an argument and
returns an awkward array

.. code:: python

    from servicex_client.dataset_identifier import FileListDataset
    from servicex_client.servicex_client import ServiceXClient

    sx = ServiceXClient(backend="testing4")
    dataset_id = FileListDataset("root://eospublic.cern.ch//eos/opendata/atlas/OutreachDatasets/2020-01-22/4lep/MC/mc_345060.ggH125_ZZ4lep.4lep.root")  # NOQA 501

    ds = sx.python_dataset(dataset_id, codegen="python", title="Python")


    def run_query(input_filenames=None):
        import uproot
        o = uproot.lazy({input_filenames: "mini"})
        return o.lep_pt


    sx3 = ds.with_uproot_function(run_query).as_pandas()
    print(sx3)
    

Bigger Uproot
---------------------
.. .. code-include :: :func:`examples.Submit_from_YAML.submit_from_yaml`



.. ..     automethod:: submit_from_yaml
.. .. literalinclude:: ../examples/Submit_from_YAML.py
.. testsetup::

    from servicex import deliver

.. testcode::

    servicex_yaml = "../examples/config_Uproot_FuncADL.yaml"
    print(deliver(servicex_yaml))
    
.. testoutput::
    :hide:
    :options: +IGNORE_EXCEPTION_DETAIL  

    ...

