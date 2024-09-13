Examples
========
Here are some examples of how to use the ServiceX client to extract data from a dataset. Each
examples shows the three ways to specify a request: as a YAML file, as a Python dictionary, and
as a typed Python object.

The ServiceX Deliver Function
-----------------------------
The `deliver` function is used to submit a request to ServiceX. It takes a request in one of the
three formats and returns a list `TransformedResult` objects. Each `TransformedResult` object
is represents the result of a single sample in the request. The `TransformedResult` object
contains the name of the sample, the output format, and a list of URLs or local file paths
where the results can be found.

How to Use YAML Specification
-----------------------------
In order to consume the YAML specification and pass it to the ServiceX `deliver` function
you can use the following code:

.. code:: python

    from servicex import deliver

    print(
        deliver("config_Uproot_FuncADL.yaml")
    )

Raw Uproot Example
------------------
This example uses the raw uproot query type to extract the `el_pt_NOSYS` branch from the `reco` tree
in `user.mtost:user.mtost.singletop.p6026.Jun13` Dataset.

.. tabs::

   .. tab:: *yaml*

        .. literalinclude:: ../examples/config_Uproot_UprootRaw.yaml
            :language: yaml

   .. tab:: *dict*

        .. literalinclude:: ../examples/Uproot_UprootRaw_Dict.py
            :language: python


Python Function Example
-----------------------
This example uses an uproot python function to extract the `el_pt_NOSYS` branch from the `reco` tree
in `user.mtost:user.mtost.singletop.p6026.Jun13` Dataset. Note that you can specify a python function
even in a yaml file.

.. tabs::

   .. tab:: *yaml*

        .. literalinclude:: ../examples/config_Uproot_PythonFunction.yaml
            :language: yaml

   .. tab:: *dict*

        .. literalinclude:: ../examples/Uproot_PythonFunction_Dict.py
            :language: python


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

