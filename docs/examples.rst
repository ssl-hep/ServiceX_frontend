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
This example uses the raw uproot query type to extract the `AnalysisElectronsAuxDyn.pt` branch from the `CollectionTree` tree
in ATLAS PHYSLITE OpenData Dataset.

.. tabs::

    .. tab:: *YAML*

        .. literalinclude:: ../examples/config_UprootRaw.yaml
            :language: yaml

    .. tab:: *Python Dict*

        .. literalinclude:: ../examples/UprootRaw_Dict.py
            :language: python
    
    .. tab:: *Python Typed Object*

        .. literalinclude:: ../examples/UprootRaw_Typed.py
            :language: python


Python Function Example
-----------------------
This example uses an uproot python function to extract the `AnalysisElectronsAuxDyn.pt` branch from the `CollectionTree` tree
in ATLAS PHYSLITE OpenData Dataset. Note that you can specify a python function
even in a yaml file.

.. tabs::

    .. tab:: *YAML*

        .. literalinclude:: ../examples/config_PythonFunction.yaml
            :language: yaml

    .. tab:: *Python Dict*

        .. literalinclude:: ../examples/PythonFunction_Dict.py
            :language: python

    .. tab:: *Python Typed Object*

        .. literalinclude:: ../examples/PythonFunction_Typed.py
            :language: python



