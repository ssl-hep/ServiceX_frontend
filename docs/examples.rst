Examples
========
Here are some examples of how to use the ServiceX client to extract data from a dataset. Each
examples shows the three ways to specify a request: as a YAML file, as a Python dictionary, and
as a typed Python object.


Examples For Each Query Type
-----------------------------
Examples for each query type (`Uproot-Raw`, `Func_ADL Uproot`, `Python Function`) 
and three different representations (`Python Dictionary`, `Python Typed Object`, and `YAML`).
Note that all examples extract the same branch of the same tree (``ROOT TTree``) from 
the same :ref:`dataset <label_dataset>`.


Uproot-Raw Query Example
~~~~~~~~~~~~~~~~~~~~~~~~~
This example uses the raw uproot query type to extract the ``AnalysisElectronsAuxDyn.pt`` branch 
from the ``CollectionTree`` tree in ATLAS PHYSLITE OpenData Dataset.

.. tabs::
    
    .. tab:: *Python Dict*

        .. literalinclude:: ../examples/UprootRaw_Dict.py
            :language: python
    
    .. tab:: *Python Typed Object*

        .. literalinclude:: ../examples/UprootRaw_Typed.py
            :language: python

    .. tab:: *YAML*

        .. literalinclude:: ../examples/config_UprootRaw.yaml
            :language: yaml


Func_ADL Uproot Query Example
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
This example uses an Func_ADL to extract the ``AnalysisElectronsAuxDyn.pt`` branch 
from the ``CollectionTree`` tree in ATLAS PHYSLITE OpenData Dataset. 

.. tabs::

    .. tab:: *Python Dict*

        .. literalinclude:: ../examples/FuncADL_Uproot_Dict.py
            :language: python

    .. tab:: *Python Typed Object*

        .. literalinclude:: ../examples/FuncADL_Uproot_Typed.py
            :language: python

    .. tab:: *YAML*

        .. literalinclude:: ../examples/config_FuncADL_Uproot.yaml
            :language: yaml


Func_ADL xAOD Query Example
~~~~~~~~~~~~~~~~~~~~~~~~~~~

The two following examples read columns of data from an ATLAS PHYSLITE xAOD file
(released by the experiment as OpenData). It uses the internal C++ framework, EventLoop, to read this data.
EventLoop can be used to read xAOD files in general, not just PHYSLITE.

THe first example uses the very simple model that is built into ServiceX:

.. tabs::

    .. tab:: *Python Dict*

        .. literalinclude:: ../examples/func_adl_xAOD_simple.py
            :language: python

The second example uses the full type information, allowing one to
access everything that could be translated in the xAOD (including ElementLink following):

.. tabs::

    .. tab:: *Python Dict*

        .. literalinclude:: ../examples/func_adl_xAOD_typed.py
            :language: python

For this second example, make sure the extra package `func_adl_servicex_xaodr22` is installed!

Python Function Query Example
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
This example uses an uproot python function to extract the ``AnalysisElectronsAuxDyn.pt`` branch 
from the ``CollectionTree`` tree in ATLAS PHYSLITE OpenData Dataset. 
Note that you can specify a python function even in a yaml file.

.. tabs::

    .. tab:: *Python Dict*

        .. literalinclude:: ../examples/PythonFunction_Dict.py
            :language: python

    .. tab:: *Python Typed Object*

        .. literalinclude:: ../examples/PythonFunction_Typed.py
            :language: python

    .. tab:: *YAML*

        .. literalinclude:: ../examples/config_PythonFunction.yaml
            :language: yaml


The ServiceX Deliver Function
------------------------------
The `deliver` function is used to submit a request to ServiceX. It takes a request in one of the
three formats and returns a python dictionary with the name of the sample as a key 
and a list of URLs or local file paths as a value. 


How to Use YAML Specification
------------------------------
YAML specification can be consumed by passing it to the ServiceX `deliver` function.
You can use the following code:

.. code:: python

    from servicex import deliver

    print(
        deliver("config_Uproot_FuncADL.yaml")
    )

.. _label_dataset:
    
The Dataset in Examples
-----------------------
The dataset in the examples is publically accessible ATLAS Open Data
(`ATLAS DAOD PHYSLITE format Run 2 2016 proton-proton collision data 
<https://opendata.cern.ch/record/80001>`_).
