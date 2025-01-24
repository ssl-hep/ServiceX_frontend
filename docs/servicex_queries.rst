Query classes
=============

All the query classes are also made available under the package ``servicex.query`` and can be imported from there:

.. list-table::
    :header-rows: 1

    * - Name in ``servicex.query``
      - Original class name
    * - ``PythonFunction``
      - :py:class:`servicex.python_dataset.PythonFunction`
    * - ``UprootRaw``
      - :py:class:`servicex.uproot_raw.uproot_raw.UprootRawQuery`
    * - ``FuncADL_Uproot``
      - :py:class:`servicex.func_adl.func_adl_dataset.FuncADLQuery_Uproot`
    * - ``FuncADL_ATLASr21``
      - :py:class:`servicex.func_adl.func_adl_dataset.FuncADLQuery_ATLASr21`
    * - ``FuncADL_ATLASr22``
      - :py:class:`servicex.func_adl.func_adl_dataset.FuncADLQuery_ATLASr22`
    * - ``FuncADL_ATLASxAOD``
      - :py:class:`servicex.func_adl.func_adl_dataset.FuncADLQuery_ATLASxAOD`
    * - ``FuncADL_CMS``
      - :py:class:`servicex.func_adl.func_adl_dataset.FuncADLQuery_CMS`

So, for example,

.. code:: python

    from servicex.query import FuncADL_Uproot

is equivalent to

.. code:: python

    from servicex.func_adl.func_adl_dataset import FuncADLQuery_Uproot

Uproot-Raw queries
------------------

.. automodule:: servicex.uproot_raw.uproot_raw
   :members: UprootRawQuery

.. automodule:: servicex.uproot_raw.uproot_raw
   :members: TreeSubQuery, CopyHistogramSubQuery

Python queries
--------------

.. automodule:: servicex.python_dataset
    :members:

FuncADL queries
---------------

.. automodule:: servicex.func_adl.func_adl_dataset
    :members:
