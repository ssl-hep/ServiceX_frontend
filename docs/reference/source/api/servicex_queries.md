# Query classes

All the query classes are also made available under the package `servicex.query` and can be imported from there:

| Name in `servicex.query` | Original class name |
|---|---|
| `PythonFunction` | {py:class}`servicex.python_dataset.PythonFunction` |
| `UprootRaw` | {py:class}`servicex.uproot_raw.uproot_raw.UprootRawQuery` |
| `FuncADL_Uproot` | {py:class}`servicex.func_adl.func_adl_dataset.FuncADLQuery_Uproot` |
| `FuncADL_ATLASr21` | {py:class}`servicex.func_adl.func_adl_dataset.FuncADLQuery_ATLASr21` |
| `FuncADL_ATLASr22` | {py:class}`servicex.func_adl.func_adl_dataset.FuncADLQuery_ATLASr22` |
| `FuncADL_ATLASxAOD` | {py:class}`servicex.func_adl.func_adl_dataset.FuncADLQuery_ATLASxAOD` |
| `FuncADL_CMS` | {py:class}`servicex.func_adl.func_adl_dataset.FuncADLQuery_CMS` |

So, for example,

```python
from servicex.query import FuncADL_Uproot
```

is equivalent to

```python
from servicex.func_adl.func_adl_dataset import FuncADLQuery_Uproot
```

## Uproot-Raw queries

```{eval-rst}
.. automodule:: servicex.uproot_raw.uproot_raw
   :members: UprootRawQuery, TreeSubQuery, CopyHistogramSubQuery
```

## Python queries

```{eval-rst}
.. automodule:: servicex.python_dataset
    :members:
```

## FuncADL queries

```{eval-rst}
.. automodule:: servicex.func_adl.func_adl_dataset
    :members:
```
