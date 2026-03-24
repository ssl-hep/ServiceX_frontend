FuncADL expresses queries as a chain of functions applied to sequences of events or objects. Queries are written as typed Python objects and translated into C++ or Python depending on the source format. Two variants are available: one for `uproot`-readable flat data, and one for xAOD datasets.

:::{warning}
FuncADL is a complex query format. A thorough reading of the [FuncADL user guide](https://tryservicex.org/funcadl/) is strongly recommended before use.
:::

#### Uproot-FuncADL Query

The Uproot-FuncADL variant applies FuncADL's functional syntax to `uproot`-readable data, suited for flat ROOT TTrees and NanoAOD formats. Queries chain method calls starting from `FuncADL_Uproot()`, selecting a tree and the desired columns:

```python
query.FuncADL_Uproot()
.FromTree("CollectionTree")
.Select(lambda e: {"el_pt": e["AnalysisElectronsAuxDyn.pt"]})
```

:::{seealso}
For full reference documentation on Uproot-FuncADL, see the [FuncADL user guide](https://tryservicex.org/funcadl/).
:::

#### FuncADL xAOD Query

The xAOD variant is designed for ATLAS PHYS and PHYSLITE datasets, where queries are translated into optimized C++ and executed in AnalysisBase. The following example fetches the {math}`p_T`, {math}`\eta`, and EM fraction of jets from a PHYSLITE file:

```python
from func_adl_servicex_xaodr22 import FuncADLQueryPHYSLITE, cpp_float

query = FuncADLQueryPHYSLITE()
jets_per_event = query.Select(lambda e: e.Jets('AnalysisJets'))
jet_info_per_event = jets_per_event.Select(
    lambda jets: {
        'pt': jets.Select(lambda j: j.pt()),
        'eta': jets.Select(lambda j: j.eta()),
        'emf': jets.Select(lambda j: j.getAttribute[cpp_float]('EMFrac'))  # type: ignore
    }
)
```

:::{seealso}
For full reference documentation on the xAOD variant, see the [FuncADL user guide](https://tryservicex.org/funcadl/).
:::