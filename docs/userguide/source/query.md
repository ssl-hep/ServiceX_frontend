# Building a Query

:::{admonition} You Will Learn:
:class: note
- What query languages are available in ServiceX
- How to select the appropriate query language for a given dataset
- How to write a basic query in each supported language
:::

ServiceX queries can be expressed using a number of query languages.
The queries are translated to actual code in the ServiceX backend.
Not all query languages support all potential input data formats,
so once the input data has been identified,
the appropriate query language can be selected.

## Query Languages

* **Uproot-Raw** passes requests to the `.arrays()` function in `uproot`. The branches of the input `TTrees` can be filtered, cuts can be specified to select events, and additional expressions can be computed. Additional non-`TTree` objects can be copied from the inputs to the outputs.

* **Uproot-Python** executes a user-specified Python function and returns the results (assumed to be provided as an Awkward Array). The function has access to the `uproot` and `awkward` libraries.

* **FuncADL** is an Analysis Description Language inspired by functional languages and C#'s LINQ. Sophisticated filtering and computation of new values can be expressed by chaining a series of simple functions. Because FuncADL is written independently of the underlying data libraries, it can run on many data formats.

This table summarizes the query types supported by ServiceX and the data formats they can be used with.

|                    | FuncADL | Uproot-Raw | Uproot-Python\* |
|--------------------|---------|------------|----------|
| Flat ROOT TTrees   | ✅      | ✅         | ✅       |
| CMS NanoAOD        | ✅      | ✅         | ✅       |
| ATLAS PHYSLITE     | ✅      | ✅†        | ✅†      |
| ATLAS PHYS         | ✅      | ❌         | ❌       |
| Parquet            | ❌      | ❌         | ✅       |

:::{note}
\*  Python queries may not be available on all ServiceX deployments.

† The subset of the PHYSLITE data format readable (and _writable_) by `uproot` is supported.
:::

:::{warning}
Although ServiceX supports multiple output formats, not all features of the query languages may be supported by all output formats. See the cautions in the detailed discussions of the query languages.
:::

## Selecting a Query Language

ServiceX supports multiple query backends to suit different workflows. The choice depends on the data type and complexity of the analysis.

| Uproot (Raw & Python) | FuncADL |
|--------|---------|
| ✅ Ideal for working with **ROOT ntuples** or flat data structures.<br>✅ Use preexisting knowledge of Uproot to build queries<br>✅ Queries run quickly and are easy to set up. | ✅ Designed for getting all possible data from **xAOD datasets**<br>✅ Allows writing queries in **Python syntax** that are translated into optimized C++ and run in AnalysisBase.<br>✅ Anything that can be done in AnalysisBase can be done with FuncADL.<br>✅ Removes need for cloning, changing, and building AnalysisBase. |
| ⚠️ Limited to simpler transformations and filtering.<br>⚠️ Does not natively handle complex object hierarchies. | ⚠️ Steeper learning curve; use only when necessary.<br>⚠️ Runs slower than Uproot ServiceX. |

For most analyses, Uproot queries will suffice. For analyses without a clear requirement, Uproot is the recommended starting point.

For analyses requiring access to xAOD data beyond the standard object set, FuncADL is the recommended choice.

## Query Language Examples

The following examples illustrate the structure of each query language to help in selecting the one that best fits the data type.

:::{seealso}
For how queries are added to a sample, see [Samples](deliver.md#samples).
:::

### Uproot-Raw Query

:::{tip}
Uproot-Raw is the recommended query type for most use cases and is a good starting point for new ServiceX users.
:::

An Uproot-Raw query is a list of dictionaries, each representing a subquery:

```{include} ../../shared/uproot_raw_query.md
```

Each dictionary is either a tree query or a copy request:

- **Tree query** dictionaries use `treename` to select the tree, `filter_name` to pick branches, and `cut` to filter events. Multiple tree queries can appear in the same list.
- **Copy request** dictionaries use `copy_histograms` to copy ROOT objects (histograms, `TGraph`s, etc.) from the input file into the output.

:::{seealso}
For the full set of available keys, detailed information, and important dangers, see the [uproot-raw reference page](https://tryservicex.org/reference/uproot-raw).
:::

### Uproot-Python Query

```{include} ../../shared/uproot_python_query.md
```

:::{seealso}
For full details on the function signature, return format, and available libraries, see the [uproot-python reference page](https://tryservicex.org/reference/uproot-python).
:::

### FuncADL Queries

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