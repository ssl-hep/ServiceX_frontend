# Uproot Raw

Uproot-Raw queries call [TTree.arrays()](https://uproot.readthedocs.io/en/latest/uproot.behaviors.TTree.TTree.html#arrays) with configurable arguments and return the result. Multiple subqueries can be bundled into one request; ROOT objects can also be copied from the input file.

A query is a **list of dictionaries**. Each dictionary is either a query dictionary (contains `treename`) or a copy dictionary (contains `copy_histograms`).

```{include} ../../shared/uproot_raw_query.md
```

## Query Dictionaries

`treename`
: A string, list of strings, or dictionary. Selects the tree(s) to query. When a dictionary is provided, keys select input trees and values set the output tree name — allowing multiple queries on the same tree with distinct outputs.

`expressions`, `cut`, `filter_name`, `aliases`
: Same meaning as in [TTree.arrays()](https://uproot.readthedocs.io/en/latest/uproot.behaviors.TTree.TTree.html#arrays), except functions are not permitted (globs and regular expressions are allowed). Uproot-Raw extends the expression language with many Awkward Array functions. See the axis warning below.

`fail_on_missing_trees`
: If `True`, the transformation fails when a requested tree is absent from any input file. By default, missing trees are silently ignored.

`use_standard_awkward_axis`
: If `True`, disables the axis overrides described below, making expressions fully compatible with Awkward Array at the cost of potentially counterintuitive behavior in particle physics expressions.

Other keys are ignored. Most queries use `filter_name` to select branches and `cut` to filter rows. `expressions` computes new values from branches; `aliases` provides shorthand for complex expressions.

:::{warning}
By default, Uproot-Raw redefines the `axis` argument of several Awkward Array functions from `axis=0` to `axis=1`. This is because `axis=0` evaluates across all events (rows) at once, whereas `axis=1` evaluates within each event — which is almost always the intended behavior in particle physics cuts.

For example, `any(jet_pt>50)` in Uproot-Raw selects events where at least one jet exceeds 50. With the standard `awkward` definition (`axis=0`), it returns a scalar and causes an array shape mismatch error.

Explicit `axis` arguments in expressions override this default. Setting `use_standard_awkward_axis` to `True` restores the original `awkward` behavior.

Modified functions: `concatenate`, `count`, `count_nonzero`, `sum`, `nansum`, `prod`, `nanprod`, `any`, `all`, `min`, `nanmin`, `max`, `nanmax`, `argmin`, `nanargmin`, `argmax`, `nanargmax`, `moment`, `mean`, `nanmedian`, `var`, `nanvar`, `std`, `nanstd`, `softmax`. `flatten` defaults to `axis=2`.
:::

## Copy Dictionaries

`copy_histograms`
: A string or list of strings passed to `uproot.ReadOnlyDirectory.items()` as `filter_name`. Accepts exact names (e.g. `metadata`), globs (e.g. `CutBookkeeper*`), or regular expressions (e.g. `/cflow.*/`). Objects other than histograms — such as `TGraph`s — can also be copied. At most one copy dictionary is typically needed per request.

:::{warning}
Uproot-Raw supports parquet output, but with limitations: ROOT objects cannot be copied to parquet, and multiple output trees are supported only by concatenating them with an added `treename` column indicating the original tree name. All concatenated trees must have exactly the same structure.
:::

## API Reference

### UprootRawQuery

```{eval-rst}
.. autoclass:: servicex.uproot_raw.uproot_raw.UprootRawQuery
   :members:
   :no-index:
```

### TreeSubQuery

```{eval-rst}
.. autoclass:: servicex.uproot_raw.uproot_raw.TreeSubQuery
   :members:
   :no-index:
```

### CopyHistogramSubQuery

```{eval-rst}
.. autoclass:: servicex.uproot_raw.uproot_raw.CopyHistogramSubQuery
   :members:
   :no-index:
```
