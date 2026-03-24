The Python query type is the most flexible option for extracting data from an `uproot`-compatible dataset, and is best suited for transformations too complex to express in Uproot-Raw. For simpler branch selection and filtering, Uproot-Raw is preferred as it is faster and easier to set up. A Python function is called once per file in the dataset, and its result is stored in the output file.

The function must be named `run_query` and accept a single argument: the path to the input file. It can return either an awkward array or a dictionary of awkward arrays, where the keys become tree names. A single returned array is stored under the tree name `servicex`.

```python
def run_query(input_filenames=None):
    import uproot  # type: ignore
    with uproot.open({input_filenames: "reco"}) as o:
        br = o.arrays("el_pt_NOSYS")
    return br
```
