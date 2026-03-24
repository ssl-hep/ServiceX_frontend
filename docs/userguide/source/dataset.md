# Specifying Datasets

:::{admonition} You Will Learn:
:class: note
- What dataset source types are supported by ServiceX
- How to define a dataset in Python and YAML for each source type
:::

Physics analyses use a wide range of data types stored in a wide range of locations. The storage location determines the dataset definition, while the data type requires no special configuration. Four dataset source types are currently accepted.

## Dataset Storage Options

Each of these will show how to define datasets in both Python and YAML formats. For all Python examples, `dataset` must be imported from `servicex`.

:::{seealso}
For how datasets are added to a sample, see [Samples](deliver.md#samples).
:::

### Rucio

This dataset declaration looks up a dataset using a query to the Rucio data management system. The request is assumed to be for a Rucio dataset or container.

::::{tab-set}

:::{tab-item} Python

```python
"Dataset": servicex.dataset.Rucio("my.rucio.dataset.name")
```
:::

:::{tab-item} YAML

```yaml
Dataset: !Rucio my.rucio.dataset.name
```
:::
::::

### EOS

For files stored on EOS, two access methods are available. For discrete file selection, `FileList` is recommended; for entire directories or wildcard patterns, `XRootD` is the appropriate dataset type.

:::{danger}
The ServiceX instance must have permissions to read these files; in particular if generic members of your experiment can't access the files, ServiceX will probably not be able to either.
:::

:::::{tab-set}

::::{tab-item} Python

**FileList**
```python
"Dataset": servicex.dataset.FileList(["root://eospublic.cern.ch//eos/opendata/mystuff/file1.root", "root://eospublic.cern.ch//eos/opendata/mystuff/file2.root"])
```

**XRootD**
:::{versionadded} 3.0.1
:::

```python
"Dataset": servicex.dataset.XRootD("root://eospublic.cern.ch//eos/opendata/mystuff/*")
```

::::

::::{tab-item} YAML

**FileList**
```yaml
Dataset: !FileList ["root://eospublic.cern.ch//eos/opendata/mystuff/file1.root", "root://eospublic.cern.ch//eos/opendata/mystuff/file2.root"]
```

**XRootD**
:::{versionadded} 3.0.1
:::

```yaml
Dataset: !XRootD root://eospublic.cern.ch//eos/opendata/mystuff/*
```

::::

:::::

### CERN Open Data Portal

Datasets from the CERN Open Data Portal are referenced by their numeric record ID.

::::{tab-set}

:::{tab-item} Python

```python
"Dataset": servicex.dataset.CERNOpenData(179)
```
:::

:::{tab-item} YAML

```yaml
Dataset: !CERNOpenData 179
```
:::

::::

### Network Accessible Files

Files accessible via HTTP or XRootD protocols can be provided directly as a list of URLs.

:::{danger}
The ServiceX instance must have permissions to read these files; in particular if generic members of your experiment can't access the files, ServiceX will probably not be able to either.
:::

::::{tab-set}

:::{tab-item} Python

```python
"Dataset": servicex.dataset.FileList(["http://server/file1.root", "root://server/file2.root"])
```
:::

:::{tab-item} YAML

```yaml
Dataset: !FileList ["http://server/file1.root", "root://server/file2.root"]
```
:::

::::
