Transformation Request
========

Specify a Request
-----------------
Transform requests are specified with one or more Sample specifications, and
optionally a General section and one or more definitions which are substituted 
into the Sample specifications.

These requests can be defined as:

1. A YAML file
2. A Python dictionary
3. Typed python objects

Regardless of how the request is specified, the request is submitted to ServiceX using the
``deliver`` function, which returns either a list of URLs or a list of local file paths.


The Sample Sections
^^^^^^^^^^^^^^^^^^^
Each Sample section represents a single query to be executed. It includes the following fields:

* ``Name``: A title for this sample.
* ``Dataset``: Rucio dataset, or a list of files via XRootD
* ``Query``: The query to be executed. This can be a func-adl query, a Python function, or a dictionary of uproot selections.
* (Optional) ``NFiles``:  Limit on the number of files to process
* (Optional) ``IgnoreLocalCache``: If set to true, don't use a local cache for this sample and always submit to ServiceX

The General Section
^^^^^^^^^^^^^^^^^^^
The General section of the request includes the following fields:

* (Optional) ``OutputFormat``: Can be ``root-ttree`` (default) or ``parquet``
* (Optional) ``Delivery``: Can be ``URLs`` or ``LocalCache`` (default)

The Definitions Sections
^^^^^^^^^^^^^^^^^^^^^^^^
The Definitions section is a dictionary of values that can be substituted into fields in the Sample
sections. This is useful for defining common values that are used in multiple samples.