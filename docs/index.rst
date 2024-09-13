.. ServiceX Client documentation master file, created by
   sphinx-quickstart on Sat Jun 17 07:46:15 2023.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

ServiceX
================

The High Luminosity Large Hadron Collider (HL-LHC) faces enormous computational challenges in the
2020s. The HL-LHC will produce exabytes of data each year, with increasingly complex event
structure due to high pileup conditions. The ATLAS and CMS experiments will record ~ 10 times as
much data from ~ 100 times as many collisions as were used to discover the Higgs boson.


Columnar data delivery
----------------------

ServiceX seeks to enable on-demand data delivery of columnar data in a variety of formats for
physics analyses. It provides a uniform backend to data storage services, ensuring the user doesn't
have to know how or where the data is stored, and is capable of on-the-fly data transformations
into a variety of formats (ROOT files, Arrow arrays, Parquet files, ...) The service offers
preprocessing functionality via an analysis description language called
`func-adl <https://pypi.org/project/func-adl/>`_ that allows users to filter events, request columns,
and even compute new variables. This enables the user to start from any format and extract only the
data needed for an analysis.

.. image:: img/organize2.png
    :alt: Organization

ServiceX is designed to feed columns to a user running an analysis (e.g. via
`Awkward <https://github.com/scikit-hep/awkward-array>`_ or
`Coffea <https://github.com/CoffeaTeam/coffea>`_ tools) based on the results of a query designed by
the user.

Connecting to ServiceX
----------------------
ServiceX is a hosted service. Depending on which experiment you work in, there are different
instances you can connect to. Some can be connected to from the outside world, while others are
accessible only from a Jupyter notebook running inside the analysis facility.

.. list-table::
    :widths: 20 40 40
    :header-rows: 1

    *   - Collaboration
        - Name
        - URL
    *   - ATLAS
        - Chicago Analysis Facility
        - `<https://servicex.af.uchicago.edu/>`_
    *   - CMS
        - Coffea-Casa Nebraska
        - `<https://coffea.casa/hub>`_
    *   - CMS
        - FNAL Elastic Analysis Facility
        - `<https://servicex.apps.okddev.fnal.gov>`_

Follow the links to learn how to enable an account and launch a Jupyter notebook.

Concepts
--------
This section describes the concepts that are important to understand when working with ServiceX.

Datasets
^^^^^^^^^
Datasets are groups of experimental data from which columnar data can be extracted. ServiceX
supports four sources of of datasets:
1. Rucio
2. CERN Open Data Portal
3. File List
4. EOS Directory

Queries
^^^^^^^
Queries are used to extract data from a dataset. They specify the columns to extract, the events to
include in the output. There are several types of queries supported by ServiceX:
1. func-adl
2. Python Function
3. Dictionary of uproot selections


Sample
^^^^^^
A sample is a request to extract columnar data from a given dataset, using a specific
query. It results in a set of output files that can be used in an analysis.

Transformation Request
^^^^^^^^^^^^^^^^^^^^^^
Multiple samples can be submitted to ServiceX at the same time. Each sample is processed
independently, and the results can be retrieved as files downloaded to a local directory or
a list of URLs.

Local Cache
^^^^^^^^^^^
ServiceX maintains a local cache of the results of queries. This cache can be used to avoid
re-running queries that have already been executed.

Specify a Request
-----------------
Transform requests are specified with a General section, one or more Sample specifications, and
optionally one or more definitions which are substituted into the Sample specifications.

These requests can be defined as:

1. A YAML file
2. A Python dictionary
3. Typed python objects

Regardless of how the request is specified, the request is submitted to ServiceX using the
``deliver`` function, which returns either a list of URLs or a list of local file paths.

The General Section
^^^^^^^^^^^^^^^^^^^
The General section of the request includes the following fields:

* OutputFormat: Can be ``root-ttree`` or ``parquet``
* Delivery: Can be ``URLs`` or ``LocalCache``

The Sample Sections
^^^^^^^^^^^^^^^^^^^
Each Sample section represents a single query to be executed. It includes the following fields:

* Name: A title for this sample.
* RucioDID: A Rucio Dataset Identifier
* XRootDFiles: A list of files to be processed without using Rucio. You must use either ``RucioDID`` or ``XRootDFiles`` but not both.
* NFiles: An optional limit on the number of files to process
* Query: The query to be executed. This can be a func-adl query, a Python function, or a dictionary of uproot selections.
* IgnoreLocalCache: If set to true, don't use a local cache for this sample and always submit to ServiceX

The Definitions Sections
^^^^^^^^^^^^^^^^^^^^^^^^
The Definitions section is a dictionary of values that can be substituted into fields in the Sample
sections. This is useful for defining common values that are used in multiple samples.


Configuration
-------------

The client relies on a YAML file to obtain the URLs of different
servicex deployments, as well as tokens to authenticate with the
service. The file should be named ``.servicex`` and the format of this
file is as follows:

.. code:: yaml

   api_endpoints:
     - endpoint: http://localhost:5000
       name: localhost

     - endpoint: https://servicex-release-testing-4.servicex.ssl-hep.org
       name: testing4
       token: ...

   default_endpoint: testing4

   cache_path: /tmp/ServiceX_Client/cache-dir
   shortened_downloaded_filename: true

The ``default_endpoint`` will be used if otherwise not specified. The
cache database and downloaded files will be stored in the directory
specified by ``cache_path``.

The ``shortened_downloaded_filename`` property controls whether
downloaded files will have their names shortened for convenience.
Setting to false preserves the full filename from the dataset. \`

The library will search for this file in the current working directory
and then start looking in parent directories until a file is found.

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   installation
   query_types
   examples
   databinder
   command_line
   getting_started
   transformer_matrix
   contribute
   troubleshoot
   about
   modules

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
