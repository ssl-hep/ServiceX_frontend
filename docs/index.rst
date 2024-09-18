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

ServiceX is a scalable data extraction, transformation and delivery system deployed in a Kubernetes cluster.

.. image:: img/organize2.png
    :alt: organize


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



.. toctree::
   :maxdepth: 2
   :caption: Contents:

   connect_servicex
   query_types
   transform_request
   examples
   command_line
   contribute
   about
   modules
   Github <https://github.com/ssl-hep/ServiceX_frontend>

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
