Query Types
===========

ServiceX queries can be expressed using a number of query languages.
The queries are translated to actual code in the ServiceX *codegens*.
Not all query languages support all potential input data formats,
so once you have determined what input data you need to manipulate,
you can decide what query language to express your query in.

This table sumarizes the query types supported by ServiceX and the data formats they can be used with.

.. list-table::
   :header-rows: 1
   :widths: 20 20 20 20

   * -
     - FuncADL
     - Uproot-Raw
     - Python*
   * - Flat ROOT TTrees
     - ✅
     - ✅
     - ✅
   * - CMS NanoAOD
     - ✅
     - ✅
     - ✅
   * - ATLAS PHYSLITE
     - ✅
     - ✅†
     - ✅†
   * - ATLAS PHYS
     - ✅
     - ❌
     - ❌
   * - Parquet
     - ❌
     - ❌
     - ✅

.. note::

   \*  Python queries may not be available on all ServiceX deployments.

   † The subset of the PHYSLITE data format readable (and _writable_) by `uproot` is supported.

.. warning::

   Note that although ServiceX supports multiple output formats, not all features of the query languages may be supported by all output formats. See the cautions in the detailed discussions of the query languages.



A brief introduction to the query languages
-------------------------------------------

* **FuncADL** is an Analysis Description Language inspired by functional languages and C#'s LINQ. Sophisticated filtering and computation of new values can be expressed by chaining a series of simple functions. Because FuncADL is written independently of the underlying data libraries, it can run on many data formats.

* **Uproot-Raw** passes user requests to the ``.arrays()`` function in ``uproot``. In particular, the branches of the input ``TTrees`` can be filtered, cuts can be specified to select events, and additional expressions can be computed. Additional non-``TTree`` objects can be copied from the inputs to the outputs.

* **Python** executes a user-specified Python function and returns the results (assumed to be provided as an Awkward Array). The function has access to the ``uproot`` and ``awkward`` libraries.

Uproot-Raw Query Type
---------------------

The Uproot-Raw query language can be thought of as essentially calling the `TTree.arrays()`_ function of ``uproot`` with the possibility to specify several of the arguments, and returning the result. Multiple queries can be bundled into one request. It is also possible to copy objects from the input file.

Let's look at the structure of an Uproot-Raw query.

.. code-block:: python

    query = [
             {
              'treename': 'reco',
              'filter_name': ['/mu.*/', 'runNumber', 'lbn', 'jet_pt_*'],
              'cut':'(count_nonzero(jet_pt_NOSYS>40e3, axis=1)>=4)'
             },
             {
              'copy_histograms': ['CutBookkeeper*', '/cflow.*/', 'metadata', 'listOfSystematics']
             }
            ]

This is a *list* of *dictionaries*, which is the standard form of an Uproot-Raw query. Each dictionary reflects a separate subquery of the request; it is possible to have just a single dictionary, for a single query.

Each dictionary either has a ``treename`` key (indicating that it is a query on a tree) or a ``copy_histograms`` key (indicating that ROOT objects are to be copied from the old file to the new one - certain objects other than histograms, such as ``TGraph``\s, can also be copied).

* **Query dictionaries**: these dictionaries contain a ``treename`` key, which specifies the tree(s) to be queried. The keys are:

  * ``treename``: either a string, a list of strings, or a dictionary. This selects the names of the tree(s) to which the query will be applied. In the case that a dictionary is passed, the keys will be used to choose the input trees, and the values will be used as the name of the tree that results from the query - this allows the user to run multiple queries on the same tree, saving the output to a different tree each time.

  * ``expressions``, ``cut``, ``filter_name``, ``aliases``: have the same meaning as for `TTree.arrays()`_ in ``uproot``, except that functions aren't permitted (but *glob*\s and *regular expressions*, which are special kinds of strings, are).

  Other keys will be ignored.

  Most queries will probably use ``filter_names``, which selects specific branches, and ``cut``, which selects specific rows. The ``expressions`` argument permits new values to be computed from the branches in the tree, and ``aliases`` can be used to introduce shorthand to make these expressions cleaner.

  The Uproot-Raw language extends the default ``uproot`` expression language by adding many functions from Awkward Array (the example above uses ``awkward.count_nonzero``). This permits very powerful expressions for cuts and expression evaluation.

* **Copy dictionaries**: these dictionaries contain the ``copy_histograms`` key, which specifies the object(s) to be copied. The one key is:

  * ``copy_histograms``: this is either a string of a list of strings. These strings can be anything that ``uproot.ReadOnlyDirectory.items()`` accepts in its ``filter_name`` argument: this can be the exact name of the object (e.g. ``metadata`` above), a *glob* (e.g. ``CutBookkeeper*``), or a *regular expression* (e.g. ``/cflow.*/``). Because of this flexibility, at most one copy dictionary should be needed per request.

.. warning::
   Uproot-Raw supports the parquet output format, but is subject to its limitations. It cannot copy ROOT objects to parquet output. "Multiple output trees" are supported by concatenating the different trees together, with the additional column "treename" which indicates the name that the tree would have had in a ROOT file; these trees *must* have exactly the same structure (no added or missing columns between the different trees).

.. _TTree.arrays(): https://uproot.readthedocs.io/en/latest/uproot.behaviors.TTree.TTree.html#arrays


FuncADL Query Type
------------------
FuncADL queries are based on functional programming concepts and allow
the user to specify complex queries in a compact form. The query is written in a functional
style, with a series of functions that are applied to the data in sequence. The query is written
in a string or as typed python objects. Depending on the source file format, the query is translated
into C++ `EventLoop <https://atlassoftwaredocs.web.cern.ch/analysis-software/AnalysisTools/el_intro/>`_
code, or uproot python code.

An example that fetches the :math:`p_T, \eta` and EM fraction of jets from an ATLAS PHYSLITE file is as follows:

.. code-block:: python
  
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

Full documentation on the func-adl query language can be found at this `JupyterBook <https://gordonwatts.github.io/xaod_usage/intro.html>`_.

Python Function Query Type
--------------------------
This query type is the most flexible for extracting data from an uproot compatible dataset.
The user provides a Python function that is called for each root file in the dataset. The function
is expected to return a dictionary of awkward arrays that will be stored in the output file.

The function must be named ``run_query`` and must accept a single argument, the path to the uproot file.
It can return either an awkward array or a dictionary of awkward arrays, where the keys are the tree names
for each array. If a single awkward array is returned, it is stored in the tree named `servicex`.

.. code-block:: python

    def run_query(input_filenames=None):
        import uproot  # type: ignore
        with uproot.open({input_filenames: "reco"}) as o:
            br = o.arrays("el_pt_NOSYS")
        return br
