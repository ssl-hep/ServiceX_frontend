Specifying input datasets
========

When making a ServiceX request, you need to specify where the source data are to be found. These is done by creating instances of ``servicex.dataset`` classes (when writing Python code) or by using appropriate YAML tags (when writing a YAML configuration). The possibilities are given below:

Rucio
^^^^
This will look up a dataset using a query to the Rucio data management system. The request is assumed to be for a Rucio dataset or container.
 * Python: ``{ "Dataset": servicex.dataset.Rucio("my.rucio.dataset.name") }``
 * YAML: ``Dataset: !Rucio my.rucio.dataset.name``

CERN Open Data Portal
^^^^
This looks up files in datasets using their integer ID in the `CERN Open Data Portal <https://opendata.cern.ch/>`_. (If you access the web page for a dataset, this is the number following ``opendata.cern.ch/record/`` in the URL.)
 * Python: ``{ "Dataset": servicex.dataset.CERNOpenData(179) }``
 * YAML: ``Dataset: !CERNOpenData 179``

A list of network-accessible files
^^^^
If you have URLs for files that ServiceX can access via either the HTTP or XRootD protocols, then these URLs can be given directly to ServiceX. Note that the ServiceX instance must have permissions to read these files; in particular if generic members of your experiment can't access the files, ServiceX will probably not be able to either.
 * Python: ``{ "Dataset": servicex.dataset.FileList(["http://server/file1.root", "root://server/file2.root"]) }``
 * YAML: ``Dataset: !FileList ["http://server/file1.root", "root://server/file2.root"]``

XRootD pattern (useful for EOS)
^^^^
Files can also be located using wildcard patterns with XRootD. So, for example, if you want to include all files in the directory ``/eos/opendata/mystuff`` in the CERN EOS system, given that the directory is made available to the world as ``root://eospublic.cern.ch//eos/opendata/mystuff``, you can ask for ``root://eospublic.cern.ch//eos/opendata/mystuff/*``.

*Note: available from ServiceX client version 3.0.1.*
 * Python: ``{ "Dataset": servicex.dataset.XRootD("root://eospublic.cern.ch//eos/opendata/mystuff/*") }``
 * YAML: ``Dataset: !XRootD root://eospublic.cern.ch//eos/opendata/mystuff/*``

You can access files in other CERN `eos` mounts with `root://eosatlas.cern.ch//eos/...`, or with any other `eos` root.
