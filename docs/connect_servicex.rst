Connecting to ServiceX
======================

You need a `ServiceX endpoint <select-endpoint_>`_ where transformation is happening and
a `client library <client-installation_>`_ to submit a transformation request.

.. _select-endpoint:

Selecting an ServiceX endpoint
------------------------------

ServiceX is a hosted service. Each ServiceX instance is deployed at the server
and dedicated to a specific experiment. Depending on which experiment you work in,
there are different instances you can connect to. Some can be connected to from
the outside world, while others are accessible only from a Jupyter notebook running
inside the analysis facility.

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


For ServiceX endpoints that can be connected from the outside, e.g. ATLAS Chicago
Analysis Facility, you need to follow steps below to download a ServiceX access file.

Click on the **Sign-in** button in the upper right hand corner. You will be asked
to authenticate via GlobusAuth and complete a registration form. Once this form is submitted,
it will be reviewed by SSL staff. You will receive an email upon approval.

At this time you may return to the ServiceX page. Click on your name in the
upper right hand corner and then select **Profile** tab. Click on the download
button to have a ``servicex.yaml`` file generated with your access token and
downloaded to your computer.

.. image:: img/download-servicex-yaml.jpg
    :alt: Download button

.. _client-installation:

ServiceX Client Installation
----------------------------
ServiceX client Python package is a python library for users to communicate
with ServiceX backend (or server) to make transformation requests and handling
of outputs.


Prerequisites
~~~~~~~~~~~~~

- Python 3.9+
- Access to ServiceX endpoint

Installation
~~~~~~~~~~~~

The ServiceX client library can be installed either with ``pip``

.. code-block:: bash

    python -m pip install servicex

or with ``conda``

.. code-block:: bash

    conda install --channel conda-forge servicex

Connecting to an analysis facility
~~~~~~~

Navigate to a directory in which you want to begin a ServiceX project and execute:

.. code-block:: bash
    $ servicex init

    ╭─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╮
    │ ServiceX Configuration Wizard                                                                                                                                                   │
    │                                                                                                                                                                                 │
    │ This wizard will help you set up ServiceX for your experiment.                                                                                                                  │
    │ You'll need to authenticate and configure your download settings.                                                                                                               │
    ╰─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╯

    Select your experiment (A for UChicago) (A): A

    Configuring for UChicago...


    ╭──────────────────────────────────────────────────────────────────────────────── Get Your Token ─────────────────────────────────────────────────────────────────────────────────╮
    │ 1. Open this URL to sign in to UChicago:                                                                                                                                        │
    │    https://servicex.af.uchicago.edu/sign-in                                                                                                                                     │
    │                                                                                                                                                                                 │
    │ 2. After signing in, navigate to:                                                                                                                                               │
    │    https://servicex.af.uchicago.edu/profile                                                                                                                                     │
    │                                                                                                                                                                                 │
    │ 3. Copy your API token and paste it below                                                                                                                                       │
    ╰─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╯

    Paste your token here:

After completing the instructions, you can execute one of the ServiceX :doc:`examples`.

.. code-block:: bash
    $ servicex deliver examples/config_FuncADL_Uproot.yaml
    Delivering examples/config_FuncADL_Uproot.yaml to ServiceX cache
    Uproot_FuncADL_YAML: Transform ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 3/3 00:41
                          Download ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 3/3 00:45

    ✓ ServiceX Delivery Complete!

                                                                                      Delivered Files
    ┏━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
    ┃ Sample              ┃ File Count ┃ Files                                                                                                                                        ┃
    ┡━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩
    │ Uproot_FuncADL_YAML │          3 │ /private/var/folders/h6/k0_2vvjd3rz1tw9d0xw9t9sc0000gn/T/ServiceX_Client/cache-dir/88b68338-6362-4011-b93f-068b1f7655a9/_9f80fe98b1688f812f… │
    │                     │            │ /private/var/folders/h6/k0_2vvjd3rz1tw9d0xw9t9sc0000gn/T/ServiceX_Client/cache-dir/88b68338-6362-4011-b93f-068b1f7655a9/_0549349995c33b10eb… │
    │                     │            │ /private/var/folders/h6/k0_2vvjd3rz1tw9d0xw9t9sc0000gn/T/ServiceX_Client/cache-dir/88b68338-6362-4011-b93f-068b1f7655a9/_2bd13349427ecdedcd… │
    └─────────────────────┴────────────┴──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘

    Total files delivered: 3

Writing your own ServiceX Access File
~~~~~~~~~~~~~~~~~~~~

The client relies on a ``servicex.yaml`` file to obtain the URLs of different
servicex deployments, as well as tokens to authenticate with the
service.

The client library will search for this file in the current working directory
and then start looking in parent directories and your home directory until a file
is found.

The format of this file is as follows:

.. code:: yaml
   api_endpoints:
     - endpoint: https://servicex.af.uchicago.edu
       name: servicex-uc-af
       token: <YOUR TOKEN>
   cache_path: /tmp/ServiceX_Client/cache-dir
   shortened_downloaded_filename: true
``cache_path`` and ``shortened_downloaded_filename`` are optional fields and default to
reasonable values.

The cache database and downloaded files will be stored in the directory
specified by ``cache_path``.

The ``shortened_downloaded_filename`` property controls whether
downloaded files will have their names shortened for convenience.
Setting to false preserves the full filename from the dataset.
