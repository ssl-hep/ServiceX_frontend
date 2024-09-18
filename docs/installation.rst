Client Installation
============

Prerequisites
-------------

- Python 3.8, or above
- Member of the ATLAS or CMS collaborations

Installation
-------------


.. code-block:: bash
    
    pip install servicex

This installs the servicex command line tool and the servicex Python package.

Selecting an endpoint
----------------------

To use the ServiceX client, you will need a ServiceX API token issued by a
ServiceX backend instance. Each ServiceX instance is dedicated to a specific
experiment and file format.
You can use one of the centrally-managed instances of ServiceX running on the
University of Chicago's Analysis Facility cluster:

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


You can view the status of these production servers along with our current
development servers by viewing the `Server Status Dashboard <https://dashboard-integration.servicex.ssl-hep.org>`_.

Visit the instance that meets your needs. Click on the **Sign-in** button in the
upper right hand corner. You will be asked to authenticate via GlobusAuth and
complete a registration form. Once this form is submitted, it will be reviewed
by SSL staff. You will receive an email upon approval.

At this time you may return to the ServiceX page. Click on your name in the
upper right hand corner and then select **Profile** tab. Click on the download
button to have a servicex.yaml file generated with your access token and
downloaded to your computer.

.. image:: img/download-servicex-yaml.jpg
    :alt: Download button

You may place this in your home directory or within
the `servicex_frontend search path <https://github.com/ssl-hep/ServiceX_frontend#configuration>`_.

The remainder of this guide will use the xAOD instance.