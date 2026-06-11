Install and Initialize
==============================

This page covers the installation and initialization of the ServiceX client, including how to authenticate with an analysis facility and access the ServiceX dashboard.

:::{admonition} Page Overview
:class: note
- How to install the ServiceX client
- How to access the ServiceX Analysis Facility dashboard
- How to authenticate the client with a backend
:::

## Setup For ATLAS

### Install ServiceX

:::{important}
Using a Python virtual environment is recommended to avoid conflicts with existing packages and to keep the ServiceX setup isolated.

For more information on virtual environments, refer to the [Python documentation](https://docs.python.org/3/library/venv.html).

Virtual environments can also be created and managed directly in VSCode. Instructions are available in the [VSCode documentation](https://code.visualstudio.com/docs/python/environments).
:::

The `servicex` client is installed using `pip`:

```shell
pip install servicex
```

### Initialize ServiceX

With `servicex` installed, the client must be connected to an Analysis Facility. This step authenticates the environment and sets up the required access. The following command launches the setup wizard:

```shell
servicex init
```

The setup wizard guides through configuring the ServiceX client. After an analysis facility is selected, the wizard provides a link to a sign-in page. On that page, select **Sign in with ATLAS** to authenticate.

After the ATLAS sign-in is complete, the second link provided by the wizard leads to the token retrieval page. On this page, users should click the button to copy the authentication token:

```{image} imgs/setup-token.png
:width: 400px
:alt: Token copy button example
```

The token should be entered into the command line when prompted. The wizard verifies that the client is successfully configured. Once verification is complete, a prompt appears to choose a downloads directory. Pressing Enter accepts the default location.

If setup completes successfully, a **Configuration Complete** message is displayed. The next step is to use the ServiceX dashboard.

## Setup For CMS

There are currently two analysis facilities that are set up for use with ServiceX for CMS:

1. [Purdue](https://analysis-facility.physics.purdue.edu)
2. [Nebraska](https://coffea.casa/hub/login)

To use ServiceX on either of these analysis facilities, log into the Coffea Casa instance and ServiceX is ready to use.

## ServiceX Dashboard

This process also authenticates access to the ServiceX dashboard, which provides additional management options beyond token retrieval. From the dashboard, users can view the status of their transformation requests, see why they failed, and cancel any that are currently running.
