Install and Initialize
==============================

:::{admonition} You Will Learn:
:class: note
- How to Install ServiceX Client
- How to access ServiceX AF dashboard
- How to authenticate the client with a backend
:::

## Install ServiceX

:::{important}
Using a Python virtual environment is recommended to avoid conflicts with existing packages and to keep the ServiceX setup isolated.

For more information on virtual environments, refer to the [Python documentation](https://docs.python.org/3/library/venv.html).

Virtual environments can also be created and managed directly in VSCode. Instructions are available in the [VSCode documentation](https://code.visualstudio.com/docs/python/environments).
:::

The `servicex` client is installed using `pip`:

```shell
pip install servicex
```

## Initialize ServiceX

With `servicex` installed, the client must be connected to an Analysis Facility. This step authenticates the environment and sets up the required access. The following command launches the setup wizard:

```shell
servicex init
```

The setup wizard guides through configuring the ServiceX client. After selecting an analysis facility, the wizard provides a link to a sign-in page. On that page, click **Sign in with ATLAS**.

After completing the ATLAS sign-in, open the second link provided by the wizard. On this page, click the button to copy the authentication token:

```{image} imgs/setup-token.png
:width: 400px
:alt: Token copy button example
```


Paste the token into the command line when prompted. The wizard verifies that the client is successfully configured. Once verification is complete, a prompt appears to choose a downloads directory. Press Enter to accept the default location.

If setup completes successfully, a **Configuration Complete** message is displayed. The next step is to use the ServiceX dashboard.

## ServiceX Dashboard

This process also authenticates access to the ServiceX dashboard. Beyond token retrieval, the dashboard provides additional management options for ServiceX resources.

:::{seealso}
[Dashboard Guide](https://tryservicex.org/guide/dashboard/)
:::
