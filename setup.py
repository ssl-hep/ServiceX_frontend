# setuptools loads some plugins necessary for use here.
from setuptools import find_packages  # noqa: F401
from distutils.core import setup
import sys
import os

# Use the readme as the long description.
with open("README.md", "r") as fh:
    long_description = fh.read()

extra_test_packages = []
if sys.version_info[1] < 8:
    extra_test_packages.append("asyncmock")

version = os.getenv("servicex_version")
if version is None:
    version = "0.1a1"
else:
    version = version.split("/")[-1]

# Awkward 2.0 is only allowed on Python 3.8+ - so we need to shift the
# awkward requirement a little bit.
# TODO: Remove this when we stop supporting 3.7.
if sys.version_info < (3, 8):
    awkward_requirements = [
        "awkward>=1.0.1,<2",
        "uproot>=4.0.1,<5",
    ]
else:
    awkward_requirements = [
        "awkward>=1.0.1",
        "dask_awkward",
        "fsspec",
        "uproot>=4.0.1",
    ]
setup(
    name="servicex",
    version=version,
    packages=["servicex"],
    scripts=[],
    description="Front-end for the ServiceX Data Server",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="G. Watts (IRIS-HEP/UW Seattle)",
    author_email="gwatts@uw.edu",
    maintainer="Gordon Watts (IRIS-HEP/UW Seattle)",
    maintainer_email="gwatts@uw.edu",
    url="https://github.com/ssl-hep/ServiceX_frontend",
    license="TBD",
    python_requires=">=3.7",
    test_suite="tests",
    install_requires=[
        "idna==2.10",  # Required to thread version needle with requests library
        "pandas~=1.0",
        "uproot>=4.0.1",
        "backoff>=2.0",
        "aiohttp~=3.6",
        "minio~=5.0",
        "tqdm~=4.0",
        "qastle>=0.10",
        "make_it_sync>= 1.0.0",
        "google-auth",
        "confuse",
        "pyarrow>=1.0",
    ]
    + awkward_requirements,
    extras_require={
        "test": [
            "pytest>=3.9",
            "pytest-asyncio",
            "pytest-mock",
            "pytest-cov",
            "coverage",
            "flake8",
            "codecov",
            "autopep8",
            "twine",
            "asyncmock",
            "jupyterlab",
            "ipywidgets",
            "black",
        ]
        + extra_test_packages,
    },
    classifiers=[
        "Development Status :: 3 - Alpha",
        # "Development Status :: 4 - Beta",
        # "Development Status :: 5 - Production/Stable",
        # "Development Status :: 6 - Mature",
        "Intended Audience :: Developers",
        "Intended Audience :: Information Technology",
        "Programming Language :: Python",
        "Topic :: Software Development",
        "Topic :: Utilities",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    package_data={
        "servicex": ["config_default.yaml"],
    },
    platforms="Any",
)
