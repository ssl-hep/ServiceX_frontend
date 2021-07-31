# setuptools loads some plugins necessary for use here.
from setuptools import find_packages  # noqa: F401
from distutils.core import setup
import sys
import os

# Use the readme as the long description.
with open("README.md", "r") as fh:
    long_description = fh.read()

if sys.version_info[0] < 3:
    raise NotImplementedError("Do not support version 2 of python")

extra_test_packages = []
if sys.version_info[1] < 8:
    extra_test_packages.append('asyncmock')

version = os.getenv('servicex_version')
if version is None:
    version = '0.1a1'
else:
    version = version.split('/')[-1]

setup(name="servicex",
      version=version,
      packages=['servicex'],
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
      python_requires='>=3.6, <3.10',
      test_suite="tests",
      install_requires=[
          "idna==2.10",  # Required to thread version needle with requests library
          "pandas~=1.0",
          "uproot>=4.0.1, <5",
          "awkward>=1.0.1, <2",
          "backoff~=1.10",
          "aiohttp~=3.6",
          "minio~=5.0",
          "tqdm~=4.0",
          "qastle>=0.10, <1.0",
          'make_it_sync==1.0.0',
          'google-auth==1.17',
          'confuse==1.3.0',
          'pyarrow>=1.0, <4.0'
      ],
      extras_require={
          'test': [
              'pytest>=3.9',
              'pytest-asyncio',
              'pytest-mock',
              'pytest-cov',
              'coverage',
              'flake8',
              'codecov',
              'autopep8',
              'twine',
              'asyncmock',
              'jupyterlab',
              'ipywidgets'
          ] + extra_test_packages,
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
          "Programming Language :: Python :: 3.6",
          "Programming Language :: Python :: 3.7",
          "Programming Language :: Python :: 3.8",
      ],
      package_data={
          'servicex': ['config_default.yaml'],
      },

      platforms="Any",
      )
