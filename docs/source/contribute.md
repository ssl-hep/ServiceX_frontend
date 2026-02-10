# Contributor Guide

Welcome to the ServiceX contributor guide, and thank you for your interest in contributing to the project!

## Overview

The `servicex` frontend code uses standard python packaging and open-source development methodologies. The code is hosted on GitHub,
and we use the GitHub issue tracker to manage bugs and feature requests. We also use GitHub pull requests for code review and merging.

- [ServiceX\_frontend](https://github.com/ssl-hep/ServiceX_frontend) - The ServiceX Python library, which enables users to send requests to ServiceX. Currently, this is the only ServiceX frontend client.

Additional repositories related to the ServiceX project can be found in the [ssl-hep GitHub organization](https://github.com/ssl-hep).

## Join us on Slack

We coordinate our efforts on the [IRIS-HEP Slack](http://iris-hep.slack.com).
Come join this intellectual hub!

## Issues

All development work on the code should start with an issue. Please submit issues for bugs and feature
requests to the [repository](https://github.com/ssl-hep/ServiceX_frontend).

## Branching Strategy

ServiceX uses a slightly modified GitLab flow. The `master` branch is used for releases, and
all development work occurs on feature branches.

## Development Workflow

1. Set up a local development environment:
    - Fork the `ServiceX_frontend`
    - Clone the (forked) repository to your local machine:

    - Set up a new environment via `conda` or `virtualenv`.
    - Install dependencies, including test dependencies:

    ```bash
    python3 -m pip install -e .[develop]
    ```

2. Develop your contribution:
    - Pull latest changes from upstream:

    ```bash
    git checkout master
    git pull upstream master
    ```

    - Create a branch for the feature you want to work on:

    ```bash
    git checkout -b fix-issue-99
    ```

    - Commit locally as you progress with `git add` and `git commit`.

3. Test your changes:
    - Run the full test suite with `python -m pytest`, or target specific test files with `python -m pytest tests/path/to/file.py`.
    - Please write new unit tests to cover any changes you make.

4. Submit a pull request to the upstream repository
