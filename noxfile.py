import shutil
from pathlib import Path

import nox

ALL_PYTHONS = ["3.7", "3.8", "3.9", "3.10"]

nox.options.sessions = ["lint", "tests"]


DIR = Path(__file__).parent.resolve()


@nox.session(reuse_venv=True)
def lint(session):
    """
    Lint with flake8.
    """
    session.install("--upgrade", "flake8")
    session.run("flake8", *session.posargs)


@nox.session(python=ALL_PYTHONS, reuse_venv=True)
def tests(session):
    """
    Run the unit tests under coverage.
    Specify a particular Python version with --python option.

    Example:

        $ nox --session tests --python 3.10
    """
    session.install("--upgrade", "--editable", ".[test]")
    session.install("--upgrade", "pytest")
    session.run(
        "pytest",
        "--ignore=setup.py",
        "--cov=servicex",
        "--cov-report=term-missing",
        "--cov-config=.coveragerc",
        "--cov-report=xml",
        *session.posargs,
    )


@nox.session(reuse_venv=True)
def coverage(session):
    """
    Generate coverage report
    """
    session.install("--upgrade", "pip")
    session.install("--upgrade", "coverage[toml]")

    session.run("coverage", "report")
    session.run("coverage", "xml")
    htmlcov_path = DIR / "htmlcov"
    if htmlcov_path.exists():
        session.log(f"rm -r {htmlcov_path}")
        shutil.rmtree(htmlcov_path)
    session.run("coverage", "html")


@nox.session
def build(session):
    """
    Build a sdist and wheel.
    """

    # cleanup previous build and dist dirs
    build_path = DIR.joinpath("build")
    if build_path.exists():
        shutil.rmtree(build_path)
    dist_path = DIR.joinpath("dist")
    if dist_path.exists():
        shutil.rmtree(dist_path)

    session.install("build")
    session.run("python", "-m", "build")
