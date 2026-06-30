from importlib.metadata import version as _pkg_version

release = _pkg_version("servicex")
version = ".".join(release.split(".")[:2])
