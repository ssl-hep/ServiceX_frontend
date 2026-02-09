# Copyright (c) 2026, IRIS-HEP
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# * Redistributions of source code must retain the above copyright notice, this
#   list of conditions and the following disclaimer.
#
# * Redistributions in binary form must reproduce the above copyright notice,
#   this list of conditions and the following disclaimer in the documentation
#   and/or other materials provided with the distribution.
#
# * Neither the name of the copyright holder nor the names of its
#   contributors may be used to endorse or promote products derived from
#   this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

import os
from pathlib import Path
from typing import Optional, Union
from servicex.servicex_client import GuardList
from servicex.query_cache import QueryCache
from servicex.configuration import Configuration
from servicex.models import TransformedResults


def read_dir(
    path: Optional[Union[str, Path]] = None,
    config_path: Optional[Union[str, Path]] = None,
    local_preferred: bool = True,
) -> dict[str, GuardList]:
    r"""
    Return the transformation output results previously saved to a directory. The format
    is the same as in the deliver() call.

    This function will not trigger any additional downloads. Either local copies or URLs will
    be returned, according to the policy set by `local_preferred`.

    If multiple results with the same name were saved to the same directory, only the most
    recent is returned.

    :param path: The directory path to read from, as a string or a :py:class:`Path` object.
            If None, the default cache path will be used from the ServiceX configuration.
    :param config_path: If `path` is :py:const:`None`, this determines the path from which the
            ServiceX configuration will be loaded to determine the default cache path.
            :py:const:`None` will search the default paths.
    :param local_preferred: Determines the behavior if both downloaded files and remote URL
            information is present. If :py:const:`True` (default) then downloaded copies of
            files are preferred, if available. If :py:const:`False` then remote URLs will
            be preferred, if available.
    :return: A dictionary mapping the name of each :py:class:`Sample` to a :py:class:`.GuardList`
            with the file names or URLs for the outputs.
    """

    if path is None:
        # Load default
        config = Configuration.read(
            str(config_path) if config_path is not None else None
        )
    else:
        # create a simple Configuration object with just the cache path
        # if directory cache non-existent, do not trigger its creation!
        if not os.path.isdir(path):
            raise ValueError(f"{path} is not an existing directory")
        if not (Path(path) / ".servicex" / "db.json").exists():
            raise RuntimeError(
                f"{path} does not contain a valid ServiceX download area"
            )
        config = Configuration(api_endpoints=[], cache_path=str(path))
    cache = QueryCache(config)
    transforms = cache.cached_queries()
    latest_transforms: dict[str, TransformedResults] = {}
    for transform in transforms:
        if transform.title not in latest_transforms:
            latest_transforms[transform.title] = transform
        else:
            current = latest_transforms[transform.title]
            if transform.submit_time > current.submit_time:
                latest_transforms[transform.title]

    if local_preferred:
        return {
            _[0]: GuardList(_[1].file_list if _[1].file_list else _[1].signed_url_list)
            for _ in latest_transforms.items()
        }
    else:
        return {
            _[0]: GuardList(
                _[1].signed_url_list if _[1].signed_url_list else _[1].file_list
            )
            for _ in latest_transforms.items()
        }
