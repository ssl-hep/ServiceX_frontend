# Copyright (c) 2022, IRIS-HEP
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
import tempfile
from pathlib import Path, PurePath
from typing import List, Optional, Dict

from pydantic import BaseModel, Field, AliasChoices, model_validator

import yaml


class Endpoint(BaseModel):
    endpoint: str
    name: str
    token: Optional[str] = ""


class Configuration(BaseModel):
    api_endpoints: List[Endpoint]
    default_endpoint: Optional[str] = Field(alias="default-endpoint", default=None)
    cache_path: Optional[str] = Field(
        validation_alias=AliasChoices("cache-path", "cache_path"), default=None
    )

    shortened_downloaded_filename: Optional[bool] = False

    @model_validator(mode="after")
    def expand_cache_path(self):
        """
        Expand the cache path to a full path, and create it if it doesn't exist.
        Expand ${USER} to be the user name on the system. Works for windows, too.
        :param v:
        :return:
        """
        # create a folder inside the tmp directory if not specified in cache_path
        if not self.cache_path:
            self.cache_path = "/tmp/servicex_${USER}"

        s_path = os.path.expanduser(self.cache_path)

        # If they have tried to use the USER or UserName as an expansion, and it has failed, then
        # translate it to maintain harmony across platforms.
        if "${USER}" in s_path and "UserName" in os.environ:
            s_path = s_path.replace("${USER}", os.environ["UserName"])
        if "${USER}" in s_path and "USER" in os.environ:
            s_path = s_path.replace("${USER}", os.environ["USER"])

        p_p = PurePath(s_path)
        if len(p_p.parts) > 1 and p_p.parts[1] == "tmp":
            p = Path(tempfile.gettempdir()) / Path(*p_p.parts[2:])
        else:
            p = Path(p_p)
        p.mkdir(exist_ok=True, parents=True)

        self.cache_path = p.as_posix()
        return self

    model_config = {"populate_by_name": True}

    def endpoint_dict(self) -> Dict[str, Endpoint]:
        return {endpoint.name: endpoint for endpoint in self.api_endpoints}

    @classmethod
    def read(cls, config_path: Optional[str] = None):
        r"""
        Read configuration from .servicex or servicex.yaml file.
        :param config_path: If provided, use this as the path to the .servicex file.
                            Otherwise, search, starting from the current working directory
                            and look in enclosing directories
        :return: Populated configuration object
        """
        if config_path:
            yaml_config = cls._add_from_path(Path(config_path), walk_up_tree=False)
        else:
            yaml_config = cls._add_from_path(walk_up_tree=True)

        if yaml_config:
            return Configuration.model_validate(yaml_config)
        else:
            path_extra = f"in {config_path}" if config_path else ""
            raise NameError(
                "Can't find .servicex or servicex.yaml config file " + path_extra
            )

    @classmethod
    def _add_from_path(cls, path: Optional[Path] = None, walk_up_tree: bool = False):
        config = None
        if path:
            path.resolve()
            name = path.name
            dir = path.parent.resolve()
            alt_name = None
        else:
            name = ".servicex"
            alt_name = "servicex.yaml"
            dir = Path(os.getcwd())

        while True:
            f = dir / name  # user-defined path or .servicex
            if f.exists():
                with open(f) as config_file:
                    config = yaml.safe_load(config_file)
                    break

            if alt_name:
                f = dir / alt_name  # if neither option above, find servicex.yaml
                if f.exists():
                    with open(f) as config_file:
                        config = yaml.safe_load(config_file)
                        break

            if not walk_up_tree:
                break

            if dir == dir.parent:
                break

            dir = dir.parent

        return config
