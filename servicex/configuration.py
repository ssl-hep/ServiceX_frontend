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
from pathlib import Path
from typing import List, Optional, Dict

from pydantic import BaseModel, Field

import yaml


class Endpoint(BaseModel):
    endpoint: str
    name: str
    token: Optional[str]


class Configuration(BaseModel):
    api_endpoints: List[Endpoint]
    default_endpoint: Optional[str] = Field(alias="default-endpoint", default=None)
    cache_path: Optional[str] = Field(alias="cache-path", default=None)
    shortened_downloaded_filename: Optional[bool] = False

    class Config:
        allow_population_by_field_name = True

    def endpoint_dict(self) -> Dict[str, Endpoint]:
        return {endpoint.name: endpoint for endpoint in self.api_endpoints}

    @classmethod
    def read(cls, config_path: Optional[str] = None):
        r"""
        Read configuration from .servicex file.

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
            return Configuration(**yaml_config)
        else:
            path_extra = f"in {config_path}" if config_path else ""
            raise NameError("Can't find .servicex config file " + path_extra)

    @classmethod
    def _add_from_path(cls, path: Optional[Path] = None, walk_up_tree: bool = False):
        config = None
        if path:
            path.resolve()
            name = path.name
            dir = path.parent.resolve()
        else:
            name = ".servicex"
            dir = Path(os.getcwd())

        while True:
            f = dir / name
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
