# Copyright (c) 2023, IRIS-HEP
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
from ccorp.ruamel.yaml.include import YAML
import pathlib
from typing import Any, Dict, Union
import rich
from .. import ServiceXSpec


yaml = YAML()

def configure_loaders():
    import sys
    if sys.version_info < (3, 10):
        from importlib_metadata import entry_points
    else:
        from importlib.metadata import entry_points

    plugins = entry_points(group='servicex.queries')
    for _ in plugins:
        yaml.register_class(_.load())


def load_databinder_config(input_config:
                           Union[str, pathlib.Path, Dict[str, Any]]
                           ) -> ServiceXSpec:
    """
    Loads, validates, and returns DataBinder configuration.
    The order of function call matters.
    Args:
        input_config (Union[str, pathlib.Path, Dict[str, Any]]):
            path to config file or config as dict
    Returns:
        Dict[str, Any]: configuration
    """
    def prepare_config(config):
        # _set_default_values(config) # Handled by pydantic
        # ndef = _replace_definition_in_sample_block(config) # handled by YAML
        # rich.print(f"Replaced {ndef} Option values from Definition block")
        return ServiceXSpec(**config)

    if isinstance(input_config, Dict):
        return prepare_config(input_config)
    else:
        file_path = pathlib.Path(input_config)
        rich.print(f"Loading DataBinder config file: {file_path}")
        configure_loaders()
        config = yaml.load(file_path)
        return prepare_config(config)
