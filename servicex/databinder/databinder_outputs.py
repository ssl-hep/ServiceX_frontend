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
import yaml
from pathlib import Path
from typing import Dict, Any
from rich import print
# from rich.panel import Panel


class OutputHandler:
    def __init__(self, updated_config: Dict[str, Any]):
        self._config = updated_config
        self.out_dict = self._initialize_out_dict()
        self._out_path = self._get_out_dir()

    def _initialize_out_dict(self):
        return {"general_info": {}, "samples": {}}

    def _get_out_dir(self):
        """
        Get output directory
        """
        if 'OutputDirectory' in self._config['General'].keys():
            self.output_path = Path(
                self._config['General']['OutputDirectory']
            ).absolute()
            self.output_path.mkdir(parents=True, exist_ok=True)
        else:
            self.output_path = Path().absolute()

    def update_out_dict(self, delivery: str, results):
        """
        Update output dict
        """
        if delivery == "objectstore":
            self.out_dict['samples'].update(
                {results.title: results.signed_url_list}
            )
        else:
            self.out_dict['samples'].update(
                {results.title: results.file_list}
            )

    def write_out_dict(self):
        file_out_paths = \
            (f"{self.output_path}/"
             f"{self._config['General']['OutFilesetName']}.yml")
        with open(file_out_paths, 'w') as f:
            yaml.dump(self.out_dict, f, default_flow_style=False)

    def copy_to_destination(self, delivery: str, results):
        sample_name = f"[bold red]{results.title}[/bold red]"
        if delivery == "objectstore":
            if len(results.signed_url_list):
                print(f"{sample_name} is available at the object store")
            else:
                print(f"Failed to deliver {sample_name}")
        else:
            if len(results.file_list):
                print(f"{sample_name} is delivered to the local cache directory")
            else:
                print(f"Failed to deliver {sample_name}")
