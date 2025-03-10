# Copyright (c) 2025, IRIS-HEP
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

# pydantic 2 API

import pydantic
from pathlib import Path

# from servicex.models import DocStringBaseModel
from typing import Optional, Union
from ..query_core import QueryStringGenerator


@pydantic.dataclasses.dataclass
class TopCPQuery(QueryStringGenerator):
    yaml_tag = "!TopCP"
    default_codegen = "topcp"

    reco: Optional[Union[Path, str]] = None
    """Path to the reco.yaml"""
    parton: Optional[Union[Path, str]] = None
    """Path to the parton.yaml"""
    particle: Optional[Union[Path, str]] = None
    """Path to the particle.yaml"""
    max_events: Optional[int] = -1
    """Number of events to process"""
    run_parton: Optional[bool] = False
    """Toggles the parton-level analysis"""
    run_particle: Optional[bool] = False
    """Toggles the particle-level analysis"""
    no_reco: Optional[bool] = False
    """Toggles off the detector-level analysis"""
    no_systematics: Optional[bool] = True
    """Toggles off the computation of systematics"""
    no_filter: Optional[bool] = False
    """Save all events regardless of analysis filters (still saves the decision)"""

    @pydantic.model_validator(mode="after")
    def check_reco(self):
        if self.reco is None and self.no_reco is False:
            raise ValueError("reco is enabled but reco.yaml is missing!")
        return self

    @pydantic.model_validator(mode="after")
    def no_input_yaml(self):
        if self.reco is None and self.parton is None and self.particle is None:
            raise ValueError("No yaml provided!")
        return self

    @pydantic.model_validator(mode="after")
    def no_parton(self):
        if self.parton is None and self.run_parton is True:
            raise ValueError("parton is set to True but no parton.yaml provided!")
        return self

    @pydantic.model_validator(mode="after")
    def no_paricle_yaml(self):
        if self.particle is None and self.run_particle is True:
            raise ValueError("particle is set to True but no particle.yaml provided!")
        return self

    @pydantic.model_validator(mode="after")
    def no_run(self):
        if (
            self.no_reco is True
            and self.run_particle is False
            and self.run_parton is False
        ):
            raise ValueError("Wrong configuration - no reco, no particle, no parton!")
        return self

    def generate_selection_string(self):
        import yaml
        import json

        recoYaml = None
        if self.reco:
            with open(Path(self.reco), "r") as reco_file:
                recoYaml = yaml.safe_load(reco_file)

        partonYaml = None
        if self.parton:
            with open(Path(self.parton), "r") as parton_file:
                partonYaml = yaml.safe_load(parton_file)

        particleYaml = None
        if self.particle:
            with open(Path(self.particle), "r") as particle_file:
                particleYaml = yaml.safe_load(particle_file)

        query = {
            "reco": recoYaml,
            "parton": partonYaml,
            "particle": particleYaml,
            "max_events": self.max_events,
            "run_parton": self.run_parton,
            "run_particle": self.run_particle,
            "no_reco": self.no_reco,
            "no_systematics": self.no_systematics,
            "no_filter": self.no_filter,
        }
        return json.dumps(query)

    @classmethod
    def from_yaml(cls, _, node):
        code = node.value
        import re

        # Use regex to split key-value pairs
        matches = re.findall(r'(\w+)="?(.*?)"?(?:,|$)', code)

        # Convert to dictionary
        result = {key: value for key, value in matches}

        print(result)
        print(type(result))
        q = cls(**result)
        return q
