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

    reco_yaml: Optional[Union[Path, str]] = None
    """Path to the reco.yaml"""
    parton_yaml: Optional[Union[Path, str]] = None
    """Path to the parton.yaml"""
    particle_yaml: Optional[Union[Path, str]] = None
    """Path to the particle.yaml"""
    max_events: Optional[int] = -1
    """Number of events to process"""
    parton: Optional[bool] = False
    """Toggles the parton-level analysis"""
    particle: Optional[bool] = False
    """Toggles the particle-level analysis"""
    no_reco: Optional[bool] = False
    """Toggles off the detector-level analysis"""
    no_systematics: Optional[bool] = True
    """Toggles off the computation of systematics"""
    no_filter: Optional[bool] = False
    """Save all events regardless of analysis filters (still saves the decision)"""

    @pydantic.model_validator(mode="after")
    def check_reco_yaml(self):
        if self.reco_yaml is None and self.no_reco is False:
            raise ValueError("reco is enabled but reco.yaml is missing!")
        return self

    @pydantic.model_validator(mode="after")
    def no_input_yaml(self):
        if (
            self.reco_yaml is None
            and self.parton_yaml is None
            and self.particle_yaml is None
        ):
            raise ValueError("No yaml provided!")
        return self

    @pydantic.model_validator(mode="after")
    def no_parton_yaml(self):
        if self.parton_yaml is None and self.parton is True:
            raise ValueError("parton is set to True but no parton.yaml provided!")
        return self

    @pydantic.model_validator(mode="after")
    def no_paricle_yaml(self):
        if self.particle_yaml is None and self.particle is True:
            raise ValueError("particle is set to True but no particle.yaml provided!")
        return self

    @pydantic.model_validator(mode="after")
    def no_run(self):
        if self.no_reco is True and self.particle is False and self.parton is False:
            raise ValueError("Wrong configuration - no reco, no particle, no parton!")
        return self

    def generate_selection_string(self):
        import yaml
        import json

        recoYaml = None
        if self.reco_yaml:
            with open(self.reco_yaml, "r") as reco_file:
                recoYaml = yaml.safe_load(reco_file)

        partonYaml = None
        if self.parton_yaml:
            with open(self.parton_yaml, "r") as parton_file:
                partonYaml = yaml.safe_load(parton_file)

        particleYaml = None
        if self.particle_yaml:
            with open(self.particle_yaml, "r") as particle_file:
                particleYaml = yaml.safe_load(particle_file)

        query = {
            "RecoYAML": recoYaml,
            "PartonYAML": partonYaml,
            "ParticleYAML": particleYaml,
            "NEvents": self.max_events,
            "RunParton": self.parton,
            "RunParticle": self.particle,
            "NoReco": self.no_reco,
            "RunSystematics": self.no_systematics,
            "NoFilter": self.no_filter,
        }

        return json.dumps(query)

    @classmethod
    def from_yaml(cls, _, node):
        code = node.value
        import json

        queries = json.loads(code)
        q = cls(queries)
        return q
