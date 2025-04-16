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
from servicex.topcp.topcp import TopCPQuery
import tempfile
from pathlib import Path
import os
import json
import pytest


def test_default_keys():
    with tempfile.NamedTemporaryFile(mode="w", delete=False) as fp:
        test_logfile_path = Path(fp.name)
        fp.write(
            """
CommonServices:
  runSystematics: False
        """
        )
        fp.close()

        topcp_query = TopCPQuery(reco=test_logfile_path)
        query_string = topcp_query.generate_selection_string()
        query = json.loads(query_string)

        query_keys = [
            "reco",
            "parton",
            "particle",
            "max_events",
            "no_systematics",
            "no_filter",
        ]
        for key in query_keys:
            assert key in query, f"Missing key: {key}"
        os.remove(test_logfile_path)


def test_yaml_serialization():
    with (
        tempfile.NamedTemporaryFile(mode="w", delete=False) as f1,
        tempfile.NamedTemporaryFile(mode="w", delete=False) as f2,
    ):
        test_parton_yaml = Path(f1.name)
        f1.write(
            """
CommonServices:
  systematicsHistogram: 'listOfSystematicsPartonLevel'
  runSystematics: True

GeneratorLevelAnalysis: {}

PartonHistory:
  - histories: 'Ttbar'
        """
        )
        f1.close()
        test_particle_yaml = Path(f2.name)
        f2.write(
            """
CommonServices:
  systematicsHistogram: 'listOfSystematicsParticleLevel'
  runSystematics: True

GeneratorLevelAnalysis: {}

PL_Electrons:
  notFromTau: False
  PtEtaSelection:
    useDressedProperties: True
    minPt: 25000.0
    maxEta: 2.5
        """
        )
        f2.close()

        topcp_query = TopCPQuery(parton=test_parton_yaml, particle=test_particle_yaml)
        query_string = topcp_query.generate_selection_string()
        query = json.loads(query_string)
        assert (
            "systematicsHistogram: 'listOfSystematicsParticleLevel'\n"
            in query["particle"]
        )
        os.remove(test_parton_yaml)
        os.remove(test_particle_yaml)


def test_no_yaml():
    with pytest.raises(ValueError):
        TopCPQuery()
