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
import pathlib
from typing import Any, Dict, Union, List

from servicex.dataset_identifier import RucioDatasetIdentifier, FileListDataset
from servicex.servicex_client import ServiceXClient
# from servicex.servicex_adapter import ServiceXAdapter
from servicex.models import ResultFormat

from servicex.databinder.databinder_configuration import load_databinder_config


class DataBinderRequests:
    """
    Prepare ServiceX requests from DataBinder configuration
    """

    def __init__(self, config: Union[str, pathlib.Path, Dict[str, Any]]):
        self._config = load_databinder_config(config)

    def get_requests(self) -> List:
        list_requests = []
        for sample in self._config['Sample']:
            list_requests.append(self._build_request(sample))
        # flatten nested lists for samples with more than two Rucio datasets
        flist_requests = [request for x in list_requests for request in x]
        return flist_requests

    def _build_request(self, sample):
        """
        Return a list containing ServiceX request(s) of the given sample
        """
        requests_sample = []

        def _get_client():
            if 'http' in self._config['General']['ServiceX']:
                url = self._config['General']['ServiceX']
                backend = None
            else:
                url = None
                backend = self._config['General']['ServiceX']
            return ServiceXClient(backend=backend, url=url)

        def _get_input_source(sample):
            if 'RucioDID' in sample.keys():
                nfiles = None
                if 'NFiles' in sample.keys():
                    nfiles = int(sample['NFiles'])
                input_source = RucioDatasetIdentifier(
                    str(sample['RucioDID']),
                    num_files=nfiles
                )
            elif 'XRootDFiles' in sample.keys():
                input_source = FileListDataset(
                    [file.strip() for file in sample['XRootDFiles'].split(",")]
                )
            else:
                raise TypeError(
                    f"Unknown input source in Sample {sample['Name']}"
                )
            return input_source

        def _get_servicex_dataset(sample):
            if sample['Codegen'] == 'python':
                return _get_client().python_dataset(
                    dataset_identifier=_get_input_source(sample),
                    title=sample['Name'],
                    codegen="python",
                    ignore_cache=True,
                    result_format=ResultFormat.parquet
                )
            else:
                raise TypeError(
                    f"Unknown code-generator in Sample {sample['Name']}"
                )

        def _servicex_dataset_query(sample):
            if sample['Codegen'] == 'python':
                query = sample['Function']
                return _get_servicex_dataset(sample).with_uproot_function(
                    query
                )

        requests_sample.append(
            {
                "Name": sample['Name'],
                "ServiceXDataset": _servicex_dataset_query(sample)
            }
        )
        return requests_sample
