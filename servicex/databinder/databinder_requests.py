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
# import pathlib
from typing import List
from rich.progress import Progress, TextColumn, BarColumn, MofNCompleteColumn, \
    TimeRemainingColumn

from servicex.dataset_identifier import RucioDatasetIdentifier, FileListDataset
from servicex.servicex_client import ServiceXClient, ServiceXSpec
from ..databinder_models import Sample


class DataBinderRequests:
    """
    Prepare ServiceX requests from DataBinder configuration
    """

    def __init__(self, updated_config: ServiceXSpec):
        self._config = updated_config
        self._client = self._get_client()
        self._progress = Progress(
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
            TimeRemainingColumn(compact=True, elapsed_when_finished=True)
        )

    def _get_client(self):
        if 'http' in self._config.General.ServiceX:
            return ServiceXClient(backend=None, url=self._config.General.ServiceX)
        else:
            return ServiceXClient(backend=self._config.General.ServiceX, url=None)

    def get_requests(self) -> List:
        list_requests = []
        for sample in self._config.Sample:
            list_requests.append(self._build_request(sample))
        # flatten nested lists for samples with more than two Rucio datasets
        flist_requests = [request for x in list_requests for request in x]
        return flist_requests

    def _build_request(self, sample: Sample):
        """
        Return a list containing ServiceX request(s) of the given sample
        """
        requests_sample = []

        def _get_input_source(sample):
            if sample.RucioDID is not None:
                nfiles = None
                if sample.NFiles is not None:
                    nfiles = int(sample.NFiles)
                input_source = RucioDatasetIdentifier(
                    str(sample.RucioDID),
                    num_files=nfiles or 0
                )
            elif sample.XRootDFiles is not None:
                input_source = FileListDataset(
                    [file.strip() for file in sample.XRootDFiles.split(",")]
                )
            else:
                raise TypeError(
                    f"Unknown input source in Sample {sample.Name}"
                )
            return input_source

        def _set_result_format():
            return self._config.General.OutputFormat

        def _get_servicex_dataset(sample):
            if sample.Codegen == 'python':
                return self._client.python_dataset(
                    dataset_identifier=_get_input_source(sample),
                    title=sample.Name,
                    codegen="python",
                    ignore_cache=sample.IgnoreLocalCache,
                    result_format=_set_result_format()
                )
            else:
                raise TypeError(
                    f"Unknown code-generator in Sample {sample.Name}"
                )

        def _servicex_dataset_query(sample):
            sample.Query.set_result_format(_set_result_format())  # why like this???
            sample.Query.dataset_identifier = _get_input_source(sample)
            sample.Query.title = sample.Name
            sample.Query.codegen = sample.Codegen if sample.Codegen else sample.Query.codegen
            sample.Query.cache = self._client.query_cache
            sample.Query.servicex = self._client.servicex
            sample.Query.configuration = self._client.config
            return sample.Query

        requests_sample.append(
            {
                "sample_name": sample.Name,
                "delivery": self._config.General.Delivery.lower(),
                "ds_query": _servicex_dataset_query(sample)
            }
        )
        return requests_sample