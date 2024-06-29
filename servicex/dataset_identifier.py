# Copyright (c) 2024, IRIS-HEP
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
from typing import List, Union, Optional

from servicex.models import TransformRequest


class DataSetIdentifier:
    r"""
    Base class for specifying the dataset to transform. This can either be a list of
    xRootD URIs or a rucio DID
    """
    def __init__(self, scheme: str, dataset: str, num_files: Optional[int] = None):
        self.scheme = scheme
        self.dataset = dataset
        self.num_files = num_files

    @property
    def did(self):
        num_files_arg = f"?files={self.num_files}" if self.num_files is not None else ""
        return f"{self.scheme}://{self.dataset}{num_files_arg}"

    def populate_transform_request(self, transform_request: TransformRequest) -> None:
        transform_request.did = self.did
        transform_request.file_list = None


class RucioDatasetIdentifier(DataSetIdentifier):
    def __init__(self, dataset: str, num_files: Optional[int] = None):
        r"""
        Rucio Dataset - this will be looked up using the Rucio data management
        service.

        :param dataset: The rucio DID - this can be a dataset or a container of datasets.
        :param num_files: Maximum number of files to return. This is useful during development
            to perform quick runs. ServiceX is careful to make sure it always
            returns the same subset of files.

        """
        super().__init__("rucio", dataset, num_files=num_files)

    yaml_tag = '!Rucio'

    @classmethod
    def from_yaml(cls, _, node):
        return cls(node.value)


class FileListDataset(DataSetIdentifier):
    def __init__(self, files: Union[List[str], str]):
        r"""
        Dataset specified as a list of XRootD URIs.

        :param files: Either a list of URIs or a single URI string
        """
        self.files: List[str]
        if isinstance(files, str):
            self.files = [files]
        else:
            self.files = files

    def populate_transform_request(self, transform_request: TransformRequest) -> None:
        transform_request.file_list = self.files
        transform_request.did = None

    @property
    def did(self):
        return None

    yaml_tag = '!FileList'

    @classmethod
    def from_yaml(cls, constructor, node):
        return cls(constructor.construct_sequence(node))


class CERNOpenDataDatasetIdentifier(DataSetIdentifier):
    def __init__(self, dataset: int, num_files: Optional[int] = None):
        r"""
        CERN Open Data Dataset - this will be looked up using the CERN Open Data DID finder.

        :param dataset: The dataset ID - this is an integer.
        :param num_files: Maximum number of files to return. This is useful during development
            to perform quick runs. ServiceX is careful to make sure it always
            returns the same subset of files.

        """
        super().__init__("cernopendata", f'{dataset}', num_files=num_files)

    yaml_tag = '!CERNOpenData'

    @classmethod
    def from_yaml(cls, _, node):
        return cls(int(node.value))
