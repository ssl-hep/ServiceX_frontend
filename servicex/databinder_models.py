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
from enum import Enum
from typing import Union, Optional, List
from pydantic import (
    BaseModel,
    Field,
    model_validator,
)

from servicex.dataset_identifier import RucioDatasetIdentifier, FileListDataset
from servicex.query_core import Query as SXQuery, QueryStringGenerator
from servicex.models import ResultFormat


class Sample(BaseModel):
    Name: str
    Codegen: Optional[str] = None
    RucioDID: Optional[str] = None
    XRootDFiles: Optional[Union[str, List[str]]] = None
    NFiles: Optional[int] = Field(default=None)
    Query: Optional[Union[str, SXQuery, QueryStringGenerator]] = Field(default=None)
    IgnoreLocalCache: bool = False

    model_config = {"arbitrary_types_allowed": True}

    @property
    def dataset_identifier(self):
        if self.RucioDID:
            return RucioDatasetIdentifier(self.RucioDID, num_files=self.NFiles or 0)
        elif self.XRootDFiles:
            return FileListDataset(self.XRootDFiles)

    @model_validator(mode="before")
    @classmethod
    def validate_did_xor_file(cls, values):
        """
        Ensure that only one of RootFile or RucioDID is specified.
        :param values:
        :return:
        """
        if "XRootDFiles" in values and "RucioDID" in values:
            raise ValueError("Only specify one of XRootDFiles or RucioDID, not both.")
        if "XRootDFiles" not in values and "RucioDID" not in values:
            raise ValueError("Must specify one of XRootDFiles or RucioDID.")
        return values


class General(BaseModel):
    class OutputFormatEnum(str, Enum):
        parquet = "parquet"
        root = "root-file"

    class DeliveryEnum(str, Enum):
        LocalCache = "LocalCache"
        SignedURLs = "SignedURLs"

    Codegen: Optional[str] = None
    OutputFormat: ResultFormat = (
        Field(default=ResultFormat.root, pattern="^(parquet|root-file)$")
    )  # NOQA F722

    Delivery: DeliveryEnum = Field(
        default=DeliveryEnum.LocalCache, pattern="^(LocalCache|SignedURLs)$"
    )  # NOQA F722

    OutputDirectory: Optional[str] = None
    OutFilesetName: str = 'servicex_fileset'


# TODO: ServiceXSpec class has a field name General and it clashes with the class name General
# when it is called General() to initialize default values for General class
_General = General


class ServiceXSpec(BaseModel):
    General: _General = General()
    Sample: List[Sample]
    Definition: Optional[List] = None
