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
from enum import Enum
from typing import Union, Optional, List
from pydantic import (
    BaseModel,
    Field,
    model_validator,
)
import logging

from servicex.dataset_identifier import (DataSetIdentifier, RucioDatasetIdentifier,
                                         FileListDataset)
from servicex.query_core import QueryStringGenerator
from servicex.models import ResultFormat

logger = logging.getLogger(__name__)


class Sample(BaseModel):
    Name: str
    Codegen: Optional[str] = None
    RucioDID: Optional[str] = None
    XRootDFiles: Optional[Union[str, List[str]]] = None
    Dataset: Optional[DataSetIdentifier] = None
    NFiles: Optional[int] = Field(default=None)
    Query: Optional[Union[str, QueryStringGenerator]] = Field(default=None)
    IgnoreLocalCache: bool = False

    model_config = {"arbitrary_types_allowed": True}

    @property
    def dataset_identifier(self) -> DataSetIdentifier:
        if self.Dataset:
            if self.NFiles:
                self.Dataset.num_files = self.NFiles
            return self.Dataset
        elif self.RucioDID:
            return RucioDatasetIdentifier(self.RucioDID, num_files=self.NFiles)
        elif self.XRootDFiles:
            return FileListDataset(self.XRootDFiles)
        else:  # pragma: no cover
            raise RuntimeError("No valid dataset found, somehow validation failed")

    @model_validator(mode="before")
    @classmethod
    def validate_did_xor_file(cls, values):
        """
        Ensure that only one of Dataset, RootFile, or RucioDID is specified.
        :param values:
        :return:
        """
        count = sum(["RucioDID" in values, "XRootDFiles" in values, "Dataset" in values])
        if count > 1:
            raise ValueError("Only specify one of Dataset, XRootDFiles, or RucioDID.")
        if count == 0:
            raise ValueError("Must specify one of Dataset, XRootDFiles, or RucioDID.")
        return values

    @model_validator(mode="before")
    @classmethod
    def truncate_long_sample_name(cls, values):
        """
        Truncate sample name to 128 characters if exceed
        Print warning message
        """
        if len(values["Name"]) > 128:
            logger.warning(f"Truncating Sample name to 128 characters for {values['Name']}")
            values["Name"] = values["Name"][0:128]
        return values


class General(BaseModel):
    class OutputFormatEnum(str, Enum):
        parquet = "parquet"
        root_ttree = "root-ttree"

        def to_ResultFormat(self) -> ResultFormat:
            """ This method is used to convert the OutputFormatEnum enum to the ResultFormat enum,
                which is what is actually used for the TransformRequest. This allows us to use
                different string values in the two enum classes to maintain backend compatibility
            """
            if self == self.parquet:
                return ResultFormat.parquet
            elif self == self.root_ttree:
                return ResultFormat.root_ttree
            else:  # pragma: no cover
                raise RuntimeError(f"Bad OutputFormatEnum {self}")

    class DeliveryEnum(str, Enum):
        LocalCache = "LocalCache"
        URLs = "URLs"

    Codegen: Optional[str] = None
    OutputFormat: OutputFormatEnum = Field(default=OutputFormatEnum.root_ttree)
    Delivery: DeliveryEnum = Field(default=DeliveryEnum.LocalCache)
    OutputDirectory: Optional[str] = None
    OutFilesetName: str = 'servicex_fileset'


# TODO: ServiceXSpec class has a field name General and it clashes with the class name General
# when it is called General() to initialize default values for General class
_General = General


class ServiceXSpec(BaseModel):
    General: _General = General()
    Sample: List[Sample]
    Definition: Optional[List] = None
