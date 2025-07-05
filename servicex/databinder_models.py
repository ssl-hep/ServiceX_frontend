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
import hashlib
from typing import Union, Optional, List
from pydantic import (
    Field,
    model_validator,
    field_validator,
)
import logging

from servicex.dataset_identifier import (
    DataSetIdentifier,
    RucioDatasetIdentifier,
    FileListDataset,
)
from servicex.query_core import QueryStringGenerator
from servicex.models import ResultFormat, DocStringBaseModel

logger = logging.getLogger(__name__)


class Sample(DocStringBaseModel):
    """
    Represents a single transform request within a larger submission.
    """

    model_config = {"use_attribute_docstrings": True}

    Name: str
    """
    The name of the sample. This makes it easier to identify the sample
    in the output.
    """

    Dataset: Optional[DataSetIdentifier] = None
    """
    Dataset identifier for the sample
    """

    NFiles: Optional[int] = Field(default=None)
    """
    Limit the Number of files to be used in the sample.
    The DID Finder will guarantee the same files will be returned
    between each invocation. Set to `None` to use all files.
    """

    Query: Optional[Union[str, QueryStringGenerator]] = Field(default=None)
    """
    Query string or query generator for the sample.
    """

    IgnoreLocalCache: bool = False
    """
    Flag to ignore local cache.
    """

    Codegen: Optional[str] = None
    """
    Code generator name, if applicable. Generally users don't need to specify this. It is
    implied by the query class
    """

    RucioDID: Optional[str] = Field(default=None, deprecated="Use 'Dataset' instead.")
    """
    Rucio Dataset Identifier, if applicable.
        Deprecated: Use 'Dataset' instead.
    """

    XRootDFiles: Optional[Union[str, List[str]]] = Field(default=None, deprecated=True)
    """
    XRootD file(s) associated with the sample.
            Deprecated: Use 'Dataset' instead.
    """

    model_config = {"arbitrary_types_allowed": True}

    @property
    def dataset_identifier(self) -> DataSetIdentifier:
        """
        Access the dataset identifier for the sample.
        """
        if self.Dataset:
            if self.NFiles is not None:
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
        count = sum(
            ["RucioDID" in values, "XRootDFiles" in values, "Dataset" in values]
        )
        if count > 1:
            raise ValueError("Only specify one of Dataset, XRootDFiles, or RucioDID.")
        if count == 0:
            raise ValueError("Must specify one of Dataset, XRootDFiles, or RucioDID.")
        return values

    @model_validator(mode="after")
    def validate_nfiles_is_not_zero(self):
        """
        Ensure that NFiles is not set to zero
        """
        if self.dataset_identifier.num_files == 0:
            raise ValueError("NFiles cannot be set to zero for a dataset.")
        return self

    def validate_title(self, length: Optional[int]) -> None:
        """
        Logic for adjusting length of the title
        """
        if length is None:
            # we adopt pre-3.2.0 behavior: truncate to 128 characters
            if len(self.Name) > 128:
                logger.warning(
                    f"Truncating Sample name to 128 characters for {self.Name}"
                )
                self.Name = self.Name[:128]
                logger.warning(f"New name is {self.Name}")
        else:
            if len(self.Name) > length:
                logger.error(f"Sample name {self.Name} over the limit ({length})")
                raise ValueError(f"Sample name {self.Name} length too long")

    @property
    def hash(self):
        sha = hashlib.sha256(
            str(
                [
                    self.dataset_identifier.hash,
                    self.NFiles,
                    (
                        self.Query
                        if (not self.Query or isinstance(self.Query, str))
                        else self.Query.generate_selection_string()
                    ),
                    self.Codegen,
                ]
            ).encode("utf-8")
        )
        return sha.hexdigest()


class General(DocStringBaseModel):
    """
    Represents a group of samples to be transformed together.
    """

    model_config = {"use_attribute_docstrings": True}

    class OutputFormatEnum(str, Enum):
        """
        Specifies the output format for the transform request.
        """

        parquet = "parquet"
        """
        Save the output as
        a parquet file https://parquet.apache.org/
        """

        root_ttree = "root-ttree"
        """
        Save the output as
        a ROOT TTree https://root.cern.ch/doc/master/classTTree.html
        """

        root_rntuple = "root-rntuple"
        """
        Save the output as an RNtuple https://root.cern/doc/master/classROOT_1_1RNTuple.html
        """

        def to_ResultFormat(self) -> ResultFormat:
            """This method is used to convert the OutputFormatEnum enum to the ResultFormat enum,
            which is what is actually used for the TransformRequest. This allows us to use
            different string values in the two enum classes to maintain backend compatibility
            """
            if self == self.parquet:
                return ResultFormat.parquet
            elif self == self.root_ttree:
                return ResultFormat.root_ttree
            elif self == self.root_rntuple:
                return ResultFormat.root_rntuple
            else:  # pragma: no cover
                raise RuntimeError(f"Bad OutputFormatEnum {self}")

    class DeliveryEnum(str, Enum):
        LocalCache = "LocalCache"
        """
        Download the files to the local computer and store them in the cache.
        Transform requests will return paths to these files in the cache
        """

        URLs = "URLs"
        """
        Return URLs to the files stored in the ServiceX object store
        """

    Codegen: Optional[str] = None
    """
    Code generator name to be applied across all of the samples, if applicable.
    Generally users don't need to specify this. It is implied by the query class
    """
    OutputFormat: OutputFormatEnum = Field(default=OutputFormatEnum.root_ttree)
    """
    Output format for the transform request.
    """

    Delivery: DeliveryEnum = Field(default=DeliveryEnum.LocalCache)
    """
    Specifies the delivery method for the output files.
    """

    OutputDirectory: Optional[str] = None
    """
    Directory to output a yaml file describing the output files.
    """

    OutFilesetName: str = "servicex_fileset"
    """
    Name of the yaml file that will be created in the output directory.
    """

    IgnoreLocalCache: bool = False
    """
    Flag to ignore local cache for all samples.
    """


# TODO: ServiceXSpec class has a field name General and it clashes with the class name General
# when it is called General() to initialize default values for General class
_General = General


class ServiceXSpec(DocStringBaseModel):
    """
    ServiceX Submission Specification - pass this into the ServiceX `deliver` function
    """

    model_config = {"use_attribute_docstrings": True}

    General: _General = General()
    """
    General settings for the transform request
    """

    Sample: List[Sample]
    """
    List of samples to be transformed
    """

    Definition: Optional[List] = None
    """
    Any reusable definitions that are needed for the transform request
    """

    @field_validator("Sample", mode="after")
    @classmethod
    def validate_unique_sample(cls, v):
        hash_set = set()
        for sample in v:
            if sample.hash in hash_set:
                raise RuntimeError(f"Duplicate samples detected: {sample.Name}")
            hash_set.add(sample.hash)
        return v
