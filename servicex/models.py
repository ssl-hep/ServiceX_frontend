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
import hashlib
from datetime import datetime
from enum import Enum

from pydantic import BaseModel, Field, field_validator
from typing import List, Optional, Any


def _get_typename(typeish) -> str:
    return typeish.__name__ if isinstance(typeish, type) else str(typeish)


def _generate_model_docstring(model: type) -> str:
    NL = "\n"
    return "\n".join(
        [(model.__doc__ if model.__doc__ else model.__name__).strip(), "", "Args:"]
        + [
            f"    {field}: ({_get_typename(info.annotation)}) "
            f'{info.description.replace(NL, NL + " " * 8) if info.description else ""}'
            for field, info in model.model_fields.items()
        ]
    )


class DocStringBaseModel(BaseModel):
    """Class to autogenerate a docstring for a Pydantic model"""

    @classmethod
    def __pydantic_init_subclass__(cls, **kwargs: Any):
        super().__pydantic_init_subclass__(**kwargs)
        # There is currently no good way of knowing we are building within Sphinx.
        # Use a hacky workaround but monitor https://github.com/sphinx-doc/sphinx/issues/9805
        import os

        if "IN_SPHINX_BUILD" not in os.environ:
            cls.__doc__ = _generate_model_docstring(cls)


class ResultDestination(str, Enum):
    r"""
    Direct the output to object store or posix volume
    """

    object_store = "object-store"
    volume = "volume"


class ResultFormat(str, Enum):
    r"""
    Specify the file format for the generated output
    """

    parquet = "parquet"
    root_ttree = "root-file"
    root_rntuple = "root-rntuple"


class Status(str, Enum):
    r"""
    Status of a submitted transform
    """

    complete = ("Complete",)
    fatal = ("Fatal",)
    canceled = ("Canceled",)
    submitted = ("Submitted",)
    looking = ("Lookup",)
    pending = "Pending Lookup"
    running = "Running"


class TransformRequest(DocStringBaseModel):
    r"""
    Transform request sent to ServiceX
    """

    title: Optional[str] = None
    did: Optional[str] = None
    file_list: Optional[List[str]] = Field(default=None, alias="file-list")
    selection: str
    codegen: str
    result_destination: ResultDestination = Field(
        serialization_alias="result-destination"
    )
    result_format: ResultFormat = Field(serialization_alias="result-format")

    model_config = {"populate_by_name": True, "use_attribute_docstrings": True}

    def compute_hash(self):
        r"""
        Compute a hash for this submission. Only include properties that impact the result
        so we have maximal ability to reuse transforms

        :return: SHA256 hash of request
        """
        sha = hashlib.sha256(
            str(
                [
                    self.did,
                    self.selection,
                    None,  # was tree_name
                    self.codegen,
                    None,  # was image
                    self.result_format.name,
                    sorted(self.file_list) if self.file_list else None,
                ]
            ).encode("utf-8")
        )
        return sha.hexdigest()


class TransformStatus(DocStringBaseModel):
    r"""
    Status object returned by servicex
    """

    model_config = {"use_attribute_docstrings": True}

    request_id: str
    did: str
    title: Optional[str] = None
    selection: str
    tree_name: Optional[str] = Field(validation_alias="tree-name")
    image: str
    result_destination: ResultDestination = Field(validation_alias="result-destination")
    result_format: ResultFormat = Field(validation_alias="result-format")
    generated_code_cm: str = Field(validation_alias="generated-code-cm")
    status: Status
    app_version: str = Field(validation_alias="app-version")
    files: int
    files_completed: int = Field(validation_alias="files-completed")
    files_failed: int = Field(validation_alias="files-failed")
    files_remaining: Optional[int] = Field(
        validation_alias="files-remaining", default=0
    )
    submit_time: datetime = Field(validation_alias="submit-time", default=None)
    finish_time: Optional[datetime] = Field(
        validation_alias="finish-time", default=None
    )
    minio_endpoint: Optional[str] = Field(
        validation_alias="minio-endpoint", default=None
    )
    minio_secured: Optional[bool] = Field(
        validation_alias="minio-secured", default=None
    )
    minio_access_key: Optional[str] = Field(
        validation_alias="minio-access-key", default=None
    )
    minio_secret_key: Optional[str] = Field(
        validation_alias="minio-secret-key", default=None
    )
    log_url: Optional[str] = Field(validation_alias="log-url", default=None)

    @field_validator("finish_time", mode="before")
    @classmethod
    def parse_finish_time(cls, v):
        if isinstance(v, str) and v == "None":
            return None
        return v


class ResultFile(DocStringBaseModel):
    r"""
    Record reporting the properties of a transformed file result
    """

    model_config = {"use_attribute_docstrings": True}

    filename: str
    size: int
    extension: str


class TransformedResults(DocStringBaseModel):
    r"""
    Returned for a submission. Gives you everything you need to know about a completed
    transform.
    """

    model_config = {"use_attribute_docstrings": True}

    hash: str
    """Unique hash for transformation (used to look up results in cache)"""
    title: str
    """Title of transformation request"""
    codegen: str
    """Code generator used (internal ServiceX information related to query type)"""
    request_id: str
    """Associated request ID from the ServiceX server"""
    submit_time: datetime
    """Time of submission"""
    data_dir: str
    """Local directory for output"""
    file_list: List[str]
    """List of downloaded files on local disk"""
    signed_url_list: List[str]
    """List of URLs to retrieve output from remote ServiceX object store"""
    files: int
    """Number of files in result"""
    result_format: ResultFormat
    """File format for results"""
    log_url: Optional[str] = None
    """URL for looking up logs on the ServiceX server"""


class ServiceXInfo(DocStringBaseModel):
    r"""
    Model for ServiceX Info properties
    """

    app_version: str = Field(alias="app-version")
    code_gen_image: dict[str, str] = Field(alias="code-gen-image")
    capabilities: list[str] = Field(default_factory=list)


class DatasetFile(BaseModel):
    """
    Model for a file in a cached dataset
    """

    id: int
    adler32: Optional[str]
    file_size: int
    file_events: int
    paths: str


class CachedDataset(BaseModel):
    """
    Model for a cached dataset held by ServiceX server
    """

    id: int
    name: str
    did_finder: str
    n_files: int
    size: int
    events: int
    last_used: datetime
    last_updated: datetime
    lookup_status: str
    is_stale: bool
    files: Optional[List[DatasetFile]] = []
