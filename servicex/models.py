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
from typing import List, Optional


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


class TransformRequest(BaseModel):
    r"""
    Transform request sent to ServiceX
    """

    title: Optional[str] = None
    did: Optional[str] = None
    file_list: Optional[List[str]] = Field(default=None, alias="file-list")
    selection: str
    image: Optional[str] = None
    codegen: str
    tree_name: Optional[str] = Field(default=None, alias="tree-name")
    result_destination: ResultDestination = Field(
        serialization_alias="result-destination"
    )
    result_format: ResultFormat = Field(serialization_alias="result-format")

    model_config = {"populate_by_name": True}

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
                    self.tree_name,
                    self.codegen,
                    self.image,
                    self.result_format.name,
                    self.file_list,
                ]
            ).encode("utf-8")
        )
        return sha.hexdigest()


class TransformStatus(BaseModel):
    r"""
    Status object returned by servicex
    """

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
    files_remaining: Optional[int] = Field(validation_alias="files-remaining", default=0)
    submit_time: datetime = Field(validation_alias="submit-time", default=None)
    finish_time: Optional[datetime] = Field(validation_alias="finish-time", default=None)
    minio_endpoint: Optional[str] = Field(validation_alias="minio-endpoint", default=None)
    minio_secured: Optional[bool] = Field(validation_alias="minio-secured", default=None)
    minio_access_key: Optional[str] = Field(validation_alias="minio-access-key", default=None)
    minio_secret_key: Optional[str] = Field(validation_alias="minio-secret-key", default=None)
    log_url: Optional[str] = Field(validation_alias="log-url", default=None)

    @field_validator("finish_time", mode="before")
    @classmethod
    def parse_finish_time(cls, v):
        if isinstance(v, str) and v == "None":
            return None
        return v


class ResultFile(BaseModel):
    r"""
    Record reporting the properties of a transformed file result
    """

    filename: str
    size: int
    extension: str


class TransformedResults(BaseModel):
    r"""
    Returned for a submission. Gives you everything you need to know about a completed
    transform.
    """

    hash: str
    title: str
    codegen: str
    request_id: str
    submit_time: datetime
    data_dir: str
    file_list: List[str]
    signed_url_list: List[str]
    files: int
    result_format: ResultFormat
    log_url: Optional[str] = None
