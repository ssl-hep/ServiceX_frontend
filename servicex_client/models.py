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

from pydantic import BaseModel, Field, validator
from typing import List, Optional


class ResultDestination(str, Enum):
    object_store = 'object-store'
    volume = 'volume'


class ResultFormat(str, Enum):
    parquet = "parquet",
    root_file = "root-file"


class Status(str, Enum):
    complete = "Complete",
    fatal = "Fatal",
    canceled = "Canceled",
    submitted = "Submitted",
    running = "Running"


class TransformRequest(BaseModel):
    title: Optional[str] = None
    did: Optional[str] = None
    file_list: Optional[List[str]] = Field(None, alias='file-list')
    selection: str
    image: Optional[str] = None
    codegen: str
    tree_name: Optional[str] = Field(None, alias='tree-name')
    result_destination: ResultDestination = Field(alias="result-destination")
    result_format: ResultFormat = Field(alias="result-format")

    class Config:
        allow_population_by_field_name = True

    def compute_hash(self):
        sha = hashlib.sha256(str([self.did, self.selection, self.tree_name,
                                  self.codegen, self.image,
                                  self.result_format.name, self.file_list]).
                             encode("utf-8"))
        return sha.hexdigest()


class TransformStatus(BaseModel):
    request_id: str
    did: str
    selection: str
    tree_name: Optional[str] = Field(alias="tree-name")
    image: str
    result_destination: ResultDestination = Field(alias="result-destination")
    result_format: ResultFormat = Field(alias="result-format")
    workflow_name: str = Field(alias="workflow-name")
    generated_code_cm: str = Field(alias="generated-code-cm")
    status: Status
    app_version: str = Field(alias="app-version")
    files: int
    files_completed: int = Field(alias="files-completed")
    files_failed: int = Field(alias="files-failed")
    files_remaining: Optional[int] = Field(alias="files-remaining")
    submit_time: datetime = Field(alias="submit-time")
    finish_time: Optional[datetime] = Field(alias="finish-time")
    minio_endpoint: Optional[str] = Field(alias="minio-endpoint")
    minio_secured: Optional[bool] = Field(alias="minio-secured")
    minio_access_key: Optional[str] = Field(alias="minio-access-key")
    minio_secret_key: Optional[str] = Field(alias="minio-secret-key")

    @validator("finish_time", pre=True)
    def parse_finish_time(cls, v):
        if isinstance(v, str) and v == "None":
            return None
        return v


class ResultFile(BaseModel):
    filename: str
    size: int
    extension: str


class TransformedResults(BaseModel):
    hash: str
    title: str
    codegen: str
    request_id: str
    submit_time: datetime
    data_dir: str
    file_list: List[str]
    files: int
    result_format: ResultFormat
