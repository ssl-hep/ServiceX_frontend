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
import json
import os
from pathlib import Path
from typing import List, Optional
from filelock import FileLock
from tinydb import TinyDB, Query, where

from servicex.configuration import Configuration
from servicex.models import TransformRequest, TransformStatus, TransformedResults


class CacheException(Exception):
    pass


class QueryCache:
    def __init__(self, config: Configuration):
        self.config = config
        Path(self.config.cache_path).mkdir(parents=True, exist_ok=True)
        self.db = TinyDB(os.path.join(self.config.cache_path, "db.json"))
        self.lock = FileLock(os.path.join(self.config.cache_path, "db.lock"))

    def close(self):
        self.db.close()

    def transformed_results(self, transform: TransformRequest,
                            completed_status: TransformStatus, data_dir: str,
                            file_list: List[str],
                            signed_urls) -> TransformedResults:
        return TransformedResults(
            hash=transform.compute_hash(),
            title=transform.title,
            codegen=transform.codegen,
            request_id=completed_status.request_id,
            submit_time=completed_status.submit_time,
            data_dir=data_dir,
            file_list=file_list,
            signed_url_list=signed_urls,
            files=completed_status.files,
            result_format=transform.result_format,
            log_url=completed_status.log_url
        )

    def cache_transform(self, record: TransformedResults):
        transforms = Query()
        with self.lock:
            self.db.upsert(json.loads(record.model_dump_json()), transforms.hash == record.hash)

    def update_record(self, record: TransformedResults):
        transforms = Query()
        with self.lock:
            self.db.update(json.loads(record.model_dump_json()), transforms.hash == record.hash)

    def contains_hash(self, hash: str) -> bool:
        """
        Check if the cache has completed records for a hash
        """
        transforms = Query()
        with self.lock:
            records = self.db.search((transforms.hash == hash)
                                     & ~(transforms.status == 'SUBMITTED'))
        return len(records) > 0

    def is_transform_request_submitted(self, hash_value: str) -> bool:
        """
        Returns True if request is submitted
        Returns False if the request is not in the cache at all
        or not submitted
        """
        transform = Query()
        with self.lock:
            records = self.db.search((transform.hash == hash_value))

        if not records:
            return False

        if "status" in records[0] and records[0]["status"] == 'SUBMITTED':
            return True
        return False

    def get_transform_request_id(self, hash_value: str) -> Optional[str]:
        """
        Return the request id of cached record
        """
        transform = Query()

        with self.lock:
            records = self.db.search(transform.hash == hash_value)

        if not records or 'request_id' not in records[0]:
            raise CacheException("Request Id not found")
        return records[0]["request_id"]

    def update_transform_status(self, hash_value: str, status: str) -> None:
        """
        Update the cached record status
        """
        transform = Query()
        with self.lock:
            self.db.upsert({"hash": hash_value, "status": status}, transform.hash == hash_value)

    def update_transform_request_id(self, hash_value: str, request_id: str) -> None:
        """
        Update the cached record request id
        """
        transform = Query()
        with self.lock:
            self.db.upsert({"hash": hash_value, "request_id": request_id},
                           transform.hash == hash_value)

    def get_transform_by_hash(self, hash: str) -> Optional[TransformedResults]:
        """
        Returns completed transformations by hash
        """
        transforms = Query()
        with self.lock:
            records = records = self.db.search((transforms.hash == hash)
                                               & ~(transforms.status == 'SUBMITTED'))

        if not records:
            return None

        if len(records) != 1:
            raise CacheException("Multiple records found in db for hash")
        else:
            return TransformedResults(**records[0])

    def get_transform_by_request_id(self, request_id: str) -> Optional[TransformedResults]:
        """
        Returns completed transformed results using a request id
        """
        transforms = Query()

        with self.lock:
            records = self.db.search(transforms.request_id == request_id)

        if not records:
            return None

        if len(records) != 1:
            raise CacheException("Multiple records found in db for request_id")
        else:
            return TransformedResults(**records[0])

    def cache_path_for_transform(self, transform_status: TransformStatus) -> Path:
        base = Path(self.config.cache_path)
        result = Path(os.path.join(base, transform_status.request_id))
        result.mkdir(parents=True, exist_ok=True)
        return result

    def cached_queries(self) -> List[TransformedResults]:
        transforms = Query()

        with self.lock:
            result = [TransformedResults(**doc) for doc in
                      self.db.search(transforms.request_id.exists())]
        return result

    def delete_record_by_request_id(self, request_id: str):
        with self.lock:
            self.db.remove(where('request_id') == request_id)

    def delete_record_by_hash(self, hash: str):
        transforms = Query()
        with self.lock:
            self.db.remove(transforms.hash == hash)

    def get_codegen_by_backend(self, backend: str) -> Optional[dict]:
        codegens = Query()
        with self.lock:
            records = self.db.search(codegens.backend == backend)

        if not records:
            return None

        if len(records) != 1:
            raise CacheException("Multiple records found in db for same backend")
        else:
            return records[0]

    def update_codegen_by_backend(self, backend: str, codegen_list: list):
        transforms = Query()
        with self.lock:
            self.db.upsert({'backend': backend, 'codegens': codegen_list},
                           transforms.backend == backend)

    def delete_codegen_by_backend(self, backend: str):
        with self.lock:
            self.db.remove(where('backend') == backend)
